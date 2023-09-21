#!/usr/bin/env python
# coding: utf-8

#get_ipython().run_line_magic('env', 'OMP_NUM_THREADS 4')
#importing the required libraries
import pandas as pd
import numpy as np
from sklearn import cluster
import geopandas
from decimal import Decimal, ROUND_HALF_UP
from threadpoolctl import threadpool_limits
pd.options.mode.chained_assignment = None
from classes import Database as data
from classes import Convert_Gpd as gpd
import os
import config
DB=data.Database()

def deduplication():
    # ***CRM Data***
    crm_data = DB.get_crm_data()
    print("got crm data municipal assets")
    crm_data.rename(columns={'latitude': 'LATITUDE', 'longitude': 'LONGITUDE','pxcreatedatetime':'PXCREATEDATETIME','interactiontype':'INTERACTIONTYPE','closure_date':'CLOSURE_DATE','short_status':'SHORT_STATUS','priority':'PRIORITY','submunic_3':'SUBMUNIC_3','pyid': 'CaseId'}, inplace=True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace(' ', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace(' ', '', regex = True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace('\(', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace('\(', '', regex = True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace('\)', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace('\)', '', regex = True)
    crm_data.drop_duplicates(inplace=True)
    crm_data = crm_data.dropna(subset = ['LATITUDE', 'LONGITUDE']) # 4328095
    crm_data = crm_data[crm_data['VISUAL POLLUTION CATEGORY'] != 'Not VP'] # 2821313
    crm_data = crm_data[crm_data['SHORT_STATUS'] != 'Close'] # 111577
    crm_data['PXCREATEDATETIME'] = pd.to_datetime(crm_data['PXCREATEDATETIME'])

    mapping = pd.read_excel(config.mapping_path)
    mapping[['BCG_rating_en']] = mapping[['BCG_rating_en']].apply(lambda x: x.str.strip())
    crm_mapped = pd.merge(crm_data, mapping, on = "SP_Classificaion", how='inner')
    

    crm_gc1 = gpd.convert_gpd(crm_mapped, x=crm_mapped.LONGITUDE, y=crm_mapped.LATITUDE)
    crm_gc1 = crm_gc1.to_crs(epsg='32637')
    # Filtering Duplicated Cases
    crm_data_sorted = crm_gc1.sort_values(by=['CaseId', 'Satisfaction'], ascending=[True, False])
    crm_data_sorted1 = crm_data_sorted.drop_duplicates(subset=['CaseId'], keep='last')
    
    crm_data_sorted1.columns = crm_data_sorted1.columns.str.replace(' ', '')
    crm_data_sorted1.rename(columns = {'BCG_rating_en':'VP_element'},inplace = True)
    
    crm_data_sorted1 = crm_data_sorted1[['CaseId','PXCREATEDATETIME','SHORT_STATUS', 'LATITUDE', 'LONGITUDE','PRIORITY','Satisfaction',
               'VP_element','Shock_wave','geometry']]
    clustering_path = config.input_path
    vp_unique = list(crm_data_sorted1['VP_element'].unique())

    for i in vp_unique:
        df = crm_data_sorted1[crm_data_sorted1['VP_element'] == i ]
        df.to_excel(clustering_path + i + '.xlsx', index = False)
        

        
    directory = config.input_path
    

    cluster_df_final = pd.DataFrame()

    for filename in os.listdir(directory):
        f = os.path.join(directory,filename)
        #Reading input potholes complaints data
        ph = pd.read_excel(f)
        phgeo = ph[["CaseId",'LONGITUDE','LATITUDE','VP_element']]
        phgeo.head(1)
        n=len(phgeo)
        Max_Clusters=n  #dynamic flag to update the maximum number of clusters
        buffer=0  #dynamic flag to update the minimum number of clusters
        k=int(Decimal(n/2).to_integral_value(rounding=ROUND_HALF_UP))   #number of clusters
        X=phgeo[["LATITUDE","LONGITUDE"]]
        phgeo_X = X.copy()
        coverage=0  #initiating coverage as a condition in the while loop
        #adding a condition for percentage cases to break the while loop
        percentage_cases = 0.02 if n > 500 else 0.05 if n > 50 else 0.2 

        #adding a condition for percentage cases to break the while loop
        percentage_cases = 0.02 if n > 500 else 0.05 if n > 50 else 0.2 

        #Run K Means algo to identify the clusters for all the potholes and calculate the distance between potholes and corresponding clusters
        #if the number of cases are greater than 5, then run K Means, else consider each case an individual case
        with threadpool_limits(limits=4):
            if n > 5:
                while ((Max_Clusters-k)>(percentage_cases*n)) or (coverage<(1-percentage_cases)):
            #         print((Max_Clusters-k)>(percentage_cases*n))
                    model = cluster.KMeans(n_clusters=k, init='k-means++', max_iter = 5,n_init=1)
                    #Step to assign potholes to clusters
                    phgeo_X["cluster"]=model.fit_predict(X)
                    centers=model.cluster_centers_
        #             print(centers)
                    #print(centers)

                    labels=model.labels_
                    #print(labels)
                    #Extracting the lat long values of cluster centroids
                    center_labels = [centers[i] for i in labels]
        #             print(center_labels)

                    columns=list(zip(*center_labels))
        #             print(columns)
                    phgeo_X["cluster_latitude"] = columns[0]
                    phgeo_X["cluster_longitude"] = columns[1]
                    #Creating GeoDataframes of lat long values of individual cases and lat long values of cluster centroids
                    phgeocases = geopandas.GeoDataFrame(phgeo_X,geometry=geopandas.points_from_xy(phgeo_X.LONGITUDE,phgeo_X.LATITUDE),crs='epsg:4326')
                    clustercenters = geopandas.GeoDataFrame(phgeo_X,geometry=geopandas.points_from_xy(phgeo_X.cluster_longitude,phgeo_X.cluster_latitude),crs='epsg:4326')
                    #Converting the geometries
                    phgeocases.to_crs(epsg=32637,inplace=True)
                    clustercenters.to_crs(epsg=32637,inplace=True)
                    #Calculating the distance between pothole cases and cluster centroids
                    final_distances=phgeocases.distance(clustercenters)
                    #Calculating the coverage of the potholes with less than 50m distance from potholes
                    coverage = len([i for i in final_distances if i<50])/len(final_distances)

                    #binary search to find the optimal number of clusters
                    if coverage < 1:
                        buffer=k #to save the last value of K
                        k=int(Decimal((k+Max_Clusters)/2).to_integral_value(rounding=ROUND_HALF_UP))
                    elif coverage == 1:
                        Max_Clusters=k
                        k=int(Decimal((Max_Clusters+buffer)/2).to_integral_value(rounding=ROUND_HALF_UP))


            else:
                phgeo_X["cluster"] = np.arange(len(phgeo))
                phgeo_X["cluster_longitude"] = phgeo["LONGITUDE"]
                phgeo_X["cluster_latitude"] = phgeo["LATITUDE"]
                final_distances = 0



        #Creating the final output dataframe
        phgeo.insert(3,"Cluster_Label",phgeo_X["cluster"])
        phgeo.insert(4,"Cluster_Longitude",phgeo_X["cluster_longitude"])
        phgeo.insert(5,"Cluster_Latitude",phgeo_X["cluster_latitude"])
        phgeo.insert(6,"Distance",final_distances)


        cluster_df_final = cluster_df_final.append(phgeo)


    return cluster_df_final
        