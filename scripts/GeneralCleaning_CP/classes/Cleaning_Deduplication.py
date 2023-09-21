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
DB=data.Database()
from datetime import datetime
import logging
from classes import CP_GeneralCleaning
def deduplication():
    #Reading input potholes complaints data
    #ph = pd.read_excel('C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\5. General Cleaning\\0. Raw Data\\CRM_Cases_GC_Madinah_25102022.xlsx')
    ph=CP_GeneralCleaning.crm_clean()
    logging.info("crm data in deduplication shape is"+str(ph.shape))
    print("got crm data deduplication")
    ph.rename(columns={'latitude': 'LATITUDE', 'longitude': 'LONGITUDE', 'pxcreatedatetime': 'PXCREATEDATETIME',
                             'interactiontype': 'INTERACTIONTYPE', 'closure_date': 'CLOSURE_DATE',
                             'short_status': 'SHORT_STATUS', 'priority': 'PRIORITY', 'submunic_3': 'SUBMUNIC_3', 'pyid': 'CaseId'},
                    inplace=True)

    phgeo = ph[["CaseId",'LONGITUDE','LATITUDE']]
    n=len(phgeo)
    logging.info((" clustering start time is :"+str(datetime.now())))
    logging.info("max clusters value is "+str(n))
    Max_Clusters=n  #dynamic flag to update the maximum number of clusters
    buffer=0  #dynamic flag to update the minimum number of clusters
    k=int(Decimal(n/2).to_integral_value(rounding=ROUND_HALF_UP))   #number of clusters
    X=phgeo[["LATITUDE","LONGITUDE"]]
    phgeo_X = X.copy()
    coverage=0  #initiating coverage as a condition in the while loop

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
                labels=model.labels_
                #Extracting the lat long values of cluster centroids
                center_labels = [centers[i] for i in labels]
                columns=list(zip(*center_labels))
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
    logging.info((" clustering end time is :" + str(datetime.now())))
    return phgeo
    #Exporting the excel file with the output data
    #phgeo.to_excel(r'C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\5. General Cleaning\\0. Raw Data\\CRM_Cases_Clusters_25102022.xlsx',index= False)

