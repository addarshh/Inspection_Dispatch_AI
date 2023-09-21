import sys
from ast import Return
from lib2to3.pgen2.pgen import DFAState
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing, cluster
import scipy
import geopandas
import cx_Oracle
import plotly.graph_objects as go
# import tqdm
pd.options.mode.chained_assignment = None
from sklearn.cluster import AgglomerativeClustering # For HAC clustering
import logging
from decimal import Decimal, ROUND_HALF_UP
from IPython.display import display

def run_case_clustering(phgeo,VP_Lable = None, Range_ = 50):
    try:
    
        if len(phgeo.columns) == 10:
            phgeo.columns = ['VP_Label','XC','CaseId','CreationDate', 'dLatitude','dLongitude','MunicipalityId','SubMunicipalityId','Sub_SubMunicipalityId','VPId']
        else:
            phgeo.columns = ['CaseId','CreationDate', 'dLatitude','dLongitude','MunicipalityId','SubMunicipalityId','Sub_SubMunicipalityId','VPId']

        n=len(phgeo)
        Max_Clusters=n  #dynamic flag to update the maximum number of clusters
        buffer=0  #dynamic flag to update the minimum number of clusters
        # k=round(n/2)   #number of clusters
        k=int(Decimal(n/2).to_integral_value(rounding=ROUND_HALF_UP))   #number of clusters
        phgeo_1 = phgeo[['CaseId','dLatitude','dLongitude']]
        X=phgeo_1[["dLatitude","dLongitude"]]
        phgeo_X = X.copy()
        coverage=0  #initiating coverage as a condition in the while loop
        #adding a condition for percentage cases to break the while loop
        percentage_cases = 0.02 if n > 500 else 0.05 if n > 50 else 0.2 
        logging.info('all presetting for case clustering engine done successfully')
    except Exception as err:
        logging.exception('Exception occured whle presetting values')

    try: 
        #Run K Means algo to identify the clusters for all the calculate the distance between potholes and corresponding clusters
        np.random.seed(1234)
        if n > 5:
            while((Max_Clusters-k)>(percentage_cases*n)) or (coverage<(1-percentage_cases)):
                    model = cluster.KMeans(n_clusters=k, init='k-means++')
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
                    phgeocases = geopandas.GeoDataFrame(phgeo_X,geometry=geopandas.points_from_xy(phgeo_X.dLongitude,phgeo_X.dLatitude),crs='epsg:4326')
                    clustercenters = geopandas.GeoDataFrame(phgeo_X,geometry=geopandas.points_from_xy(phgeo_X.cluster_longitude,phgeo_X.cluster_latitude),crs='epsg:4326')
                    #Converting the geometries
                    phgeocases.to_crs(epsg=32637,inplace=True)
                    clustercenters.to_crs(epsg=32637,inplace=True)
                    #Calculating the distance between pothole cases and cluster centroids
                    final_distances=phgeocases.distance(clustercenters)
                    #Calculating the coverage of the potholes with less than 50m distance from potholes
                    coverage = len([i for i in final_distances if i<Range_])/len(final_distances)

                    #binary search to find the optimal number of clusters
                    # if coverage < 1:
                    #     buffer=k #to save the last value of K
                    #     k=round((k+Max_Clusters)/2)
                    # elif coverage == 1:
                    #     Max_Clusters=k
                    #     k=round((Max_Clusters+buffer)/2)
                    if coverage < 1:
                        buffer=k #to save the last value of K
                        k=int(Decimal((k+Max_Clusters)/2).to_integral_value(rounding=ROUND_HALF_UP))
                    elif coverage == 1:
                        Max_Clusters=k
                        k=int(Decimal((Max_Clusters+buffer)/2).to_integral_value(rounding=ROUND_HALF_UP))
        
            #Creating the final output dataframe
            if len(phgeo.columns) ==10:
                # print("in sp step_1")
                
                #phgeo.insert(9,"VP_Label",VP_Lable)
                phgeo.insert(9,"Cluster_Label",phgeo_X["cluster"])
                phgeo.insert(10,"Cluster_Longitude",phgeo_X["cluster_longitude"])
                phgeo.insert(11,"Cluster_Latitude",phgeo_X["cluster_latitude"])
                phgeo.insert(12,"Distance",final_distances)
            else:
                phgeo.insert(8,"VP_Label",VP_Lable)
                phgeo.insert(9,"Cluster_Label",phgeo_X["cluster"])
                phgeo.insert(10,"Cluster_Longitude",phgeo_X["cluster_longitude"])
                phgeo.insert(11,"Cluster_Latitude",phgeo_X["cluster_latitude"])
                phgeo.insert(12,"Distance",final_distances)
        else:
            final_distances = 0
            #phgeo.insert(9,"VP_Label",VP_Lable)
            phgeo.insert(9,"Cluster_Label",np.arange(len(phgeo)))
            phgeo.insert(10,"Cluster_Longitude",phgeo["dLongitude"])
            phgeo.insert(11,"Cluster_Latitude",phgeo["dLatitude"])
            phgeo.insert(12,"Distance",final_distances)
            
            # phgeo["cluster"] = np.arange(len(phgeo))
            # phgeo["cluster_longitude"] = phgeo["dLongitude"]
            # phgeo["cluster_latitude"] = phgeo["dLatitude"]
            # final_distances = 0
        logging.info('kemeans and binary search ran successfully')
    except Exception as err:
        logging.exception('exception occurded while running kemeans and binary search algorithm')
        
        
    return phgeo