from datetime import datetime
from difflib import SequenceMatcher
import pandas as pd
import geopandas as gpd
import logging

def getDateString(format = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now().strftime(format)

def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

def getStageNameDictionary(stageNames):
    dict_transform={}
    for count1, i in enumerate(stageNames):
        for count2, j in enumerate(stageNames):
            if similar(i,j)>0.8:
                dict_transform[i]=j
                stageNames[count1]=stageNames[count2]
    return dict_transform

def generatePOIFile(populationsGrids : pd.DataFrame, licensesDf : pd.DataFrame, pois1Df : pd.DataFrame, pois2Df : pd.DataFrame) -> pd.DataFrame:
    logging.info("in helpers.generatePOIFiles function")
    populationsGrids['GridNumber']=populationsGrids.index
    logging.info("populationgrids shape is :"+str(populationsGrids.shape))
    licensesDf['Latitude']=pd.to_numeric(licensesDf['Latitude'], errors='coerce')
    licensesDf['Longitude']=pd.to_numeric(licensesDf['Longitude'], errors='coerce')

    licensesDf = gpd.GeoDataFrame(
        licensesDf, geometry=  gpd.points_from_xy( licensesDf.Latitude,  licensesDf.Longitude), crs="EPSG:4326")
    logging.info("pois1Df shape is:"+str(pois1Df.shape))
    logging.info("pois2Df shape is:"+str(pois2Df.shape))
    poiDf = pd.concat([pois1Df, pois2Df])
    logging.info("after concatenation of pois1&2, shape is :"+str(poiDf.shape))
    poiDf = gpd.GeoDataFrame(
        poiDf, geometry=gpd.points_from_xy(poiDf.longitude, poiDf.latitude), crs="EPSG:4326")
    
    licensesDf = licensesDf [~licensesDf ['Latitude'].isna()]
    licensesDf = licensesDf [licensesDf ['License Expiry Date']>'2022-07-30']
    logging.info("after filtering licences after july 30 2022 shape is :"+str(licensesDf.shape))

    logging.info("right join of poiDf and populationgrids")
    poiGrids=gpd.sjoin(poiDf, populationsGrids, how='right')
    logging.info("after join, shape is :"+str(poiGrids.shape))

    logging.info("right joining licencesDf and populationgrids")
    licenseGrids=gpd.sjoin(licensesDf, populationsGrids, how='right')
    logging.info("after joining shape is : "+str(licenseGrids.shape))
    n_licenses_per_grid = licenseGrids.groupby('GridNumber')['License ID (MOMRAH)'].nunique().reset_index()
    n_pois_per_grid = poiGrids.groupby('GridNumber')['name'].nunique().reset_index()
    logging.info("after groupby on grid number and taking unique names, shape is :"+str(n_pois_per_grid.shape))
    logging.info("outer join on n_licenses_per_grid and n_poid_per_grid on gridnumber")
    pois_licenses_comparison = n_licenses_per_grid.merge(n_pois_per_grid, how='outer', on='GridNumber')
    logging.info("after joins, shape is :"+n_pois_per_grid.shape)
    pois_licenses_comparison.rename(columns={'License ID (MOMRAH)':'n_licenses','name':'n_POIs'}, inplace = True)
    pois_licenses_comparison['pois_licenses_difference']=pois_licenses_comparison['n_POIs']-pois_licenses_comparison['n_licenses']
    logging.info("merging pois_licences_comparison and population grids")
    pois_licenses_comparison=pois_licenses_comparison.merge(populationsGrids[['GridNumber','geometry']], on='GridNumber', how='inner')
    logging.info("after merge shape is :"+str(pois_licenses_comparison.shape))

    pois_licenses_comparison=gpd.GeoDataFrame(pois_licenses_comparison, geometry='geometry',crs='epsg:4326')
    
    pois_licenses_comparison=pois_licenses_comparison.merge(populationsGrids[['GridNumber','GRID_ID']], on='GridNumber')

    return pois_licenses_comparison[['GridNumber','GRID_ID','pois_licenses_difference']]
    

