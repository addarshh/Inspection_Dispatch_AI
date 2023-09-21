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
    populationsGrids['GridNumber']=populationsGrids.index

    licensesDf['Latitude']=pd.to_numeric(licensesDf['Latitude'], errors='coerce')
    licensesDf['Longitude']=pd.to_numeric(licensesDf['Longitude'], errors='coerce')

    licensesDf = gpd.GeoDataFrame(
        licensesDf, geometry=  gpd.points_from_xy( licensesDf.Latitude,  licensesDf.Longitude), crs="EPSG:4326")
    
    poiDf = pd.concat([pois1Df, pois2Df])

    poiDf = gpd.GeoDataFrame(
        poiDf, geometry=gpd.points_from_xy(poiDf.longitude, poiDf.latitude), crs="EPSG:4326")
    
    licensesDf = licensesDf [~licensesDf ['Latitude'].isna()]
    licensesDf = licensesDf [licensesDf ['License Expiry Date']> pd.to_datetime('today')]

    logging.info("""CHECK:SJOIN function:getModelResults df1:poiDf Shape: {}
    	and populationsGrids Shape: {} how="right"
        """.format(poiDf.shape, populationsGrids.shape))
    poiGrids=gpd.sjoin(poiDf, populationsGrids, how='right')
    logging.info("""CHECK:SJOIN_OUTPUT df:poiGrids {}""".format(poiGrids.shape))

    logging.info("""CHECK:SJOIN function:getModelResults df1:licensesDf Shape: {}
    	and populationsGrids Shape: {} how="right"
        """.format(poiDf.shape, populationsGrids.shape))    
    licenseGrids=gpd.sjoin(licensesDf, populationsGrids, how='right')
    logging.info("""CHECK:SJOIN_OUTPUT df:licenseGrids {}""".format(licenseGrids.shape))
        

    n_licenses_per_grid = licenseGrids.groupby('GridNumber')['License ID (MOMRAH)'].nunique().reset_index()
    n_pois_per_grid = poiGrids.groupby('GridNumber')['name'].nunique().reset_index()

    logging.info("""CHECK:MERGE df1:n_licenses_per_grid Shape: {}
    and df2:n_pois_per_grid Shape: {}""".format(n_licenses_per_grid.shape, n_pois_per_grid.shape))
    pois_licenses_comparison = n_licenses_per_grid.merge(n_pois_per_grid, how='outer', on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:pois_licenses_comparison {}
    """.format(pois_licenses_comparison.shape))

    pois_licenses_comparison.rename(columns={'License ID (MOMRAH)':'n_licenses','name':'n_POIs'}, inplace = True)
    pois_licenses_comparison['pois_licenses_difference']=pois_licenses_comparison['n_POIs']-pois_licenses_comparison['n_licenses']

    logging.info("""CHECK:MERGE df1:pois_licenses_comparison Shape: {}
    and df2:populationsGrids Shape: {} on GridNumber, how:inner""".format(pois_licenses_comparison.shape, populationsGrids.shape))
    pois_licenses_comparison=pois_licenses_comparison.merge(populationsGrids[['GridNumber','geometry']], on='GridNumber', how='inner')
    logging.info("""CHECK:MERGE_OUTPUT df:pois_licenses_comparison {}
    """.format(pois_licenses_comparison.shape))

    pois_licenses_comparison=gpd.GeoDataFrame(pois_licenses_comparison, geometry='geometry',crs='epsg:4326')
    
    logging.info("""CHECK:MERGE df1:pois_licenses_comparison Shape: {}
    and df2:populationsGrids Shape: {} on GridNumber""".format(pois_licenses_comparison.shape, populationsGrids.shape))
    pois_licenses_comparison=pois_licenses_comparison.merge(populationsGrids[['GridNumber','GRID_ID']], on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:pois_licenses_comparison {}
    """.format(pois_licenses_comparison.shape))

    return pois_licenses_comparison[['GridNumber','GRID_ID','pois_licenses_difference']]
    

