import pandas as pd
import os

DB_LIN = {
    'user': os.getenv('DB_DL_USER'),
    'password': os.getenv('DB_DL_PSWD'),
    'host': os.getenv('DB_DL_HOST'),
    'service': os.getenv('DB_DL_BASE'),
    'port': os.getenv('DB_DL_PORT'),
    'instantclient': r"../../../docker/python/drivers/lib",
    'instaclientpath': 'C:\\instantclient_21_6',
    'input_schema': os.getenv('DB_DL_INPUT_SCHEMA'),
    'outputschema': os.getenv('DB_DL_OUTPUT_SCHEMA'),
    'connectiontype': 'service_name',
    'gisuser': os.getenv('DB_GIS_USER'),
    'gispassword': os.getenv('DB_GIS_PSWD'),
    'gishost': os.getenv('DB_GIS_HOST'),
    'gisservice': os.getenv('DB_GIS_BASE'),
    'gisport': os.getenv('DB_GIS_PORT'),
    'GIS_PATH': '/var/www/html/scripts/GISdata',
    'DEBUG': os.getenv('APP_DEBUG')
}

DB_STAGE_WIN = {
    'user': 'USER_AELSAADI',
    'password': 'bACkS9cn',
    'host': 'ruhmsv-ora19c-scan.momra.net',
    'service': 'ACIOSTG',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'instaclientpath': 'C:\\instantclient_21_6',
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'connectiontype': 'service_name',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB_DEV_WIN = {
    'user': 'SYSTEM',
    'password': 'UN8GbDKcQV',
    'host': '10.80.122.102',
    'service': 'ORCLCDB',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'instaclientpath': 'C:\\instantclient_21_6',
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'connectiontype': 'SID',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}
DB_PROD_WIN = {
    'user' : 'ACIOPRD',
    'password' : 'SIAY79xh2',
    'host' : 'ruhmpp-exa-scan.momra.net',
    'service' : 'high_ACIOPRD.momra.net',
    'port' : 1521,
    'connectiontype' : 'service_name',
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'instaclientpath': 'C:\\instantclient_21_6',
    'gisuser' : 'USER_AELSAADI',
    'gispassword' : 'UsRA3lSaAd12030',
    'gishost' : 'ruhmpp-exa-scan.momra.net',
    'gisservice' : 'high_FMEGIS.momra.net',
    'gisport' : 1521,
    'input_schema': 'MOMRAH',
    'outputschema' : 'OUTPUTS_MOMRAH',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB = DB_LIN
fpath = DB['GIS_PATH']

FEATURE_CREATION_OUTPUT_FOLDER = r'../output/feature_creation'
MODEL_SCORING_OUTPUT_FOLDER = r'../../output/model_scoring'
AMANA_CODE = None
MODELS_TO_RUN = ["CP"]
MODEL_NAME = 'CP_MUNICIPALASSETS_ENGINE'
OUTPUT_TABLE_NAME='CP_MUNICIPALASSETS_CASES'

direc = os.path.dirname(__file__)
mapping_path = os.path.join(direc,'data','VP_Category_Mapping_v2.xlsx')
# shpGrid_path = os.path.join(direc,'data','Population','Population_grids.shp')
shpRegions_path = os.path.join(direc,'data','Shape files','Region_Desc.shp')
# bldg_path = os.path.join(direc,'data','Shape files','Buildings','BuildingFootPrint.shp')
poi_path = os.path.join(direc,'data','POI','POI full.xlsx')
# priority_areas_path = os.path.join(direc,'data','Priority Areas')
# Amana_path = os.path.join(direc,'data','Amana','Amana.shp')
input_path = os.path.join(direc, 'CaseClusteringInput', '')

priority_areas_path=os.path.join(fpath,'VDPRIORITYAREAS.csv')
shpGrid_path=os.path.join(fpath,'GGINSPECTIONGRIDS.csv')
Amana_path=os.path.join(fpath,'BBAMANABOUNDARYS.csv')
bldg_path=os.path.join(fpath,'buBuildingFootPrintS.csv')


features_columns=['days_elapsed_score','DN_score','no_of_priority_areas_score','Landuse_score',
                 'count_of_repetitions_score','count_of_pois_score','count_of_buildings_score',
                 'Category_score','Priority_score','Customer_score']
labels_column = ['non_compliant']
weights = {
    "dn_score" : 1,
    "no_of_priority_areas_score" : 1,
    "days_elapsed_score" : 1,
    "priority_score" : 1,
    "customer_score" : 1,
    "landuse_priority_score" : 1,
    "count_of_repetitions_score" : 1,
    "count_of_pois_score" : 1,
    "count_of_buildings_score" : 1,
    "category_score" : 1
}
meta_data=pd.DataFrame()