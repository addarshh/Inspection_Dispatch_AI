import os
import pandas as pd

DB_LIN = {
    'user': os.getenv('DB_DL_USER'),
    'password': os.getenv('DB_DL_PSWD'),
    'host': os.getenv('DB_DL_HOST'),
    'service': os.getenv('DB_DL_BASE'),
    'port': os.getenv('DB_DL_PORT'),
    'instantclient': r"../../../docker/python/drivers/lib",
    'gisuser': os.getenv('DB_GIS_USER'),
    'gispassword': os.getenv('DB_GIS_PSWD'),
    'gishost': os.getenv('DB_GIS_HOST'),
    'gisservice': os.getenv('DB_GIS_BASE'),
    'gisport': os.getenv('DB_GIS_PORT'),
    'input_schema': os.getenv('DB_DL_INPUT_SCHEMA'),
    'outputschema': os.getenv('DB_DL_OUTPUT_SCHEMA'),
    'Output_table_name': 'CP_GROUPHOUSING_ENGINE',
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
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'Output_table_name': 'CP_GROUPHOUSING_ENGINE',
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
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'SAADI2030',
    'gishost': 'ruhmsv-ora19c-scan.momra.net',
    'gisservice': 'SDIGIS',
    'gisport': 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'Output_table_name': 'CP_GROUPHOUSING_ENGINE',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB = DB_LIN
fpath = DB['GIS_PATH']
priority_areas = os.path.join(fpath,'VDPRIORITYAREAS.csv')
population_grids_path = os.path.join(fpath,'GGINSPECTIONGRIDS.csv')
amana_shp_path = os.path.join(fpath,'BBAMANABOUNDARYS.csv')

# Model to run
MODELS_TO_RUN = ["GROUPHOUSING"]
# Model Name
MODEL_NAME = 'CP_GROUPHOUSING_ENGINE'
# CRM cases Predicted Output Table
Output_table_name = 'CP_GROUPHOUSING_CASES'
#CRM START date based on months, eg: 12 means last 12 months
CRM_START_DATE = 12
meta_data=pd.DataFrame()