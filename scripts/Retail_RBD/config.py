import os
FEATURE_CREATION_INPUT_FOLDER = '../data/feature_creation'
FEATURE_CREATION_OUTPUT_FOLDER = r'../output/feature_creation'
MODEL_SCORING_OUTPUT_FOLDER = r'../output/model_scoring'
AMANA_CODE = None
MODELS_TO_RUN = ["RBD"]
INSPECTION_CATEGORIES = ["Retail"]
MODEL_NAME = 'RBD_RETAIL_ENGINE'
FEATURES_OUTPUT_TABLE_NAME='RBD_RETAIL_FEATURES'
LICENSES_OUTPUT_TABLE='RBD_RETAIL_LICENSES'
GRIDS_OUTPUT_TABLE_NAME='RBD_RETAIL_GRIDS'

DB_LIN = {
    'user': os.getenv('DB_DL_USER'),
    'password': os.getenv('DB_DL_PSWD'),
    'host': os.getenv('DB_DL_HOST'),
    'service': os.getenv('DB_DL_BASE'),
    'port': os.getenv('DB_DL_PORT'),
    'connectiontype': 'service_name',
    'instantclient': r"../../../docker/python/drivers/lib",
    'gisuser': os.getenv('DB_GIS_USER'),
    'gispassword': os.getenv('DB_GIS_PSWD'),
    'gishost': os.getenv('DB_GIS_HOST'),
    'gisservice': os.getenv('DB_GIS_BASE'),
    'gisport': os.getenv('DB_GIS_PORT'),
    'input_schema': os.getenv('DB_DL_INPUT_SCHEMA'),
    'outputschema': os.getenv('DB_DL_OUTPUT_SCHEMA'),
    'GIS_PATH': '/var/www/html/scripts/GISdata',
    'DEBUG': os.getenv('APP_DEBUG')
}

DB_STG_WIN = {
    'user': 'USER_AELSAADI',
    'password': 'bACkS9cn',
    'host': 'ruhmsv-ora19c-scan.momra.net',
    'service': 'ACIOSTG',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'connectiontype': 'service_name',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'GIS_PATH':'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': os.getenv('APP_DEBUG')
}

DB_DEV_WIN = {
    'user': 'SYSTEM',
    'password': 'UN8GbDKcQV',
    'host': '10.80.122.102',
    'service': 'ORCLCDB',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'connectiontype': 'SID',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': os.getenv('APP_DEBUG')
}

DB_PROD = {
    'user' : 'ACIOPRD',
    'password' : 'SIAY79xh2',
    'host' : 'ruhmpp-exa-scan.momra.net',
    'service' : 'high_ACIOPRD.momra.net',
    'port' : 1521,
    'connectiontype' : 'service_name',
    'instantclient' : r"../../../docker/python/drivers/lib",
    'instaclientpath' : 'C:\\instantclient_21_6',
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
GISPATH = DB['GIS_PATH']

PATHS = {
    'class_config': f'{FEATURE_CREATION_INPUT_FOLDER}/Translated Mapping doc_arabic_1.xlsx',
    'water_data': f'{FEATURE_CREATION_INPUT_FOLDER}/water_data.xlsx',
    'pois1': f'{FEATURE_CREATION_INPUT_FOLDER}/pois/20220808121409a756.xlsx',
    'pois2': f'{FEATURE_CREATION_INPUT_FOLDER}/pois/20220807092142e65d (1).xlsx'
   }
 