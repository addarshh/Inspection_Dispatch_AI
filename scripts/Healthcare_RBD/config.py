import os

FEATURE_CREATION_INPUT_FOLDER = '../data/feature_creation'
FEATURE_CREATION_OUTPUT_FOLDER = r'../output/feature_creation'
MODEL_SCORING_OUTPUT_FOLDER = r'../output/model_scoring'
AMANA_CODE = None
MODELS_TO_RUN = ["RBD"]
INSPECTION_CATEGORIES = ["Health"]
MODEL_NAME = 'RBD_HEALTH_ENGINE'
FEATURES_OUTPUT_TABLE_NAME='RBD_HEALTH_FEATURES'
LICENSES_OUTPUT_TABLE='RBD_HEALTH_LICENSES'
GRIDS_OUTPUT_TABLE_NAME='RBD_HEALTH_GRIDS'

DB_LIN = {
    'user': os.getenv('DB_DL_USER'),
    'password': os.getenv('DB_DL_PSWD'),
    'host': os.getenv('DB_DL_HOST'),
    'service': os.getenv('DB_DL_BASE'),
    'port': os.getenv('DB_DL_PORT'),
    'connectiontype': 'service_name',
    'instantclient': r"../../../docker/python/drivers/lib",
    'instaclientpath': 'C:\\instantclient_21_6',
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

DB_WIN_STG = {
    'user': 'USER_AELSAADI',
    'password': 'bACkS9cn',
    'host': 'ruhmsv-ora19c-scan.momra.net',
    'service': 'ACIOSTG',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'instaclientpath': 'C:\\instantclient_21_6',
    'connectiontype': 'service_name',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB_WIN_DEV = {
    'user': 'SYSTEM',
    'password': 'UN8GbDKcQV',
    'host': '10.80.122.102',
    'service': 'ORCLCDB',
    'port': 1521,
    'instantclient': r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'instaclientpath': 'C:\\instantclient_21_6',
    'connectiontype': 'SID',
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
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
priority_areas=os.path.join(GISPATH,'VDPRIORITYAREAS.csv')
amana_shp_path=os.path.join(GISPATH,'BBAMANABOUNDARYS.csv')

PATHS = {
    'municipality': f'{FEATURE_CREATION_INPUT_FOLDER}/Amana.shp',
    'inspections': f'{FEATURE_CREATION_INPUT_FOLDER}/Inspections from Medinah.xlsx',
    'licenses': f'{FEATURE_CREATION_INPUT_FOLDER}/Retail License Data_all.xlsx',
    'population_data': f'{FEATURE_CREATION_INPUT_FOLDER}/standardized_grids/standardized_grids.shp',
    'population_data2': f'{FEATURE_CREATION_INPUT_FOLDER}/Population_grids.shp',
    'population_data3': f'{FEATURE_CREATION_INPUT_FOLDER}/standardized_grids_population_municipality_20220220/standardized_grids_population_municipality_20220220.shp',
    'licenses_keys': f'{FEATURE_CREATION_INPUT_FOLDER}/activites with momtathelid.xlsx',
    'class_config': f'{FEATURE_CREATION_INPUT_FOLDER}/Translated Mapping doc_arabic_1.xlsx',
    'water_data': f'{FEATURE_CREATION_INPUT_FOLDER}/water_data.xlsx',
    #'priority_areas': f'{FEATURE_CREATION_INPUT_FOLDER}/madina_priority_areas.csv',
    'all_crm': f'{FEATURE_CREATION_INPUT_FOLDER}/CRM data.xlsx',
    'drilling_inspections': f'{FEATURE_CREATION_INPUT_FOLDER}/Drilling Inspections.xlsx',
    'drilling_licenses': f'{FEATURE_CREATION_INPUT_FOLDER}/DataRequest_Excavations.xlsx',
    'excavation_phases': f'{FEATURE_CREATION_INPUT_FOLDER}/Excavation phases.xlsx',
    'pois1': f'{FEATURE_CREATION_INPUT_FOLDER}/pois/20220808121409a756.xlsx',
    'pois2': f'{FEATURE_CREATION_INPUT_FOLDER}/pois/20220807092142e65d (1).xlsx',
    'construction_licenses': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_new.csv',
    'building_use': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/BuildingMainUseMap.csv',
    'building_type': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/BuildingTypeMap.csv',
    'building_subuse': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/buildingSubUsageMap.csv',
    'amana_name_map': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/Amana_name_eng_map.csv',
    'construction_floor1': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Floor_v2_sheet1.csv',
    'construction_floor2': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Floor_v2_sheet2.csv',
    'construction_floor3': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Floor_v2_sheet3.csv',
    'floor_type': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/floortype_eng_map.csv',
    'component_usage': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/component_usage_eng_map.csv',
    'construction_inspections': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/Building Inspections 29 - 8.csv',
    'construction_setback_rebound1':f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Setback_Rebound_sheet1.csv',
    'construction_setback_rebound2': f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Setback_Rebound_sheet2.csv',
    'construction_setback_rebound3':f'{FEATURE_CREATION_INPUT_FOLDER}/construction/DataRequest_Construction_Setback_Rebound_sheet3.csv'
}
 