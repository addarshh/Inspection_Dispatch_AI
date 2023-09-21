import pandas as pd
import os

FEATURE_CREATION_INPUT_FOLDER = '../data/feature_creation'
FEATURE_CREATION_OUTPUT_FOLDER = r'../output/feature_creation'
MODEL_SCORING_OUTPUT_FOLDER = r'../../output/model_scoring'
AMANA_CODE = "003"
MODELS_TO_RUN = ["CP"]
INSPECTION_CATEGORIES = ["EXCAVATION"]
MODEL_NAME = 'CP_EXCAVATION_ENGINE'
OUTPUT_TABLE_NAME='CP_EXCAVATION_CASES'

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

DB_WIN_STG = {
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

DB_WIN_DEV = {
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

DB = DB_LIN
fpath = DB['GIS_PATH']
working_dir=os.path.dirname(__file__)
priority_areas=os.path.join(fpath,'VDPRIORITYAREAS.csv')
population_grids_path=os.path.join(fpath,'GGINSPECTIONGRIDS.csv')
amana_shp_path=os.path.join(fpath,'BBAMANABOUNDARYS.csv')
translation_doc_path=os.path.join(working_dir, 'data','Translated Mapping doc_arabic_1.xlsx' )
region_desc_path=os.path.join(working_dir, 'data','Shape files','Region_Desc.shp')

PATHS = {
    'municipality': f'{FEATURE_CREATION_INPUT_FOLDER}/Amana.shp',
    'inspections': f'{FEATURE_CREATION_INPUT_FOLDER}/Inspections from Medinah.xlsx',
    'licenses': f'{FEATURE_CREATION_INPUT_FOLDER}/Retail License Data_Madena_v1.xlsx',
    'population_data': f'{FEATURE_CREATION_INPUT_FOLDER}/standardized_grids/standardized_grids.shp',
    'population_data2': f'{FEATURE_CREATION_INPUT_FOLDER}/Population_grids.shp',
    'licenses_keys': f'{FEATURE_CREATION_INPUT_FOLDER}/activites with momtathelid.xlsx',
    'class_config': f'{FEATURE_CREATION_INPUT_FOLDER}/Translated Mapping doc_arabic_1.xlsx',
    'water_data': f'{FEATURE_CREATION_INPUT_FOLDER}/water_data.xlsx',
    'priority_areas': f'{FEATURE_CREATION_INPUT_FOLDER}/madina_priority_areas.csv',
    'all_crm': f'{FEATURE_CREATION_INPUT_FOLDER}/CRM data.xlsx',
}
 
features_columns=[
    'Business activity', 'Business Activity Weight', 'Facility type',
    'inspection number', 'previously issued fines amount',
    'cumulative_paid_fines', 'previously issued fines count',
    'days_since_last_inspection', 'days_since_establishment',
    'last_inspection_compliance','last_3_inspections_average_compliance',
    'paid_fines_percentage_amount', 'new_business', 'last_inspection_high_risk_violations',
    'last_inspection_fine_issued','last_3_inspections_percentage_of_compliance',
    'last_inspection_clauses_non_compliance_percentage', 'Tenancy (Own/Rented)'
]
labels_column=['non_compliant']

weights={"DN_score": 1,
  "no_of_priority_areas_score": 1,
  "days_elapsed_score" : 1,
  "landuse_priority_score": 1,
  "Priority_score" : 1,
  "digging_duration_score" : 1,
  "Customer_score" : 1,
  "path_length_sum_score" : 1,
  "volume_score": 1,
  "days_left_to_expiry_score": 1,
  "proportion_failed_causes_score": 1
}

meta_data=pd.DataFrame()