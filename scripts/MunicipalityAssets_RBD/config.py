import os
FEATURE_CREATION_INPUT_FOLDER = '../data/feature_creation'
FEATURE_CREATION_OUTPUT_FOLDER = r'../output/feature_creation'
MODEL_SCORING_OUTPUT_FOLDER = r'../../output/model_scoring'
AMANA_CODE = "003"
MODELS_TO_RUN = ["RBD"]
INSPECTION_CATEGORIES = [""]
MODEL_NAME = 'RBD_MUNICIPALITY_ASSETS'
#other categories: Retail, GeneralCleaning, Pothole, Sidewalk, Streetlight
#Initialising the flag to use 2 week old momthatel cases instead of historic cases
SET_MOMTATHEL = False

very_high_thresh = 0.75
high_thresh = 0.5
percentile_value = 0.97
thresh_percentage = 0.03
incremental_value = 0.0025
scores_backup=False

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

DB_STG_WIN = {
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
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCodeV2\scripts\GISdata',
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
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCodeV2\scripts\GISdata',
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
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCodeV2\scripts\GISdata',
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
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCodeV2\scripts\GISdata',
    'DEBUG': 1
}

DB = DB_LIN

working_dir=os.path.dirname(__file__)
fpath=DB['GIS_PATH']
priority_areas=os.path.join(fpath,'VDPRIORITYAREAS.csv')
population_grids_path=os.path.join(fpath,'GGINSPECTIONGRIDS.csv')
amana_shp_path=os.path.join(fpath,'BBAMANABOUNDARYS.csv')
planned_inspection_path=os.path.join(working_dir,'data','Visual Distortion inspections','Visual Distortion inspections.xlsx')
vp_category_mapping_path=os.path.join(working_dir,'data','VP_Category_Mapping.xlsx')
# ADDED NEW CLAUSE MAPPING
vp_category_clause_mapping_path=os.path.join(working_dir,'data','VD_Inspections_Clause_File.xlsx')
street_grids_path=os.path.join(working_dir,'data','Street_grids_standard.csv')
POI_1_path=os.path.join(working_dir,'data','POI','20220807092142e65d.xlsx')
POI_2_path=os.path.join(working_dir,'data','POI','20220808121409a756.xlsx')
parking_areas_path=os.path.join(fpath,'tnParkingAreaS.csv')
street_lights_path=os.path.join(fpath,'tnStreetLightingP.csv')
traffic_lights_path=os.path.join(fpath,'tnTrafficLightP.csv')
buildings_footprint_path=os.path.join(fpath,'buBuildingFootPrintS.csv')
pavements_path=os.path.join(fpath,'tnPavementsS.csv')
parks_path=os.path.join(fpath,'LUPARKS.csv')
publicfacilities_path=os.path.join(fpath,'LMPUBLICFACILTITIESP.csv')
##To be integrated
Region_Desc_path=os.path.join(working_dir,'data','Region_Desc','Region_Desc.shp')
grid_zone_path=os.path.join(fpath,"GGGRIDINSPECTIONZONEST.csv")

# working_dir=os.path.dirname(__file__)
# priority_areas=os.path.join(working_dir,'data','VP_Plan_Analysis','Data','Priority Areas')
# planned_inspection_path=os.path.join(working_dir,'data','Visual Distortion inspections','Visual Distortion inspections.xlsx')
# vp_category_mapping_path=os.path.join(working_dir,'data','VP_Category_Mapping.xlsx')
# population_grids_path=os.path.join(working_dir,'data','Standard_grids','Population_grids.shp')
# Region_Desc_path=os.path.join(working_dir,'data','Region_Desc','Region_Desc.shp')
# amana_shp_path=os.path.join(working_dir,'data','Amana','Amana.shp')
# street_grids_path=os.path.join(working_dir,'data','Street_grids_standard.csv')
# POI_1_path=os.path.join(working_dir,'data','POI','20220807092142e65d.xlsx')
# POI_2_path=os.path.join(working_dir,'data','POI','20220808121409a756.xlsx')
# parking_areas_path=os.path.join(working_dir,'data','New_shape_files','tnParkingAreaS.shp')
# street_lights_path=os.path.join(working_dir,'data','New_shape_files','tnStreetLightingP.shp')
# traffic_lights_path=os.path.join(working_dir,'data','New_shape_files','tnTrafficLightP.shp')
# buildings_footprint_path=os.path.join(working_dir,'data','Buildings','BuildingFootPrint.shp')

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