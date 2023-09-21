import os

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
    'Output_table_name_Final_CRM_Cases' : 'INSPECTOR_FORECASTING_FINAL_CRM_CASES',
    'Output_table_name_Forecasted_Inspectors': 'INSPECTOR_FORECASTING_FORECASTED_INSPECTORS',
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
    'gisuser': 'USER_AELSAADI',
    'gispassword': 'UsRA3lSaAd12030',
    'gishost': 'ruhmpp-exa-scan.momra.net',
    'gisservice': 'high_FMEGIS.momra.net',
    'gisport': 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema': 'C##OUTPUTS_MOMRAH',
    'Output_table_name_Final_CRM_Cases' : 'INSPECTOR_FORECASTING_FINAL_CRM_CASES',
    'Output_table_name_Forecasted_Inspectors': 'INSPECTOR_FORECASTING_FORECASTED_INSPECTORS',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB_DEV_WIN = {
    'user' : 'SYSTEM',
    'password' : 'UN8GbDKcQV',
    'host' : '10.80.122.102',
    'service' : 'ORCLCDB',
    'port' : 1521,
    'instantclient' : r"../../instantclient-basic-windows.x64-21.6.0.0.0dbru/instantclient_21_6",
    'gisuser' : 'USER_AELSAADI',
    'gispassword' : 'SAADI2030',
    'gishost' : 'ruhmsv-ora19c-scan.momra.net',
    'gisservice' : 'SDIGIS',
    'gisport' : 1521,
    'input_schema': 'C##MOMRAH',
    'outputschema' : 'C##OUTPUTS_MOMRAH',
    'Output_table_name_Final_CRM_Cases' : 'INSPECTOR_FORECASTING_FINAL_CRM_CASES',
    'Output_table_name_Forecasted_Inspectors': 'INSPECTOR_FORECASTING_FORECASTED_INSPECTORS',
    'GIS_PATH': 'C:\Environment\MOMRAH_WORKING\ProductionCode\GISdata',
    'DEBUG': 1
}

DB = DB_LIN
GISPATH = DB['GIS_PATH']
# Model to run
MODELS_TO_RUN = ["INSPECTOR_FORECASTING"]
# Model Name
MODEL_NAME = 'INSPECTOR_FORECASTING_ENGINE'
# Precition Window
PREDICTION_WINDOW_IN_WEEKS = 13 # No of Weeks
# Prediction Forecasted months
PREDICTION_WINDOW_IN_MONTHS = 3 # 3 Months
# Predcition Flag
PREDICTION_FLAG = 52
# CRM cases Predicted Output Table
Output_table_name_Final_CRM_Cases = 'INSPECTOR_FORECASTING_FINAL_CRM_CASES'
# Inspectors predicted Output Table
Output_table_name_Forecasted_Inspectors = 'INSPECTOR_FORECASTING_FORECASTED_INSPECTORS'
# Features table name in the Output Table
INSPECTOR_FORECASTING_FEATURES = 'INSPECTOR_FORECASTING_FEATURES'
#This is the file name for the Planned Inspection output file. Please update this to match the correct name in future builds
PLANNED_INSPECTOR_FILE_NAME = "PLANNED_VISITS_INSPECTORS"
#CRM START date based on months, eg: 12 means last 12 months
CRM_START_DATE = 12
#Initialising the default street lenth. This can be updated by the developer based on their discretion
DEFAULT_STREET_LENGTH = 100
#Setting the speed of the inspector by foot to visit a particular area
INSPECTOR_SPEED = 4 #kms/hr
#Setting the number of productive hours
INSPECTOR_PRODUCTIVE_HOURS = 30 #hours/week
#Time taken per CRM Visit
TIME_PER_VISIT = 30 #minutes per visit
#Frequency of visits for low risk grids
LOW_RISK_FREQUENCY = 4 #visits/year
#Frequency of visits for medium risk grids
MEDIUM_RISK_FREQUENCY = 12 #visits/year
#Frequency of visits for high risk grids
HIGH_RISK_FREQUENCY = 24 #visits/year
#Frequency of visits for very high risk grids
VERY_HIGH_FREQUENCY = 48 #visits/year
#Frequency of visits for priority areas
PRIORITY_AREA_FREQUENCY = VERY_HIGH_FREQUENCY
#Low risk grids RBD score threshold
LOW_RISK_THR = 10
#Medium risk grids RBD score threshold
MEDIUM_RISK_THR = 30
#High risk grids RBD score threshold
HIGH_RISK_THR = 70
#Very high risk grids RBD score threshold
VERY_HIGH_RISK_THR = 71
# Function defining the Risk Level
def frequency_factor(row):
    x = row['OVERALL_SCORE']
    if row['DN']==0:
        return 0
    if row['PRIORITY_FLAG'] == 1:
        return PRIORITY_AREA_FREQUENCY
    else:
        if x < LOW_RISK_THR:
            return LOW_RISK_FREQUENCY
        if x < MEDIUM_RISK_THR:
            return MEDIUM_RISK_FREQUENCY
        if x < HIGH_RISK_THR:
            return HIGH_RISK_FREQUENCY
        return VERY_HIGH_FREQUENCY