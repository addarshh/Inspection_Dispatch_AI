import os
import sys
import Helper as help
import FetchFromDB
import config
import logging
from RBDEngine.Health_Model_Creation import Create_Health_Model
from RBDEngine.pois_analysis import generate_pois
from RBDEngine.Perform_Predictions import Perform_Predictions_Risk_Calculations
import datetime
import pandas as pd
import numpy as np
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase
eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)

if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
Helper=help.Helper()
Helper.Engine_Start_Metadata_Update()
logging.basicConfig(filename="log.txt", level=logging.INFO)
logging.basicConfig(filename="log.txt", level=logging.ERROR)

logging.info("#############ENGINE RUN STARTS ###################")

Retail_Licenses_medina_df = FetchFromDB.getLicensesData()
logging.info("""Fetched Retail_Licenses_medina_df shape: {}
""".format(Retail_Licenses_medina_df.shape))

Inspections_df = FetchFromDB.getInspectionsData()
logging.info("""Fetched Inspections_df shape: {}
""".format(Inspections_df.shape))

print("HEREHEREHERE")

#Health_df = pd.read_csv(path.join(path.dirname(__file__), 'Datav2', 'Health_Activities.csv'))
Health_df = FetchFromDB.getLicensesKeysData()
logging.info("""Fetched/Read Health_df shape: {}
""".format(Health_df.shape))

print(Health_df.columns)

print(Retail_Licenses_medina_df.shape)
print(Inspections_df.shape)

transformer, model = Create_Health_Model(Retail_Licenses_medina_df, Inspections_df, Health_df)
print(transformer, model)

population_grids = FetchFromDB.getPopulationGrids()
logging.info("""Fetched population_grids shape: {}
""".format(population_grids.shape))
print(population_grids.shape)

Amana = FetchFromDB.getAMANA()
logging.info("""Fetched population_grids shape: {}
""".format(Amana.shape))
print(Amana.shape)
pois_licenses_comparison = generate_pois(Retail_Licenses_medina_df, Health_df, population_grids)
print("POI")


#pois_licenses_comparison=pd.read_csv(os.path.join(os.path.dirname(__file__), 'Datav2', 'pois_licenses_comparison.csv'))
print(pois_licenses_comparison.shape)

logging.info("""Read pois_licenses_comparison shape: {}
""".format(pois_licenses_comparison.shape))

points_for_qgis , tooltip_df = Perform_Predictions_Risk_Calculations(
    Retail_Licenses_medina_df, Inspections_df,Health_df,model,transformer, 
    pois_licenses_comparison, population_grids, Amana)


if points_for_qgis is not None:
  
    output_table_name = config.LICENSES_OUTPUT_TABLE
    #Helper.backup(output_table_name)
    print(points_for_qgis.shape)

    #ADDED
    Zone_df = FetchFromDB.get_grid_zone()
    logging.info("""Fetched zone_grids shape: {}
    """.format(Zone_df.shape))
    print(Zone_df.shape)
    points_for_qgis_withzone=points_for_qgis.merge(Zone_df, how="left", left_on="GridNumber", right_on="GRIDUNIQUECODE")
    logging.info("""OUTPUT points_for_qgis_withzone shape: {}
    """.format(points_for_qgis_withzone.shape))

    # Extracting last inspection date and adding last inspection date, license start and expiry dates to output
    inspections_date = Inspections_df.loc[:, ['LICENSE NUMBER', 'Inspection Date']]
    license_date = Retail_Licenses_medina_df.loc[:, ['License ID (MOMRAH)', 'License Start Date', 'License Expiry Date']]
    inspections_date['Inspection Date'] = pd.to_datetime(inspections_date['Inspection Date'])
    inspections_last_date = inspections_date.groupby(['LICENSE NUMBER']).agg(LastInspectiondate=('Inspection Date', np.max))
    date_data = license_date.merge(inspections_last_date, how='left', left_on='License ID (MOMRAH)',right_on='LICENSE NUMBER')
    points_for_qgis_subset_dates = points_for_qgis_withzone.merge(date_data, how='left', on='License ID (MOMRAH)')
    points_for_qgis_subset_dates = points_for_qgis_subset_dates.rename(columns={'License ID (MOMRAH)':'License_ID','License Start Date': 'License_Start_Date', 'License Expiry Date': 'License_Expiry_Date','LastInspectiondate': 'Last_Inspection_date'})



    Helper.backup(output_table_name)
    Helper.insert_df_Batchwise( points_for_qgis_subset_dates, output_table_name, 10000)
    logging.info("""OUTPUT points_for_qgis shape: {}
    """.format(points_for_qgis_withzone.shape))
if tooltip_df is not None:
  
    output_table_name = config.GRIDS_OUTPUT_TABLE_NAME
    #Helper.backup(output_table_name)
    print(tooltip_df.columns)
    logging.info("""OUTPUT tooltip_df shape: {}
    """.format(tooltip_df.shape))
    tooltip_df_subset = tooltip_df[tooltip_df['Number of Licenses'] > 0]
    tooltip_df_subset = tooltip_df_subset[['GridNumber', 'AMANACODE_x_x', 
        'License risk', 'Number of Licenses',  'AMANA',
    #    'Unnamed: 0', 'GRID_ID_x', 'pois_licenses_difference',
    #    'potential unlicensed', 'Population', 'Population percentile',
    #    'OBJECTID_x', 'GRIDUNIQUECODE_x', #'AMANACODE_y_x',
       'MUNICIPALITY', 'MUNICIPA_1',  'DN', #'REGION_x',
    #    'SHAPE_AREA_x', 'SHAPE_LEN_x', 'GEOMETRY_x', 'geometry_x',
    #    'n_priority_intersection', 'AMANACODE_x_y', 'AMANA_x', 'MUNICIPALITY_x',
    #    'MUNICIPA_1_x', 'DN_x', 'OBJECTID_y', 'GRIDUNIQUECODE_y', 'AMANA_y',
    #    'AMANACODE_y_y', 'MUNICIPALITY_y', 'MUNICIPA_1_y', 'REGION_y', 'DN_y',
       #'GRID_ID_x', 'SHAPE_AREA_y', 'SHAPE_LEN_y', 
       #'GEOMETRY_y', 'geometry_y',
       'CRM_CleanlinessCases_Count', 'SewerLinesCount', 'SewerManholesCount',
       'Health_risk', 'impact_risk', 'visibility_risk', 'location_risk',
       'Risk from businesses', 'Total risk', 'Total risk (discrete)'
    #    'number of high (75% - 100%) licenses',
    #    'number of low (0% - 50%) licenses',
    #    'number of medium (50% - 75%) licenses', 'high-risk license %',
    #    'medium-risk license %', 'low-risk license %', 'Area facility type'
       ]]
    print(tooltip_df_subset.shape)
    print(tooltip_df.shape)

    #ADDED
    Zone_df = FetchFromDB.get_grid_zone()
    logging.info("""Fetched zone_grids shape: {}
    """.format(Zone_df.shape))
    print(Zone_df.shape)
    tooltip_df_subset_withzone=tooltip_df_subset.merge(Zone_df, how="left", left_on="GridNumber", right_on="GRIDUNIQUECODE")
    logging.info("""OUTPUT tooltip_df_subset_withzone shape: {}
    """.format(tooltip_df_subset_withzone.shape))
    print(tooltip_df_subset_withzone.shape)
    print(tooltip_df_subset_withzone.columns)
    tooltip_df_subset_withzone.to_csv('tooltip_df_subset_withzone.csv')

    logging.info("OUTPUT tooltip_df_subset shape: {}".format(tooltip_df_subset.shape))
    Helper.insert_df_Batchwise( tooltip_df_subset_withzone, output_table_name, 10000)

Helper.Engine_End_Metadata_Update()
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)