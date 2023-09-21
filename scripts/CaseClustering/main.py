# Importing the required python package to run the case clustering engine
import os
import sys
sys.path.insert(0, '/var/www/html')
from operator import index
from pydoc import describe
import datetime
if os.getenv('APP_DEBUG'):
    start_time = datetime.datetime.now()
from docker.python.helper.database import DataBase
import pandas as pd
import datetime
import numpy as np
from pyparsing import col
import scipy
from sqlalchemy import desc, true
# from tqdm import tqdm
from my_pandas_extensions.run_case_clustering_model import run_case_clustering
from my_pandas_extensions.database import collect_data, write_engines_output_to_database, collect_caseclust_model_inputs, selectData
from my_pandas_extensions.utility_functions import unique_cluster_SC_labels, myFunc, unique_cluster_SC_labels_1
from time import gmtime, strftime
import logging
from pandas.io.sql import DatabaseError
import threading

eng_name = os.path.basename(os.path.dirname(__file__))
# Creating a log file setting for all exceptions and errors to trouble shoot furhter if any;
format_ = '%(asctime)s :: %(levelname)s :: Module %(module)s :: Line No %(lineno)s :: %(message)s'
log_filename = os.path.join(os.path.dirname(__file__), '..', '..', 'docker', 'python', 'scheduler', 'log', f"CaseClustering_{strftime('%Y-%m-%d_%H-%M')}.log")

pd.options.mode.chained_assignment = None

config_start_time = strftime('%I:%M %p')

# collect data fucntion to fetch data point from MOMRA CRM table (from DR replica/clone database)
df_collect_inputs = collect_caseclust_model_inputs(sqlQuery_= "SELECT * FROM {}.VW_MOMRA_REPEATEDINC_CONFIG "
    "WHERE MUNICIPALITYID IS NOT NULL "
    "AND SPECIALCLASSIFICATIONID IS NOT NULL "
    "AND RANGE IS NOT NULL "
    "AND REPETITIONPERIODDAYS IS NOT NULL "
    "AND REPETITIONPERIODHOURS IS NOT NULL "
    "AND NOOFTIMES IS NOT NULL "
    "AND (STARTTIME IS NOT NULL "
    "AND regexp_like(STARTTIME, '^[A-Za-z0-9: ]+$')) "
    # "AND (STARTTIME LIKE '%12:00 AM%')".format(os.getenv('DB_CRM_SCM_PEGADATA')))
    "AND (STARTTIME LIKE '%{}%')".format(os.getenv('DB_CRM_SCM_PEGADATA'), config_start_time)) #TODO

# scaling the data extraction part to fetch all incidents from crm balady database as per the amanas and special classification id define in the input config talbe
df = selectData(sqlQuery_= "SELECT W.SPLCLASSIFICATIONID, 0, W.PYID, W.PXCREATEDATETIME, W.LATITUDE, "
        "W.LONGITUDE, W.MUNICIPALITYID, W.SUBMUNICIPALITYID, W.SUB_SUBMUNICIPALITYID, W.SPLCLASSIFICATIONID "
    "FROM {}.PCA_MOMRA_RYD_CS_WORK W INNER JOIN {}.VW_MOMRA_REPEATEDINC_CONFIG CF "
        "ON W.SPLCLASSIFICATIONID IN CF.SPECIALCLASSIFICATIONID "
        "AND W.PXCREATEDATETIME >= (current_date - CF.REPETITIONPERIODDAYS - CF.REPETITIONPERIODHOURS/24) "
    "WHERE W.PYID LIKE 'INC-%' "
        # "AND (CF.STARTTIME LIKE '%12:00 AM%') "
        "AND (CF.STARTTIME LIKE '%{}%') " #TODO
        "AND W.PYSTATUSWORK NOT IN ('InProgress-EscalationInitiated', 'Pending - Quality Check', 'Pending- QMO Response', "
           "'Pending- Quality follow up initiated', 'Pending- QualityFollowUp', 'Resolved - ExternalDepartment', "
           "'Resolved-Completed', 'Resolved-Incident completed', 'Resolved-QualityClosure-Followup', 'Resolved-QualityNoReOpen', "
           "'Resolved-QualFollupReopen', 'Resolved – External completed', 'Rejected', 'Closed', 'Resolved - No need to Reopen', "
           "'Resolved-Rejected', 'Resolved–Service Request created', 'Resolved-AIRejected', 'Resolved - Completed') "
        "AND W.LATITUDE IS NOT NULL AND W.LONGITUDE IS NOT NULL AND NVL (W.DEPARTMENTID, '-1') NOT IN ('DEPT-003504', "
           "'DEPT-003505', 'DEPT-003506', 'DEPT-003507', 'DEPT-003508', 'DEPT-003509', 'DEPT-003510', 'DEPT-003511', "
           "'DEPT-003512', 'DEPT-003513', 'DEPT-003514', 'DEPT-003515', 'DEPT-003516', 'DEPT-003517', 'DEPT-003518', "
           "'DEPT-003519', 'DEPT-003520') "
        "AND (W.PYSTATUSWORK NOT IN ('New', 'Pending-CIS Process', 'Pending-Inspection', 'Pending-Approval', "
           "'Pending-Review', 'Rejected') OR W.MUNICIPALITYID != '003') "
        # "ORDER BY W.SPLCLASSIFICATIONID DESC".format(os.getenv('DB_CRM_SCM_PEGADATA'), os.getenv('DB_CRM_SCM_PEGADATA')))
        "ORDER BY W.SPLCLASSIFICATIONID DESC".format(os.getenv('DB_CRM_SCM_PEGADATA'), os.getenv('DB_CRM_SCM_PEGADATA'), strftime('%I:%M %p'))) #TODO

if df.shape[0] > 0:
    # Creating log file for recording all exceptions and errors to trouble shoot furhter if any;
    logging.basicConfig(level=logging.DEBUG if int(os.getenv('APP_DEBUG')) == 1 else logging.INFO, filename=log_filename, filemode='w', format=format_, force=True)

    # Making a record to the database as indicator of engine launching
    db = DataBase()
    eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
    if os.getenv('APP_DEBUG'): logging.info('Launch ID:' + str(eng_launch_id))

    df.columns = ['level_0', 'level_1', 'CaseId', 'CreationDate', 'dLatitude', 'dLongitude', 'MunicipalityId', 'SubMunicipalityId', 'Sub_SubMunicipalityId', 'VPId']
    # applying condition to chech if the resulted record is zero to run the engine simply exit by logging the message into log file in else statement

    # creating dictiionaries to hold the engine output in intermediate form
    sp_data_dict = {}
    sp_model_results = {}

    # Scaling up the case clustering engine to run for all spl classification id or amanas which are fetched from input config table
    for sp in df['VPId'].unique():
        # fecthing the respective distance measurement from the input config table for each respective spl classification ids.
        filterr = df_collect_inputs.SPECIALCLASSIFICATIONID.isin([sp])
        range_d = df_collect_inputs.loc[filterr, 'RANGE'].values[0]

        sp_data_dict[sp] = df[df.VPId.isin([sp])]
        sp_model_results[sp] = run_case_clustering(sp_data_dict[sp], VP_Lable=sp, Range_=abs(int(range_d)))


    # converting the intermediate model output into panda dataframe
    df_0 = pd.concat(sp_model_results)
    # Removing Unecessesary columns
    df_0 = df_0.reset_index()
    del df_0['level_0']
    del df_0['level_1']
    del df_0['XC']

    # df_0.to_pickle("df_00.pkl")
    # df_0 = pd.read_pickle(r"C:\MOMRA\1_RawData\df_0.pkl")
    # Generating Unique cluster id by group similar incidents per SpecialClassification ID
    df_0 = unique_cluster_SC_labels_1(df_0)

    # Adding the loading datetime column to the final dataset to update when the model output generated
    df_0["Load_date"] = strftime("%d-%b-%Y %I:%M:%S %p")

    # df_0.columns

    # Exporting Engine output to MOMRA production table for model maintaining model output history
    df_0['dLatitude'] = df_0['dLatitude'].astype(str)
    df_0['dLongitude'] = df_0['dLongitude'].astype(str)
    df_0['Distance'] = df_0['Distance'].astype(str)

    if os.getenv('APP_DEBUG'): logging.info(df_0.applymap(type).nunique())

    df_0['CreationDate'] = pd.to_datetime(df_0['CreationDate'], format='%d-%b-%Y %I:%M:%S %p', errors='coerce')
    # df_0['Load_date'] = pd.to_datetime(df_0['Load_date'], format='%d-%b-%Y %I:%M:%S %p', errors='coerce')
    df_0['Load_date'] = pd.to_datetime(df_0['Load_date'], errors='coerce')

    df_0['Unique_Cluster_Id'] = df_0['Unique_Cluster_Label'] + '_' + df_0['Load_date'].dt.strftime('%d%m%y%I%M')

    df_0['MunicipalityId'].replace('', np.nan, inplace=True)

    df_0.dropna(subset=['MunicipalityId', 'SubMunicipalityId', 'Sub_SubMunicipalityId'], inplace=True)
    df_0.columns = map(str.lower, df_0.columns)

    df_0.to_pickle(os.path.join(os.path.dirname(__file__), 'raw_data', 'df_0.pkl'))

    df_0.to_csv(os.path.join(os.path.dirname(__file__), 'raw_data', f"CASE_CLUSTERING_SC_TBL_{strftime('%Y-%m-%d_%H-%M')}.csv"))
    # write_engines_output_to_database(df_0, table_name="case_clustering_sc_tbl", if_exists='append') #TODO recording to db

    # Renaming the model output colums for better naming convention
    df_0_0 = df_0[
        [
            'load_date',
            'caseid',
            'dlatitude',
            'dlongitude',
            'creationdate',
            'unique_cluster_label',
            'vpid',
            'distance'
        ]
    ]

    df_0_0.rename(columns = {
        "load_date": "UPDATEDATETIME",
        "caseid": "INCIDENTNUMBER",
        "dlatitude": "INCIDENTLAT",
        "dlongitude": "INCIDENTLONG",
        "creationdate": "INCIDENTCREATIONDATE",
        "unique_cluster_label": "CLUSTERID",
        "vpid": "SPLCLASSIFICATIONID",
        "distance": "DISTANCE_FROM_CENTROID"
    }, inplace=true)

    # Converting lat and long into string value
    df_0_0['INCIDENTLAT'] = df_0_0['INCIDENTLAT'].astype(str)
    df_0_0['INCIDENTLONG'] = df_0_0['INCIDENTLONG'].astype(str)
    df_0_0['DISTANCE_FROM_CENTROID'] = df_0_0['DISTANCE_FROM_CENTROID'].astype(str)

    # Creating unique spl classification id list to pass as database function parameter for house keeping purpose
    SCID_lst_0_0 = df_0_0['SPLCLASSIFICATIONID'].unique()
    SCID_lst_0_0 = "'" + "','".join(SCID_lst_0_0) + "'"

    # Creating unique INCIDENTNUMBER id list to pass as database function parameter for house keeping purpose
    INCID_lst_0_0 = df_0_0['INCIDENTNUMBER'].unique()
    # INCID_lst_0_0 = "'" + "','".join(INCID_lst_0_0) + "'"

    # df_0_0.to_pickle("df_0.pkl")
    # df_0_0 = pd.read_pickle("df_0.pkl")
    # The database utility for writing case clustering model out as current snapshot into MOMRA production database.
    # for (Oracle 12C version) connection string (CRM PEGA).
    # logging.info( df_0_0.applymap(type).nunique())

    df_lst = pd.DataFrame(list(INCID_lst_0_0), columns=['vpid'])
    # df_lst.rename(columns={0:"vpid"},inplace=True)
    df_lst.columns = map(str.lower, df_lst.columns)

    df_lst.to_csv(os.path.join(os.path.dirname(__file__), 'raw_data', f"TEMP_SCID_TBL_{strftime('%Y-%m-%d_%H-%M')}.csv"))
    # write_engines_output_to_database(df_lst, table_name="temp_scid_tbl", if_exists='append') #TODO recording to db

    df_0_0.to_pickle(os.path.join(os.path.dirname(__file__), 'raw_data', 'df_0_0.pkl'))
    df_0_0.to_csv(os.path.join(os.path.dirname(__file__), 'raw_data', f"PR_MOMRA_FW_CS_DATA_AICRMCASES_{strftime('%Y-%m-%d_%H-%M')}.csv"))

    # write_engines_output_to_database(df_0_0, table_name="pr_momra_fw_cs_data_aicrmcases", if_exists="append",  #TODO recording to db
    #     uid_pwd=os.getenv('DB_CRM_USER')+':'+os.getenv('DB_CRM_PASS'),  # DB User name and Password(with ":")
    #     hostname=os.getenv('DB_CRM_HOST'),  # Database server IP
    #     service_name_=os.getenv('DB_CRM_BASE'),  # DB service name
    #     schema_=os.getenv('DB_CRM_SCM_PEGADATA'),  # DB Schema name
    #     SCID_lst_=SCID_lst_0_0,
    #     INCID_lst_=INCID_lst_0_0)
    db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)
# else:
#     logging.info("There are no incidents returned related to SPECIAL CLASSIFICATION IDs WHICh ARE SCHEDULED to run at this time!")

if os.getenv('APP_DEBUG'):
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()) + ' for time ' + config_start_time)