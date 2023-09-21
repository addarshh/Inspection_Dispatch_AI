# Loading the required package for database connectivity and data preprocessing
import os
import sys
from glob import escape
from logging import exception
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# from sklearn import preprocessing, cluster
import scipy
# import geopandas
import cx_Oracle
# import plotly.graph_objects as go
import sqlalchemy.sql.sqltypes
# from tqdm import tqdm
import sqlalchemy as sql
# from sqlalchemy.types import String, Numeric
# import pandas_flavor as pf
from sqlalchemy.types import NVARCHAR
from sqlalchemy.types import DATETIME
import logging

from pandas.io.sql import DatabaseError
from sqlalchemy.exc import SQLAlchemyError
from IPython.display import display

# Locate the Oracle client DLLs to establish database connectivity
cx_Oracle.init_oracle_client(lib_dir=os.path.join('/var', 'www', 'html', 'docker', 'python', 'drivers', 'lib'))

# The utility for getting CRM incidents from BALADY DR database
def collect_data(sqlQuery_, connectString_ ="aiengine/AyC8KUJi@//ruhmpp-exa-scan:1521/medium_crmprod.momra.net" ):
    # Connect to database
    try:
        pool = cx_Oracle.SessionPool(os.getenv('DB_CRM_USER'), os.getenv('DB_CRM_PASS'),
                                     os.getenv('DB_CRM_HOST')+":"+os.getenv('DB_CRM_PORT')+"/"+os.getenv('DB_CRM_BASE'),
                                     min = 2, max = 5, increment = 1, threaded = True,
                                     getmode = cx_Oracle.SPOOL_ATTRVAL_WAIT)
        pool.ping_interval = 1
        conn = pool.acquire()
    except Exception as err:
       logging.exception('Exeption occured while trying to connect ')
    else:
        try:
            curr = conn.cursor()
        except Exception as err:
            logging.exception('Exception occured while trying to fetch records ')
        else:
            curr.execute(sqlQuery_)
            row = curr.fetchall()
    df = pd.DataFrame(row)
    if df.shape[0] > 0:
        df.columns = ['CaseId','CreationDate', 'dLatitude','dLongitude','MunicipalityId','SubMunicipalityId','Sub_SubMunicipalityId','VPId']
    return df

# The database utility for writing case clustering model out into MOMRA production database.
# for (Oracle 19C version) connection string (MEDIUM_AIDBPRO).
def write_engines_output_to_database(
    data,
    table_name      = "CASE_CLUSTERING_SC_TBL",
    if_exists       = None,
    uid_pwd         = os.getenv('DB_AI_USER')+":"+os.getenv('DB_AI_PASS'), # DB User and Password (with ":" )
    hostname        = os.getenv('DB_AI_HOST'), # DB server IP address/Hostname
    service_name_   = os.getenv('DB_AI_BASE'), # Database service name
    schema_         = os.getenv('DB_AI_USER'), # Database schema name
    SCID_lst_       = None,
    INCID_lst_      = None  
):

    oracle_connection_string = f'oracle+cx_oracle://{uid_pwd}' + '@' + cx_Oracle.makedsn(hostname, '1521', service_name_)
    # oracle_connection_string = "AIDB_USER/f8s5x8@//ruhmsv-ora19c-scan.net:1521/AID_B.momra.net"
    try:
        engine = sql.create_engine(oracle_connection_string)
        conn = engine.connect()
        if schema_ == os.getenv('DB_CRM_SCM_PEGADATA'):
            engine.execute(f"DELETE FROM {os.getenv('DB_CRM_SCM_PEGADATA')}.pr_momra_fw_cs_data_aicrmcases "
                           f"where SPLCLASSIFICATIONID in ({SCID_lst_})")
            if os.getenv('APP_ENV') == 'local':
                engine.execute(f"DELETE FROM {os.getenv('DB_CRM_SCM_PEGADATA')}.pr_momra_fw_cs_data_aicrmcases where INCIDENTNUMBER in (select distinct vpid from {os.getenv('DB_AI_USER')}.temp_scid_tbl)")
            else:
                engine.execute(f"DELETE FROM {os.getenv('DB_CRM_SCM_PEGADATA')}.pr_momra_fw_cs_data_aicrmcases where INCIDENTNUMBER in (select distinct vpid from {os.getenv('DB_AI_USER')}.temp_scid_tbl@AIDBPROD)")
    except Exception as e:
        logging.exception(f'Exeption occured while trying to connect\n {e} ')
    else:
        try:
            df = data

            if table_name == 'case_clustering_sc_tbl':
                df = df.astype({
                    'vp_label': np.str, 'dlatitude': np.str,
                    'dlongitude': np.str, 'municipalityid': np.str,
                    'submunicipalityid': np.str, 'sub_submunicipalityid': np.str,
                    'cluster_label': np.str, 'cluster_longitude': np.str,
                    'cluster_latitude': np.str, 'distance': np.str,
                    'vpid': np.str, 'unique_cluster_label': np.str,
                    'unique_cluster_id': np.str,
                })

            if table_name == 'temp_scid_tbl':
                df = df.astype({'vpid': np.str})

            # Make Table
            if schema_ == os.getenv('DB_CRM_SCM_PEGADATA'):
                    # logging.info(df.head())
                    logging.info( df.applymap(type).nunique())
                    dtyp = {
                        c:sql.types.VARCHAR(df[c].str.len().max())
                            for c in df.columns[df.dtypes == 'object'].tolist()}
                    logging.info(f"In {os.getenv('DB_CRM_SCM_PEGADATA')}")
                    df.to_sql(
                    con       = conn,
                    schema    = schema_,
                    dtype     = dtyp,
                    name      = table_name,
                    if_exists = if_exists,
                    index     = False,
                    chunksize = 10**4
                    )
                    logging.info(f"Model Output completed/exported successfully {schema_}")
                    # engine.execute("delete from  AIDB_USER.temp_scid_tbl@AIDBPROD")
            else:
                try:
                    logging.info("In AIDB_USER>>>>>")
                    dtyp = {
                        c: sql.types.VARCHAR(df[c].str.len().max())
                        for c in df.columns[df.dtypes == 'object'].tolist()}
                    df.to_sql(
                    con       = conn,
                    schema    = schema_,
                    dtype     = dtyp,
                    name      = table_name,
                    if_exists = if_exists,
                    index     = False,
                    chunksize = 10**4
                    )
                    logging.info(f"Added into historical table>>>>")
                    logging.info(f"Model Output completed/exported successfully {schema_}")
                except Exception as e:
                    logging.exception(f"Error while exporting the model output into historical table \n\n {e}")
        except Exception as e:
            logging.exception(f"Error while exporting the model output \n\n {e}")
pass

def collect_caseclust_model_inputs(sqlQuery_ = None, connectString_ =f"{os.getenv('DB_CRM_USER')}/{os.getenv('DB_CRM_PASS')}@//{os.getenv('DB_CRM_HOST')}:{os.getenv('DB_CRM_PORT')}/{os.getenv('DB_CRM_BASE')}"):
    # Connect to database
    row = []
    try:
        conn = cx_Oracle.connect(connectString_)
    except Exception as err:
        logging.exception('Exeption occured while trying to connect ')
    else:
        try:
            curr = conn.cursor()
            curr.execute(sqlQuery_)
        except Exception as err:
            logging.exception('Exception occured while trying to fetch records ')
        else:
            row = curr.fetchall()
            logging.info('Input config fetched successfully')

    df = pd.DataFrame(row)
    # if df.shape[0] > 0 and len(df.columns) > 1:
    if df.shape[0] > 0:
        if len(df.columns) > 1:
            df.columns = ['GUID','MUNICIPALITYID','SPECIALCLASSIFICATION','SPECIALCLASSIFICATIONID','RANGE','REPITITTIONPERIODDAYS','REPITITTIONPERIODHOURS','NOOFTIMES','SPLCLASSCATEGORY','STARTTIME']
        else:
            df.columns = ['SPLCLASSIFICATIONID']
    return df

def selectData (sqlQuery_ = None, connectString_ =f"{os.getenv('DB_CRM_USER')}/{os.getenv('DB_CRM_PASS')}@//{os.getenv('DB_CRM_HOST')}:{os.getenv('DB_CRM_PORT')}/{os.getenv('DB_CRM_BASE')}"):
    # Connect to database
    row = []
    try:
        conn = cx_Oracle.connect(connectString_)
    except Exception as err:
        logging.exception('Exeption occured while trying to connect ')
    else:
        try:
            curr = conn.cursor()
            curr.execute(sqlQuery_)
        except Exception as err:
            logging.exception('Exception occured while trying to fetch records ')
        else:
            row = curr.fetchall()
            logging.info('Input config fetched successfully')

    return pd.DataFrame(row)