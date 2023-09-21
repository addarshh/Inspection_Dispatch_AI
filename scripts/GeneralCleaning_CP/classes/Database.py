import pandas as pd
import config
import os.path as path
from classes.engines import Helper as Help
#from classes.engines.Helper import insert_df_Batchwise, backup
from abc import ABC
import sqlalchemy as sql
import cx_Oracle
import sys
Helper=Help.Helper()
class Database(ABC):
    def __init__(self, amanaCode=None):
        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
                                + cx_Oracle.makedsn(config.DB['host'], config.DB['port'],
                                                    service_name=config.DB['service'])
        self.connection = None
        self._connect()

        self.CRM_full: pd.DataFrame = None
        self.datareq_excavations: pd.DataFrame = None
        self.drilling_inspections: pd.DataFrame = None
        self.amanaCode = amanaCode

    def _connect(self):
        #lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        lib_dir = config.DB['instaclientpath']
        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            #sys.exit(1)
            pass

        try:
            engine = sql.create_engine(self.connectionString)
            self.connection = engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            pass
            #sys.exit(1)

    def get_crm_data(self):

        sqlQuery = """SELECT c.PYID AS "PYID",c.INTERACTIONTYPE AS "INTERACTIONTYPE",c.PXCREATEDATETIME AS "PXCREATEDATETIME",
        c.PYRESOLVEDTIMESTAMP AS "CLOSURE_DATE" ,c.PYSTATUSWORK AS "SHORT_STATUS" ,c.LATITUDE  AS "LATITUDE",
        c.LONGITUDE AS "LONGITUDE",c.MAINCLASSIFICATION AS "MAIN_Classificaion",c.SPLCLASSIFICATION AS "SP_Classificaion",
        c.CATEGORY AS "Category",c.PRIORITY AS "PRIORITY",c.EXTERNALCONTRACTOR AS "IS_Contractor",c.RESOLUTIONSATISFYBYPETITIONER AS  "Satisfaction",
        c.VISUAL_POLLUTION_CATEGORY AS "VISUAL POLLUTION CATEGORY",c.SUBMUNICIPALITYID AS "SUBMUNIC_3"
        FROM %s.CASES c""" % config.DB['input_schema']

        try:
            self.CRM_full = self._executeQuery(sqlQuery)
        except Exception as error:
            print(str(error))
            sys.exit(1)
        print('self.CRM_full.shape')
        print(self.CRM_full.shape)
        return self.CRM_full

    def get_datareq_excavations(self):
        sqlQuery = """SELECT LICENCES_ID "LIC_ID", REQUEST_ID "REQ_ID", LATITUDE "START_POINT_X", LONGITUDE "START_POINT_Y", AMANA "AMMANA", MUNICIPALITY "Municipality", SUB_MUNICIPALITY "Sub-Municipality", DISTRICT_ID "DISTRICT_ID", DISTRICT_NAME "DISTRICT_NAME", EMERGENCY_LICENSE "EMERGENCY_LICENSE", LICENSE_START_DATE "ISSUE_DATE", LICENSE_EXPIRY_DATE "EXPIRATION_GDATE", DIGGING_START_DATE "DIGGING_START_DATE", DIGGING_END_DATE "DIGGING_END_DATE", DIGGING_STATUS "DIGGING_STATUS", SITE_NAME "SITE_NAME", PROJECT_NAME "PROJECT_NAME", PROJECT_DESC "PROJECT_DESC", NAME "NAME", PROJECT_START_DATE "PROJECT_START_DATE", WORK_START_DATE "WORK_START_DATE", DIGGING_DURATION "DIGGING_DURATION", PROJECT_END_DATE "PROJECT_END_DATE", DIGGING_METHOD_ID "DIGGING_METHOD_ID", DIGGING_METHOD "DIGGING_METHOD", WORK_NATURE_ID "WORK_NATURE_ID", WORK_NATURE "WORK_NATURE", PATH_LENGTH_SUM "PATH_LENGTH_SUM", NETWORK_TYPE_ID "NETWORK_TYPE_ID", NETWORK_TYPE "NETWORK_TYPE", MAP_NO "MAP_NO", HEAVY_EQUIPMENT_PERMISSION "HEAVY_EQUIPMENT_PERMISSION", CAMPING_ROOM_COUNT "CAMPING_ROOM_COUNT", CONSULTANT_NAME "CONSULTANT_NAME", CONSULTANT_CR "CONSULTANT_CR", CONTRACTOR_NAME "CONTRACTOR_NAME", CONTRACTOR_CR "CONTRACTOR_CR", PATH_CODE "PATH_CODE", LENGTH "LENGTH", WIDTH "WIDTH", DEPTH "DEPTH", DIGGING_LATE_STATUS_ID "DIGGING_LATE_STATUS_ID", DIGGING_LATE_STATUS "DIGGING_LATE_STATUS", DIGGING_CATEGORY_ID "DIGGING_CATEGORY_ID", DIGGING_CATEGORY "DIGGING_CATEGORY", SERVICE_CONFIG "SERVICE_CONFIG", REQUEST_TYPE_ID "REQUEST_TYPE_ID", REQUEST_TYPE "REQUEST_TYPE"
        FROM %s.LICENSES_DATA_EXCAVATIONS""" % config.DB['input_schema']
        self.datareq_excavations = self._executeQuery(sqlQuery)
        #self.datareq_excavations.columns = self.datareq_excavations.columns.str.lower()
        return self.datareq_excavations

    def get_licencedata_constructions(self):
        sqlQuery = """SELECT BUILDING_ID "Building ID" ,LICENCES_ID "License ID",REQUEST_ID "Request ID",AMANA "Amana",MUNICIPALITY "Municipality",SUB_MUNICIPALITY "Sub-Municipality",LICENSE_START_DATE "ISSUE_DATE",LICENSE_EXPIRY_DATE "EXPIRATION_DATE",LATITUDE "lat",LONGITUDE "long",CONSULTANT_NAME "consultant name",CONSULTANT_LICENSE_ID "Consultant license id",BUILDING_TYPE "BUILDING TYPE",BUILDING_TYPE_ID "BUILDING TYPE ID",BUILDING_MAIN_USE "Building main use",BUILDING_MAIN_USE_ID "Building main use id",BUILDING_SUB_USE "Building Sub use",BUILDING_SUB_USE_ID "Building sub use id",PARCEL_AREA "Parcel area",CONTRACTOR "Contractor",CONTRACTOR_LICENSE_ID "Contractor license ID",BUILDING_HEIGHT "Building height",OWNER_ID "owner_id"
                    FROM %s.LICENSES_DATA_CONSTRUCTIONS""" % config.DB['input_schema']
        self.datareq_constructions = self._executeQuery(sqlQuery)
        # self.datareq_excavations.columns = self.datareq_excavations.columns.str.lower()
        return self.datareq_constructions

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)


def save_final(Final_df, output_table_name):
    print(Final_df.columns)
    print(config.meta_data.columns)
    df_final1=pd.merge(Final_df, config.meta_data, on='CaseId', how='left')
    print("df_final1 shape" + df_final1.columns )
    # df_final1 = df_final1.drop('CaseId', axis=1, inplace=True)
	#df_final1.columns  = map(str.upper, df_final1.columns)
    Helper.backup(output_table_name)
    # insert_df( df, output_table_name)

    Helper.insert_df_Batchwise(df_final1, output_table_name, 50000)
