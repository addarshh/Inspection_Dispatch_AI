from pyexpat import model
from statistics import multimode
from typing import Dict, List
import sqlalchemy as sql
import cx_Oracle
import pandas as pd
import config
import geopandas
import os.path as path
from shapely import wkt
import sys 
from helpers import getStageNameDictionary
import numpy as np
import logging

class Database():

    def __init__(self, amanaCode = None):

        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
            + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
        self.connection = None
        self.engine = None
        self._connect()

        self.inspectionsWithLicensesDf : pd.DataFrame = None
        self.licensesDf : pd.DataFrame = None
        self.licensesKeysDf : pd.DataFrame = None
        self.waterDf : pd.DataFrame = None
        self.priorityAreas : pd.DataFrame = None
        self.inspectionsDf : pd.DataFrame = None
        self.classConfig : pd.DataFrame = None
        self.amanaDataGdf = None
        self.amanaPopulationOverlay = None
        self.populationData = None
        self.amanaPopulationOverlay2 = None
        self.populationData2 = None
        self.amanaCode = amanaCode

        self.excavationPhases = None
        self.drillingInspectionsDf = None
        self.drillingLicensesDf = None

    def _connect(self):
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])

        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            #sys.exit(1)
            pass

        try:
            self.engine = sql.create_engine(self.connectionString)
            self.connection = self.engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            #sys.exit(1)
            pass

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):
        print("1 HERE")
        filterStr = ''
        if self.amanaCode is not None:
            filterStr += f"AND AM.AMANACODE = '{self.amanaCode}'"
        # self.inspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))
        sqlQuery = """SELECT  STATUS_OF_WORK "PYSTATUSWORK", LICENSE_NUMBER "LIC_ID", INSEPECTION_ID "INSPECTION_ID", INSPECTION_DATE "INSPECTION_DATE", PHASE_NUMBER "PHASE_NUMBER", PHASE_NAME "PHASE_NAME", STAGENAME "STAGENAME", STAGENO "STAGENO", NUMBEROFFAILEDCLAUSES "NUMBEROFFAILEDCLAUSES", COMPLIANCE "COMPLIANCE", COMPLYINGITEMS "COMPLYINGITEMS", WORKS_STOPPED "WORKS_STOPPED", NO_LICENSE "HASNOLICENSE"
        FROM %s.INSPECTION_DATA_EXCAVATIONS""" % config.DB['input_schema']
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.inspectionsDf = self._executeQueryToDf(sqlQuery)
        self.inspectionsDf['Issued fine amount'] = self.inspectionsDf['Issued fine amount'].astype(float)
        print("inspectionsdata")
        print(self.inspectionsDf.shape)


    def getLicensesData(self):
        sqlQuery = """SELECT LICENCES_ID "LIC_ID", REQUEST_ID "REQ_ID", LATITUDE "START_POINT_X", LONGITUDE "START_POINT_Y", AMANA "AMMANA", MUNICIPALITY "Municipality", SUB_MUNICIPALITY "Sub-Municipality", DISTRICT_ID "DISTRICT_ID", DISTRICT_NAME "DISTRICT_NAME", EMERGENCY_LICENSE "EMERGENCY_LICENSE", LICENSE_START_DATE "ISSUE_DATE", LICENSE_EXPIRY_DATE "EXPIRATION_GDATE", DIGGING_START_DATE "DIGGING_START_DATE", DIGGING_END_DATE "DIGGING_END_DATE", DIGGING_STATUS "DIGGING_STATUS", SITE_NAME "SITE_NAME", PROJECT_NAME "PROJECT_NAME", PROJECT_DESC "PROJECT_DESC", NAME "NAME", PROJECT_START_DATE "PROJECT_START_DATE", WORK_START_DATE "WORK_START_DATE", DIGGING_DURATION "DIGGING_DURATION", PROJECT_END_DATE "PROJECT_END_DATE", DIGGING_METHOD_ID "DIGGING_METHOD_ID", DIGGING_METHOD "DIGGING_METHOD", WORK_NATURE_ID "WORK_NATURE_ID", WORK_NATURE "WORK_NATURE", PATH_LENGTH_SUM "PATH_LENGTH_SUM", NETWORK_TYPE_ID "NETWORK_TYPE_ID", NETWORK_TYPE "NETWORK_TYPE", MAP_NO "MAP_NO", HEAVY_EQUIPMENT_PERMISSION "HEAVY_EQUIPMENT_PERMISSION", CAMPING_ROOM_COUNT "CAMPING_ROOM_COUNT", CONSULTANT_NAME "CONSULTANT_NAME", CONSULTANT_CR "CONSULTANT_CR", CONTRACTOR_NAME "CONTRACTOR_NAME", CONTRACTOR_CR "CONTRACTOR_CR", PATH_CODE "PATH_CODE", LENGTH "LENGTH", WIDTH "WIDTH", DEPTH "DEPTH", DIGGING_LATE_STATUS_ID "DIGGING_LATE_STATUS_ID", DIGGING_LATE_STATUS "DIGGING_LATE_STATUS", DIGGING_CATEGORY_ID "DIGGING_CATEGORY_ID", DIGGING_CATEGORY "DIGGING_CATEGORY", SERVICE_CONFIG "SERVICE_CONFIG", REQUEST_TYPE_ID "REQUEST_TYPE_ID", REQUEST_TYPE "REQUEST_TYPE"
        FROM %s.LICENSES_DATA_EXCAVATIONS,""" % config.DB['input_schema']
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.licensesDf = self._executeQueryToDf(sqlQuery)
        print("licenses data")
        print(self.licensesDf.shape)


    def getLicensesKeysData(self):
        sqlQuery = \
        f"SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, \
            ACTIVITIE_TYPE_NAME, ACTIVITYNO AS \"MOMTATHEL ACTIVITY NUMBER\"\
        FROM %s.MOMTHATEL_DATA" % config.DB['input_schema']

        self.licensesKeysDf = self._executeQueryToDf(sqlQuery)
        self.licensesKeysDf.columns = self.licensesKeysDf.columns.str.upper()
        print("Momtathel data")
        print(self.licensesKeysDf.shape)

    def getWaterData(self):
        self.waterDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['water_data']))
        self.waterDf['X']=self.waterDf['XY Google'].str.split(' |\t|,').str[0]
        self.waterDf['Y']=self.waterDf['XY Google'].str.split(' |\t|,').str[1]

        self.waterDf=self.waterDf[~self.waterDf['Y'].isna()]

        self.waterDf=self.waterDf[self.waterDf['Water Agreement Status']=='Active']

        self.waterDf[['X','Y']]=self.waterDf[['X','Y']].apply(pd.to_numeric, errors='coerce')
        self.waterDf=self.waterDf[self.waterDf['X']<60]
        self.waterDf=self.waterDf[self.waterDf['Y']<60]

        self.waterDf[['new_X','new_Y']]=self.waterDf[['X','Y']].copy()

        self.waterDf.loc[self.waterDf['Y']<32, 'new_X']=self.waterDf[self.waterDf['Y']<33]['Y']
        self.waterDf.loc[self.waterDf['X']>33, 'new_Y']=self.waterDf[self.waterDf['X']>40]['X']

        self.waterDf[['X','Y']]=self.waterDf[['new_X','new_Y']].copy()

        self.waterDf = geopandas.GeoDataFrame(
            self.waterDf, geometry = geopandas.points_from_xy( self.waterDf['Y'], self.waterDf['X'],), crs="EPSG:4326")


    def getPopulationDataGdf(self):
        self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data']))

    def getPopulationDataGdf2(self):
        self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data2']))

    def getPriorityAreasData(self):
        self.priorityAreas = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['priority_areas']))
        self.priorityAreas = self.priorityAreas[self.priorityAreas['geometry'].notna()]
        self.priorityAreas = self._fromPdToGdf(self.priorityAreas)
        self.priorityAreas= self.priorityAreas.drop(columns=['Unnamed: 0'])


    def getCrmCases(self):
        sqlQuery = "SELECT DISTINCT PYID \"CaseId\", INTERACTIONTYPE, PXCREATEDATETIME,  \
            CLOSURE_DATE, SHORT_STATUS, LATITUDE \"LATITUDE\", LONGITUDE,  \
            MAIN_CLASSIFICATION \"MAIN_Classificaion\", SUB_CLASSIFICATION \"Sub_Classificaion\",  \
            SP_CLASSIFICATION \"SP_Classificaion\",\
            CATEGORY, PRIORITY \"Priority\", SATISFACTION \"Satisfaction\" \
        FROM %s.CRM_INSPECTION_CASES \
        WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL AND SHORT_STATUS <> 'Close'" % config.DB['input_schema']
        # FETCH FIRST 2000 ROWS ONLY
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME

        self.crmData = self._executeQueryToDf(sqlQuery)
        self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
            'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
        #Dropping all the missing values present in Latitude and Longitude columns of the dataset
        # self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
        self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
        self.crmData.Priority_Value.replace(
            {'حرج' : 3, 'عالي': 3, 'متوسط' : 2, 'عادي' : 1, 'High' : 3, 'high' : 3,'medium' : 2,  'Medium' : 2,'Low' : 1, 'low' : 1}, 
            inplace = True
        )
        print("get crm data")
        print(self.crmData.shape)
        
    def getAmanaDataGdf(self):
        self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))

        if self.amanaCode is not None:
            self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

    def getAmanaPopulationOverlay(self):
        if self.populationData is None:
            self.getPopulationDataGdf()

        if self.amanaDataGdf is None:
            self.getAmanaDataGdf()

        self.amanaPopulationOverlay = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
        self.amanaPopulationOverlay.drop(labels = self.amanaPopulationOverlay.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)


    def getAmanaPopulationOverlay2(self):
        if self.populationData2 is None:
            self.getPopulationDataGdf2()

        if self.amanaDataGdf is None:
            self.getAmanaDataGdf()

        self.amanaPopulationOverlay2 = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
        self.amanaPopulationOverlay2.drop(labels = self.amanaPopulationOverlay2.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)
        self.amanaPopulationOverlay2.to_crs(epsg= 32637,inplace = True)

    
    def _fromPdToGdf(self, data, x = True,y = True):
        if 'geometry' in data.columns:
            try:
                gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
            except:
                data['geometry'] = data['geometry'].astype(str)
                data['geometry'] = data['geometry'].apply(wkt.loads)
                gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
                
        else:
             gpd = geopandas.GeoDataFrame(data,geometry = geopandas.points_from_xy(x,y), crs = 'epsg:4326')
        #gpd.to_crs('epsg:32637',inplace = True)
        return gpd 

    def getClassConfig(self):
        self.classConfig = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['class_config']))

    def getCommonData(self):
        self.getLicensesData()
        self.getLicensesKeysData()
        self.getInspectionsData()

    def insertModelStatus(self, modelName : str, startTimestamp : str, status : str) -> str:
        cursor = self.connection.connection.cursor()
        id = cursor.var(cx_Oracle.NUMBER)
        sqlQuery = "INSERT INTO %s.MLMODEL_DETAILS (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME)\
            VALUES(:name, :status, :time) RETURNING ID into :id" % config.DB['input_schema']
        parameters= dict( name = modelName, status = status, time = startTimestamp, id = id)
        
        cursor.execute(sqlQuery, parameters)
        self.connection.connection.commit()
        return str(id.getvalue()[0])
        
        # print()
        # self._executeQueryToList(sqlQuery=sqlQuery, )
    def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
        sqlQuery = "UPDATE %s.MLMODEL_DETAILS SET MODEL_STATUS = :status, MODEL_END_TIME = :time \
            WHERE ID = :id" % config.DB['input_schema']
        parameters= dict( status = status, time = endTimestamp, id = recordId)
        
        self.connection.execute(sqlQuery, parameters)


    def _executeQueryToDf(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)

    def deleteRBDOutputLicenses(self, inspectionType : str, tableName : str):
        sqlQuery = f"DELETE FROM \"C##ACIO\".{tableName}"
        self.connection.execute(sqlQuery)
   

    def deleteRBDOutputGrids(self, inspectionType : str, tableName : str ):
        sqlQuery = f"DELETE FROM \"C##ACIO\".{tableName}"
        self.connection.execute(sqlQuery)

    def saveRBDOutputLicenses(self, dataframe : pd.DataFrame, table_name : str):
        splits = []
        batch_size = 10000
        cursor = self.connection.connection.cursor()

        for column in dataframe.columns:
            if dataframe[column].dtype == object:
                dataframe[column].fillna('', inplace = True)
            else:
                dataframe[column].replace({np.nan : None}, inplace = True)
        columns = ','.join(list(dataframe.columns)) 
        values = ','.join([':' + str(i + 1) for i in range(0, len(list(dataframe.columns)))])

        insert_sql = 'INSERT INTO "C##ACIO"."{}" ({}) VALUES ({})'.format(table_name.upper(), columns, values)
        for i in range(0, dataframe.shape[0], 10000):
            splits.append(dataframe[i:i+batch_size])
        
        for i in range(len(splits)):
            
            df_row_list = [tuple(x) for x in splits[i].values.tolist()]

            cursor.executemany(insert_sql, df_row_list, batcherrors=True)
            self.connection.connection.commit()
        cursor.close()
        # dataframe.to_sql(name = table_name, con =  self.engine, schema = 'C##ACIO', if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : sql.types.CLOB, 'LONGITUDE' : sql.types.FLOAT, 'LATITUDE' : sql.types.FLOAT, 
        #     'LIC_ID' : sql.types.BIGINT, 'HASNOLICENSE' : sql.types.BIGINT, 'START_POINT_X' : sql.types.Float,
        #     'START_POINT_Y' : sql.types.Float }
        # )

    
    def saveRBDOutputGrids(self, dataframe : pd.DataFrame, table_name : str):
        dataframe.to_sql(name = table_name, con =  self.engine, schema = 'C##ACIO', if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : sql.types.CLOB })

    def getDrillingInspections(self):
            logging.info("Fetching data from Drilling Inspectino")
            sqlQuery = "SELECT CAST(LICENSE_NUMBER AS VARCHAR2(30)) \"LIC_ID\", INSEPECTION_ID \"INSPECTION_ID\", \
                    TO_CHAR(INSPECTION_DATE, 'YYYY-MM-DD') INSPECTION_DATE, \
                    STAGENAME \"STAGENAME\", \
                    CASE WHEN NUMBEROFFAILEDCLAUSES > 0 THEN 1 ELSE 0  END ISVIOLATION\
                FROM %s.INSPECTION_DATA_EXCAVATIONS \
                WHERE STATUS_OF_WORK IN ('Pending-ReceiveSample', 'Resolved-Completed', 'Under Review and Approval')" % config.DB['input_schema']
            # FROM C##ACIO.DRILLING_INSPECTION_DATA \
            # self.drillingInspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['drilling_inspections']), dtype = {'LIC_ID': str, 'INSPECTION_DATE' : str})
            self.drillingInspectionsDf = self._executeQueryToDf(sqlQuery)
            self.drillingInspectionsDf.rename(
                inplace=True,
                columns={'lic_id' : 'LIC_ID', 'inspection_id' : 'INSPECTION_ID', 'inspection_date' : 'INSPECTION_DATE',
                'isviolation' : 'ISVIOLATION','stagename' : 'STAGENAME'},
            )
            # self.drillingInspectionsDf['INSPECTION_DATE']=self.drillingInspectionsDf['INSPECTION_DATE'].astype(str).str.split(' ').str[0]
            self.drillingInspectionsDf['INSPECTION_DATE']=pd.to_datetime(self.drillingInspectionsDf['INSPECTION_DATE'])
            # self.drillingInspectionsDf.loc[self.drillingInspectionsDf['NUMBEROFFAILEDCLAUSES']>0, 'ISVIOLATION']=True
            # self.drillingInspectionsDf['ISVIOLATION']=self.drillingInspectionsDf['ISVIOLATION'].fillna(False)
            # self.drillingInspectionsDf=self.drillingInspectionsDf[self.drillingInspectionsDf['PYSTATUSWORK']
            #     .isin(['Pending-ReceiveSample', 'Resolved-Completed', 'Under Review and Approval'])]

            dictionary = getStageNameDictionary(self.drillingInspectionsDf['STAGENAME'].dropna().unique())
            self.drillingInspectionsDf=self.drillingInspectionsDf.replace({'STAGENAME':dictionary})
            logging.info("Shape of Drilling Inspection Data: " + str(self.drillingInspectionsDf.shape))
            print("Shape of Drilling Inspection Data: " + str(self.drillingInspectionsDf.shape))


    def getDrillingLicenses(self):
        logging.info("Fetching data from Drilling Licenses ")
        sqlQuery = "SELECT LICENCES_ID LIC_ID, LATITUDE START_POINT_X, LONGITUDE START_POINT_Y, AMANA, MUNICIPALITY , SUB_MUNICIPALITY,\
                DISTRICT_ID, DISTRICT_NAME, DIGGING_START_DATE, DIGGING_END_DATE, DIGGING_METHOD_ID, WORK_NATURE_ID, \
                NETWORK_TYPE_ID, HEAVY_EQUIPMENT_PERMISSION, CAMPING_ROOM_COUNT, CONTRACTOR_CR, PATH_CODE, ROAD_TYPE, STREET_CLOSE_TYPE, \
                \"LENGTH\", WIDTH, \"DEPTH\",  \
                CASE REQUEST_TYPE WHEN 'اصدار تصريح حفرية طارئة' THEN 1 ELSE 0 END AS EMERGENCY_LICENSE, \
                CASE DIGGING_CATEGORY WHEN 'الأولى' THEN 1 \
                    WHEN 'الثانية' THEN 2 WHEN 'الثالثة' THEN 3 WHEN 'الرابعة' THEN 4 \
                    WHEN 'الخامسة' THEN 5 WHEN 'السادسة' THEN 6 WHEN 'السابعة' THEN 7 \
                    WHEN 'الثامنة' THEN 8 ELSE NULL END AS DIGGING_CATEGORY\
            FROM %s.LICENSES_DATA_EXCAVATIONS \
            ORDER BY LIC_ID ASC" % config.DB['input_schema']
        #  FROM C##ACIO.DRILLING_LICENSES_DATA \
        self.drillingLicensesDf = self._executeQueryToDf(sqlQuery)
        self.drillingLicensesDf['lic_id'] = self.drillingLicensesDf['lic_id'].astype(str)
        self.drillingLicensesDf['length'] = self.drillingLicensesDf['length'].astype(float)
        self.drillingLicensesDf['width'] = self.drillingLicensesDf['width'].astype(float)
        self.drillingLicensesDf['depth'] = self.drillingLicensesDf['depth'].astype(float)

        self.drillingLicensesDf.rename(columns={'lic_id' : 'LIC_ID', 'start_point_x' : 'START_POINT_X',
            'start_point_y' : 'START_POINT_Y', 'depth':'DEPTH', 'path_code' : 'PATH_CODE', 
            'width': 'WIDTH', 'length' : 'LENGTH', 'district_id' : 'DISTRICT_ID', 'district_name' : 'DISTRICT_NAME',
            'amana' : 'AMMANA', 'municipality' : 'Municipality', 'sub_municipality' : 'Sub-Municipality',
            'emergency_license' : 'EMERGENCY_LICENSE', 'digging_end_date' : 'DIGGING_END_DATE', 'digging_start_date' : 'DIGGING_START_DATE',
            'digging_category' : 'DIGGING_CATEGORY', 'contractor_cr' : 'CONTRACTOR_CR', 
            'heavy_equipment_permission' : 'HEAVY_EQUIPMENT_PERMISSION', 'camping_room_count' :  'CAMPING_ROOM_COUNT',
            'network_type_id' : 'NETWORK_TYPE_ID', 'digging_method_id' : 'DIGGING_METHOD_ID', 'work_nature_id' : 'WORK_NATURE_ID', 'road_type':'ROAD_TYPE',
            'street_close_type':'STREET_CLOSE_TYPE'
            }
            , inplace=True
        )
        logging.info("Shape of Drilling Inspectino DataFrame: " + str(self.drillingLicensesDf.shape))
        print("Shape of Drilling Inspectino DataFrame: " + str(self.drillingLicensesDf.shape))
        # pd.read_excel(path.join(path.dirname(__file__),config.PATHS['drilling_licenses']), dtype = {'LIC_ID': str})

        # self.drillingLicensesDf.sort_values('LIC_ID', inplace=True)
        # self.drillingLicensesDf.loc[self.drillingLicensesDf['REQUEST_TYPE']=='اصدار تصريح حفرية طارئة', 'EMERGENCY_LICENSE']=True
        # self.drillingLicensesDf['EMERGENCY_LICENSE'].fillna(False, inplace=True)
        # digging_category_dict={'الأولى':1,
        #               'الثانية':2,
        #               'الثالثة':3,
        #               'الرابعة':4,
        #               'الخامسة':5,
        #               'السادسة':6,
        #               'السابعة':7,
        #               'الثامنة':8}
        
        # self.drillingLicensesDf.replace({'DIGGING_CATEGORY':digging_category_dict}, inplace=True)

    def getExcavationPhases(self):
        self.excavationPhases = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['excavation_phases']))

    def getExcavationData(self):
        self.getDrillingInspections()
        self.getDrillingLicenses()
        print(self.drillingInspectionsDf.shape)
        print(self.drillingLicensesDf.shape)




    


        
