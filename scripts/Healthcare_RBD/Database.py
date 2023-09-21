# import sqlalchemy as sql
# import cx_Oracle
# import pandas as pd
# import config
# import geopandas
# import os.path as path
# from shapely import wkt
# import sys 
# import numpy as np
# import logging

# class Database():

#     def __init__(self, amanaCode = None):

#         self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
#         self.connection = None
#         self.engine = None
#         self._connect()
#         self.inspectionsWithLicensesDf : pd.DataFrame = None
#         self.licensesDf : pd.DataFrame = None
#         self.licensesKeysDf : pd.DataFrame = None
#         self.waterDf : pd.DataFrame = None
#         self.priorityAreas : pd.DataFrame = None
#         self.inspectionsDf : pd.DataFrame = None
#         self.classConfig : pd.DataFrame = None
#         self.amanaDataGdf = None
#         self.amanaPopulationOverlay = None
#         self.populationData = None
#         self.amanaPopulationOverlay2 = None
#         self.populationData2 = None
#         self.amanaCode = amanaCode
#         self.poiData = None
#         self.excavationPhases = None
#         self.drillingInspectionsDf = None
#         self.drillingLicensesDf = None

#     def _connect(self):
#         lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])

#         try:
#             cx_Oracle.init_oracle_client(lib_dir = lib_dir)

#         except Exception as error:
#             print("Error handling cx_Oracle.init_oracle_client")
#             print(error)
#             sys.exit(1)

#         try:
#             self.engine = sql.create_engine(self.connectionString)
#             self.connection = self.engine.connect()
#         except Exception as error:
#             print("Error with creating connection")
#             print(error)
#             sys.exit(1)

#     def wkt_loads(self, x):
#         try:
#             return wkt.loads(x)
#         except Exception:
#             return None

#     def getInspectionsData(self):

#         filterStr = ''
#         if self.amanaCode is not None:
#             filterStr += f"AND AM.AMANACODE = '{self.amanaCode}'"
#         sqlQuery = 'SELECT LICENSE_NUMBER "LICENSE NUMBER" , INSEPECTION_ID "INSEPECTION ID", INSPECTYPE_TYPE_ID "INSPECTYPE ID" ,INSPECTION_NAME "INSPECTION NAME" , ESTABLISHMENT_NAME "Establishment Name", BUSINESS_ACTIVITY_DESCRIPTION "Business Activity Description" , STATUS_OF_WORK "Status of Work" , TYPE_OF_VISIT "TYPE OF VISIT", BUSINESS_ACTIVITY_NUMBER "Business Activity Number" , BUSINESS_ACTIVITY_WEIGHT "Business Activity Weight", INSPECTION_DATE "Inspection Date" , DEGREE_OF_COMPLIANCE "Degree of Compliance" , NUMBER_OF_CLAUSES "Number of clauses", NUMBER_OF_COMPLIANT_CLAUSES "Number of compliant clauses" , NUMBER_OF_NONCOMPLIANT_CLAUSES "Number of non-compliant clauses", NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_HIGH_RISK "Number of non-compliant clauses and High risk", NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_MEDIUM_RISK "Number of non-compliant clauses and medium risk", ISSUED_FINE_AMOUNT "Issued fine amount" , SADAD_NO "SADAD NO" , FINE_PAYMENT_STATUS "Fine payment status", SADAD_PAYMENT_DATE "SADAD PAYMENT DATE" , INSPECTOR_ACTION "Inspector_Action" , APPROVER_CONFISCATION "APPROVER CONFISCATION", APPROVER_FOLLOWUP "APPROVER FOLLOWUP" , APPROVER_DESTROY "APPROVER DESTROY" , APPROVER_SAMPLE "APPROVER SAMPLE", APPROVER_CLOSE "APPROVER CLOSE" , NO_LICENSE "NO LICENSE" FROM ' + config.DB['input_schema'] + '.INSPECTION_DATA'
#         self.inspectionsDf = self._executeQueryToDf(sqlQuery)
#         self.inspectionsDf['Issued fine amount'] = self.inspectionsDf['Issued fine amount'].astype(float)
#         self.inspectionsDf = self._executeQueryToDf(sqlQuery)
#         self.inspectionsDf['Issued fine amount'] = self.inspectionsDf['Issued fine amount'].astype(float)
#         logging.info("CHECK:INPUT df:inspectionsDf {}".format(self.inspectionsDf.shape))

#     def getLicensesData(self):
#         sqlQuery = f'SELECT LICENCES_ID "License ID (MOMRAH)", LATITUDE "Latitude", LONGITUDE "Longitude", FACILITY_TYPE "Facility type", BUSINESS_ACTIVITY "Business activity", LICENSE_START_DATE "License Start Date", LICENSE_EXPIRY_DATE "License Expiry Date", LAST_LICENSE_RENEWAL_DATE "Last License renewal date", AREA_OF_THE_RETAIL_OUTLET "Area of the retail outlet", TENANCY_OWN_OR_RENTED "Tenancy (Own/Rented)" FROM ' + config.DB['input_schema'] + '.LICENSES_DATA WHERE LATITUDE NOT LIKE \'%،%\' AND LONGITUDE NOT LIKE \'%،%\' AND LONGITUDE NOT LIKE \'%\' || chr(10) ||\'%\' AND LATITUDE NOT LIKE \'%\' || chr(10) ||\'%\' AND LONGITUDE NOT LIKE \'%,%\' AND LATITUDE NOT LIKE \'%,%\''
#         self.licensesDf = self._executeQueryToDf(sqlQuery)
#         logging.info("CHECK:INPUT df: licensesDf {}".format(self.licensesDf.shape))

#     def getLicensesKeysData(self):
#         sqlQuery = f'SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, ACTIVITIE_TYPE_NAME, ACTIVITYNO AS "MOMTATHEL ACTIVITY NUMBER" FROM ' + config.DB['input_schema'] + '.MOMTHATEL_DATA'
#         self.licensesKeysDf = self._executeQueryToDf(sqlQuery)
#         self.licensesKeysDf.columns = self.licensesKeysDf.columns.str.upper()
#         logging.info("CHECK:INPUT df: licensesKeysDf {}".format(self.licensesKeysDf.shape))

#     def getWaterData(self):
#         self.waterDf = pd.read_excel(path.join(path.dirname(__file__), config.PATHS['water_data']))
#         self.waterDf['X']=self.waterDf['XY Google'].str.split(' |\t|,').str[0]
#         self.waterDf['Y']=self.waterDf['XY Google'].str.split(' |\t|,').str[1]

#         self.waterDf=self.waterDf[~self.waterDf['Y'].isna()]

#         self.waterDf=self.waterDf[self.waterDf['Water Agreement Status']=='Active']

#         self.waterDf[['X','Y']]=self.waterDf[['X','Y']].apply(pd.to_numeric, errors='coerce')
#         self.waterDf=self.waterDf[self.waterDf['X']<60]
#         self.waterDf=self.waterDf[self.waterDf['Y']<60]

#         self.waterDf[['new_X','new_Y']]=self.waterDf[['X','Y']].copy()

#         self.waterDf.loc[self.waterDf['Y']<32, 'new_X']=self.waterDf[self.waterDf['Y']<33]['Y']
#         self.waterDf.loc[self.waterDf['X']>33, 'new_Y']=self.waterDf[self.waterDf['X']>40]['X']

#         self.waterDf[['X','Y']]=self.waterDf[['new_X','new_Y']].copy()

#         self.waterDf = geopandas.GeoDataFrame(
#             self.waterDf, geometry = geopandas.points_from_xy( self.waterDf['Y'], self.waterDf['X'],), crs="EPSG:4326")


#     def getPopulationDataGdf(self):
#         self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data']))

#     def getPopulationDataGdf2(self):
#         self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data2']))


#     def getPriorityAreasData(self):
#         self.priorityAreas = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['priority_areas']))
#         self.priorityAreas = self.priorityAreas[self.priorityAreas['geometry'].notna()]
#         self.priorityAreas = self._fromPdToGdf(self.priorityAreas)
#         self.priorityAreas= self.priorityAreas.drop(columns=['Unnamed: 0'])


#     def getCrmCases(self):
#         sqlQuery = f'SELECT DISTINCT PYID "CaseId", INTERACTIONTYPE, PXCREATEDATETIME, CLOSURE_DATE, SHORT_STATUS, LATITUDE "LATITUDE", LONGITUDE, MAIN_CLASSIFICATION "MAIN_Classificaion", SUB_CLASSIFICATION "Sub_Classificaion", SP_CLASSIFICATION "SP_Classificaion", CATEGORY, PRIORITY "Priority_Value", SATISFACTION "Satisfaction" FROM ' + config.DB['input_schema'] + '.CRM_INSPECTION_CASES WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL'
#         # FETCH FIRST 2000 ROWS ONLY
#             #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
#         self.crmData = self._executeQueryToDf(sqlQuery)
#         self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
#             'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
#         #Dropping all the missing values present in Latitude and Longitude columns of the dataset
#         # self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
#         self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
#         self.crmData.Priority_Value.replace(
#             {'حرج' : 3, 'عالي': 3, 'متوسط' : 2, 'عادي' : 1, 'High' : 3, 'high' : 3,'medium' : 2,  'Medium' : 2,'Low' : 1, 'low' : 1}, 
#             inplace = True
#         )
#         logging.info("""CHECK:INPUT df: crmData {}
#         """.format(self.crmData.shape))

#     def getAmanaDataGdf(self):
#         self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))

#         if self.amanaCode is not None:
#             self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

#     def getAmanaPopulationOverlay(self):
#         if self.amanaPopulationOverlay is None:
#             self.amanaPopulationOverlay = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data3']))


#     def getAmanaPopulationOverlay2(self):
#         if self.populationData2 is None:
#             self.getPopulationDataGdf2()

#         if self.amanaDataGdf is None:
#             self.getAmanaDataGdf()

#         self.amanaPopulationOverlay2 = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
#         self.amanaPopulationOverlay2.drop(labels = self.amanaPopulationOverlay2.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)
#         self.amanaPopulationOverlay2.to_crs(epsg= 32637,inplace = True)

    
#     def _fromPdToGdf(self, data, x = True,y = True):
#         if 'geometry' in data.columns:
#             try:
#                 gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
#             except:
#                 data['geometry'] = data['geometry'].astype(str)
#                 data['geometry'] = data['geometry'].apply(wkt.loads)
#                 gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
                
#         else:
#              gpd = geopandas.GeoDataFrame(data,geometry = geopandas.points_from_xy(x,y), crs = 'epsg:4326')
#         #gpd.to_crs('epsg:32637',inplace = True)
#         return gpd 

#     def getClassConfig(self):
#         self.classConfig = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['class_config']))

#     def getCommonData(self):
#         self.getLicensesData()
#         self.getLicensesKeysData()
#         self.getInspectionsData()

#     def insertModelStatus(self, modelName : str, startTimestamp : str, status : str) -> str:
#         cursor = self.connection.connection.cursor()
#         id = cursor.var(cx_Oracle.NUMBER)
#         sqlQuery = 'INSERT INTO "C##ACIO".MLMODEL_DETAILS (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME) VALUES(:name, :status, :time) RETURNING ID into :id'
#         parameters= dict( name = modelName, status = status, time = startTimestamp, id = id)
#         cursor.execute(sqlQuery, parameters)
#         self.connection.connection.commit()
#         return str(id.getvalue()[0])
        
#     def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
#         sqlQuery = 'UPDATE "C##ACIO".MLMODEL_DETAILS SET MODEL_STATUS = :status, MODEL_END_TIME = :time WHERE ID = :id'
#         parameters= dict( status = status, time = endTimestamp, id = recordId)
#         self.connection.execute(sqlQuery, parameters)
        
#     def _executeQueryToDf(self, sqlQuery : str) -> pd.DataFrame:
#         result = self.connection.execute(sqlQuery)
#         columnsNames = result.keys()
#         result = result.fetchall()
#         return pd.DataFrame(result, columns = columnsNames)

#     def deleteFromTable(self, tableName : str):
#         tableName = tableName.upper()
#         sqlQuery = f'DELETE FROM "C##ACIO".{tableName}'

#         self.connection.execute(sqlQuery)
   
#     def getPoiDataGdf(self):
#         file_exists = path.exists(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois\pois_licenses_comparison.csv"))

#         if file_exists is False:
#             self.getAmanaPopulationOverlay()
#             self.getLicensesData()
#             pois1=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois1']))
#             pois2=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois2']))
        
#             self.poiData = generatePOIFile(self.amanaPopulationOverlay, self.licensesDf, pois1, pois2)
#             pois1 = None
#             pois2 = None
#         else:
#             self.poiData = pd.read_csv(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois\pois_licenses_comparison.csv"))
#             self.poiData.drop(columns=['Unnamed: 0'], inplace=True)

#     def saveRBDOutputLicenses(self, dataframe : pd.DataFrame, table_name : str):
#         dataframe.to_sql(name = table_name, con =  self.engine, schema = 'C##ACIO', if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : sql.types.CLOB, 'LONGITUDE' : sql.types.FLOAT, 'LATITUDE' : sql.types.FLOAT, 
#             'LIC_ID' : sql.types.BIGINT, 'HASNOLICENSE' : sql.types.BIGINT, 'START_POINT_X' : sql.types.Float,
#             'START_POINT_Y' : sql.types.Float }
#         )
    
#     def saveRBDOutput(self, dataframe : pd.DataFrame, table_name : str):

#         splits = []
#         batch_size = 10000
#         cursor = self.connection.connection.cursor()

#         for column in dataframe.columns:
#             if dataframe[column].dtype == object:
#                 dataframe[column].fillna('', inplace = True)
#             else:
#                 dataframe[column].replace({np.nan : None}, inplace = True)
        
#         columns = ','.join(list(dataframe.columns)) 
#         values = ','.join([':' + str(i + 1) for i in range(0, len(list(dataframe.columns)))])

#         insert_sql = 'INSERT INTO "C##ACIO"."{}" ({}) VALUES ({})'.format(table_name.upper(), columns, values)
#         for i in range(0, dataframe.shape[0], 10000):
#             splits.append(dataframe[i:i+batch_size])
        
#         for i in range(len(splits)):
            
#             df_row_list = [tuple(x) for x in splits[i].values.tolist()]

#             cursor.executemany(insert_sql, df_row_list, batcherrors=True)
#             self.connection.connection.commit()
#         cursor.close()


#     def getDrillingInspections(self):
#             sqlQuery = 'SELECT LIC_ID, INSPECTION_ID, TO_CHAR(INSPECTION_DATE, \'YYYY-MM-DD\') INSPECTION_DATE, CASE WHEN NUMBEROFFAILEDCLAUSES > 0 THEN 1 ELSE 0  END ISVIOLATION, STAGE_NAME "STAGENAME" FROM C##ACIO.DRILLING_INSPECTION_DATA WHERE PYSTATUSWORK IN (\'Pending-ReceiveSample\', \'Resolved-Completed\', \'Under Review and Approval\')'
#             self.drillingInspectionsDf = self._executeQueryToDf(sqlQuery)
#             self.drillingInspectionsDf.rename(
#                 inplace=True,
#                 columns={'lic_id' : 'LIC_ID', 'inspection_id' : 'INSPECTION_ID', 'inspection_date' : 'INSPECTION_DATE',
#                 'isviolation' : 'ISVIOLATION','stagename' : 'STAGENAME'},
#             )
#             # self.drillingInspectionsDf['INSPECTION_DATE']=self.drillingInspectionsDf['INSPECTION_DATE'].astype(str).str.split(' ').str[0]
#             self.drillingInspectionsDf['INSPECTION_DATE']=pd.to_datetime(self.drillingInspectionsDf['INSPECTION_DATE'])

#             dictionary = getStageNameDictionary(self.drillingInspectionsDf['STAGENAME'].dropna().unique())
#             self.drillingInspectionsDf=self.drillingInspectionsDf.replace({'STAGENAME':dictionary})
#             print()

#     def getDrillingLicenses(self):
#         sqlQuery = "\
#             SELECT LIC_ID LIC_ID, START_POINT_X, START_POINT_Y, AMANA, MUNICIPALITY , \"SUB-MUNICIPALITY\",\
#                 DISTRICT_ID, DISTRICT_NAME, DIGGING_START_DATE, DIGGING_END_DATE, DIGGING_METHOD_ID, WORK_NATURE_ID, \
#                 NETWORK_TYPE_ID, HEAVY_EQUIPMENT_PERMISSION, CAMPING_ROOM_COUNT, CONTRACTOR_CR, PATH_CODE, \
#                 \"LENGTH\", WIDTH, \"DEPTH\",  \
#                 CASE REQUEST_TYPE WHEN 'اصدار تصريح حفرية طارئة' THEN 1 ELSE 0 END AS EMERGENCY_LICENSE, \
#                 CASE DIGGING_CATEGORY WHEN 'الأولى' THEN 1 \
#                     WHEN 'الثانية' THEN 2 WHEN 'الثالثة' THEN 3 WHEN 'الرابعة' THEN 4 \
#                     WHEN 'الخامسة' THEN 5 WHEN 'السادسة' THEN 6 WHEN 'السابعة' THEN 7 \
#                     WHEN 'الثامنة' THEN 8 ELSE NULL END AS DIGGING_CATEGORY\
#             FROM C##ACIO.DRILLING_LICENSES_DATA \
#             ORDER BY LIC_ID ASC"
        
#         self.drillingLicensesDf = self._executeQueryToDf(sqlQuery)
#         self.drillingLicensesDf['lic_id'] = self.drillingLicensesDf['lic_id'].astype(str)
#         self.drillingLicensesDf.rename(columns={'lic_id' : 'LIC_ID', 'start_point_x' : 'START_POINT_X',
#             'start_point_y' : 'START_POINT_Y', 'depth':'DEPTH', 'path_code' : 'PATH_CODE', 
#             'width': 'WIDTH', 'length' : 'LENGTH', 'district_id' : 'DISTRICT_ID', 'district_name' : 'DISTRICT_NAME',
#             'amana' : 'AMMANA', 'municipality' : 'Municipality', 'SUB-MUNICIPALITY' : 'Sub-Municipality',
#             'emergency_license' : 'EMERGENCY_LICENSE', 'digging_end_date' : 'DIGGING_END_DATE', 'digging_start_date' : 'DIGGING_START_DATE',
#             'digging_category' : 'DIGGING_CATEGORY', 'contractor_cr' : 'CONTRACTOR_CR', 
#             'heavy_equipment_permission' : 'HEAVY_EQUIPMENT_PERMISSION', 'camping_room_count' :  'CAMPING_ROOM_COUNT',
#             'network_type_id' : 'NETWORK_TYPE_ID', 'digging_method_id' : 'DIGGING_METHOD_ID', 'work_nature_id' : 'WORK_NATURE_ID'
#             }
#             , inplace=True
#         )
    
#         print(self.drillingLicensesDf.head())

#     def getExcavationPhases(self):
#         self.excavationPhases = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['excavation_phases']))

#     def getExcavationData(self):
#         self.getDrillingInspections()
#         self.getDrillingLicenses()
