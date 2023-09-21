import sqlalchemy as sql
import cx_Oracle
import pandas as pd
import config
import geopandas
import os.path as path
from shapely import wkt
import sys 
import classes.engines.Helper as Helper
from abc import ABC
class Database(ABC):

    def __init__(self, amanaCode = None):
        print("1")

        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
            + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
    
        self.connection = None
        self._connect(self.connectionString)
        
        self.commercialLicenses = None
        self.constructionLicenses = None
        self.riskBasedEngineOutput = None
        self.streetsInspectorDemand = None
        self.inspectionsDf = None
        self.licensesDf = None
        self.crmData = None
        self.WEATHER_2021 = None
        self.WEATHER_2022 = None
        self.MUNICIPALITY = None
        self.class_config_df = None
  
    def _connect(self, connectionString : str):
        #print("1")
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        #print(lib_dir)
        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)
        except Exception as error:
            pass

        try:
            engine = sql.create_engine(connectionString)
            self.connection = engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            sys.exit(1)

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None


    def getInspectionsData(self):

        sqlQuery = \
            f"SELECT * FROM %s.INSPECTION_DATA_GROUPHOUSING" % config.DB['input_schema']
        self.inspectionsDf = self._executeQuery(sqlQuery)
        #self.licensesDf = self._fromPdToGdf(self.licensesDf, self.licensesDf.LONGITUDE, self.licensesDf.LATITUDE)
        print('self.inspectionsDf.shape')
        print(self.inspectionsDf.shape)
        #print(self.inspectionsDf.head(5))
        return self.inspectionsDf

    def getTranslatedMappingDocument(self):

        sqlQuery = \
            f"SELECT * FROM %s.TRANSLATED_MAPPING_DOC_ARABIC" % config.DB['input_schema']
        self.class_config_df = self._executeQuery(sqlQuery)
        self.class_config_df.columns  = map(str.lower, self.class_config_df.columns)
        print('self.class_config_df.shape')
        print(self.class_config_df.shape)
        return self.class_config_df



    def getLicensesData(self):
        sqlQuery = \
                f"SELECT \
                ORDER_NUMBER	\"Order Number\", \
                APPLICATION_DATE	\"Application Date\",\
                LICENSE_ID	\"LICENSE_ID\",\
                ISSUE_DATE	\"ISSUE_DATE\",\
                LICENSE_END_DATE	\"License_end_date\",\
                BENEFICIARY_ID	\"Beneficiary ID\",\
                SERVICE_TYPE	\"Service Type\",\
                BENEFICIARY	\"Beneficiary\",\
                APPLICANT	\"Applicant\",\
                APPLICANT_MOBILE	\"Applicant Mobile No\",\
                ORDER_STATUS	\"Order Status\",\
                MONITORING_STATUS	\"Monitoring Status\",\
                MUNICIPALITY	\"Municipality\",\
                AMANA	\"Amana\",\
                AREA	\"AREA\",\
                ESTIMATED_CAPACITY	\"ESTIMATED_CAPACITY\",\
                ROOMS_COUNT	\"ROOMS_COUNT\",\
                TOILETS_COUNT	\"TOILETS_COUNT\",\
                ACCOMODATION_TYPE	\"Accomodation Type\",\
                HR_PATH_TYPE	\"HR_Path_Type\",\
                ENG_OFFICE	\"Eng_office\",\
                X	\"X\",\
                Y	\"Y\",\
                PROPERTY_RATE	\"PROPERTY_RATE\"\
                FROM %s.LICENSES_DATA_GROUPHOUSING" % config.DB['input_schema']
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.licensesDf = self._executeQuery(sqlQuery)
        #self.licensesDf = self._fromPdToGdf(self.licensesDf, self.licensesDf.LONGITUDE, self.licensesDf.LATITUDE)
        print('self.licensesDf.shape')
        print(self.licensesDf.shape)
        #print(self.licensesDf.head(5))
        return self.licensesDf

   
    def getCrmCases(self):
       
        sqlQuery_IN_CASES ="""SELECT c.PYID "PYID", c.INTERACTIONTYPE "INTERACTIONTYPE", c.PXCREATEDATETIME "PXCREATEDATETIME",
            c.PYRESOLVEDTIMESTAMP "CLOSURE_DATE" , c.PYSTATUSWORK "SHORT_STATUS" , c.LATITUDE "LATITUDE",
            c.LONGITUDE "LONGITUDE", c.MAINCLASSIFICATION "MAIN_Classificaion", c.SPLCLASSIFICATION "SP_Classificaion",
            c.CATEGORY "Category", c.PRIORITY "PRIORITY", c.EXTERNALCONTRACTOR "IS_Contractor", c.RESOLUTIONSATISFYBYPETITIONER "Satisfaction",
            c.VISUAL_POLLUTION_CATEGORY "VISUAL POLLUTION CATEGORY", c.SUBMUNICIPALITYID "SUBMUNIC_3" FROM %s.CASES c WHERE c.PYSTATUSWORK <> 'Close'""" % config.DB['input_schema']
           
        # FETCH FIRST 2000 ROWS ONLY
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        print(sqlQuery_IN_CASES)
        self.crmData = self._executeQuery(sqlQuery_IN_CASES)
        self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
            'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
        
        #Dropping all the missing values present in Latitude and Longitude columns of the dataset

        print('self.crmData.shape')
        print(self.crmData.shape)
        return self.crmData

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

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)