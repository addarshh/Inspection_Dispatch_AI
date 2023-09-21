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
        self.POINTS_OF_INTERESTS1 = None
        self.POINTS_OF_INTERESTS2 = None
  
    def _connect(self, connectionString : str):
        #print("1")
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        #print(lib_dir)
        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)
        except Exception as error:
            pass
            # print("Error handling cx_Oracle.init_oracle_client")
            # print(error)
            # sys.exit(1)

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

    def getPOINTS_OF_INTERESTS1(self):

        sqlQuery = f"SELECT * FROM %s.POINTS_OF_INTERESTS1" % config.DB['input_schema']
        self.POINTS_OF_INTERESTS1 = self._executeQuery(sqlQuery)
        print('self.POINTS_OF_INTERESTS1.shape')
        print(self.POINTS_OF_INTERESTS1.shape)

    def getPOINTS_OF_INTERESTS2(self):

        sqlQuery = f"SELECT * 	FROM %s.POINTS_OF_INTERESTS2" % config.DB['input_schema']
        self.POINTS_OF_INTERESTS2 = self._executeQuery(sqlQuery)
        print('self.POINTS_OF_INTERESTS2.shape')
        print(self.POINTS_OF_INTERESTS2.shape)

   
    def getCrmCases(self):
        sqlQuery_IN_CASES = f"SELECT c.\"CASE_ID_MOMRA\"  \"PYID\", \
            c.\"INTERACTION_TYPE\" INTERACTIONTYPE, \
            c.\"CREATE_DATE\" PXCREATEDATETIME,   \
            c.RESOLVE_DATE  CLOSURE_DATE, \
            c.WORK_STATUS SHORT_STATUS, \
            c.CASE_LATITUDE  LATITUDE , \
            c.CASE_LONGITUDE  LONGITUDE, \
            cmc.MAIN_CLASSIFICATION_LABEL_AR MAIN_Classificaion, \
            csc.SUB_CLASSIFICATION_LABEL_AR \"Sub_Classificaion\", \
            cl.SPL_CLASSIFICATION_LABEL_AR \"SP_Classificaion\", \
            c.CATEGORY  CATEGORY, \
            c.PRIORITY \"PRIORITY\", \
            c.EXTERNAL_CONTRACTOR AS \"IS_Contractor\",\
            c.RESOLUTION_SATISFIES_PETITIONER \"Satisfaction\", \
            CASE \
                WHEN vp.VP_TYPE_LABEL IS NULL\
                THEN 'NOT_VP'\
                WHEN VP.VP_TYPE_LABEL = 'SIDEWALKS'\
                THEN 'DILAPIDATED_SIDE_WALK'\
                ELSE vp.VP_TYPE_LABEL\
            END	 \"VISUAL POLLUTION CATEGORY\",\
            c.SUB_MUNICIPALITY_ID AS \"SUBMUNIC_3\"\
            FROM " + config.DB['input_schema'] + ".IN_CASES c  \
            LEFT JOIN " + config.DB['input_schema'] + ".CFG_MAIN_CLASSIFICATIONS cmc ON c.MAIN_CLASSIFICATION_ID  = cmc.MAIN_CLASSIFICATION_ID \
            LEFT JOIN " + config.DB['input_schema'] + ".CFG_SUB_CLASSIFICATIONS csc ON c.SUB_CLASSIFICATION_ID  = csc.SUB_CLASSIFICATION_ID  \
            LEFT JOIN " + config.DB['input_schema'] + ".CFG_SPL_CLASSIFICATIONS cl ON cl.SPL_CLASSIFICATION_ID = c.SPL_CLASSIFICATION_ID  \
            LEFT JOIN " + config.DB['input_schema'] + ".CFG_VP_TYPES vp ON vp.VP_TYPE_LABEL_AR = cl.SPL_CLASSIFICATION_LABEL_AR \
            WHERE c.CASE_LATITUDE IS NOT NULL \
            AND c.CASE_LONGITUDE IS NOT NULL  \
            AND  VP_TYPE_LABEL_AR IS NOT NULL \
            AND c.\"CREATE_DATE\" > ADD_MONTHS(SYSDATE, -( " + str(config.CRM_START_DATE) + "))"
        # FETCH FIRST 2000 ROWS ONLY
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        #print(sqlQuery_IN_CASES)
        self.crmData = self._executeQuery(sqlQuery_IN_CASES)
        self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
            'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
        #Dropping all the missing values present in Latitude and Longitude columns of the dataset
        # self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
        self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
        #print(self.crmData.head(5))
        self.crmData.priority.replace(
            {'حرج' : 3, 'عالي': 3, 'متوسط' : 2, 'عادي' : 1, 'High' : 3, 'high' : 3,'medium' : 2,  'Medium' : 2,'Low' : 1, 'low' : 1}, 
            inplace = True
        )
        print('self.crmData.shape')
        print(self.crmData.shape)
        #print(self.crmData.head(5))

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





        
