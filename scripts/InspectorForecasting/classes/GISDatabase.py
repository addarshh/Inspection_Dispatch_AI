import sys
import config
import geopandas
import cx_Oracle
import pandas as pd
import os.path as path
from shapely import wkt
import sqlalchemy as sql
import classes.engines.Helper as Helper
from abc import ABC

class GISDatabase(ABC):

    def __init__(self, amanaCode = None):
        # self.connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' \
        #     + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        
        # self.connection = None
        self.MUNICIPALITY = None
        self.SUBMUNICIPALITY = None
        self.priorityAreas = None
        # self.shpGrid = None
        # self._connect()
  

    # def _connect(self):
    #     try:
    #         print("3")
    #         engine = sql.create_engine(self.connectionString)
    #         self.connection = engine.connect()
    #     except Exception as error:
    #         print("Error with creating connection")
    #         print(error)
    #         sys.exit(1)

    # def wkt_loads(self, x):
    #     try:
    #         return wkt.loads(x)
    #     except Exception:
    #         return None

    

    ## *****Not used, currently using static POPULATION_GRID from the database ****** ##
    ## *******************************************************************************##
    

    # def getPopulationData(self):
    #     # sqlQuery = \
    #     #     f"SELECT g.GRIDUNIQUECODE GridNumber,g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
    #     #     FROM  GISOWNER.GGMUNICIPALITYGRID g"
    #     # self.shpGrid = self._executeQuery(sqlQuery)
    #     # self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
    #     # self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
    #     # self.shpGrid.dn = self.shpGrid.dn.astype('float')

    #     population_file = path.join(config.GISPATH,'GGMUNICIPALITYGRID.csv')
    #     #print(population_file)
    #     self.shpGrid = pd.read_csv(population_file, dtype ={'municipalitycode':'str'})
    #     self.shpGrid.columns = map(str.upper, self.shpGrid.columns)
    #     self.shpGrid = self.shpGrid.rename(columns={'GEOMETRY': 'geometry'})
    #     self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4326'))
    #     self.shpGrid.drop(columns=['GRID_ID.1'], inplace=True)
    #     self.shpGrid = self.shpGrid.rename(columns={ 'GRIDNAME':'GridName','MUNICIPALITY': 'MUNICIPALI','MUNICIPALITYCODE':'MUNICIPA_1'})
    #     self.shpGrid = self.shpGrid.rename(columns={'GRIDNUMBER': 'GridNumber'})
    #     print('self.shpGrid.shape')
    #     print(self.shpGrid.shape)
    #     print(self.shpGrid.head(2))


    def getPriorityAreasData(self):
        # sqlQuery = \
        #     f"SELECT OBJECTID	, \
        #     GLOBALID	, \
        #     AREANAME	, \
        #     AREA_ID	, \
        #     AREATYPE	, \
        #     PRIORITY	, \
        #     REGION_ID	, \
        #     AMANA_ID	, \
        #     GOVERNORATE_ID 	, \
        #     MUNICIPALITY_ID	, \
        #     CITY_ID	, \
        #     DISTRICT_ID	, \
        #     DATASOURCE	, \
        #     VDPRIORITYAREA_UNIFIED_ID	, \
        #     DATAOWNER	, \
        #     DESCRIPTION	, \
        #     CREATED_USER	, \
        #     CREATED_DATE	, \
        #     LAST_EDITED_USER	, \
        #     LAST_EDITED_DATE	, \
        #     AMANATRACKING	, \
        #     AMANABALADI	, \
        #     MUNICIPALITYBALADI	, \
        #     DISTRICTBALADI	, \
        #     SERVICETYPE	, \
        #     SDE.ST_AREA(SHAPE) SHAPE_AREA	, \
        #     SDE.ST_LENGTH(SHAPE) SHAPE_LEN	, \
        #     SDE.ST_ASTEXT(SHAPE) geometry \
        #     FROM GISOWNER.VDPRIORITYAREAS "
        # self.priorityAreas = self._executeQuery(sqlQuery)
        # self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
        # self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4326'))
        priority_file = path.join(config.GISPATH,'VDPRIORITYAREAS.csv')
        self.priorityAreas = pd.read_csv(priority_file)
        self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4326'))
        self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry','AREANAME': 'NAME'})
        print('self.priorityAreas.shape')
        print(self.priorityAreas.shape)
        print(self.priorityAreas.head(3))
       
    def getMUNICIPALITY(self):
        # sqlQuery = \
        #     f"SELECT  \
        #     r.AMANACODE AMANACODE, \
        #     r.MUNICIPALITYUNIQUECODE MUNICIPALI, \
        #     r.MUNICIPALITYARNAME MUNICIPA_1, \
        #     r.MUNICIPALITYENAME MUNICIPA_2,  \
        #     r.CATEGORY          CATEGORY, \
        #     SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
        #     SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN,  \
        #     SDE.ST_ASTEXT(r.SHAPE) geometry  \
        #     FROM GISOWNER.BBMUNICIBALITY940S r  \
        #     where r.CATEGORY = '1' \
        #     "
        # self.MUNICIPALITY = self._executeQuery(sqlQuery)
        # self.MUNICIPALITY.geometry = self.MUNICIPALITY.geometry.astype('str')
        # self.MUNICIPALITY = geopandas.GeoDataFrame(self.MUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.MUNICIPALITY.geometry, crs = 'epsg:4326'))
        # self.MUNICIPALITY = self.MUNICIPALITY.drop_duplicates()

        municipality_file = path.join(config.GISPATH,'MUNICIPALITY.csv')
        self.MUNICIPALITY = pd.read_csv(municipality_file,dtype = 'str')
        self.MUNICIPALITY = geopandas.GeoDataFrame(self.MUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.MUNICIPALITY.geometry, crs = 'epsg:4326'))
        self.MUNICIPALITY = self.MUNICIPALITY.drop_duplicates()

        print('self.MUNICIPALITY.shape')
        print(self.MUNICIPALITY.shape)
        #print(self.priorityAreas.head(3))

    def getSUBMUNICIPALITY(self):
        # sqlQuery = \
        #     f"SELECT  \
        #     r.AMANACODE AMANACODE, \
        #     substr(r.MUNICIPALITYUNIQUECODE, 0,6)  MUNICIPALI, \
        #     r.MUNICIPALITYARNAME MUNICIPA_1,  \
        #     r.MUNICIPALITYENAME SUBMUNIC_1,  \
        #     r.MUNICIPALITYUNIQUECODE SUBMUNIC_3,  \
        #     SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
        #     SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN ,  \
        #     SDE.ST_ASTEXT(r.SHAPE) geometry  \
        #     FROM GISOWNER.BBMUNICIBALITY940S r  \
        #     where r.CATEGORY = '2' \
        #     "
        # self.SUBMUNICIPALITY = self._executeQuery(sqlQuery)
        # self.SUBMUNICIPALITY.geometry = self.SUBMUNICIPALITY.geometry.astype('str')
        # self.SUBMUNICIPALITY = geopandas.GeoDataFrame(self.SUBMUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.SUBMUNICIPALITY.geometry, crs = 'epsg:4326'))
        # self.SUBMUNICIPALITY = self.SUBMUNICIPALITY.drop_duplicates()

        submunicipality_file = path.join(config.GISPATH,'SUBMUNICIPALITY.csv')
        self.SUBMUNICIPALITY = pd.read_csv(submunicipality_file,dtype = 'str')
        self.SUBMUNICIPALITY = geopandas.GeoDataFrame(self.SUBMUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.SUBMUNICIPALITY.geometry, crs = 'epsg:4326'))
        self.SUBMUNICIPALITY = self.SUBMUNICIPALITY.drop_duplicates()

        print('self.SUBMUNICIPALITY.shape')
        print(self.SUBMUNICIPALITY.shape)

    # def _fromPdToGdf(self, data, x = True,y = True):
    #     if 'geometry' in data.columns:
    #         try:
    #             gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
    #         except:
    #             data['geometry'] = data['geometry'].astype(str)
    #             data['geometry'] = data['geometry'].apply(wkt.loads)
    #             gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
                
    #     else:
    #          gpd = geopandas.GeoDataFrame(data,geometry = geopandas.points_from_xy(x,y), crs = 'epsg:4326')
    #     #gpd.to_crs('epsg:32637',inplace = True)
    #     return gpd 

    # def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
    #     result = self.connection.execute(sqlQuery)
    #     columnsNames = result.keys()
    #     result = result.fetchall()
    #     return pd.DataFrame(result, columns = columnsNames)





        
