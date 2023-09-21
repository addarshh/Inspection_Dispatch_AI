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
import logging

def getPopulationData():
    # sqlQuery = \
    #     f"SELECT g.GRIDUNIQUECODE GridNumber,g.MUNICIPALITY MUNICIPA_1, g.AMANACODE AMANACODE, g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
    #     FROM  GISOWNER.GGMUNICIPALITYGRID g "
    # self.shpGrid = self._executeQuery(sqlQuery)
    # self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
    # self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
    # self.shpGrid.dn = self.shpGrid.dn.astype('float')
    # logging.info("shape of population data is :"+str(self.shpGrid.shape))
    # return self.shpGrid

    shpGrid=pd.read_csv(config.population_grids_path,dtype={"amanacode":"str","municipalitycode":"str","regioncode":"str"})
    #self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
    shpGrid = geopandas.GeoDataFrame(shpGrid , geometry=geopandas.GeoSeries.from_wkt(shpGrid.geometry, crs = 'epsg:4326'))
    shpGrid.columns  = map(str.upper, shpGrid.columns)
    shpGrid = shpGrid.rename(columns={'GEOMETRY':'geometry'})
    shpGrid = shpGrid.rename(columns={'GRIDUNIQUECODE':'GridNumber'})
    shpGrid = shpGrid.rename(columns={'GRIDNAME':'GridName','MUNICIPALITY':'MUNICIPALI', 'MUNICIPALITYCODE':'MUNICIPA_1' })
    print('self.shpGrid.shape')
    print(shpGrid.shape)
    return shpGrid

def get_grid_zone():
    # if not config.DB['instaclientpath'] in os.environ['PATH']:
    #     os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']

    # #conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    # conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
    # cursor = conn.cursor()
    # #sql_query = """SELECT * FROM "C##MOMRAH".INSPECTION_DATA"""
    
    # sql_query = """SELECT * FROM (SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE 
    #                     ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk
    #                     FROM "C##MOMRAH".GGGRIDINSPECTIONZONEST g WHERE g.INSPECTIONTYPE=1
    #                     ) WHERE RNK =1"""
    # #zone_data = self._executeQueryToDf(sqlQuery)
    # cursor.execute(sql_query)
    # rows = cursor.fetchall()
    # zone_data = pd.DataFrame(rows)
    # #zone_data = self._executeQuery(sqlQuery)
    # return zone_data

    grid_file =  path.join(config.GISPATH,'GGGRIDINSPECTIONZONEST.csv')
    GridZones = pd.read_csv(grid_file)
    GridZones.columns = map(str.upper, GridZones.columns)
    #self.GridZones.drop(columns=['RNK'], inplace=True)
    GridZones = GridZones.loc[GridZones['INSPECTIONTYPE'] == 7]
    logging.info("Function:priorityAreas df:GridZones Shape: {}".format(GridZones.shape))


    return GridZones

class GISDatabase(ABC):

    def __init__(self, amanaCode = None):
    
        # self.connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' \
        #     + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        
        # self.connection = None
        # self.MUNICIPALITY = None
        # self.SUBMUNICIPALITY = None
        # self.shpGrid = None
        # self.priorityAreas = None
        # self._connect()
        pass
 
    def _connect(self):
        try:
            print("3")
            engine = sql.create_engine(self.connectionString)
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
    

    def getPopulationData(self):
        sqlQuery = \
            f"SELECT g.GRIDUNIQUECODE GridNumber,g.MUNICIPALITY MUNICIPA_1, g.AMANACODE AMANACODE, g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
            FROM  GISOWNER.GGMUNICIPALITYGRID g "
        self.shpGrid = self._executeQuery(sqlQuery)
        self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
        self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
        self.shpGrid.dn = self.shpGrid.dn.astype('float')
        return self.shpGrid

    def getPriorityAreasData(self):
        sqlQuery = \
            """SELECT AREANAME, SDE.ST_ASTEXT(SHAPE) geometry FROM GISOWNER.VDPRIORITYAREAS """
        self.priorityAreas = self._executeQuery(sqlQuery)
        self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
        self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4269'))
        self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry'})
        self.priorityAreas = self.priorityAreas.rename(columns={'AREANAME': 'Name'})
        return self.priorityAreas
       
    def getMUNICIPALITY(self):
        sqlQuery = \
            f"SELECT  \
            r.OBJECTID OBJECTID, \
            r.AMANA_ID AMANACODE, \
            r.MUNICIPALITY_ID MUNICIPALI, \
            r.MUNICIPALITYNAME_AR MUNICIPA_1, \
            r.MUNICIPALITYNAME_EN MUNICIPA_2,  \
            SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
            SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN,  \
            SDE.ST_ASTEXT(r.SHAPE) geometry  \
            FROM GISOWNER.RAMUNICIPALITYS r  \
            "
        self.MUNICIPALITY = self._executeQuery(sqlQuery)
        self.MUNICIPALITY.geometry = self.MUNICIPALITY.geometry.astype('str')
        self.MUNICIPALITY = geopandas.GeoDataFrame(self.MUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.MUNICIPALITY.geometry, crs = 'epsg:4269'))
        print('self.MUNICIPALITY.shape')
        print(self.MUNICIPALITY.shape)
        print(self.priorityAreas.head(3))

    def getSUBMUNICIPALITY(self):
        sqlQuery = \
            f"SELECT  \
            r.OBJECTID OBJECTID, \
            r.AMANA_ID AMANACODE, \
            r.MUNICIPALITY_ID MUNICIPALI, \
            r.SUBMUNICIPALITYNAME_AR MUNICIPA_1,  \
            r.SUBMUNICIPALITYNAME_EN SUBMUNIC_1,  \
            r.SUBMUNICIPALITY_ID SUBMUNIC_3,  \
            SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
            SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN ,  \
            SDE.ST_ASTEXT(r.SHAPE) geometry  \
            FROM GISOWNER.RASUBMUNICIPALITYS r  \
            "
        self.SUBMUNICIPALITY = self._executeQuery(sqlQuery)
        self.SUBMUNICIPALITY.geometry = self.SUBMUNICIPALITY.geometry.astype('str')
        self.SUBMUNICIPALITY = geopandas.GeoDataFrame(self.SUBMUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.SUBMUNICIPALITY.geometry, crs = 'epsg:4269'))
        print('self.SUBMUNICIPALITY.shape')
        print(self.SUBMUNICIPALITY.shape)
        print(self.priorityAreas.head(3))

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





        
