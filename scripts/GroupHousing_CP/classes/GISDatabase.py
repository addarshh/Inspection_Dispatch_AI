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
        pass
        # self.connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' \
        #     + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        #
        # self.connection = None
        # self.shpGrid = None
        # self.AMANA = None
        # self.priorityAreas = None
        # self.POPULATION = None
        # self._connect()
  

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


    def getPriorityAreasData(self):

        # sqlQuery = \
        #     """SELECT AREANAME, SDE.ST_ASTEXT(SHAPE) geometry FROM GISOWNER.VDPRIORITYAREAS """
        # self.priorityAreas = self._executeQuery(sqlQuery)
        self.priorityAreas = pd.read_csv(config.priority_areas, dtype={"geometry": "str"})
        self.priorityAreas = self.priorityAreas[["areaname", "geometry"]]
        #self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
        self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas,
                                                    geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry,
                                                                                          crs='epsg:4269'))
        self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry'})
        self.priorityAreas = self.priorityAreas.rename(columns={'AREANAME': 'Name'})
        return self.priorityAreas


    def getAMANA(self):
        # sqlQuery = \
        #     f"SELECT b.OBJECTID, \
        #     b.REGION, \
        #     b.AMANACODE, \
        #     b.AMANAARNAME AMANAARNAM, \
        #     b.AMANAENAME, \
        #     SDE.ST_AREA(b.SHAPE) SHAPE_AREA	, \
        #     SDE.ST_LENGTH(b.SHAPE) SHAPE_LEN, \
        #     SDE.ST_ASTEXT(b.SHAPE) geometry \
        #     FROM GISOWNER.BBAMANABOUNDARYS b  \
        #     "
        # self.AMANA = self._executeQuery(sqlQuery)
        self.AMANA = pd.read_csv(config.amana_shp_path, dtype={"amanacode": "str"})
        self.AMANA.geometry = self.AMANA.geometry.astype('str')
        self.AMANA = geopandas.GeoDataFrame(self.AMANA , geometry=geopandas.GeoSeries.from_wkt(self.AMANA.geometry, crs = 'epsg:4326'))
        self.AMANA.columns  = map(str.upper, self.AMANA.columns)
        self.AMANA = self.AMANA.rename(columns={'GEOMETRY':'geometry'})
        print('self.AMANA.shape')
        print(self.AMANA.shape)
        return self.AMANA

    def getPOPULATION(self):
        # sqlQuery = \
        #     f"SELECT \
        #         g.GRIDUNIQUECODE, \
        #         g.AMANA, \
        #         g.AMANACODE, \
        #         g.GRIDNAME, \
        #         g.MUNICIPALITY, \
        #         g.MUNICIPALITYCODE, \
        #         g.REGION, \
        #         g.REGIONCODE, \
        #         g.GRID_ID, \
        #         g.DN, \
        #         SDE.ST_AREA(g.SHAPE) SHAPE_AREA	, \
        #         SDE.ST_LENGTH(g.SHAPE) SHAPE_LEN, \
        #         SDE.ST_ASTEXT(g.SHAPE) geometry \
        #         FROM GISOWNER.GGMUNICIPALITYGRID g where g.DN > 0 \
        #     "
        # self.POPULATION = self._executeQuery(sqlQuery)
        self.POPULATION = pd.read_csv(config.population_grids_path,
                                   dtype={"amanacode": "str", "municipalitycode": "str", "regioncode": "str"})
        self.POPULATION.geometry = self.POPULATION.geometry.astype('str')
        self.POPULATION = geopandas.GeoDataFrame(self.POPULATION , geometry=geopandas.GeoSeries.from_wkt(self.POPULATION.geometry, crs = 'epsg:4326'))
        self.POPULATION.columns  = map(str.upper, self.POPULATION.columns)
        self.POPULATION = self.POPULATION.rename(columns={'GEOMETRY':'geometry'})
        self.POPULATION = self.POPULATION.rename(columns={'GRIDUNIQUECODE':'GridUniqueCode'})
        self.POPULATION = self.POPULATION.rename(columns={'GRIDNAME':'GridName','MUNICIPALITY':'MUNICIPALI', 'MUNICIPALITYCODE':'MUNICIPA_1', 'GRID_ID': 'GridNumber' })

        print('self.POPULATION.shape')
        print(self.POPULATION.shape)
        return self.POPULATION
       
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





        
