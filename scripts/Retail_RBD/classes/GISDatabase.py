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

class GISDatabase(ABC):

    def __init__(self, amanaCode = None):
    
        # self.connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' \
        #     + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        
        # self.connection = None
        # self.MUNICIPALITY = None
        # self.SUBMUNICIPALITY = None
        # self._connect()

        self.shpGrid = None
        self.priorityAreas = None
        self.GridZones = None
 
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
        # sqlQuery = \
        #     f"SELECT \
        #         g.GRIDUNIQUECODE, \
        #         g.AMANA, \
        #         g.AMANACODE, \
        #         g.GRID_ID, \
        #         g.GRID_ID GridNumber, \
        #         g.GRIDNAME, \
        #         g.MUNICIPALITY, \
        #         g.MUNICIPALITYCODE, \
        #         g.REGION, \
        #         g.REGIONCODE, \
        #         g.DN, \
        #         SDE.ST_AREA(g.SHAPE) SHAPE_AREA	, \
        #         SDE.ST_LENGTH(g.SHAPE) SHAPE_LEN, \
        #         SDE.ST_ASTEXT(g.SHAPE) geometry \
        #         FROM GISOWNER.GGMUNICIPALITYGRID g where g.DN > 0 \
        #     "
        # self.shpGrid = self._executeQuery(sqlQuery)
        # self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
        # self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4326'))
        # self.shpGrid.dn = self.shpGrid.dn.astype('float')
        # self.shpGrid.columns = map(str.upper, self.shpGrid.columns)
        # self.shpGrid = self.shpGrid.rename(columns={'GEOMETRY': 'geometry'})
        # self.shpGrid = self.shpGrid.rename(columns={'GRIDUNIQUECODE': 'CHECK', 'GRIDNAME':'GridName','MUNICIPALITY': 'MUNICIPALI','MUNICIPALITYCODE':'MUNICIPA_1'})
        # self.shpGrid = self.shpGrid.rename(columns={'GRIDNUMBER': 'GridNumber'})

        population_file = path.join(config.GISPATH,'GGINSPECTIONGRIDS.csv')
        self.shpGrid = pd.read_csv(population_file, dtype ={'municipalitycode':'str','amanacode':'str'})
        self.shpGrid.columns = map(str.upper, self.shpGrid.columns)
        self.shpGrid = self.shpGrid.rename(columns={'GEOMETRY': 'geometry'})
        self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4326'))
        self.shpGrid = self.shpGrid.rename(columns={ 'GRIDNAME':'GridName','MUNICIPALITY': 'MUNICIPALI','MUNICIPALITYCODE':'MUNICIPA_1'})
        self.shpGrid = self.shpGrid.rename(columns={'GRIDNUMBER': 'GridNumber'})

        logging.info("Function:getPopulationData df:shpGrid Shape: {}".format(self.shpGrid.shape))
        return self.shpGrid

    def getPriorityAreasData(self):
        # sqlQuery = \
        #     """SELECT AREANAME, SDE.ST_ASTEXT(SHAPE) geometry FROM GISOWNER.VDPRIORITYAREAS """
        # self.priorityAreas = self._executeQuery(sqlQuery)
        # self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
        # self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4326'))
        # self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        # self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry'})

        priority_file = path.join(config.GISPATH,'VDPRIORITYAREAS.csv')
        self.priorityAreas = pd.read_csv(priority_file)
        self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4326'))
        self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry','AREANAME': 'NAME'})

        logging.info("Function:priorityAreas df:priorityAreas Shape: {}".format(self.priorityAreas.shape))


        return self.priorityAreas

    def getGridZones(self):
        # sqlQuery = \
        #     """SELECT * FROM 
        #         (
        #         SELECT g.GRIDUNIQUECODE, g.GUID , ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE 
        #         ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk
        #         FROM GISOWNER.GGGRIDINSPECTIONZONEST g 
        #         WHERE g.INSPECTIONTYPE=2
        #         ) WHERE RNK =1
        #     """
        # self.GridZones = self._executeQuery(sqlQuery)
        grid_file =  path.join(config.GISPATH,'GGGRIDINSPECTIONZONEST.csv')
        self.GridZones = pd.read_csv(grid_file)
        self.GridZones.columns = map(str.upper, self.GridZones.columns)
        #self.GridZones.drop(columns=['RNK'], inplace=True)
        self.GridZones = self.GridZones.loc[self.GridZones['INSPECTIONTYPE'] == 2]
        logging.info("Function:priorityAreas df:GridZones Shape: {}".format(self.GridZones.shape))


        return self.GridZones
    

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





        
