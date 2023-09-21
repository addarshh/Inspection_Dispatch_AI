# import sys
# import config
# import geopandas
# import cx_Oracle
# import pandas as pd
# import os.path as path
# from shapely import wkt
# import sqlalchemy as sql
# import Helper
# from abc import ABC

# class GISDatabase(ABC):

#     def __init__(self, amanaCode = None):
    
#         # self.connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' \
#         #     + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        
#         # self.connection = None
#         # self.MUNICIPALITY = None
#         # self.SUBMUNICIPALITY = None
#         # self.shpGrid = None
#         # self.priorityAreas = None
#         # self._connect()
#         pass
 
#     def _connect(self):
#         try:
#             print("3")
#             engine = sql.create_engine(self.connectionString)
#             self.connection = engine.connect()
#         except Exception as error:
#             print("Error with creating connection")
#             print(error)
#             sys.exit(1)

#     def wkt_loads(self, x):
#         try:
#             return wkt.loads(x)
#         except Exception:
#             return None
    

#     def getPopulationData(self):
#         sqlQuery = \
#             f"SELECT g.GRIDUNIQUECODE GridNumber,g.MUNICIPALITY MUNICIPA_1, g.AMANACODE AMANACODE, g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
#             FROM  GISOWNER.GGMUNICIPALITYGRID g "
#         self.shpGrid = self._executeQuery(sqlQuery)
#         self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
#         self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
#         self.shpGrid.dn = self.shpGrid.dn.astype('float')
#         return self.shpGrid

#     def getPriorityAreasData(self):
#         # sqlQuery = \
#         #     """SELECT AREANAME, SDE.ST_ASTEXT(SHAPE) geometry FROM GISOWNER.VDPRIORITYAREAS """
#         # self.priorityAreas = self._executeQuery(sqlQuery)
#         # self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
#         # self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4269'))
#         # self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
#         # self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry'})
#         # self.priorityAreas = self.priorityAreas.rename(columns={'AREANAME': 'Name'})
#         # return self.priorityAreas
#         # population_file = path.join(config.GISPATH,'GGMUNICIPALITYGRID.csv')
#         #print(population_file)
#         self.priorityAreas=pd.read_csv(config.priority_areas, dtype={"geometry":"str"})
#         self.priorityAreas=self.priorityAreas[["areaname","geometry"]]
#         # self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
#         self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4269'))
#         self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
#         self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry'})
#         self.priorityAreas = self.priorityAreas.rename(columns={'AREANAME': 'Name'})
#         return self.priorityAreas
       
#     def getMUNICIPALITY(self):
#         sqlQuery = \
#             f"SELECT  \
#             r.OBJECTID OBJECTID, \
#             r.AMANA_ID AMANACODE, \
#             r.MUNICIPALITY_ID MUNICIPALI, \
#             r.MUNICIPALITYNAME_AR MUNICIPA_1, \
#             r.MUNICIPALITYNAME_EN MUNICIPA_2,  \
#             SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
#             SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN,  \
#             SDE.ST_ASTEXT(r.SHAPE) geometry  \
#             FROM GISOWNER.RAMUNICIPALITYS r  \
#             "
#         self.MUNICIPALITY = self._executeQuery(sqlQuery)
#         self.MUNICIPALITY.geometry = self.MUNICIPALITY.geometry.astype('str')
#         self.MUNICIPALITY = geopandas.GeoDataFrame(self.MUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.MUNICIPALITY.geometry, crs = 'epsg:4269'))
#         print('self.MUNICIPALITY.shape')
#         print(self.MUNICIPALITY.shape)
#         print(self.priorityAreas.head(3))

#     def getSUBMUNICIPALITY(self):
#         sqlQuery = \
#             f"SELECT  \
#             r.OBJECTID OBJECTID, \
#             r.AMANA_ID AMANACODE, \
#             r.MUNICIPALITY_ID MUNICIPALI, \
#             r.SUBMUNICIPALITYNAME_AR MUNICIPA_1,  \
#             r.SUBMUNICIPALITYNAME_EN SUBMUNIC_1,  \
#             r.SUBMUNICIPALITY_ID SUBMUNIC_3,  \
#             SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
#             SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN ,  \
#             SDE.ST_ASTEXT(r.SHAPE) geometry  \
#             FROM GISOWNER.RASUBMUNICIPALITYS r  \
#             "
#         self.SUBMUNICIPALITY = self._executeQuery(sqlQuery)
#         self.SUBMUNICIPALITY.geometry = self.SUBMUNICIPALITY.geometry.astype('str')
#         self.SUBMUNICIPALITY = geopandas.GeoDataFrame(self.SUBMUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(self.SUBMUNICIPALITY.geometry, crs = 'epsg:4269'))
#         print('self.SUBMUNICIPALITY.shape')
#         print(self.SUBMUNICIPALITY.shape)
#         print(self.priorityAreas.head(3))

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

#     def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
#         result = self.connection.execute(sqlQuery)
#         columnsNames = result.keys()
#         result = result.fetchall()
#         return pd.DataFrame(result, columns = columnsNames)





        
