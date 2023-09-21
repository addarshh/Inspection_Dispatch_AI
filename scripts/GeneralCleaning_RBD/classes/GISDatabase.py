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
        # self.MUNICIPALITY = None
        # self.SUBMUNICIPALITY = None
        # self._connect()

        self.GridZones = None
        self.shpGrid = None
        self.priorityAreas = None
        self.buildingdata = None
        self.usManholepData = None
 
    # def _connect(self):
    #     try:
    #         print("3")
    #         engine = sql.create_engine(self.connectionString)
    #         self.connection = engine.connect()
    #     except Exception as error:
    #         print("Error with creating connection")
    #         print(error)
    #         sys.exit(1)

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None
    

    def getPopulationData(self):
        # sqlQuery = \
        #     f"SELECT g.GRIDUNIQUECODE GridNumber,g.MUNICIPALITY MUNICIPA_1, g.AMANACODE AMANACODE, g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
        #     FROM  GISOWNER.GGMUNICIPALITYGRID g where g.DN > 0"
        # self.shpGrid = self._executeQuery(sqlQuery)
        # self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
        # self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
        # self.shpGrid.dn = self.shpGrid.dn.astype('float')
        # print('self.shpGrid.shape')
        # print(self.shpGrid.shape)
        # print(self.shpGrid.head(2))
        population_file = path.join(config.GISPATH,'GGINSPECTIONGRIDS.csv')
        #print(population_file)
        self.shpGrid = pd.read_csv(population_file, dtype ={'municipalitycode':'str','amanacode':'str'})
        self.shpGrid.columns = map(str.upper, self.shpGrid.columns)
        self.shpGrid = self.shpGrid.rename(columns={'GEOMETRY': 'geometry'})
        self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4326'))
        self.shpGrid = self.shpGrid.rename(columns={'GRIDUNIQUECODE':'GridNumber'})
        self.shpGrid = self.shpGrid.rename(columns={'GRIDNAME':'GridName','MUNICIPALITY':'MUNICIPALI', 'MUNICIPALITYCODE':'MUNICIPA_1' })

        print('self.shpGrid.shape')
        print(self.shpGrid.shape)
        print(self.shpGrid.head(2))
        return self.shpGrid


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
        # self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4269'))
        # print('self.priorityAreas.shape')
        # print(self.priorityAreas.shape)
        # print(self.priorityAreas.head(3))

        priority_file = path.join(config.GISPATH,'VDPRIORITYAREAS.csv')
        self.priorityAreas = pd.read_csv(priority_file)
        self.priorityAreas = geopandas.GeoDataFrame(self.priorityAreas , geometry=geopandas.GeoSeries.from_wkt(self.priorityAreas.geometry, crs = 'epsg:4326'))
        self.priorityAreas.columns = map(str.upper, self.priorityAreas.columns)
        self.priorityAreas = self.priorityAreas.rename(columns={'GEOMETRY': 'geometry','AREANAME': 'NAME'})

        print('self.priorityAreas.shape')
        print(self.priorityAreas.shape)
        print(self.priorityAreas.head(3))


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
        self.GridZones = self.GridZones.loc[self.GridZones['INSPECTIONTYPE'] == 10]
        print('self.GridZones.shape')
        print(self.GridZones.shape)
        print(self.GridZones.head(3))
        return self.GridZones

    def get_buildingdata(self):
        # self.cursor_gis.execute(
        # """SELECT OBJECTID, sde.st_astext(b.SHAPE) AS geometry FROM GISOWNER.BUBUILDINGFOOTPRINTS b where rownum<5""")
        # buildingdata = self.cursor_gis.fetchall()
        building_file =  path.join(config.GISPATH,'buBuildingFootPrintS.csv')
        self.buildingdata=pd.read_csv(building_file, dtype={"geometry":"str"})
        self.buildingdata=self.buildingdata[['objectid','geometry']]
        self.buildingdata = geopandas.GeoDataFrame(self.buildingdata , geometry=geopandas.GeoSeries.from_wkt(self.buildingdata.geometry, crs = 'epsg:4326'))
        self.buildingdata.columns=self.buildingdata.columns.str.upper()
        self.buildingdata = self.buildingdata.rename(columns={'GEOMETRY': 'geometry'})
        return self.buildingdata

    def get_usManholep(self):
        # self.cursor_gis.execute(
        # """SELECT OBJECTID, sde.st_astext(b.SHAPE) AS geometry FROM GISOWNER.BUBUILDINGFOOTPRINTS b where rownum<5""")
        # buildingdata = self.cursor_gis.fetchall()
        building_file =  path.join(config.GISPATH,'usManholeP.csv')
        self.usManholepData=pd.read_csv(building_file, dtype={"geometry":"str"})
        self.usManholepData=self.usManholepData[['objectid','geometry']]
        self.usManholepData = geopandas.GeoDataFrame(self.usManholepData , geometry=geopandas.GeoSeries.from_wkt(self.usManholepData.geometry, crs = 'epsg:4326'))
        self.usManholepData.columns=self.usManholepData.columns.str.upper()
        self.usManholepData = self.usManholepData.rename(columns={'GEOMETRY': 'geometry'})
        return self.usManholepData
   
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





        
