#Importing libraries without acronyms
import os
import glob
import json
import math
import plotly
import shutil
#import chardet
import warnings
# import matplotlib
import cx_Oracle
import datetime
import config  # username / password
import logging
import cx_Oracle

#Importing libraries with acronyms
import numpy as np
import pandas as pd
import geopandas as gpd
import geopandas as geopandas
import plotly.express as px
# import matplotlib.pyplot as plt
import plotly.graph_objects as go
import classes.Database as DB
# import matplotlib.image  as mpimg
import classes.engines.Helper as Helper


#Importing Specific Functions and Modules
from shapely import wkt
from datetime import date
from datetime import datetime
from sklearn import linear_model
from shapely import geometry, ops
from geopandas.tools import sjoin
from shapely.geometry import shape
# from matplotlib.pyplot import figure
from sklearn.metrics import r2_score
# from hijri_converter import Hijri, Gregorian
from sklearn.metrics import mean_absolute_error
# from statsmodels.tsa.arima.model import ARIMA
from statistics import mean
from xgboost import XGBRegressor
from pandas.api.types import is_numeric_dtype, is_string_dtype
# from statsmodels.tsa.api import ExponentialSmoothing,SimpleExpSmoothing, Holt
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_percentage_error
#from classes.engines.Helper import  AddMonths,insert_df, Engine_End_Metadata_Update, backup,insert_df_Batchwise
from classes.engines.Inspection import Inspection
from classes.GISDatabase import GISDatabase
from classes.Database import Database

# Formatting and Set up
#warnings.filterwarnings('ignore')
#%matplotlib inline
#sns.set_style(style = 'whitegrid')
#pd.set_option('display.max_columns',None)
# Setting some configurations
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
warnings.filterwarnings("ignore")

class GeneralCleaningRBD(Inspection):


		def __init__(self, data : Database, gisdata : GISDatabase):
			super().__init__(data,gisdata)
			logging.info("InspectionRBD Initialized")
			self.features = {}
			self.filesDumpTime = datetime.now()

		def PrepareData(self, Helper : Helper):

			#dsn_tns = cx_Oracle.makedsn('10.80.122.102','1521',service_name='ORCLCDB')
			#conn = cx_Oracle.connect(user=r'SYSTEM',password='UN8GbDKcQV',dsn=dsn_tns)
			cursor_targetDB = Helper.conn_targetDB.cursor()

			# ### Data Preprocessing

			# ***Base File Path for Shape files***
			# base_file_path = r'C:\Environment\MOMRAH_WORKING\7. CV expansion\Data'

			# # **1) Population grids**
			# #Load updated Shapefiles of the population data with Grid Number
			# shpGrid = geopandas.read_file(r'C:\Environment\MOMRAH_WORKING\7. CV expansion\Data\standardized_grids_population_municipality_20221010\standardized_grids_population_municipality_20221010.shp')
			# shpGrid.head(3)
			shpGrid = self.gisdata.shpGrid
			shpGrid.columns  = map(str.upper, shpGrid.columns)
			shpGrid = shpGrid.rename(columns={'GEOMETRY':'geometry'})
			print(shpGrid.head(2))
			logging.info("shpGrid.shape")
			logging.info(shpGrid.shape)

			#Overlay the population grid layer on the Madinah layer
			Pop_Grids = shpGrid.copy()
			Pop_Grids = Pop_Grids.rename(columns={'GRIDNUMBER':'GridNumber'})
			Pop_Grids['GridNumber']= Pop_Grids['GridNumber'].astype(str)


			# <font color = 'maroon'>***Check #1: One Grid in multiple Amanas***</font>
			print(Pop_Grids.shape)
			print(Pop_Grids.GridNumber.nunique())
			#Creating the base data with grid numbers and the corresponding geometries
			base_data = Pop_Grids[['AMANA', 'AMANACODE','MUNICIPALI', 'MUNICIPA_1','GridNumber','geometry','DN']].copy()
			base_data.drop_duplicates(inplace=True)

			#Calculating the index of population feature
			pop_per_grid = base_data['DN'].sum()/len(base_data)
			base_data['Index of Population'] = base_data['DN']/pop_per_grid
			logging.info("base_data.shape")
			logging.info(base_data.shape)

			# # CRM data cleaning
			CRM_full = self.data.crmData
			CRM_full.columns  = map(str.upper, CRM_full.columns)
			print(CRM_full['PXCREATEDATETIME'].min())
			print(CRM_full['PXCREATEDATETIME'].max())

			# ***Filter CRM data for latest 6 months***
			CRM_6mths = CRM_full.loc[CRM_full['PXCREATEDATETIME'] > pd.to_datetime('now')-pd.DateOffset(months=6)]
			logging.info("CRM_6mths.shape")
			logging.info(CRM_6mths.shape)
			#CRM_6mths.shape
			print(CRM_6mths['PXCREATEDATETIME'].min())
			print(CRM_6mths['PXCREATEDATETIME'].max())
			#CRM data cleaning (available: sep'18 to sep'22  selected:latest  6months)
			print('CRM_6mths')
			print(CRM_6mths.head(2))
			GDF_VP_cases = gpd.GeoDataFrame(CRM_6mths,geometry=gpd.points_from_xy(CRM_6mths['LONGITUDE'],CRM_6mths['LATITUDE']),crs='epsg:4326')
			logging.info("GDF_VP_cases.shape")
			logging.info(GDF_VP_cases.shape)
			print('GDF_VP_cases')
			print(GDF_VP_cases.head(2))
			GenCleaning_crm = GDF_VP_cases.loc[(GDF_VP_cases['VISUAL POLLUTION CATEGORY'] == "Public waste / trash bins")]
			logging.info("GenCleaning_crm.shape")
			logging.info(GenCleaning_crm.shape)
			if(GenCleaning_crm.shape[0] ==0):
				#print("GenCleaning_crm is empty, Please check DB")
				logging.error("GenCleaning_crm is empty, Please check DB")
				raise Exception("GenCleaning_crm is empty")
			print("GenCleaning_crm")
			print(GenCleaning_crm.head(2))

			#Extracting Historical General Cleaning VP cases feature
			Historical_Cases = GenCleaning_crm[['PYID','LONGITUDE','LATITUDE']].copy()
			Historical_Cases.drop_duplicates(inplace=True)
			GDF_Cases = geopandas.GeoDataFrame(Historical_Cases,geometry=geopandas.points_from_xy(Historical_Cases.LONGITUDE,Historical_Cases.LATITUDE),crs='epsg:4326')
			logging.info("GDF_Cases.shape")
			logging.info(GDF_Cases.shape)

			#Joining the base data with the cases to get the VP cases associated with each grid
			Cases_join = geopandas.sjoin(base_data,GDF_Cases,how="left",op="intersects")
			logging.info("Cases_join.shape")
			logging.info(Cases_join.shape)

			#Calculating number of cases per grid
			count = Cases_join.groupby(['GridNumber']).PYID.nunique()
			CountDf = count.to_frame().reset_index()
			CountDf.columns.values[1] = "Count of VP Cases"

			#merging base data with 'number of cases' feature
			base_data1 = base_data.copy()
			base_data1 = pd.merge(base_data1,CountDf,on='GridNumber')
			logging.info("base_data1.shape")
			logging.info(base_data1.shape)

			#Calculating Index
			average_cases=CountDf['Count of VP Cases'].sum()/len(base_data1)
			base_data1['Average VP Cases'] = average_cases
			base_data1['Historic_GC_Cases_index']=base_data1['Count of VP Cases']/base_data1['Average VP Cases']
			base_data1['Historic_GC_Cases_index'].fillna(0,inplace=True)
			logging.info("base_data1.shape")
			logging.info(base_data1.shape)


			# **3)Landuse**
			#Import OSM analysis output to categorize the areas into commercial, residential, industrial and forest 
			cwd = os.path.dirname(__file__)
			base_dir = os.path.join(cwd,'../../../Data')
			target_dir = os.path.join(base_dir,'Landuse')
			shpRegionFile = os.path.join(target_dir,'Region_Desc.shp')
			shpRegions = geopandas.read_file(shpRegionFile)
			print('shpRegions')
			print(shpRegions.head(2))
			logging.info("shpRegions.shape")
			logging.info(shpRegions.shape)


			#LandUse Feature
			#Classification of Grids by Type of Use
			LandUse = shpRegions[['landuse','geometry']]

			#joining landuse with the grid dataset
			LandUse_join = geopandas.sjoin(base_data,LandUse,how="left",op="intersects")
			logging.info("LandUse_join.shape")
			logging.info(LandUse_join.shape)

			LandUse_join.drop('index_right',axis=1,inplace=True)
			LandUse_join.drop_duplicates(inplace=True)
			logging.info("After droppping duplicated LandUse_join.shape")
			logging.info(LandUse_join.shape)

			#converting landuse types to numbers using the precedence/importance heuristic
			#creating precedence for land use type
			data = [['commercial',1],['residential',2],['industrial',3],['forest',4]]
			order_landuse = pd.DataFrame(data, columns=['landuse','Precedence'])
			logging.info("order_landuse.shape")
			logging.info(order_landuse.shape)

			#joining the precedence rank with land use data
			LandUse_join1 = pd.merge(LandUse_join,order_landuse,how='left',on='landuse')
			logging.info("LandUse_join1.shape")
			logging.info(LandUse_join1.shape)
			

			LandUse_Distinct = LandUse_join1[['GridNumber','landuse','Precedence']]

			#Sort the Grids using the precedence scores and keep only the first record per grid
			LandUse_Distinct=LandUse_Distinct.sort_values(by=['GridNumber','Precedence'],ascending=[True,True],na_position='last')
			LandUse_Distinct.drop_duplicates(subset=['GridNumber'], keep='first', inplace=True)
			LandUse_Distinct.drop('Precedence',axis=1,inplace=True)
			logging.info("LandUse_Distinct.shape")
			logging.info(LandUse_Distinct.shape)
			

			#joining the landuse type with the base data file
			base_data2 = base_data1.copy()
			base_data2 = pd.merge(base_data2,LandUse_Distinct,on='GridNumber')
			logging.info("base_data2.shape")
			logging.info(base_data2.shape)


			base_data2['landuse'].fillna(0,inplace=True)
			base_data2['landuse'] = base_data2['landuse'].replace(['commercial'], 4)
			base_data2['landuse'] = base_data2['landuse'].replace(['residential'], 3)
			base_data2['landuse'] = base_data2['landuse'].replace(['industrial'], 2)
			base_data2['landuse'] = base_data2['landuse'].replace(['forest'], 1)


			# **3)POI**
			#POIs
			# POI_1 = pd.read_excel(r'C:\Environment\MOMRAH_WORKING\7. CV expansion\Data\POI\20220807092142e65d (1).xlsx')
			# POI_2 = pd.read_excel(r'C:\Environment\MOMRAH_WORKING\7. CV expansion\Data\POI\20220808121409a756.xlsx')
			POI_1 = self.data.POINTS_OF_INTERESTS1
			POI_2 = self.data.POINTS_OF_INTERESTS2

			List_of_POIs = POI_1.append(POI_2)
			print(List_of_POIs.head(2))
			logging.info("List_of_POIs.shape")
			logging.info(List_of_POIs.shape)

			#Converting the POIs data into a geodataframe so that it can be merged with base data
			List_of_POIs.drop_duplicates(inplace= True)

			GDF_POIs = geopandas.GeoDataFrame(List_of_POIs,geometry=geopandas.points_from_xy(List_of_POIs.longitude,List_of_POIs.latitude),crs='epsg:4326')
			logging.info("GDF_POIs.shape")
			logging.info(GDF_POIs.shape)
			POI_join = geopandas.sjoin(base_data,GDF_POIs,how="left",op="intersects")
			logging.info("POI_join.shape")
			logging.info(POI_join.shape)


			#logic to count number of POIs by grid
			count = POI_join.groupby(['GridNumber'])['place_id'].count()
			CountDf = count.to_frame().reset_index()
			CountDf.columns.values[1] = "Count of POIs"
			logging.info("CountDf.shape")
			logging.info(CountDf.shape)

			#joining the number of POIs at grid level feature with the base data
			base_data3 = base_data2.copy()
			base_data3 = pd.merge(base_data3,CountDf,on='GridNumber')


			base_data3['Count of POIs'].fillna(0,inplace=True)


			#Calculating Index
			average_cases=CountDf['Count of POIs'].sum()/len(base_data3)
			base_data3['Average POI'] = average_cases
			base_data3['POI_index']=base_data3['Count of POIs']/base_data3['Average POI']
			base_data3['POI_index'].fillna(0,inplace=True)
			base_data3['POI_index']=round(base_data3['POI_index'],2)
			logging.info("base_data3.shape")
			logging.info(base_data3.shape)


			# <font color = 'maroon'>***Check #2:POI data only available for Medina***</font>

			result = base_data3.groupby(['AMANACODE'])['Count of POIs'].agg('sum').reset_index()
			result['sum']=result['Count of POIs'].sum()
			result['%']=(result['Count of POIs']/result['sum'])*100

			#result


			# **4)Priority Areas: Combine mutiple csv files for Medina and shape files for all other regions to create one consolidated file**

			#Priority Areas
			# PRIORITYAREAS_shp = r"C:\Environment\VM_Files\MOMRAH_WORKING\7. CV expansion\99. Misc\VP_Plan_Analysis\Data\Priority Areas"

			# file_names = []
			# for file in glob.glob(PRIORITYAREAS_shp+r"\*.shp"):
			# 	file_names.append(file)

			# gpd_aggregated = pd.DataFrame()

			# for file in file_names:
			# 	temp = gpd.read_file(file,  crs='epsg:32637')
			# 	temp['file_name']=file
			# 	gpd_aggregated = gpd_aggregated.append(temp)

			# #Medina CSv Files
			# csv_files = []
			# for file in glob.glob(PRIORITYAREAS_shp+r"\*.csv"):
			# 	csv_files.append(file)

			# medina_areas = pd.read_csv(csv_files[0])
			# medina_areas['geometry'] = medina_areas['geometry'].apply(wkt.loads)
			# medina_areas = gpd.GeoDataFrame(medina_areas, crs = 'EPSG:4326')
			# medina_areas['file_name'] ='Medina.csv'
			# medina_areas['PRIORITY'] =1

			# gpd_aggregated = gpd_aggregated.append(medina_areas)
			# gpd_aggregated = gpd_aggregated.reset_index(drop=True)
			# gpd_aggregated = gpd_aggregated.to_crs('epsg:4326')

			
			self.gisdata.priorityAreas.columns  = map(str.upper, self.gisdata.priorityAreas.columns)
			self.gisdata.priorityAreas = self.gisdata.priorityAreas.rename(columns={'GEOMETRY':'geometry'})
			gpd_aggregated = self.gisdata.priorityAreas
			PriorityAreas = gpd_aggregated
			print(PriorityAreas.head(2))
			PriorityAreas.to_crs(epsg=32637, inplace=True)
			logging.info("PriorityAreas.shape")
			logging.info(PriorityAreas.shape)

			#Mapping priority areas
			base_copy = base_data.copy()
			base_copy.to_crs(epsg=32637,inplace=True)

			PriorityAreas = PriorityAreas[(PriorityAreas.PRIORITY == 1)]
			logging.info("PriorityAreas.shape where PRIORITY == 1")
			logging.info(PriorityAreas.shape)

			Grid_priority_areas = geopandas.sjoin(base_copy,PriorityAreas,how="inner",op="intersects")
			logging.info("Grid_priority_areas.shape")
			logging.info(Grid_priority_areas.shape)

			Areas_per_grid = Grid_priority_areas.groupby(['GridNumber']).index_right.nunique()
			Grid_priority_AreasDF = Areas_per_grid.to_frame().reset_index()
			Grid_priority_AreasDF.columns.values[1] = "Number of priority Areas"
			Grid_priority_AreasDF['Number of priority Areas'].fillna(0,inplace=True)
			logging.info("Grid_priority_AreasDF.shape")
			logging.info(Grid_priority_AreasDF.shape)

			#merging both priority areas count back to base data
			base_data4=pd.merge(base_data3,Grid_priority_AreasDF,on='GridNumber',how='left')
			logging.info("base_data4.shape")
			logging.info(base_data4.shape)

			#Calculating Index
			average_cases=base_data4['Number of priority Areas'].sum()/len(base_data4)
			logging.info("average_cases.shape")
			logging.info(average_cases.shape)

			base_data4['Average Priority areas'] = average_cases
			base_data4['Priority_Areas_Index']=base_data4['Number of priority Areas']/base_data4['Average Priority areas']
			base_data4['Priority_Areas_Index'].fillna(0,inplace=True)
			base_data4['Priority_Areas_Index']=round(base_data4['Priority_Areas_Index'],2)
			logging.info("base_data4.shape")
			logging.info(base_data4.shape)



			# **5)Sewer Manholes**

			#importing sewer data
			# import sqlalchemy as sql
			# import cx_Oracle

			# connectionString = 'oracle+cx_oracle://' + "USER_AELSAADI" + ':' + "SAADI2030" + '@'             + cx_Oracle.makedsn("ruhmsv-ora19c-scan", 1521, service_name = "SDIGIS")
			# engine = sql.create_engine(connectionString)
			# connection = engine.connect() 
			# cursor_gis = connection.connection.cursor()

			# cursor_gis.execute('''
			# SELECT OBJECTID, sde.st_astext(u.SHAPE) as geometry FROM GISOWNER.USMANHOLEP u 
			# ''')
			# sewer_data = cursor_gis.fetchall()
			# sewer_data = gpd.GeoDataFrame.from_records(sewer_data, 
			# 											columns=[x[0] for x in cursor_gis.description])
			# sewer_data.shape
			sewer_data = self.gisdata.usManholepData
			print('Sewer Data parsed\n')

			#Sewer data

			# # #mapping sewer manholes data to grid data
			base_copy = base_data4.copy()
			base_copy.to_crs(epsg=32637,inplace=True)

			sewer_mapping = geopandas.sjoin(sewer_data,base_copy,how="inner",op="intersects")
			logging.info("sewer_mapping.shape")
			logging.info(sewer_mapping.shape)


			sewer_count = sewer_mapping.groupby(['GridNumber']).OBJECTID.nunique()
			sewer_countDF = sewer_count.to_frame().reset_index()
			sewer_countDF.columns.values[1] = "Count of Sewer Manholes"
			base_data5 = pd.merge(base_copy,sewer_countDF,how='left',on='GridNumber')
			base_data5['Count of Sewer Manholes'].fillna(0,inplace=True)
			logging.info("base_data5.shape")
			logging.info(base_data5.shape)

			#Calculating Index
			average_cases=base_data5['Count of Sewer Manholes'].sum()/len(base_data5)
			base_data5['Average Sewer Manholes'] = average_cases
			base_data5['Sewer_Manholes_Index']=base_data5['Count of Sewer Manholes']/base_data5['Average Sewer Manholes']
			base_data5['Sewer_Manholes_Index'].fillna(0,inplace=True)
			base_data5['Sewer_Manholes_Index']=round(base_data5['Sewer_Manholes_Index'],2)
			logging.info("base_data5.shape")
			logging.info(base_data5.shape)


			# <font color = 'maroon'>***Check #3:Sewer Manholes only available for Medina***</font>

			result = base_data5.groupby(['AMANACODE'])['Count of Sewer Manholes'].agg('sum').reset_index()
			result['sum']=result['Count of Sewer Manholes'].sum()
			result['%']=(result['Count of Sewer Manholes']/result['sum'])*100
			logging.info("Count of Sewer Manholes result.shape")
			logging.info(result.shape)
			#result


			# **6)Buildings**

			# cursor_gis.execute('''
			# SELECT OBJECTID, sde.st_astext(b.SHAPE) AS geometry FROM GISOWNER.BUBUILDINGFOOTPRINTS b
			# ''')
			# building_data = cursor_gis.fetchall()
			# building_data = gpd.GeoDataFrame.from_records(building_data, 
			# 											columns=[x[0] for x in cursor_gis.description])
			# building_data.shape
			# print('Building Data parsed\n')

			# building_data = building_data.rename(columns={"GEOMETRY":'geometry'})

			# # Converting the dataframe into a geopandas dataframe
			# buildingdata = building_data[building_data['geometry'].notna()]
			# buildingdata['geometry'] = buildingdata['geometry'].astype(str)
			# buildingdata['geometry'] = buildingdata['geometry'].apply(wkt.loads)
			# building_data = gpd.GeoDataFrame(buildingdata, geometry='geometry',crs='epsg:4326')
			building_data = self.gisdata.buildingdata

			#building_data

			#Buildings (latest)
			buildings_mapping = geopandas.sjoin(base_copy,building_data,how="left",op="intersects")
			Buildings_per_grid = buildings_mapping.groupby(['GridNumber']).OBJECTID.nunique()
			Buildings_per_gridDF = Buildings_per_grid.to_frame().reset_index()
			Buildings_per_gridDF.columns.values[1] = "Number of buildings"
			Buildings_per_gridDF['Number of buildings'].fillna(0,inplace=True)
			logging.info("Buildings_per_gridDF.shape")
			logging.info(Buildings_per_gridDF.shape)

			base_data6=pd.merge(base_data5,Buildings_per_gridDF,on='GridNumber',how='left')

			#Calculating Index
			average_cases=base_data6['Number of buildings'].sum()/len(base_data6)
			base_data6['Average Buildings'] = average_cases
			base_data6['Buildings_Index']=base_data6['Number of buildings']/base_data6['Average Buildings']
			base_data6['Buildings_Index'].fillna(0,inplace=True)
			base_data6['Buildings_Index']=round(base_data6['Buildings_Index'],2)
			logging.info("Number of buildings base_data6.shape")
			logging.info(base_data6.shape)


			# <font color = 'maroon'>***Check #4:Buildings only available for Medina***</font>

			result = base_data6.groupby(['AMANACODE'])['Number of buildings'].agg('sum').reset_index()
			result['sum']=result['Number of buildings'].sum()
			result['%']=(result['Number of buildings']/result['sum'])*100
			logging.info("Number of buildings result.shape")
			logging.info(result.shape)
			#result


			# **7)Excavation Licenses**

			cursor_targetDB.execute('''SELECT * FROM %s.LICENSES_DATA_EXCAVATIONS''' % config.DB['input_schema'])
			dfExcavationLicense = cursor_targetDB.fetchall()
			dfExcavationLicense = gpd.GeoDataFrame.from_records(dfExcavationLicense, 
														columns=[x[0] for x in cursor_targetDB.description])
			dfExcavationLicense2 = dfExcavationLicense.groupby(['LICENCES_ID','LICENSE_START_DATE','LICENSE_EXPIRY_DATE','DIGGING_START_DATE','DIGGING_END_DATE'])['LATITUDE','LONGITUDE'].agg('max').reset_index()
			


			# ***Filter for active excavation licences for latest 6 months***
			dfExcavationLicense_Active = dfExcavationLicense2.loc[dfExcavationLicense2['LICENSE_EXPIRY_DATE'] > pd.to_datetime('now')]

			dfExcavationLicense_6mths = dfExcavationLicense_Active.loc[(dfExcavationLicense_Active['DIGGING_END_DATE'] > pd.to_datetime('now')-pd.DateOffset(months=6))
																	&(dfExcavationLicense_Active['DIGGING_START_DATE'] <= pd.to_datetime('now'))]


			Excavation_gdf = geopandas.GeoDataFrame(dfExcavationLicense_6mths,geometry=geopandas.points_from_xy(dfExcavationLicense_6mths.LONGITUDE,dfExcavationLicense_6mths.LATITUDE),crs='epsg:4326')

			Excavation_gdf = Excavation_gdf[Excavation_gdf['geometry'].notna()]
			Excavation_gdf['geometry'] = Excavation_gdf['geometry'].astype(str)
			Excavation_gdf['geometry'] = Excavation_gdf['geometry'].apply(wkt.loads)
			Excavation_gdf = geopandas.GeoDataFrame(Excavation_gdf, geometry='geometry',crs='epsg:4326')

			Excavation_gdf.to_crs(epsg=32637, inplace=True)
			logging.info("Excavation_gdf.shape")
			logging.info(Excavation_gdf.shape)

			#Count of historical excavations that are linked to a grid
			hist_exc_join = geopandas.sjoin(base_copy,Excavation_gdf, how='left',op='intersects')
			logging.info("hist_exc_join.shape")
			logging.info(hist_exc_join.shape)

			exc_count = hist_exc_join.groupby(['GridNumber']).LICENCES_ID.nunique()
			exc_countDf = exc_count.to_frame().reset_index()
			exc_countDf.columns.values[1] = "Count of Excavation licenses"
			logging.info("exc_countDf.shape")
			logging.info(exc_countDf.shape)


			#merging back with base data
			base_data7=pd.merge(base_data6,exc_countDf,on='GridNumber')
			logging.info("base_data7.shape")
			logging.info(base_data7.shape)

			#Calculating Index
			average_cases=base_data7['Count of Excavation licenses'].sum()/len(base_data7)
			base_data7['Average Excavation licenses'] = average_cases
			base_data7['ExcavationLicense_Index']=base_data7['Count of Excavation licenses']/base_data7['Average Excavation licenses']
			base_data7['ExcavationLicense_Index'].fillna(0,inplace=True)
			base_data7['ExcavationLicense_Index']=round(base_data7['ExcavationLicense_Index'],2)
			logging.info("base_data7.shape")
			logging.info(base_data7.shape)


			# **8)Construction Licenses**
			cursor_targetDB.execute('''SELECT * FROM %s.LICENSES_DATA_CONSTRUCTIONS''' % config.DB['input_schema'])
			dfConstructionLicense = cursor_targetDB.fetchall()
			dfConstructionLicense = gpd.GeoDataFrame.from_records(dfConstructionLicense, 
														columns=[x[0] for x in cursor_targetDB.description])

			dfConstructionLicense2 = dfConstructionLicense.dropna(subset = ['LATITUDE','LONGITUDE'])

			dfConstructionLicense2['LATITUDE'] = dfConstructionLicense2['LATITUDE'].str.replace(',','.')
			dfConstructionLicense2['LONGITUDE'] = dfConstructionLicense2['LONGITUDE'].str.replace(',','.')

			dfConstructionLicense2['LATITUDE']= dfConstructionLicense2['LATITUDE'].str.replace('\n','')
			dfConstructionLicense2['LONGITUDE'] = dfConstructionLicense2['LONGITUDE'].str.replace('\n','')

			dfConstructionLicense2['LATITUDE'] = dfConstructionLicense2['LATITUDE'].str.replace('،','')
			dfConstructionLicense2['LONGITUDE'] = dfConstructionLicense2['LONGITUDE'].str.replace('،','')

			df1 = dfConstructionLicense2[dfConstructionLicense2['LATITUDE'].str.match('.*[\.].*[\.].*') == True]
			dfConstructionLicense2 = dfConstructionLicense2.drop(df1.index,axis = 0)

			df2 = dfConstructionLicense2[dfConstructionLicense2['LONGITUDE'].str.match('.*[\.].*[\.].*') == True]
			dfConstructionLicense2 = dfConstructionLicense2.drop(df2.index,axis = 0)

			dfConstructionLicense2['LONGITUDE']=dfConstructionLicense2['LONGITUDE'].str.strip()
			dfConstructionLicense2['LATITUDE']=dfConstructionLicense2['LATITUDE'].str.strip()

			constr_lic_long = list(dfConstructionLicense2['LONGITUDE'])
			constr_lic_lat = list(dfConstructionLicense2['LATITUDE'])

			dfConstructionLicense3  =dfConstructionLicense2.copy()
			logging.info("dfConstructionLicense3.shape")
			logging.info(dfConstructionLicense3.shape)

			#cleaning lat/logs 
			for idx, x in enumerate(constr_lic_lat):
				y = constr_lic_long[idx]
				try:
					if(float(x) > 50 or float(y) > 50):
						constr_lic_lat[idx] = np.NaN
						constr_lic_long[idx] = np.NaN
						continue
					if(float(x) > float(y)):
						constr_lic_lat[idx] = y
						constr_lic_long[idx] = x
				except ValueError:
					constr_lic_lat[idx] = np.NaN
					constr_lic_long[idx] = np.NaN

			dfConstructionLicense3['LATITUDE'] = constr_lic_lat        
			dfConstructionLicense3['LONGITUDE'] = constr_lic_long       
			dfConstructionLicense3.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

			dfConstructionLicense3['LATITUDE'] = dfConstructionLicense3['LATITUDE'].astype(float).round(decimals = 4)
			dfConstructionLicense3['LONGITUDE'] = dfConstructionLicense3['LONGITUDE'].astype(float).round(decimals = 4)

			# Filtering for outlier latitude and longitude values
			dfConstructionLicense4 = dfConstructionLicense3[(dfConstructionLicense3['LATITUDE'] < 60) & (dfConstructionLicense3['LATITUDE'] > 0)]
			dfConstructionLicense5 = dfConstructionLicense4[(dfConstructionLicense4['LONGITUDE'] < 60) & (dfConstructionLicense4['LONGITUDE'] > 0)]


			dfConstruction_gpd = geopandas.GeoDataFrame(dfConstructionLicense5,geometry = geopandas.points_from_xy(dfConstructionLicense5.LONGITUDE,dfConstructionLicense5.LATITUDE), crs = 'epsg:4326')


			dfConstruction_gpd.to_crs(epsg=32637, inplace=True)
			dfConstruction_gpd.drop_duplicates(inplace = True)
			logging.info("dfConstruction_gpd.shape")
			logging.info(dfConstruction_gpd.shape)


			# ***Filter for active construction licences for latest 6 months***
			dfConstruction_gpd_active = dfConstruction_gpd.loc[dfConstruction_gpd['LICENSE_EXPIRY_DATE'] > pd.to_datetime('now')]
			dfConstructionLicense_6mths = dfConstruction_gpd_active.loc[(dfConstruction_gpd_active['LICENSE_START_DATE'] > pd.to_datetime('now')-pd.DateOffset(months=6))]
			logging.info("dfConstructionLicense_6mths.shape")
			logging.info(dfConstructionLicense_6mths.shape)

			#Count of constructions that are linked to a grid
			constr_join = geopandas.sjoin(base_copy,dfConstructionLicense_6mths, how='left',op='intersects')
			logging.info("constr_join.shape")
			logging.info(constr_join.shape)

			constr_count = constr_join.groupby(['GridNumber'])['LICENCES_ID'].nunique()
			constr_countDF = constr_count.to_frame().reset_index()
			constr_countDF.columns.values[1] = "Count of Construction Licenses"
			logging.info("constr_countDF.shape")
			logging.info(constr_countDF.shape)
			


			#merging back with base data
			base_data8=pd.merge(base_data7,constr_countDF,on='GridNumber')
			logging.info("base_data8.shape")
			logging.info(base_data8.shape)

			#Calculating Index
			average_cases=base_data8['Count of Construction Licenses'].sum()/len(base_data8)
			base_data8['Average Construction licenses'] = average_cases
			base_data8['ConstructionLicense_Index']=base_data8['Count of Construction Licenses']/base_data8['Average Construction licenses']
			base_data8['ConstructionLicense_Index'].fillna(0,inplace=True)
			base_data8['ConstructionLicense_Index']=round(base_data8['ConstructionLicense_Index'],2)
			logging.info("base_data8.shape")
			logging.info(base_data8.shape)

			cwd = os.getcwd()
			base_dir = os.path.join(cwd,'amanas_data')
			target_dir = os.path.join(base_dir,'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					result.to_csv('ConstructionLicense_Amana.csv')
				else:
					os.mkdir(target_dir)
					result.to_csv('ConstructionLicense_Amana.csv')
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				result.to_csv('ConstructionLicense_Amana.csv')

			#result.to_csv('ConstructionLicense_Amana.csv')
			logging.info("ConstructionLicense_Amana result.shape")
			logging.info(result.shape)


			# **9)Retail Licenses**

			cursor_targetDB.execute('''SELECT * FROM %s.LICENSES_DATA''' % config.DB['input_schema'])
			dfRetailLicense = cursor_targetDB.fetchall()
			dfRetailLicense = gpd.GeoDataFrame.from_records(dfRetailLicense, 
														columns=[x[0] for x in cursor_targetDB.description])


			# ***Filter for active commercial licences for latest 6 months***

			dfRetailLicense_active = dfRetailLicense.loc[dfRetailLicense['LICENSE_EXPIRY_DATE'] > pd.to_datetime('now')]

			dfRetailLicense_6mths = dfRetailLicense_active.loc[(dfRetailLicense_active['LICENSE_START_DATE'] > pd.to_datetime('now')-pd.DateOffset(months=6))]

			dfRetailLicense_6mths['LATITUDE'] = dfRetailLicense_6mths['LATITUDE'].astype(float).round(decimals = 4)
			dfRetailLicense_6mths['LONGITUDE'] = dfRetailLicense_6mths['LONGITUDE'].astype(float).round(decimals = 4)

			dfRetailLicense_2 = dfRetailLicense_6mths[['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'SUB_MUNICIPALITY', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
			dfRetailLicense_2.rename(columns={'LATITUDE' : 'LONGITUDE', 'LONGITUDE' : 'LATITUDE'}, inplace=True)
			dfRetailLicense_2['LICENSE_START_DATE'] = pd.to_datetime(dfRetailLicense_2['LICENSE_START_DATE'], format = '%Y-%m-%d')
			dfRetailLicense_2['LICENSE_EXPIRY_DATE'] = pd.to_datetime(dfRetailLicense_2['LICENSE_EXPIRY_DATE'], format = '%Y-%m-%d')
			dfRetailLicense_2['LATITUDE'] = dfRetailLicense_2['LATITUDE'].apply(pd.to_numeric, errors = 'coerce')
			dfRetailLicense_2['LONGITUDE'] = dfRetailLicense_2['LONGITUDE'].apply(pd.to_numeric, errors = 'coerce')
			dfRetailLicense_2.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

			dfRetailLicense_2.drop_duplicates(inplace = True)

			gdfRetail = geopandas.GeoDataFrame(dfRetailLicense_2, geometry=geopandas.points_from_xy(dfRetailLicense_2.LONGITUDE, dfRetailLicense_2.LATITUDE),crs='epsg:4326')
			logging.info("gdfRetail.shape")
			logging.info(gdfRetail.shape)

			gdfRetail = gdfRetail[gdfRetail['geometry'].notna()]
			gdfRetail['geometry'] = gdfRetail['geometry'].astype(str)
			gdfRetail['geometry'] = gdfRetail['geometry'].apply(wkt.loads)
			gdfRetail = geopandas.GeoDataFrame(gdfRetail, geometry='geometry',crs='epsg:4326')
			gdfRetail.to_crs(epsg=32637, inplace = True)

			retail_join = geopandas.sjoin(base_copy,gdfRetail, how='left',op='intersects')
			retail_count = retail_join.groupby(['GridNumber'])['LICENCES_ID'].nunique()
			retail_countDF = retail_count.to_frame().reset_index()
			retail_countDF.columns.values[1] = "Count of Retail Licenses"
			logging.info("retail_countDF.shape")
			logging.info(retail_countDF.shape)

			#merging back with base data
			base_data9=pd.merge(base_data8,retail_countDF,on='GridNumber')
			logging.info("base_data9.shape")
			logging.info(base_data9.shape)

			#Calculating Index
			average_cases=base_data9['Count of Retail Licenses'].sum()/len(base_data9)
			base_data9['Average Retail licenses'] = average_cases
			base_data9['RetailLicense_Index']=base_data9['Count of Retail Licenses']/base_data9['Average Retail licenses']
			base_data9['RetailLicense_Index'].fillna(0,inplace=True)
			base_data9['RetailLicense_Index']=round(base_data9['ConstructionLicense_Index'],2)


			# **10)excavation barrier VP cases**
			#Loading the excavation barrier VP cases for latest 6 months
			excavation_crm = GDF_VP_cases.loc[(GDF_VP_cases['VISUAL POLLUTION CATEGORY'] == "ExcavationBarriers")]
			logging.info("excavation_crm.shape")
			logging.info(excavation_crm.shape)

			#Extracting Historical excavation barrier VP cases feature
			Historical_excavation_VP_Cases = excavation_crm[['PYID','LONGITUDE','LATITUDE']].copy()
			GDF_excavation_Cases = geopandas.GeoDataFrame(Historical_excavation_VP_Cases,geometry=geopandas.points_from_xy(Historical_excavation_VP_Cases.LONGITUDE,Historical_excavation_VP_Cases.LATITUDE),crs='epsg:4326')
			GDF_excavation_Cases=GDF_excavation_Cases.drop_duplicates()
			GDF_excavation_Cases.to_crs(epsg=32637, inplace=True)
			logging.info("GDF_excavation_Cases.shape")
			logging.info(GDF_excavation_Cases.shape)

			#Joining the base data with the cases data to get the VP cases associated with each grid
			Excavation_VP_Cases_join = geopandas.sjoin(base_copy,GDF_excavation_Cases,how="left",op="intersects")
			logging.info("Excavation_VP_Cases_join.shape")
			logging.info(Excavation_VP_Cases_join.shape)

			#Calculating number of cases per grid
			count = Excavation_VP_Cases_join.groupby(['GridNumber']).PYID.nunique()
			CountDf = count.to_frame().reset_index()
			CountDf.columns.values[1] = "Count of Excavation VP Cases"
			logging.info("CountDf.shape")
			logging.info(CountDf.shape)

			#Calculating average cases per grid
			average_excavation_VP_cases=CountDf['Count of Excavation VP Cases'].sum()/len(base_copy)

			#merging base data with 'number of cases' feature
			base_data10 = base_data9.copy()
			base_data10 = pd.merge(base_data10,CountDf,on='GridNumber')
			logging.info("base_data10.shape")
			logging.info(base_data10.shape)

			#Calculating Index
			average_cases=base_data10['Count of Excavation VP Cases'].sum()/len(base_data10)
			base_data10['Average Excavation VP Cases'] = average_cases
			base_data10['Excavation_Cases_Index']=base_data10['Count of Excavation VP Cases']/base_data10['Average Excavation VP Cases']
			base_data10['Excavation_Cases_Index'].fillna(0,inplace=True)
			base_data10['Excavation_Cases_Index']=round(base_data10['Excavation_Cases_Index'],2)


			#result.to_csv('ExcavationCases_Amana.csv')
			cwd = os.getcwd()
			base_dir = os.path.join(cwd,'amanas_data')
			target_dir = os.path.join(base_dir,'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					result.to_csv('ExcavationCases_Amana.csv')
				else:
					os.mkdir(target_dir)
					result.to_csv('ExcavationCases_Amana.csv')
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				result.to_csv('ExcavationCases_Amana.csv')

			logging.info("ExcavationCases_Amana result.shape")
			logging.info(result.shape)


			# **11)Construction VP cases**
			#Loading the construction VP cases for latest 6 months

			construction_crm = GDF_VP_cases.loc[(GDF_VP_cases['VISUAL POLLUTION CATEGORY'] == "Construction")]
			logging.info("construction_crm construction_crm.shape")
			logging.info(construction_crm.shape)

			#Extracting Historical excavation barrier VP cases feature
			Historical_construction_VP_Cases = construction_crm[['PYID','LONGITUDE','LATITUDE']].copy()
			GDF_construction_Cases = geopandas.GeoDataFrame(Historical_construction_VP_Cases,geometry=geopandas.points_from_xy(Historical_construction_VP_Cases.LONGITUDE,Historical_construction_VP_Cases.LATITUDE),crs='epsg:4326')
			GDF_construction_Cases=GDF_construction_Cases.drop_duplicates()
			GDF_construction_Cases.to_crs(epsg=32637, inplace=True)
			logging.info("GDF_construction_Cases construction_crm.shape")
			logging.info(GDF_construction_Cases.shape)
			

			#Joining the base data with the cases data to get the VP cases associated with each grid
			Construction_VP_Cases_join = geopandas.sjoin(base_copy,GDF_construction_Cases,how="left",op="intersects")
			logging.info("Construction_VP_Cases_join.shape")
			logging.info(Construction_VP_Cases_join.shape)

			#Calculating number of cases per grid
			count = Construction_VP_Cases_join.groupby(['GridNumber']).PYID.nunique()
			CountDf = count.to_frame().reset_index()
			CountDf.columns.values[1] = "Count of Construction VP Cases"

			#Calculating average cases per grid
			average_construction_VP_cases=CountDf['Count of Construction VP Cases'].sum()/len(base_copy)

			#merging base data with 'number of cases' feature
			base_data11 = pd.merge(base_data10,CountDf,on='GridNumber')
			logging.info("base_data11.shape")
			logging.info(base_data11.shape)


			#Calculating Index
			average_cases=base_data11['Count of Construction VP Cases'].sum()/len(base_data11)
			base_data11['Average Construction VP Cases'] = average_cases
			base_data11['Construction_Cases_Index']=base_data11['Count of Construction VP Cases']/base_data11['Average Construction VP Cases']
			base_data11['Construction_Cases_Index'].fillna(0,inplace=True)
			base_data11['Construction_Cases_Index']=round(base_data11['Construction_Cases_Index'],2)

			result = base_data11.groupby(['AMANACODE'])['Count of Construction VP Cases'].agg('sum').reset_index()
			result['sum']=result['Count of Construction VP Cases'].sum()
			result['%']=(result['Count of Construction VP Cases']/result['sum'])*100
			logging.info("base_data11.shape")
			logging.info(base_data11.shape)
			#result

			#result.to_csv('ConstructionCases_Amana.csv')
			cwd = os.getcwd()
			base_dir = os.path.join(cwd,'amanas_data')
			target_dir = os.path.join(base_dir,'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					result.to_csv('ConstructionCases_Amana.csv')
				else:
					os.mkdir(target_dir)
					result.to_csv('ConstructionCases_Amana.csv')
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				result.to_csv('ConstructionCases_Amana.csv')

			logging.info("ConstructionCases_Amana result.shape")
			logging.info(result.shape)

			# ### EDA functions

			def df_info(df):
				x= df.dtypes.value_counts()
				y = df.head(2)
				z = df.dtypes
				print("Data has {} rows and {} columns".format(df.shape[0],df.shape[1])),print('\033[1m' + "\nDatatypes"+ '\033[0m'),print(x),print(z),print(y)

			def categorical_stats(df):
				ds_cat_stats = pd.DataFrame(columns = ['Column Name', 'Column Values','Row Count', 'Distinct Value Count (inc. NA)',
													'Distinct Value Count (exc. NA)','Null count', '%Null'])
				tmp = pd.DataFrame()

				for c in df.columns:
					if df[c].dtypes == 'object':
						tmp['Column Name'] = [c]
						tmp['Column Values'] = [df[c].unique()]
						tmp['Row Count'] = [df[c].shape[0]]
						tmp['Distinct Value Count (inc. NA)'] = len(list(df[c].unique()))
						tmp['Distinct Value Count (exc. NA)'] = int(df[c].nunique())
						tmp['Null count'] = df[c].isnull().sum()
						tmp['%Null'] = (df[c].isnull().sum()/ len(df)).round(3)*100
						ds_cat_stats = ds_cat_stats.append(tmp)
						ds_cat_stats.sort_values(by = 'Distinct Value Count (inc. NA)', inplace = True, ascending = True)
					else:
						ds_cat_stats = ds_cat_stats
						
				
				return ds_cat_stats

			def numerical_stats(df):
				return df.describe().apply(lambda s: s.apply('{0:.1f}'.format))

			def value_counts(df,col_list):
				result = pd.DataFrame()
				for c in list(col_list):
					temp = pd.DataFrame()
					if is_numeric_dtype(df[c]):
						temp['col'] = [c]
						temp['row count'] = len(df)
						temp['column sum'] = round(df[c].sum(),2)
						temp['zero count'] = len(df[df[c]==0])
						temp['% zeros'] = round(len(df[df[c]==0])/len(df[c]),3)*100
						result = result.append(temp)   
					else:
						result = result
				return result.sort_values(by=['% zeros'],ascending = False) 

			#creating the VP event flag
			base_data11['VP event'] = base_data11['Count of VP Cases'].apply(lambda x: '1' if x > 0 else '0')
			#base_data11.head(2)

			#Rename Columns 

			base_data11 = base_data11.rename(columns = {'DN':'Grid Population','Count of VP Cases':'#GC VP Cases',
														'Number of buildings':'#Buildings', 
														'Count of Excavation licenses':'#Excavation licenses',
													'Count of Construction Licenses':'#Construction Licenses',
													'Count of Excavation VP Cases':'#Excavation VP Cases',
													'Count of Construction VP Cases':'#Construction VP Cases',
													'Count of Retail Licenses':'#Retail Licenses',
														'Count of POIs':'#POIs',
													'Count of Sewer Manholes':'#Sewer Manholes',
														'Number of priority Areas':'#Priority Areas'})

			base_data13 = base_data11[base_data11['Grid Population']>0]

			# # Target class Distribution

			#Check distribution of target class
			# from tabulate import tabulate
			e = pd.DataFrame(base_data13['VP event'].value_counts())
			d = pd.DataFrame(base_data13['VP event'].value_counts(normalize = True).mul(100))

			# df = tabulate(e, headers=['VP event','Count'], tablefmt='fancy_grid')
			# df1 = tabulate(d, headers=['VP event','Percentage'], tablefmt='fancy_grid')


			df1= d.reset_index()
			# fig = plt.figure(figsize = (15,4))
			# plt.bar(df1['index'],df1['VP event'])
			# plt.xlabel("VP event")
			# plt.ylabel("Percentage")
			# plt.title("VP distribution")
			#plt.show()
			#print(e)
			#print(d)
			# print(tabulate(e, headers=['VP event','Count'], tablefmt='fancy_grid'))
			# print(tabulate(d, headers=['VP event','Percentage'], tablefmt='fancy_grid'))


			# # Missing Values and 0s

			# print('Null Count')
			# print(base_data13.isnull().sum()/ len(base_data13))

			col_list = ['Grid Population','#Buildings','#GC VP Cases','#Excavation licenses','#Construction Licenses',
					'#Excavation VP Cases','#Construction VP Cases','#Retail Licenses',
						'#POIs','#Sewer Manholes','#Priority Areas','landuse']
			value_counts(base_data13,col_list)


			# # Univariate Analysis

			#print(numerical_stats(base_data13))

			# sns.set(rc={"figure.figsize":(15, 4)})
			# plt.show(sns.boxplot(y="VP event",x="Grid Population",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Buildings",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x='#Excavation licenses',data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Construction Licenses",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Retail Licenses",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Construction VP Cases",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Excavation VP Cases",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="landuse",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#POIs",data=base_data13,showmeans= True))
			# plt.show(sns.boxplot(y="VP event",x="#Sewer Manholes",data=base_data13,showmeans= True))


			# # Correlation Matrix

			base_final = base_data13[[ '#GC VP Cases','GridNumber','Grid Population',
				'#Buildings','#Construction Licenses','#Retail Licenses',
									'#Excavation licenses',
				'#Construction VP Cases', 
				'#Excavation VP Cases','#POIs','#Sewer Manholes','#Priority Areas']]

			matrix = base_final.corr().round(2)

			# fig = plt.figure(figsize = (45,10))
			# sns.set(font_scale=1.8)
			# sns.heatmap(matrix, annot=True ,vmax=1, vmin=-1, center=0, cmap='vlag')
			# plt.xticks(rotation=10)
			# plt.show()

			data = matrix.reset_index()
			# data.to_clipboard()

			# # OLS

			base_data13
			RiskFeatures_DF = base_data13[["AMANACODE","MUNICIPA_1",'GridNumber', 'geometry', 'VP event', 'Grid Population', '#GC VP Cases',
					'#POIs', '#Priority Areas', '#Sewer Manholes', '#Buildings',
				'#Excavation licenses', '#Construction Licenses',
				'#Excavation VP Cases', '#Construction VP Cases', '#Retail Licenses',
				'landuse']]
			#Approach 1 : Increase from lowest to highest bucket

			#Create buckets for Features

			#binning across all the features
			for column in RiskFeatures_DF.columns[20:30]:
				RiskFeatures_DF[column+'_ex']=pd.cut(RiskFeatures_DF[column], 5, duplicates='drop')

			#Creating a dataframe to store weights pertaining to each feature
			final_weights2 = pd.DataFrame(columns = ['Feature', 'Weight'])

			#Calulcating weights for each feature
			for column in RiskFeatures_DF.columns[30:40]:
				Weight_DF=RiskFeatures_DF[[column,'#GC VP Cases']].groupby(column).mean().sort_values('#GC VP Cases')
				Weight_DF=Weight_DF.loc[Weight_DF['#GC VP Cases']>0]
				Weight_DF[column+'_weight']=(max(Weight_DF['#GC VP Cases'])-Weight_DF['#GC VP Cases'])/Weight_DF['#GC VP Cases']
				column_names = ["Feature", "Weight"]
				final_weights2=final_weights2.append({'Feature':column,'Weight':max(Weight_DF[column+'_weight'])},ignore_index=True)
				final_weights2

			#1:forest
			#2:industrial
			#3:residential
			#4:commercial

			RiskFeatures_DF.groupby(['landuse'])['#GC VP Cases'].count()

			Weight_DF_landuse=RiskFeatures_DF[['landuse','#GC VP Cases']].groupby('landuse').mean().sort_values('#GC VP Cases')
			Weight_DF_landuse=Weight_DF_landuse.loc[Weight_DF_landuse['#GC VP Cases']>0]
			Weight_DF_landuse['landuse_weight']=(max(Weight_DF_landuse['#GC VP Cases'])-Weight_DF_landuse['#GC VP Cases'])/Weight_DF_landuse['#GC VP Cases']
			Weight_DF_landuse['landuse_weight']=max(Weight_DF_landuse['landuse_weight'])

			final_weights2 = final_weights2.append({'Feature':'landuse_ex','Weight':max(Weight_DF_landuse['landuse_weight'])},ignore_index=True)
			#final weight calculation for each feature
			final_weights2['Final_weight'] = round(final_weights2['Weight'] / final_weights2['Weight'].sum(),3)

			RiskFeatures_DF['VP event'] = RiskFeatures_DF['VP event'].astype(int)

			# Approach 2 : Increase from lowest to highest bucket

			# Create buckets for Features

			#binning across all the features
			for column in RiskFeatures_DF.columns[20:30]:
				RiskFeatures_DF[column+'_ex']=pd.cut(RiskFeatures_DF[column], 5, duplicates='drop')

			#Creating a dataframe to store weights pertaining to each feature
			final_weights3 = pd.DataFrame(columns = ['Feature', 'Weight'])

			#Calulcating weights for each feature
			for column in RiskFeatures_DF.columns[30:40]:
				Weight_DF=RiskFeatures_DF[[column,'VP event']].groupby(column).mean().sort_values('VP event')
				Weight_DF=Weight_DF.loc[Weight_DF['VP event']>0]
				Weight_DF[column+'_weight']=(max(Weight_DF['VP event'])-Weight_DF['VP event'])/Weight_DF['VP event']
				column_names = ["Feature", "Weight"]
				final_weights3=final_weights3.append({'Feature':column,'Weight':max(Weight_DF[column+'_weight'])},ignore_index=True)
				final_weights3

			#1:forest
			#2:industrial
			#3:residential
			#4:commercial


			Weight_DF_landuse=RiskFeatures_DF[['landuse','VP event']].groupby('landuse').mean().sort_values('VP event')
			Weight_DF_landuse=Weight_DF_landuse.loc[Weight_DF_landuse['VP event']>0]
			Weight_DF_landuse['landuse_weight']=(max(Weight_DF_landuse['VP event'])-Weight_DF_landuse['VP event'])/Weight_DF_landuse['VP event']
			Weight_DF_landuse['landuse_weight']=max(Weight_DF_landuse['landuse_weight'])

			final_weights3 = final_weights3.append({'Feature':'landuse_ex','Weight':max(Weight_DF_landuse['landuse_weight'])},ignore_index=True)

			#final weight calculation for each feature
			final_weights3['Final_weight'] = round(final_weights3['Weight'] / final_weights3['Weight'].sum(),3)

			final_weights2 = final_weights2.sort_values(by=['Final_weight'],ascending = False)

			print(final_weights2)

			# final_weights2.to_clipboard()

			final_weights3 = final_weights3.sort_values(by=['Final_weight'],ascending = False)

			# final_weights3.to_clipboard()

			RiskFeatures_DF.shape

			RiskFeatures_DF.isnull().sum()

			# RiskFeatures_DF['Excavation VP Cases Index wt']=final_weights2.iloc[0]['Final_weight']
			# RiskFeatures_DF['Buildings Index wt']=final_weights2.iloc[1]['Final_weight']
			# RiskFeatures_DF['Priority Areas Index wt']=final_weights2.iloc[2]['Final_weight']
			# RiskFeatures_DF['Population Index wt']=final_weights2.iloc[3]['Final_weight']
			# RiskFeatures_DF['POIs Index wt']=final_weights2.iloc[4]['Final_weight']
			# RiskFeatures_DF['Sewer Manholes Index wt']=final_weights2.iloc[5]['Final_weight']
			# RiskFeatures_DF['landuse wt']=final_weights2.iloc[6]['Final_weight']
			# RiskFeatures_DF['Retail Licenses Index wt']=final_weights2.iloc[7]['Final_weight']
			# RiskFeatures_DF['Construction Licenses Index wt']=final_weights2.iloc[8]['Final_weight']
			# RiskFeatures_DF['Construction VP Cases Index wt']=final_weights2.iloc[9]['Final_weight']
			# RiskFeatures_DF['Excavation licenses Index wt']=final_weights2.iloc[10]['Final_weight']


			#RiskFeatures_DF.to_csv(r'C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\FEATURE_ENGINEERING\GC_Features.csv')
			cwd = os.getcwd()
			base_dir = os.path.join(cwd,'amanas_data')
			target_dir = os.path.join(base_dir,'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					fileName_RiskFeatures_DF = os.path.join(target_dir,'{}.csv'.format(config.RiskFeatures_DF))
					if os.path.exists(fileName_RiskFeatures_DF):
						#new_fileName = target_dir + "\\{}".format(config.RiskFeatures_DF) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
						#new_fileName = os.path.join(target_dir, '{}'.format(config.RiskFeatures_DF) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						#os.rename(fileName_RiskFeatures_DF, new_fileName)
						RiskFeatures_DF.to_csv(fileName_RiskFeatures_DF, index=False)
					else:
						RiskFeatures_DF.to_csv(fileName_RiskFeatures_DF, index=False)
				else:
					os.mkdir(target_dir)
					fileName_RiskFeatures_DF = os.path.join(target_dir,'{}.csv'.format(config.RiskFeatures_DF))
					RiskFeatures_DF.to_csv(fileName_RiskFeatures_DF, index=False)
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				fileName_RiskFeatures_DF = os.path.join(target_dir,'{}.csv'.format(config.RiskFeatures_DF))
				RiskFeatures_DF.to_csv(fileName_RiskFeatures_DF, index=False)

			logging.info("RiskFeatures_DF.shape")
			logging.info(RiskFeatures_DF.shape)

			# #Scale features before performing model scoring across grids
			# from sklearn.preprocessing import MinMaxScaler
			# scaler = MinMaxScaler()

			
			# RiskFeatures_DF[['landuse', 'Population Index','Construction Licenses Index', 'Priority Areas Index','Buildings Index',
			# 				'Retail Licenses Index', 'Excavation licenses Index','Construction VP Cases Index','POIs Index',
			# 				'Sewer Manholes Index','Excavation VP Cases Index']]= scaler.fit_transform(RiskFeatures_DF[['landuse', 'Population Index','Construction Licenses Index', 'Priority Areas Index','Buildings Index',
			# 				'Retail Licenses Index', 'Excavation licenses Index','Construction VP Cases Index','POIs Index',
			# 				'Sewer Manholes Index','Excavation VP Cases Index']])

			# ModeScore = RiskFeatures_DF.copy()
			# ModeScore['Model_final_score']=  ModeScore['landuse']*ModeScore['landuse wt']+ModeScore['Population Index']*ModeScore['Population Index wt']+ModeScore['Construction Licenses Index']*ModeScore['Construction Licenses Index wt']+ModeScore['Priority Areas Index']*ModeScore['Priority Areas Index wt']+ModeScore['Buildings Index']*ModeScore['Buildings Index wt']+ModeScore['Retail Licenses Index']*ModeScore['Retail Licenses Index wt']+ModeScore['Excavation licenses Index']*ModeScore['Excavation licenses Index wt']+ModeScore['Construction VP Cases Index']*ModeScore['Construction VP Cases Index wt']+ModeScore['POIs Index']*ModeScore['POIs Index wt']+ModeScore['Sewer Manholes Index']*ModeScore['Sewer Manholes Index wt']+ModeScore['Excavation VP Cases Index']*ModeScore['Excavation VP Cases Index wt']

			# #Create fixed range for final model scores
			# ModeScore['final_score']=(((100-1)/(max(ModeScore['Model_final_score'])- min(ModeScore['Model_final_score'])))*ModeScore['Model_final_score'])+1

			# ModeScore.to_excel('GC_ModelScores_1309.xlsx')
			# ModeScore.groupby(['final_score'])['GridNumber'].agg('count').to_excel('GC_GridCountsByRisk.xlsx')
			# # 51112
			# ModeScore[ModeScore['GridNumber']=='51112']

			# RiskFeatures_DF[RiskFeatures_DF['GridNumber']=='51112']


	
		def Process(self,Helper : Helper):
			print("1")


			cwd = os.getcwd()
			base_dir = os.path.join(cwd,'amanas_data')
			target_dir = os.path.join(base_dir,'full_amana_data')
			fileName_RiskFeatures_DF = os.path.join( target_dir,'{}.csv'.format(config.RiskFeatures_DF))

			#Features data
			#data = pd.read_csv(r'C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\FEATURE_ENGINEERING\GC_Features.csv')
			data = pd.read_csv(fileName_RiskFeatures_DF, dtype ={'MUNICIPA_1':'str','AMANACODE':'str'})
			logging.info("After reading RiskFeatures_DF.shape")
			logging.info(data.shape)
			data.columns

			ads = data[["AMANACODE","MUNICIPA_1","GridNumber",'Grid Population',
				'VP event', '#POIs', '#Priority Areas', '#Sewer Manholes',
				'#Buildings', '#Excavation licenses', '#Construction Licenses',
				'#Excavation VP Cases', '#Construction VP Cases', '#Retail Licenses','landuse']]

			ads.head(2)

			from sklearn import cluster
			from sklearn.mixture import GaussianMixture 
			model_ma = GaussianMixture(n_components = 2, covariance_type = 'spherical',random_state=143)
			model_cases = GaussianMixture(n_components = 2, covariance_type = 'spherical',random_state=143)

			final_grids = ads.copy()

			final_grids.columns

			# ### Municipal Assets Cluster Engine
			gm = model_ma.fit(ads[[ 'landuse',
				'Grid Population', 'VP event',
				'#POIs',  '#Sewer Manholes', '#Buildings',
				'#Excavation licenses', '#Construction Licenses', "#Retail Licenses"]])
			final_grids['ma_clusters_gc'] = gm.predict(ads[['landuse',
				'Grid Population', 'VP event',
				'#POIs', '#Sewer Manholes', '#Buildings',
				'#Excavation licenses', '#Construction Licenses', "#Retail Licenses"]])


			# ### CRM Cases Engine

			gm_crm = model_cases.fit(ads[['#Excavation VP Cases', '#Construction VP Cases']])
			final_grids['cases_clusters_gc'] = gm_crm.predict(ads[['#Excavation VP Cases', '#Construction VP Cases']])

			final_grids[['ma_clusters_gc', 'cases_clusters_gc']].value_counts()
			final_grids.groupby(['ma_clusters_gc','cases_clusters_gc']).agg('count').reset_index()
			final_grids.groupby(['ma_clusters_gc','cases_clusters_gc']).agg('median').reset_index()
			final_grids.groupby(['ma_clusters_gc','cases_clusters_gc']).agg('mean').reset_index()

			final_grids[(final_grids['ma_clusters_gc']==1) & (final_grids['cases_clusters_gc']==0)]
			final_grids['Grid Population'].sum()
			final_grids[final_grids['Grid Population']>34000]

			final_grids[(final_grids['ma_clusters_gc']==1) & (final_grids['cases_clusters_gc']==0)]['Grid Population'].max()

			# import seaborn as sns
			# sns.boxplot(final_grids[(final_grids['ma_clusters_gc']==1) & (final_grids['cases_clusters_gc']==0)]['Grid Population'])


			# ### Hardcoding Cluster Identities
			# 
			# To do this, we will take 3 features and compare the total values in each class. The class with the higher total in all the three cases will be tagged to 1 

			switch_flag = 0
			if (final_grids[final_grids['ma_clusters_gc']==1]['Grid Population'].sum()>=final_grids[final_grids['ma_clusters_gc']==0]['Grid Population'].sum()) or(final_grids[final_grids['ma_clusters_gc']==1]['#Sewer Manholes'].sum()>= final_grids[final_grids['ma_clusters_gc']==0]['#Sewer Manholes'].sum()) or(final_grids[final_grids['ma_clusters_gc']==1]['landuse'].sum()>= final_grids[final_grids['ma_clusters_gc']==0]['landuse'].sum()):
				pass
			else:
				switch_flag = 1
			switch_flag

			if switch_flag:
				final_grids['ma_clusters_gc'] = final_grids['ma_clusters_gc'].replace({0:1,1:0})

			switch_flag = 0
			if (final_grids[final_grids['cases_clusters_gc']==1]['#Excavation VP Cases'].sum()>=final_grids[final_grids['cases_clusters_gc']==0]['#Excavation VP Cases'].sum()) and(final_grids[final_grids['cases_clusters_gc']==1]['#Construction VP Cases'].sum()>= final_grids[final_grids['cases_clusters_gc']==0]['#Construction VP Cases'].sum()):
				pass
			else:
				switch_flag = 1
			switch_flag

			if switch_flag:
				final_grids['cases_clusters_gc'] = final_grids['cases_clusters_gc'].replace({0:1,1:0})


			#final_grids.to_csv(r"C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\MODEL_SCORES\GC_Scores.csv")
			if os.path.exists(target_dir):
					fileName_FINAL_GRIDS = os.path.join(target_dir, '{}.csv'.format(config.FINAL_GRIDS))
					if os.path.exists(fileName_FINAL_GRIDS):
						new_fileName = os.path.join(target_dir, "{}".format(config.FINAL_GRIDS) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						os.rename(fileName_FINAL_GRIDS, new_fileName)
						final_grids.to_csv(fileName_FINAL_GRIDS, index=False)
					else:
						final_grids.to_csv(fileName_FINAL_GRIDS, index=False)
			else:
				os.mkdir(target_dir)
				fileName_FINAL_GRIDS = os.path.join(target_dir, '{}.csv'.format(config.FINAL_GRIDS))
				final_grids.to_csv(fileName_FINAL_GRIDS, index=False)


			# ## Score Generation
			#GC_scores = pd.read_csv(r"C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\MODEL_SCORES\GC_Scores.csv")
			GC_scores = final_grids

			GC_scores = GC_scores[['AMANACODE', 'MUNICIPA_1','GridNumber', 'ma_clusters_gc', 'cases_clusters_gc']]

			GC_scores[(GC_scores['ma_clusters_gc']==0) & (GC_scores['cases_clusters_gc']==1)]

			#generating scores for GC

			def generate_gc(row):
				if row["ma_clusters_gc"]==0:
					if row['cases_clusters_gc']==0:
						return 0.25
					else:
						return 0.50
				elif row["cases_clusters_gc"]==0:
					return 0.75
				else:
					return 1
				
			GC_scores['final_scores'] = GC_scores[['ma_clusters_gc','cases_clusters_gc']].apply(lambda x : generate_gc(x),axis=1)

			GC_scores['final_scores'].value_counts()/len(GC_scores)

			GC_scores['final_scores'].value_counts()

			GC_scores_df = GC_scores[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'final_scores']]
			#print(GC_scores_df.head(1))

			GC_scores_df['final_scores'].describe()

			# ### Grids without Population

			# RAW_DATA_PATH = r"C:\Environment\MOMRAH_WORKING\7. CV expansion\5. Municipality Assets Inspection\0.RAW_DATA"
			# # #Load updated Shapefiles of the population data with Grid Number
			# shpGrid = gpd.read_file(RAW_DATA_PATH + r'\Standard_grids\Population_grids.shp')

			# shpGrid_nopop = shpGrid[shpGrid['DN']<=0]

			GC_scores_df_all = GC_scores_df  #pd.concat([GC_scores_df,shpGrid_nopop[['AMANACODE','MUNICIPA_1','GridNumber']]])

			GC_scores_df_all = GC_scores_df_all.replace(np.nan,0)

			#GC_scores_df_all.to_csv(r"C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\MODEL_SCORES\final_scores_all.csv", index=False)
			if os.path.exists(target_dir):
					fileName_GC_scores_df_all = os.path.join(target_dir, "{}.csv".format(config.GC_scores_df_all))
					if os.path.exists(fileName_GC_scores_df_all):
						new_fileName = os.path.join(target_dir , "{}".format(config.GC_scores_df_all) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						os.rename(fileName_GC_scores_df_all, new_fileName)
						GC_scores_df_all.to_csv(fileName_GC_scores_df_all, index=False)
					else:
						GC_scores_df_all.to_csv(fileName_GC_scores_df_all, index=False)
			else:
				os.mkdir(target_dir)
				fileName_GC_scores_df_all = os.path.join(target_dir, "{}.csv".format(config.GC_scores_df_all))
				GC_scores_df_all.to_csv(fileName_GC_scores_df_all, index=False)


			#GC_scores_df_all = pd.read_csv(r"C:\Environment\MOMRAH_WORKING\7. CV expansion\6.General Cleaning Inspection(Phase-II)\MODEL_SCORES\final_scores_all.csv")


			# ### Assinging Risk Cadences

			#GC_scores_df_all = GC_scores_df_all

			very_high_thresh = 1

			high_thresh = 0.75

			medium_thresh = 0.50

			def get_cadence(score):
				if score>=very_high_thresh:
					return "Very High Risk"
				if score>=high_thresh:
					return "High Risk"
				if score>=medium_thresh:
					return "Medium Risk"
				return "Low Risk"

			GC_scores_df_all['Riskiness'] = GC_scores_df_all['final_scores'].apply(lambda x: get_cadence(x))


			100*(GC_scores_df_all['Riskiness'].value_counts()/len(GC_scores_df_all))

			#print(GC_scores_df_all.head(1))

			
			GC_scores_df_all = GC_scores_df_all.merge(self.gisdata.GridZones, how = 'left', left_on = 'GridNumber',right_on='GRIDUNIQUECODE')
			GC_scores_df_all = GC_scores_df_all.rename(columns={'MUNICIPALI':'MUNICIPALITY', 'GUID':'GRID_ZONE'})


			GC_scores_df_all.columns  = map(str.upper, GC_scores_df_all.columns)
			#GC_scores_df_all['final_scores'].hist()
			Helper.backup(config.Output_table_name)

			Helper.insert_df_Batchwise( GC_scores_df_all, config.Output_table_name,10000)
			logging.info("GC_scores_df_all.shape")
			logging.info(GC_scores_df_all.shape)
		




