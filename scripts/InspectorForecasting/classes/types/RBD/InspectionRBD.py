#Importing libraries without acronyms
import os
#import glob
#import json
import math
import plotly
#import shutil
#import chardet
import warnings
# import matplotlib
import cx_Oracle
import datetime
import config  # username / password
import logging

#Importing libraries with acronyms
import numpy as np
import pandas as pd
import geopandas as gpd
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
from matplotlib.pyplot import figure
from sklearn.metrics import r2_score
from hijri_converter import Hijri, Gregorian
from sklearn.metrics import mean_absolute_error
from statsmodels.tsa.arima.model import ARIMA
from statistics import mean
from xgboost import XGBRegressor
from statsmodels.tsa.api import ExponentialSmoothing,SimpleExpSmoothing, Holt
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_percentage_error
#from classes.engines.Helper import  AddMonths,insert_df, Engine_End_Metadata_Update, backup,insert_df_Batchwise
from classes.engines.Inspection import Inspection
from classes.GISDatabase import GISDatabase
from classes.Database import Database


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
warnings.filterwarnings("ignore")

class InspectionRBD(Inspection):


		def __init__(self, data : Database, gisdata : GISDatabase):
			super().__init__(data,gisdata)
			logging.info("InspectionRBD Initialized")
			self.features = {}
			self.filesDumpTime = datetime.now()
	

		def PrepareData(self, Helper : Helper):
		
			#change colums to Upper and only geomtry column to lower
			self.gisdata.MUNICIPALITY.columns  = map(str.upper, self.gisdata.MUNICIPALITY.columns)
			self.gisdata.MUNICIPALITY = self.gisdata.MUNICIPALITY.rename(columns={'GEOMETRY':'geometry'})
			self.gisdata.SUBMUNICIPALITY.columns  = map(str.upper, self.gisdata.SUBMUNICIPALITY.columns)
			self.gisdata.SUBMUNICIPALITY = self.gisdata.SUBMUNICIPALITY.rename(columns={'GEOMETRY':'geometry'})

			numberOfUniqueMunicipalityNames = len(self.gisdata.MUNICIPALITY['MUNICIPALI'].unique())
			logging.info("numberOfUniqueMunicipalityNames: " + str(numberOfUniqueMunicipalityNames))
			
			numberOfUniqueMunicipalityNamesInSub = len(self.gisdata.SUBMUNICIPALITY['MUNICIPALI'].unique())
			logging.info("numberOfUniqueMunicipalityNamesInSub: " + str(numberOfUniqueMunicipalityNamesInSub))

			numberOfUniqueSubMunicipalityNamesInSub = len(self.gisdata.SUBMUNICIPALITY['SUBMUNIC_3'].unique())
			logging.info("numberOfUniqueSubMunicipalityNamesInSub: " + str(numberOfUniqueSubMunicipalityNamesInSub))

			totalSubmunicipality = numberOfUniqueMunicipalityNames - numberOfUniqueMunicipalityNamesInSub + numberOfUniqueSubMunicipalityNamesInSub
			print('Total Submunicipalitys: ' + str(totalSubmunicipality))

			
			#Combining data to an Amana level
			submunic = self.gisdata.SUBMUNICIPALITY
			munic = self.gisdata.MUNICIPALITY
			municip_with_subs = submunic['MUNICIPALI'].unique()
			municip_without_subs = set(list(munic['MUNICIPALI'])) - set(municip_with_subs)
			munic_without_sub_df = munic[munic['MUNICIPALI'].isin(municip_without_subs)]
			submunic_amana = submunic.loc[(submunic['MUNICIPALI'].isin(municip_with_subs))]

			#Collating data at category level
			munic_without_sub_df['CATEGORY'] = munic_without_sub_df['MUNICIPALI'].copy()
			submunic_amana_2 = submunic_amana.copy()
			munic_without_sub_df.rename(columns = {'CATEGORY' : 'SUBMUNIC_3', 'MUNICIPA_2' : 'SUBMUNIC_1'}, inplace=True)
			amana_areas = pd.concat([munic_without_sub_df.reset_index(drop=True), submunic_amana_2.reset_index(drop=True)]).reset_index(drop=True)

			#Combined Dataframe for downstream usage
			combined_df = amana_areas
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df1.csv"))
				else:
					os.mkdir(target_dir)
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df1.csv"))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df1.csv"))
			
			#### Population density calculation per submunicipality

			#Processing Population grids to join with Area
			shpGrid = self.data.shpGrid
			shpGrid.columns  = map(str.upper, shpGrid.columns)
			shpGrid = shpGrid.rename(columns={'GEOMETRY':'geometry'})
			grid_for_density = gpd.sjoin(combined_df, shpGrid, how='left', predicate='intersects')
			grid_for_density['SHAPE_AREA'] = grid_for_density['geometry'].area/10**6
			logging.info('grid_for_density.shape')
			logging.info(grid_for_density.shape)
		
			#Generating Population level data
			logging.info(grid_for_density.head(5))
			population_submunicip = grid_for_density.groupby(['SUBMUNIC_3'])['DN'].sum().to_frame()
			logging.info(population_submunicip.head(5))
			submunicip_areas = grid_for_density.groupby(['SUBMUNIC_3'])['SHAPE_AREA'].mean().to_frame()
			logging.info('submunicip_areas.shape')
			logging.info(submunicip_areas.shape)

			#Calculating Population Density
			population_submunicip = population_submunicip.join(submunicip_areas).reset_index()
			population_submunicip['POPULATION_DENSITY'] = population_submunicip['DN'] / population_submunicip['SHAPE_AREA']
			logging.info(population_submunicip.shape)
			logging.info('population_submunicip.shape')

			#Aggregating it back to the original dataframe
			combined_df = combined_df.set_index('SUBMUNIC_3', drop=False).join(population_submunicip.set_index('SUBMUNIC_3'), lsuffix='', rsuffix='RIGHT')
			combined_df = combined_df.rename(columns = {'DN' : 'POPULATION'})
			logging.info('combined_df.shape')
			logging.info(combined_df.shape)
			#print("2")
			print(combined_df.head(2))
			print(len(combined_df['SUBMUNIC_3'].unique()))
			# #### 1. Construction licenses
			# Cleaning and Processing the construction licenses to made it model ready
			#preparing data for construction licenses and cleaning up dates in Hijri calendar
			constr_licenses = self.data.constructionLicenses
			constr_licenses.columns  = map(str.upper, constr_licenses.columns)
			constr_licenses_short = constr_licenses[['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
			constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE'], inplace=True)
			constr_lic_lat = list(constr_licenses_short['LONGITUDE'])
			constr_lic_long = list(constr_licenses_short['LATITUDE'])
			logging.info('constr_licenses_short.shape')
			logging.info(constr_licenses_short.shape)

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

			constr_licenses_short['LATITUDE'] = constr_lic_lat
			constr_licenses_short['LONGITUDE'] = constr_lic_long
			constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
			logging.info('constr_licenses_short.shape')
			logging.info(constr_licenses_short.shape)


			# #### 2. Commercial licenses

			# Cleaning and Processing the commercial licenses to make it model ready
			#preparing data for commercial licenses
			comm_licenses_madina = self.data.commercialLicenses
			comm_licenses_madina.columns  = map(str.upper, comm_licenses_madina.columns)
			comm_lic_short = comm_licenses_madina[['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'SUB_MUNICIPALITY', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
			comm_lic_short.rename(columns={'LATITUDE' : 'LONGITUDE', 'LONGITUDE' : 'LATITUDE'}, inplace=True)
			comm_lic_short['LICENSE_START_DATE'] = pd.to_datetime(comm_lic_short['LICENSE_START_DATE'], format = '%Y-%m-%d')
			comm_lic_short['LICENSE_EXPIRY_DATE'] = pd.to_datetime(comm_lic_short['LICENSE_EXPIRY_DATE'], format = '%Y-%m-%d')
			comm_lic_short['LATITUDE'] = comm_lic_short['LATITUDE'].apply(pd.to_numeric, errors = 'coerce')
			comm_lic_short['LONGITUDE'] = comm_lic_short['LONGITUDE'].apply(pd.to_numeric, errors = 'coerce')
			comm_lic_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
			logging.info('comm_lic_short.shape')
			logging.info(comm_lic_short.shape)


			# # Weekly mapping of licenses
			start_date = pd.Timestamp(str(Helper.AddMonths(datetime.now(),- config.CRM_START_DATE)))
			end_date = pd.Timestamp(str(Helper.AddMonths(datetime.now(),0)))


			w1=start_date.week
			days_length = (end_date - start_date).days
			weeks_length = math.ceil(days_length / 7)

			#print(combined_df['SUBMUNIC_3'].unique())
			index = combined_df['SUBMUNIC_3'].unique()



			print("2")
			print(combined_df.head(2))
			number_submunic = len(combined_df['SUBMUNIC_3'].unique())
			number_licenses_constr = np.zeros((weeks_length, number_submunic))
			number_licenses_comm = np.zeros((weeks_length, number_submunic))
			number_licenses_excav = np.zeros((weeks_length, number_submunic))

			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df2.csv"))
				else:
					os.mkdir(target_dir)
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df2.csv"))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df2.csv"))


			# ## 1. Weekly license mapping - commercial licenses

			#calculation of weekly number of licenses for submunicipalities
			GDF_comm_lic_short = gpd.GeoDataFrame(comm_lic_short, geometry=gpd.points_from_xy(comm_lic_short['LONGITUDE'], comm_lic_short['LATITUDE']), crs='epsg:4326')
			comm_lic_submunic_short = gpd.sjoin(combined_df, GDF_comm_lic_short, how='left', predicate='intersects').reset_index(drop=True)
			comm_lic_submunic_short = comm_lic_submunic_short[['SUBMUNIC_3', 'LICENCES_ID', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'geometry']]
			logging.info('comm_lic_submunic_short.shape')
			logging.info(comm_lic_submunic_short.shape)
			print("2")
			print(comm_lic_submunic_short.head(2))

			x = start_date
			i = 0

			while x < end_date:
				active_licenses = comm_lic_submunic_short.loc[(comm_lic_submunic_short['LICENSE_START_DATE'] < x) & (comm_lic_submunic_short['LICENSE_EXPIRY_DATE'] >= x)].groupby(['SUBMUNIC_3'])['LICENCES_ID'].nunique().reindex(index, fill_value=0).sort_index(ascending=True)
				active_licenses = active_licenses.to_list()
				current_week = math.floor((x - start_date).days / 7)
				x = x + pd.DateOffset(1)
				for j, y in enumerate(active_licenses):
					number_licenses_comm[current_week][j] += active_licenses[j]
				i = i + 1
				if(i % 1000 == 0):
					logging.info(x)
					logging.info(active_licenses)

			number_licenses_comm = number_licenses_comm[:-1] / 7
			#plt.plot(number_licenses_comm)


			# ## 2. Weekly license mapping - construction licenses

			#calculation of weekly number of licenses for submunicipalities
			GDF_constr_lic_short = gpd.GeoDataFrame(constr_licenses_short, geometry=gpd.points_from_xy(constr_licenses_short['LONGITUDE'],constr_licenses_short['LATITUDE']), crs='epsg:4326')
			logging.info('GDF_constr_lic_short.shape')
			logging.info(GDF_constr_lic_short.shape)
			print("2")
			print(GDF_constr_lic_short.head(2))


			constr_lic_submunic_short = gpd.sjoin(combined_df, GDF_constr_lic_short, how='left', predicate='intersects').reset_index(drop=True)
			constr_lic_submunic_short = constr_lic_submunic_short[['SUBMUNIC_3', 'LICENCES_ID', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'geometry']]

			logging.info('constr_lic_submunic_short.shape')
			logging.info(constr_lic_submunic_short.shape)

			x = start_date
			i = 0

			while x < end_date:
				active_licenses = constr_lic_submunic_short.loc[(constr_lic_submunic_short['LICENSE_START_DATE'] < x) & (constr_lic_submunic_short['LICENSE_EXPIRY_DATE'] >= x)].groupby(['SUBMUNIC_3'])['LICENCES_ID'].nunique().reindex(index, fill_value=0).sort_index(ascending=True)
				active_licenses = active_licenses.to_list()
				current_week = math.floor((x - start_date).days / 7)
				x = x + pd.DateOffset(1)
				for j, y in enumerate(active_licenses):
					number_licenses_constr[current_week][j] += active_licenses[j]
				i = i + 1
				if(i % 1000 == 0):
					logging.info(x)
					logging.info(active_licenses)

			number_licenses_constr = number_licenses_constr[:-1] / 7
			logging.info('number_licenses_constr.shape')
			logging.info(number_licenses_constr.shape)

			# # CRM data cleaning
			CRM_full = self.data.crmData
			CRM_full.columns  = map(str.upper, CRM_full.columns)
			CRM_VP = CRM_full.loc[(CRM_full['VISUAL POLLUTION CATEGORY'] != "NaN")]
			CRM_VP = CRM_VP[['PYID', 'INTERACTIONTYPE', 'PXCREATEDATETIME', 'LATITUDE', 'LONGITUDE', 'VISUAL POLLUTION CATEGORY']]
			CRM_VP = CRM_VP.dropna(subset=['LATITUDE', 'LONGITUDE', 'VISUAL POLLUTION CATEGORY'])
			CRM_VP = CRM_VP.drop_duplicates()

			GDF_VP_cases = gpd.GeoDataFrame(CRM_VP,geometry=gpd.points_from_xy(CRM_VP['LONGITUDE'],CRM_VP['LATITUDE']),crs='epsg:4326')

			cases_submunic = gpd.sjoin(combined_df, GDF_VP_cases, how='left', predicate='intersects').reset_index(drop = True)
			print("2")
			print(cases_submunic.head(2))

			logging.info("cases_submunic['PYID'].count()")
			logging.info(cases_submunic['PYID'].count())
			cases_submunic['PXCREATEDATETIME'] = pd.to_datetime(cases_submunic['PXCREATEDATETIME'], format = '%m/%d/%Y %I:%M:%S.%f %p')
			cases_submunic['INSPECTIONCOUNT'] = cases_submunic.groupby(['SUBMUNIC_3', 'INTERACTIONTYPE', 'VISUAL POLLUTION CATEGORY'])['PYID'].transform('count')
			cases_submunic = cases_submunic.drop_duplicates(subset=['SUBMUNIC_3', 'INTERACTIONTYPE', 'VISUAL POLLUTION CATEGORY'])
			cases_submunic = cases_submunic[['INSPECTIONCOUNT', 'SUBMUNIC_3', 'INTERACTIONTYPE', 'VISUAL POLLUTION CATEGORY', 'POPULATION', 'geometry']]
			logging.info("cases_submunic")
			logging.info(cases_submunic.shape)
			logging.info(cases_submunic.head(2))


			# # Weekly CRM data plotting across submunicipalities

			start_date_CRM = start_date
			end_date_CRM = end_date
			days_length_CRM = (end_date_CRM - start_date_CRM).days
			weeks_length_CRM = math.ceil(days_length / 7)
			number_cases_CRM = np.zeros((weeks_length, number_submunic))

			CRM_time_series = gpd.sjoin(combined_df, GDF_VP_cases, how='left', predicate='intersects').reset_index(drop=True)
			CRM_time_series['PXCREATEDATETIME'] = pd.to_datetime(CRM_time_series['PXCREATEDATETIME'], format = '%m/%d/%Y %I:%M:%S.%f %p')
			CRM_time_series['PXCREATEDATETIME'] = CRM_time_series['PXCREATEDATETIME'].dt.strftime("%Y-%m-%d")
			CRM_time_series['PXCREATEDATETIME'] = pd.to_datetime(CRM_time_series['PXCREATEDATETIME'], format = '%Y-%m-%d')
			CRM_time_series = CRM_time_series[['SUBMUNIC_3', 'PYID', 'PXCREATEDATETIME', 'VISUAL POLLUTION CATEGORY', 'geometry']]
			CRM_time_series = CRM_time_series.loc[(CRM_time_series['PXCREATEDATETIME'] < end_date_CRM) & (CRM_time_series['PXCREATEDATETIME'] >= start_date_CRM)]

			plot_data = CRM_time_series.groupby(['PXCREATEDATETIME', 'SUBMUNIC_3'])['PYID'].count().to_frame().reset_index()
			x = start_date_CRM
			i = 0

			#type(number_licenses)
			while x < end_date_CRM:
				#logging.info(constr_lic_submunic_short['LICENSE START DATE'])
				cases = CRM_time_series.loc[(CRM_time_series['PXCREATEDATETIME'] == x)].groupby(['SUBMUNIC_3'])['PYID'].nunique().reindex(index, fill_value=0).sort_index(ascending=True)
				cases = cases.to_list()
				current_week = math.floor((x - start_date).days / 7)
				#logging.info(current_week)
				x = x + pd.DateOffset(1)
				for j, y in enumerate(cases):
					number_cases_CRM[current_week][j] += cases[j]
				i = i + 1
				if(i % 1000 == 0):
					logging.info(x)
					logging.info(cases)

			cwd = os.getcwd()
# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df3.csv"))
				else:
					os.mkdir(target_dir)
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df3.csv"))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df3.csv"))

			combined_df_temp = combined_df['MUNICIPA_1'].sort_index()
			combined_df_temp = combined_df_temp[~combined_df_temp.index.duplicated(keep='first')]
			#"data_dictionary": combined_df['MUNICIPA_1'].sort_index().drop_duplicates()}


			# ### Creating Folder Structure using Amana Code
			#
			# This is used to create the folder structure and will act as an input to the next notebook
			print("combined_df")
			print(combined_df.head(2))
			self.features = {"commercial":number_licenses_comm,
			"construction":number_licenses_constr,
			"excavation":number_licenses_excav,
			"cases": number_cases_CRM,
			"data_dictionary": combined_df_temp}

			
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					combined_df_temp.to_csv(os.path.join(target_dir, "combined_df_temp.csv"))
				else:
					os.mkdir(target_dir)
					combined_df_temp.to_csv(os.path.join(target_dir, "combined_df_temp.csv"))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				combined_df_temp.to_csv(os.path.join(target_dir, "combined_df_temp.csv"))

			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df4.csv"))
				else:
					os.mkdir(target_dir)
					combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df4.csv"))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				combined_df['SUBMUNIC_3'].to_csv(os.path.join(target_dir, "combined_df4.csv"))
			
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					#shutil.rmtree(target_dir)
					#os.mkdir(target_dir)

					for feature in self.features:
						fileName = os.path.join(target_dir, "full_amana_data_{}.csv".format(feature))
						if os.path.exists(fileName):
							#new_fileName = os.path.join(target_dir, "full_amana_data_{}".format(feature) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
							#os.rename(fileName, new_fileName)
							pd.DataFrame(self.features[feature]).to_csv(fileName, index=False)
						else:
							pd.DataFrame(self.features[feature]).to_csv(fileName, index=False)

						if feature=="data_dictionary":
							fileName = os.path.join(target_dir, "full_amana_{}.csv".format(feature))
							if os.path.exists(fileName):
								#new_fileName = os.path.join(target_dir, "full_amana_{}".format(feature) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
								#os.rename(fileName, new_fileName)
								pd.DataFrame(self.features[feature]).to_csv(fileName)
							else:
								pd.DataFrame(self.features[feature]).to_csv(fileName)
				else:
					os.mkdir(target_dir)
					for feature in self.features:
						pd.DataFrame(self.features[feature]).to_csv(os.path.join(target_dir, "full_amana_data_{}.csv".format(feature)), index=False)
						if feature=="data_dictionary":
							pd.DataFrame(self.features[feature]).to_csv(os.path.join(target_dir, "full_amana_{}.csv".format(feature)))
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				for feature in self.features:
					pd.DataFrame(self.features[feature]).to_csv(os.path.join(target_dir, "full_amana_data_{}.csv".format(feature), index=False))
					if feature=="data_dictionary":
						pd.DataFrame(self.features[feature]).to_csv(os.path.join(target_dir, "full_amana_{}.csv".format(feature)))

			## Generating Planned Inspections
			#Importing RBD Output Files.
			pd_aggregated = self.data.riskBasedEngineOutput
			pd_aggregated.columns  = map(str.upper, pd_aggregated.columns)
			pd_aggregated= pd_aggregated.reset_index(drop=True)
			pd_aggregated.shape
			logging.info("pd_aggregated")
			logging.info(pd_aggregated.shape)

			#This is the path for the street grid file. Please update this to reflect the correct path in future builds.
			STREET_grids = self.data.streetsInspectorDemand
			STREET_grids.columns  = map(str.upper, STREET_grids.columns)

			logging.info('STREET_grids parsed\n')
			logging.info("STREET_grids")
			logging.info(STREET_grids.shape)

			if len(pd_aggregated[pd_aggregated['GEOMETRY_X'].isnull()])!=0:
				logging.info("Error!, stop the run")
			else:
				pass

			# Converting the dataframe into a geopandas dataframe
			pd_aggregated = pd_aggregated[pd_aggregated['GEOMETRY_X'].notna()]
			pd_aggregated['GEOMETRY_X'] = pd_aggregated['GEOMETRY_X'].astype(str)
			pd_aggregated['GEOMETRY_X'] = pd_aggregated['GEOMETRY_X'].apply(wkt.loads)
			pd_aggregated_new = gpd.GeoDataFrame(pd_aggregated, geometry='GEOMETRY_X',crs='epsg:4326')

			pd_aggregated['Overall_score'] = (pd_aggregated['FINAL_SCORE_EXCAVATIONBARRIERS'] +
											pd_aggregated['FINAL_SCORE_GENERALCLEANING'] +
											pd_aggregated['FINAL_SCORE_POTHOLES'] +
											pd_aggregated['FINAL_SCORE_SIDEWALKS'] +
											pd_aggregated['FINAL_SCORE_STREETLIGHTS'] )/5

			#Getting the RBD scores at a grid level
			new_pd_aggregated = pd.DataFrame(pd_aggregated.groupby(['BASE_GRID_NUMBER','DN_X']).agg('mean')['Overall_score'])
			new_pd_aggregated = new_pd_aggregated.reset_index()
			RBD_grid_and_scores = new_pd_aggregated[['BASE_GRID_NUMBER', 'DN_X', 'Overall_score']]
			unique_grids = RBD_grid_and_scores['BASE_GRID_NUMBER'].nunique()

			#Reading the streets grid file
			agg_street_grids_final = STREET_grids

			logging.info("agg_street_grids_final.shape")
			logging.info(agg_street_grids_final.shape)
			#Merging RBD outputs with street files
			grid_with_streets = pd.merge(RBD_grid_and_scores,agg_street_grids_final, left_on = 'BASE_GRID_NUMBER',right_on='GRIDNUMBER', how='inner')
			total_street_length = grid_with_streets.groupby('GRIDNUMBER').agg('sum')['ROADLENGTH_NEW']

			if grid_with_streets['GRIDNUMBER'].nunique()!=total_street_length.shape[0]:
				logging.info("!!Error, please stop the run")
			else:
				pass
			logging.info("RBD_grid_and_scores.shape")
			logging.info(RBD_grid_and_scores.shape)
			RBD_grid_and_scores = RBD_grid_and_scores.set_index('BASE_GRID_NUMBER')
			RBD_grid_and_scores['RoadLength'] = total_street_length

			if RBD_grid_and_scores.shape[0]!=unique_grids:
				logging.info("!Error, please stop the run")
			else:
				pass

			logging.info("RBD_grid_and_scores.shape")
			logging.info(RBD_grid_and_scores.shape)
			logging.info("total_street_length.sum()")
			logging.info(total_street_length.sum())

			rbdDatadata = RBD_grid_and_scores.copy()
			rbdDatadata['RoadLength'] = rbdDatadata['RoadLength'].replace(np.NaN, 0)
			logging.info('rbdDatadata[\'RoadLength\'].sum()/1000')
			logging.info(rbdDatadata['RoadLength'].sum()/1000)


			# ### Engine logic to deal with grids that dont have streets
			#
			# In this section, we look at the grids that dont have streets and apply the following resolution:
			# 1. If the population in a particular grid is greater than the 75th population of the entire rbdDatadata, we assign the 75th percentile  street lenght of grids with roads.
			# 2. If the population is lower than the median population of grids without roads, we assign a default value of 100 meters. This assumptions is derived from the following understanding:
			#     - A 800 meter road can run through a total of 8 empty grids (4 grids on each side). This essentially means that we traverse 100 meters per grid.

			seventyfifth_population = rbdDatadata['DN_X'].describe()['75%']
			seventyfifth_population_value = rbdDatadata[rbdDatadata['RoadLength']>=0.0]['RoadLength'].describe()["75%"]

			def get_resolution_distance(row):
				if row['RoadLength'] == 0.0:
					if row['DN_X']>seventyfifth_population:
						return seventyfifth_population_value
					return config.DEFAULT_STREET_LENGTH
				return row['RoadLength']
			rbdDatadata['RoadLength'] = rbdDatadata.apply(lambda x: get_resolution_distance(x), axis=1)
			rbdDatadata = rbdDatadata.reset_index()


			rbdDatadata = pd.merge(rbdDatadata, shpGrid, how='left',left_on='BASE_GRID_NUMBER', right_on = 'GRIDNUMBER')
			logging.info('rbdDatadata.shape')
			logging.info(rbdDatadata.shape)

			#Change geometry to lower case
			rbdDatadata = rbdDatadata.rename(columns={'GEOMETRY':'geometry'})

			amana_level = gpd.sjoin(gpd.GeoDataFrame(rbdDatadata), combined_df,how='inner', predicate='intersects')
			logging.info('amana_level.shape')
			logging.info(amana_level.shape)
			logging.info('amana_level[\'GRIDNUMBER\'].nunique()')
			logging.info(amana_level['GRIDNUMBER'].nunique())

			logging.info('amana_level[\'geometry\'].nunique()')
			logging.info(amana_level['geometry'].nunique())

			logging.info('len(combined_df[\'SUBMUNIC_3\'].unique())')
			logging.info(len(combined_df['SUBMUNIC_3'].unique()))

			logging.info('len(amana_level[\'SUBMUNIC_3\'].unique())')
			logging.info(len(amana_level['SUBMUNIC_3'].unique()))

			amana_level.columns  = map(str.upper, amana_level.columns)
			amana_level = amana_level.rename(columns={'GEOMETRY':'geometry'})
			amana_level = amana_level[['MUNICIPALI','SUBMUNIC_3','GRIDNUMBER', 'geometry', 'DN','OVERALL_SCORE','ROADLENGTH']].drop_duplicates()

			#Assign priority Areas
			gpd_aggregated = self.gisdata.priorityAreas
			amana_level[['SUBMUNIC_3','GRIDNUMBER','DN','OVERALL_SCORE','ROADLENGTH', 'geometry']]['GRIDNUMBER'].nunique()
			priority_area_grids = gpd.sjoin(amana_level[['SUBMUNIC_3','GRIDNUMBER','DN','OVERALL_SCORE','ROADLENGTH', 'geometry']],gpd_aggregated, how="inner",predicate="intersects")
			priority_area_grids = priority_area_grids.reset_index(drop=True)
			idx = priority_area_grids[['SUBMUNIC_3','GRIDNUMBER','DN','ROADLENGTH']].drop_duplicates().index
			priority_area_grids = priority_area_grids.loc[idx,['SUBMUNIC_3','GRIDNUMBER','DN','ROADLENGTH','geometry','OVERALL_SCORE']].reset_index(drop=True)
			logging.info('priority_area_grids.shape')
			logging.info(priority_area_grids.shape)


			# ### Inspector Calculation Logic
			#
			# This section deals with a lot of configurable variables. Please ensure to update the values for these variables in the configuration file before running it

			# #### Create a Priority Area Flag
			priority_grids = list(priority_area_grids['GRIDNUMBER'].unique())

			def get_priority(row):
				if row['GRIDNUMBER'] in (priority_grids):
					return 1
				else:
					return 0

			amana_level['PRIORITY_FLAG'] = amana_level.apply(lambda x: get_priority(x), axis=1)
			amana_level['PRIORITY_FLAG']==1
			logging.info('len(amana_level[(amana_level[\'ROADLENGTH\']==0) & (amana_level[\'PRIORITY_FLAG\']==1)] )')
			logging.info(len(amana_level[(amana_level['ROADLENGTH']==0) & (amana_level['PRIORITY_FLAG']==1)] ))

			amana_level[amana_level['PRIORITY_FLAG']==1][amana_level[amana_level['PRIORITY_FLAG']==1]['ROADLENGTH']==0]
			logging.info('amana_level.shape')
			logging.info(amana_level.shape)

			logging.info('amana_level[\'GRIDNUMBER\'].nunique()')
			logging.info(amana_level['GRIDNUMBER'].nunique())


			# ### !!! In the current shape file, the grids overlap which causes a double counting of grids and in turn a double counting of inspectors. The new shapefile will solve this problem.
			logging.info("KSA_priority")


			#logging.info(KSA_priority.shape)
			KSA_priority = amana_level[['MUNICIPALI','SUBMUNIC_3','GRIDNUMBER','DN','OVERALL_SCORE','ROADLENGTH', 'PRIORITY_FLAG']].drop_duplicates()
			KSA_priority['Average Time per Square Kilometer'] = KSA_priority['ROADLENGTH'].apply(lambda x:(x/1000)/config.INSPECTOR_SPEED)
			KSA_priority['Inspectors_without_risk'] = KSA_priority['Average Time per Square Kilometer']/config.INSPECTOR_PRODUCTIVE_HOURS
			KSA_priority['FreF'] = KSA_priority.apply(lambda x : config.frequency_factor(x), axis=1)

			#Getting inspectors at a weekly level
			KSA_priority['Planned_Visits_Inspectors'] = KSA_priority['Inspectors_without_risk']*KSA_priority['FreF']/52
			KSA_priority['FreF'].value_counts()

			KSA_priority['AMANA_CODE'] = (KSA_priority['SUBMUNIC_3'].astype('str')).apply(lambda x: x[:3])
			#KSA_priority['AMANA_CODE'] = KSA_priority['AMANA_CODE'].astype('float').astype('int')
			KSA_priority = KSA_priority[['MUNICIPALI','AMANA_CODE','SUBMUNIC_3','GRIDNUMBER','DN','OVERALL_SCORE','ROADLENGTH', 'PRIORITY_FLAG','Planned_Visits_Inspectors']]
			logging.info(KSA_priority.shape)
			logging.info(KSA_priority.dtypes)
			#logging.info(KSA_priority.head(5))

			# ### Calculating Historic CRM cases at a Grid Level
			cases_grid = GDF_VP_cases.sjoin(shpGrid, how='left')
			cases_grid.dropna(inplace=True,axis=0)
			cases_grid['GRIDNUMBER'] = cases_grid['GRIDNUMBER'].astype('int')
			grid_wise_cases = cases_grid.groupby("GRIDNUMBER").agg('count')['PYID']
			logging.info('grid_wise_cases.shape')
			logging.info(grid_wise_cases.shape)

			KSA_priority = pd.merge(KSA_priority, grid_wise_cases, how='left',on='GRIDNUMBER')
			# KSA_priority = KSA_priority[['AMANA_CODE','SUBMUNIC_3','GridNumber','DN','Overall_score','ROADLENGTH', 'Priority_Flag']]
			priority_percentages = KSA_priority.groupby(['AMANA_CODE','PRIORITY_FLAG']).agg('sum').xs(1,level='PRIORITY_FLAG')['PYID']/(KSA_priority.groupby(['AMANA_CODE','PRIORITY_FLAG']).agg('sum').xs(1,level='PRIORITY_FLAG')['PYID']+KSA_priority.groupby(['AMANA_CODE','PRIORITY_FLAG']).agg('sum').xs(0,level='PRIORITY_FLAG')['PYID'])
			priority_percentages = priority_percentages.rename("Priority_percentages")
			KSA_priority = pd.merge(KSA_priority,priority_percentages,on='AMANA_CODE', how='left')
			KSA_priority['Planned_Visits_Inspectors'].sum()
			logging.info('KSA_priority.shape 1')
			logging.info(KSA_priority.shape)

			KSA_priority = KSA_priority[['MUNICIPALI','AMANA_CODE', 'SUBMUNIC_3','GRIDNUMBER','PRIORITY_FLAG','Priority_percentages','Planned_Visits_Inspectors']].drop_duplicates()
			logging.info("KSA_priority")
			logging.info(KSA_priority.head(2))
			# ### Writing the file to the defined location
			self.features.update({"planned_visits_inspectors":KSA_priority})
			
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					fileName_KSA_priority = os.path.join(target_dir, "full_amana_{}.csv".format(config.PLANNED_INSPECTOR_FILE_NAME))
					if os.path.exists(fileName_KSA_priority):
						#new_fileName = os.path.join(target_dir, "full_amana_data_{}".format(config.PLANNED_INSPECTOR_FILE_NAME) + "_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						#os.rename(fileName_KSA_priority, new_fileName)
						KSA_priority.to_csv(fileName_KSA_priority, index=False)
					else:
						KSA_priority.to_csv(fileName_KSA_priority, index=False)
				else:
					os.mkdir(target_dir)
					fileName_KSA_priority = os.path.join(target_dir, "full_amana_{}.csv".format(config.PLANNED_INSPECTOR_FILE_NAME))
					KSA_priority.to_csv(fileName_KSA_priority, index=False)
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				fileName_KSA_priority = os.path.join(target_dir, "full_amana_{}.csv".format(config.PLANNED_INSPECTOR_FILE_NAME))
				KSA_priority.to_csv(fileName_KSA_priority, index=False)



	
		def Process(self, Helper : Helper):
			print("1")

			# ## Data initialization
			start_date_date = datetime.now()
			start_date = pd.Timestamp(str(Helper.AddMonths(start_date_date,- config.CRM_START_DATE)))
			end_date = pd.Timestamp(str(Helper.AddMonths(start_date_date,0)))

			start_date_year = start_date.year
			end_date_year = end_date.year

			# ### Combining Weather Data and bringing it to a weekly level

			#weather_1 = pd.read_csv(config.WEATHER_2021, index_col=[0])
			#weather_2 = pd.read_csv(config.WEATHER_2022, index_col=[0])
			weather_1 = self.data.WEATHER_2021
			weather_2 = self.data.WEATHER_2022

			weather_1['time'] =  pd.to_datetime(weather_1.time)
			weather_2['time'] =  pd.to_datetime(weather_2.time)
            #weather_2['time'].max()

			weather_future = weather_1.copy()
			weather_future['time'] = weather_1['time'] + pd.DateOffset(years=1)
			weather_2 = weather_2.append(weather_future[weather_future['time']>weather_2['time'].max()])
			weather = weather_1.append(weather_2)
            
            ### Ensuring Weather matches the year

			offset_years = start_date.year - weather_1['time'].dt.year[0]
			weather_1['time'] =  weather_1['time'] + pd.DateOffset(years=offset_years)

			offset_years = end_date.year - weather_2['time'].dt.year[0]
			weather_2['time'] =  weather_2['time'] + pd.DateOffset(years=offset_years)
			#offset_years

			weather = weather_1.append(weather_2)
			weather['amana_code'] = weather['amana_code'].astype('str')
			weather[weather['amana_code'].str.startswith("2")]

			def generate_zeroes(x):
				if (len(x)==4) or (len(x)==7):
					return "00"+x
				elif (len(x)==5) or (len(x)==8):
				    return "0"+x

			weather['amana_code'] = weather['amana_code'].apply(lambda x: generate_zeroes(x))
			weather = weather.sort_values(['amana_code','time'])

			weather.time = pd.to_datetime(weather.time)
			weather = weather.set_index("time")
			weather['is_haj'] = 0
			#weather.index

			### Updating the Haj Dates for year 1
			weather.loc[(weather.index>=pd.Timestamp('{}-07-17'.format(start_date_year))) & (weather.index<=pd.Timestamp('{}-07-22'.format(start_date_year))), 'is_haj'] = 1
			### Updating the Haj Dates for year 2
			weather.loc[(weather.index>=pd.Timestamp('{}-07-07'.format(end_date_year))) & (weather.index<=pd.Timestamp('{}-07-12'.format(end_date_year))), 'is_haj'] = 1
			### Creating Summer/Winter Based on dates
			weather['is_summer'] = 1
			weather.loc[(weather.index>=pd.Timestamp('{}-11-01'.format(start_date_year))) & (weather.index<=pd.Timestamp('{}-02-01'.format(end_date_year))), 'is_summer'] = 0
			weather_required = weather[(weather.index>= start_date)]
			#This will be used to generate the list of weather data in the order needed
			amana_names = list(weather_required['amana_code'].unique())
	
			# ## Gathering Data
			print("2")

			# Lets import the data for 52 weeks starting from **2021-06-01** and ending at **2022-08-01** from the files created in the first notebook
			#
			features = ["commercial","construction","excavation","cases","data_dictionary", config.PLANNED_INSPECTOR_FILE_NAME]
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# data_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			data_dir = os.path.join(base_dir, 'full_amana_data')

			#commercial_df =pd.DataFrame(self.features["commercial"]) 
			commercial_df = pd.read_csv(os.path.join(data_dir, "full_amana_data_{}.csv".format(features[0])))
			logging.info("commercial_df")
			logging.info(commercial_df.shape)
			#construction_df = pd.DataFrame(self.features["construction"]) 
			construction_df = pd.read_csv(os.path.join(data_dir, "full_amana_data_{}.csv".format(features[1])))
			logging.info("construction_df")
			logging.info(construction_df.shape)
			#excavation = pd.DataFrame(self.features["excavation"]) 
			excavation = pd.read_csv(os.path.join(data_dir, "full_amana_data_{}.csv".format(features[2])))
			logging.info("excavation")
			logging.info(excavation.shape)
			#cases = pd.DataFrame(self.features["cases"]) 
			cases = pd.read_csv(os.path.join(data_dir, "full_amana_data_{}.csv".format(features[3])))
			logging.info("cases")
			logging.info(cases.shape)
			#data_dictionary = pd.DataFrame(self.features["data_dictionary"]) 
			data_dictionary = pd.read_csv(os.path.join(data_dir, "full_amana_{}.csv".format(features[4])))
			logging.info("data_dictionary")
			logging.info(data_dictionary.shape)
			logging.info("10")
			logging.info("data_dictionary")
			logging.info(data_dictionary.head(2))
			#planned_inspectors = pd.DataFrame(self.features["planned_visits_inspectors"]) 
			planned_inspectors = pd.read_csv(os.path.join(data_dir, "full_amana_{}.csv".format(features[5])))
			logging.info("planned_inspectors")
			logging.info(planned_inspectors.shape)
			# ### Creation of Future Forecasts
			# Setting basic values
			#commercial_df = pd.DataFrame(commercial)
			#construction_df = pd.DataFrame(construction)
	
			# ### Retrieving number of Sub Municipalities
			num_sub_muni = len(construction_df.columns)

			# ### Generating Forecasts for Construction, Commercials and Weather
	
			#Generating Commercial DF
			#plt.figure(figsize=(100, 80))
			#print("commercial_df")
			commercial_models = {}
			final_commercials = []
			#print(commercial_df.head(2))
			for submunic in range(0,num_sub_muni):

				predictions = list()
				# print(str(submunic))
				# print(submunic)
				# print(commercial_df[str(submunic)])
				history = list(commercial_df[str(submunic)])
				test = []
				#     try:
				for t in range(13):
					model = ARIMA(history, order=(5,1,0))
					model.initialize_approximate_diffuse()
					model_fit_cons = model.fit()
					output = model_fit_cons.forecast()
					yhat = output[0]
					predictions.append(yhat)
					if t==0:
						if list(commercial_df[str(submunic)])[t] :
							obs = list(commercial_df[str(submunic)])[t]
							history.append(obs)
			#
						else:
							history.append(yhat)
					else:
						history.append(yhat)
				final_commercial = commercial_df[str(submunic)].append(pd.Series(predictions)).reset_index(drop=True)
				final_commercials.append(final_commercial)

				commercial_models.update({submunic:model_fit_cons})

			#plt.figure(figsize=(100, 80))
			construction_models = {}
			final_constructions = []
			for submunic in range(0,num_sub_muni):

				predictions = list()
				history = list(construction_df[str(submunic)])
				test = []
				#     try:
				for t in range(13):
					model = Holt(history)
					model_fit_cons = model.fit(smoothing_level=0.3, smoothing_slope=0.1)
					output = model_fit_cons.forecast()
					yhat = output[0]
					predictions.append(yhat)
					if t==0:
						if list(construction_df[str(submunic)])[t] :
							obs = list(construction_df[str(submunic)])[t]
							history.append(obs)
			#
						else:
							history.append(yhat)
					else:
						history.append(yhat)

				final_construction = construction_df[str(submunic)].append(pd.Series(predictions)).reset_index(drop=True)
				final_constructions.append(final_construction)

				construction_models.update({submunic:model_fit_cons})

			# ### Training Pipeline Prediction Pipeline
			results = {}
			results_mean = {}
			models = {}

			prediction_flag = config.PREDICTION_FLAG
			prediction_window = config.PREDICTION_WINDOW_IN_WEEKS
			prediction_window_in_months = config.PREDICTION_WINDOW_IN_MONTHS
			final_predictions = {}
			for submunic in range(0, num_sub_muni):

				
				train_data = pd.DataFrame()
				train_data['construction'] = final_constructions[submunic][:prediction_flag]
				train_data['commercial'] = final_commercials[submunic][:prediction_flag]
				train_data['cases'] = cases[str(submunic)].tolist()[:prediction_flag]
				train_data['cases-2lag'] = train_data['cases'].shift(2)

				#Generating Weather data at a weekly level
				amana_level = weather_required[weather_required['amana_code']==amana_names[submunic]]
				weather_week =amana_level.groupby([amana_level.index.year,amana_level.index.week]).agg('mean').reset_index(drop=True)

				train_data["avgtemp_f"] = weather_week['tavg'][:prediction_flag]
				train_data['is_haj'] = weather_week['is_haj'][:prediction_flag]
				train_data['is_summer'] = weather_week['is_summer'][:prediction_flag]


				test_data = pd.DataFrame()
				test_data['construction'] = final_constructions[submunic][prediction_flag:prediction_flag+prediction_window]
				test_data['commercial'] = final_commercials[submunic][prediction_flag:prediction_flag+prediction_window]
				test_data['is_haj'] = weather_week['is_haj'][prediction_flag:]
				test_data['is_summer'] = weather_week['is_summer'][prediction_flag:]
				test_data['is_haj'].fillna(0)
				test_data['is_summer'].fillna(1)
				test_data['avgtemp_f'] = weather_week['tavg'][prediction_flag:]
				test_cases_2lag = []
				test_cases_2lag.append(train_data['cases-2lag'].tolist()[-2])
				test_cases_2lag.append(train_data['cases-2lag'].tolist()[-1])


				lm = XGBRegressor()
				lm.fit(train_data.loc[:, train_data.columns!='cases'][2:].values.tolist(), train_data['cases'][2:])
				predictions = []
				for i in range(0,13):

					test_data_input = test_data.loc[52+i,:].tolist()
					test_data_input.append(test_cases_2lag.pop())
					prediction = lm.predict(np.array(test_data_input).reshape(1,-1))
					if prediction[0]<0:
						prediction[0] = 0
					test_cases_2lag.append(prediction[0])
					predictions.append(prediction[0])
				models.update({data_dictionary.iloc[submunic]['SUBMUNIC_3']:lm})
				final_predictions.update({data_dictionary.iloc[submunic]['SUBMUNIC_3']:[data_dictionary.iloc[submunic]['MUNICIPA_1'],predictions]})
				#print("final_predictions")
				#print(final_predictions.shape)
			Helper.backup(config.INSPECTOR_FORECASTING_FEATURES)
			Helper.insert_df_Batchwise( train_data, config.INSPECTOR_FORECASTING_FEATURES, 10000)
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					Final_CRM_Cases = os.path.join(target_dir, "Final_CRM_Cases.csv")
					if os.path.exists(Final_CRM_Cases):
						#new_fileName = os.path.join(target_dir, "Final_CRM_Cases_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						#os.rename(Final_CRM_Cases, new_fileName)
						pd.DataFrame(final_predictions).T.to_csv(Final_CRM_Cases)
					else:
						pd.DataFrame(final_predictions).T.to_csv(Final_CRM_Cases)
				else:
					os.mkdir(target_dir)
					Final_CRM_Cases = os.path.join(target_dir, "Final_CRM_Cases.csv")
					pd.DataFrame(final_predictions).T.to_csv(Final_CRM_Cases)
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				Final_CRM_Cases = os.path.join(target_dir, "Final_CRM_Cases.csv")
				pd.DataFrame(final_predictions).T.to_csv(Final_CRM_Cases)
				
			forecasted_crm_cases = pd.DataFrame(final_predictions).T

			# ## Generating Common Output Table

			# #### Combining the CRM Forecasts with Planned Inspections
			
			forecasted_crm_cases["Forecasted_CRM_Cases"] = forecasted_crm_cases[1].apply(lambda x: sum(x))
			forecasted_crm_cases = forecasted_crm_cases.rename(columns = {0:"Area_Name"})[['Area_Name','Forecasted_CRM_Cases']]
			forecasted_crm_cases = forecasted_crm_cases.reset_index().rename(columns={"index":"SUBMUNIC_3"})
			logging.info("forecasted_crm_cases")
			logging.info(forecasted_crm_cases.head(2))

		


			Helper.backup(config.Output_table_name_Final_CRM_Cases)
			Helper.insert_df_Batchwise( forecasted_crm_cases, config.Output_table_name_Final_CRM_Cases, 10000)
			logging.info("forecasted_crm_cases")
			logging.info(forecasted_crm_cases.shape)

			logging.info('planned_inspectors.shape')
			logging.info(planned_inspectors.shape)
			overall_inspections = pd.merge(planned_inspectors,forecasted_crm_cases,on='SUBMUNIC_3', how='left')
			print('overall_inspections.shape:' + str(overall_inspections.shape))
			overall_inspections['Priority_percentages'] = overall_inspections['Priority_percentages'].replace(np.NaN,0)
			overall_inspections = overall_inspections.groupby(['MUNICIPALI','AMANA_CODE','SUBMUNIC_3','PRIORITY_FLAG','Priority_percentages','Forecasted_CRM_Cases']).agg('sum')
			print('overall_inspections.shape:' + str(overall_inspections.shape))

			#planned_inspectors = self.features[5] #pd.read_csv(data_dir+"\\full_amana_{}.csv".format(features[5]))
			logging.info("planned_inspectors.shape")
			logging.info(planned_inspectors.shape)
			logging.info("planned_inspectors")
			logging.info(planned_inspectors.head(2))

			# #### Calculating Inspector Count for Planned Inspections in entire KSA
			# In section we calculate the inspector count for planned inspections and also apply the priority area weightage to Forecasted CRM cases
			KSA_planned_inspectors = overall_inspections.xs(0,level='PRIORITY_FLAG')['Planned_Visits_Inspectors'].add(overall_inspections.xs(1,level='PRIORITY_FLAG')['Planned_Visits_Inspectors'], fill_value=0)
			KSA_planned_inspectors = KSA_planned_inspectors.reset_index()
			KSA_planned_inspectors = KSA_planned_inspectors[['MUNICIPALI','AMANA_CODE','SUBMUNIC_3','Forecasted_CRM_Cases','Planned_Visits_Inspectors']]
			logging.info(KSA_planned_inspectors.shape)

			logging.info("KSA_planned_inspectors")
			logging.info(KSA_planned_inspectors.head(2))
			logging.info("1")

			# #### Calculating Inspector Count for Planned Inspections in Priority Areas
			# In section we calculate the inspector count for planned inspections and also apply the priority area weightage to Forecasted CRM cases
			Priority_planned_inspectors = overall_inspections.xs(1,level='PRIORITY_FLAG')['Planned_Visits_Inspectors']
			Priority_planned_inspectors = Priority_planned_inspectors.reset_index()
			Priority_planned_inspectors['Forecasted_CRM_Cases'] = Priority_planned_inspectors['Forecasted_CRM_Cases']*Priority_planned_inspectors['Priority_percentages']
			Priority_planned_inspectors = Priority_planned_inspectors[['MUNICIPALI','AMANA_CODE','SUBMUNIC_3','Forecasted_CRM_Cases','Planned_Visits_Inspectors']]
			logging.info(Priority_planned_inspectors.shape)
			logging.info("Priority_planned_inspectors")
			logging.info(Priority_planned_inspectors.head(2))
			# ### Calculating Total Number of Inspectors
			#Calculating total number of Inspectors
			logging.info("2")
			KSA_planned_inspectors['Forecasted_CRM_Inspectors'] = (KSA_planned_inspectors['Forecasted_CRM_Cases']/13)*(config.TIME_PER_VISIT/60)/config.INSPECTOR_PRODUCTIVE_HOURS
			#KSA_planned_inspectors['Total_Inspectors'] = KSA_planned_inspectors['Forecasted_CRM_Inspectors'] + KSA_planned_inspectors['Planned_Visits_Inspectors']
			#KSA_planned_inspectors['Total_Inspectors'] = KSA_planned_inspectors['Total_Inspectors'].astype(int)
			
			logging.info("3")
			KSA_planned_inspectors
			logging.info(KSA_planned_inspectors.shape)
			Priority_planned_inspectors['Forecasted_CRM_Inspectors'] = (Priority_planned_inspectors['Forecasted_CRM_Cases']/13)*(config.TIME_PER_VISIT/60)/config.INSPECTOR_PRODUCTIVE_HOURS
			#Priority_planned_inspectors['Total_Inspectors'] = Priority_planned_inspectors['Forecasted_CRM_Inspectors'] + Priority_planned_inspectors['Planned_Visits_Inspectors']
			#Priority_planned_inspectors['Total_Inspectors'] = Priority_planned_inspectors['Total_Inspectors'].astype(int)
			Priority_planned_inspectors
			logging.info("KSA_planned_inspectors")
			logging.info(KSA_planned_inspectors.head(2))
			logging.info(Priority_planned_inspectors.shape)
			logging.info("4")
			print('KSA_planned_inspectors.shape:' + str(KSA_planned_inspectors.shape))
			print('Priority_planned_inspectors.shape:' + str(Priority_planned_inspectors.shape))
			#Setting Index for each type
			Priority_planned_inspectors.index = ["Priority"]*len(Priority_planned_inspectors)
			KSA_planned_inspectors.index = ['KSA']*len(KSA_planned_inspectors)
			logging.info("5")
			df_Forecasted_Inspectors = pd.concat([KSA_planned_inspectors,Priority_planned_inspectors],axis=0).sort_values('SUBMUNIC_3')
			logging.info("6")
			print('df_Forecasted_Inspectors.shape:' + str(df_Forecasted_Inspectors.shape))
			logging.info(type(df_Forecasted_Inspectors.dtypes))
			#comm_lic_short['LATITUDE'] = comm_lic_short['LATITUDE'].apply(pd.to_numeric, errors = 'coerce')
			#df_Forecasted_Inspectors['Forecasted_CRM_Inspectors']  = df_Forecasted_Inspectors['Forecasted_CRM_Inspectors'].apply(lambda x: float(x))
			logging.info("7")
			#df_Forecasted_Inspectors['Planned_Visits_Inspectors']  = df_Forecasted_Inspectors['Planned_Visits_Inspectors'].apply(lambda x: float(x))
			logging.info("8")
			df_Forecasted_Inspectors['Total_Inspectors'] = df_Forecasted_Inspectors['Forecasted_CRM_Inspectors'] + df_Forecasted_Inspectors['Planned_Visits_Inspectors']
			logging.info("9")
			df_Forecasted_Inspectors['Total_Inspectors'] = np.ceil(df_Forecasted_Inspectors['Total_Inspectors'])
			df_Forecasted_Inspectors['Total_Inspectors'] = df_Forecasted_Inspectors['Total_Inspectors'].astype(int)
			df_Forecasted_Inspectors['Start_date'] = start_date_date
			df_Forecasted_Inspectors['Forecased_Months'] = config.PREDICTION_WINDOW_IN_MONTHS
			cwd = os.getcwd()
			logging.info("7")
			logging.info("df_Forecasted_Inspectors")
			#print('df_Forecasted_Inspectors.shape:' + str(df_Forecasted_Inspectors.shape))
			logging.info(df_Forecasted_Inspectors.head(2))
			print('df_Forecasted_Inspectors.shape:' + str(df_Forecasted_Inspectors.shape))



			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			if os.path.exists(base_dir):
				if os.path.exists(target_dir):
					Forecasted_Inspectors = os.path.join(target_dir, "Forecasted_Inspectors.csv")
					if os.path.exists(Forecasted_Inspectors):
						#new_fileName =  os.path.join(target_dir, "Forecasted_Inspectors_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
						#os.rename(Forecasted_Inspectors, new_fileName)
						df_Forecasted_Inspectors.to_csv(Forecasted_Inspectors)
					else:
						df_Forecasted_Inspectors.to_csv(Forecasted_Inspectors)
				else:
					os.mkdir(target_dir)
					Forecasted_Inspectors = os.path.join(target_dir, "Forecasted_Inspectors.csv")
					df_Forecasted_Inspectors.to_csv(Forecasted_Inspectors)
			else:
				os.mkdir(base_dir)
				os.mkdir(target_dir)
				Forecasted_Inspectors = os.path.join(target_dir, "Forecasted_Inspectors.csv")
				df_Forecasted_Inspectors.to_csv(Forecasted_Inspectors)
			logging.info("8")
			print('df_Forecasted_Inspectors.shape:' + str(df_Forecasted_Inspectors.shape))
			Helper.backup(config.Output_table_name_Forecasted_Inspectors)
			logging.info("9")
			#logging.info(df_Forecasted_Inspectors.shape)
			cwd = os.getcwd()
			# base_dir = cwd+"\\amanas_data"
			# target_dir = base_dir+"\\full_amana_data"
			base_dir = os.path.join(cwd, 'amanas_data')
			target_dir = os.path.join(base_dir, 'full_amana_data')
			Forecasted_Inspectors_csv = pd.read_csv(os.path.join(target_dir, "Forecasted_Inspectors.csv"))	
			Forecasted_Inspectors_csv.rename(columns = {'Unnamed: 0' : 'PRIORITY_FLAG', 'SUBMUNIC_3' : 'CombinedCode', 'MUNICIPALI' :'MUNICIPALITY' }, inplace=True)
			Forecasted_Inspectors_csv.columns  = map(str.upper, Forecasted_Inspectors_csv)
			print('Forecasted_Inspectors_csv.shape:' + str(Forecasted_Inspectors_csv.shape))

			#Forecasted_Inspectors_csv['SUBMUNICIPALITY'] = Forecasted_Inspectors_csv['COMBINEDCODE'].apply(lambda x: x if len(str(x)) > 5 else np.NaN)
			#Forecasted_Inspectors_csv['SUBMUNICIPALITY'] = Forecasted_Inspectors_csv['SUBMUNICIPALITY'].replace(np.NaN,0)
			#Forecasted_Inspectors_csv['SUBMUNICIPALITY'] = Forecasted_Inspectors_csv['SUBMUNICIPALITY'].astype(int)
			Forecasted_Inspectors_csv['SUBMUNICIPALITY'] = Forecasted_Inspectors_csv['COMBINEDCODE']
			Forecasted_Inspectors_csv['MUNICIPALITY_AR'] = pd.merge(Forecasted_Inspectors_csv,data_dictionary, left_on = 'MUNICIPALITY',right_on='SUBMUNIC_3', how='left')['MUNICIPA_1']
			Forecasted_Inspectors_csv['SUBMUNICIPALITY_AR'] = pd.merge(Forecasted_Inspectors_csv,data_dictionary, left_on = 'SUBMUNICIPALITY',right_on='SUBMUNIC_3', how='left')['MUNICIPA_1']
			print('Forecasted_Inspectors_csv.shape:' + str(Forecasted_Inspectors_csv.shape))
			Forecasted_Inspectors_csv_file = os.path.join(target_dir, "Forecasted_Inspectors.csv")
			if os.path.exists(Forecasted_Inspectors_csv_file):
				#new_fileName =  os.path.join(target_dir, "Forecasted_Inspectors_csv_" + self.filesDumpTime.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")
				#os.rename(Forecasted_Inspectors_csv_file, new_fileName)
				Forecasted_Inspectors_csv.to_csv(Forecasted_Inspectors_csv_file)
			else:
				Forecasted_Inspectors_csv.to_csv(Forecasted_Inspectors_csv_file)
			logging.info("Forecasted_Inspectors_csv")
			logging.info(Forecasted_Inspectors_csv.shape)


			Helper.insert_df_Batchwise( Forecasted_Inspectors_csv, config.Output_table_name_Forecasted_Inspectors,10000)
			logging.info("10")



