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
from datetime import datetime
import config  # username / password
import logging
import statistics

#Importing libraries with acronyms
import numpy as np
import pandas as pd
import geopandas as gpd
import geopandas

from classes.engines import Helper as Help
Helper=Help.Helper()
from sklearn.preprocessing import MinMaxScaler
#Importing Specific Functions and Modules
from shapely import wkt
from datetime import date
from datetime import datetime
from sklearn import linear_model
from shapely import geometry, ops
from geopandas.tools import sjoin
from shapely.geometry import shape
#from matplotlib.pyplot import figure
from sklearn.metrics import r2_score
#from hijri_converter import Hijri, Gregorian
from sklearn.metrics import mean_absolute_error
#from statsmodels.tsa.arima.model import ARIMA
from statistics import mean
from xgboost import XGBRegressor
#from statsmodels.tsa.api import ExponentialSmoothing,SimpleExpSmoothing, Holt
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_percentage_error

from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
from classes import Database as DB
data=DB.Database()
from classes import Convert_Gpd as gpd
from classes.Score import feature_score

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
warnings.filterwarnings("ignore")

def clean_grouphousing():
	crm_data = data.getCrmCases()
	GH_Inspection = data.getInspectionsData()
	print(GH_Inspection.shape)
	GH_Licenses = data.getLicensesData()
	crm_data.columns = crm_data.columns.str.upper()
	crm_data.rename(columns={"CATEGORY": "Category", "IS_CONTRACTOR": "IS_Contractor", "SATISFACTION": "Satisfaction"},
					inplace=True)
	crm_data = crm_data.dropna(subset=['LATITUDE', 'LONGITUDE'])
	crm_data.rename(columns={'PYID': 'CaseId'}, inplace=True)
	crm_data.drop_duplicates(inplace=True)
	print(crm_data.columns)
	# Dropping all the missing values present in Latitude and Longitude columns of the dataset
	crm_data = crm_data.dropna(subset=['LATITUDE', 'LONGITUDE'])
	# direc = 'C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\7. Group Housing\\0. Raw data\\'
	# class_config = pd.read_excel(direc + 'Translated Mapping doc_arabic_1.xlsx')
	class_config = data.getTranslatedMappingDocument()
	# Filtering for censorship type - Market and Health Control
	class_gh = class_config.loc[class_config['censorship type'] == 'الرقابة على الحفريات']

	# Filtering for all the Main classification, Subcategory and Specialty classification adjoining to that censorship type
	crm_gh = crm_data.loc[crm_data.MAIN_CLASSIFICAION.isin(class_gh['main classification'])
						  & crm_data.SP_CLASSIFICAION.isin(class_gh['specialty classification'])]

	crm_gh = gpd.convert_gpd(crm_gh, x=crm_gh.LONGITUDE, y=crm_gh.LATITUDE)
	crm_gh = crm_gh[crm_gh['SHORT_STATUS'] != 'Close']
	# Amana = geopandas.read_file(r'C:\Environment\VM_Files\MOMRAH_DATA\GIS\REQ1 AT Kerney 15082021\Amana.shp')
	Amana = gdata.getAMANA()
	# filtering for medinah
	# Amana_bound = Amana.loc[(Amana['AMANACODE'] == "003")]

	# Joining to extract all the CRM cases within Medinah
	gh_medinah = geopandas.sjoin(crm_gh, Amana, how="inner", op="intersects")
	gh_medinah.drop_duplicates(inplace=True)
	config.meta_data = gh_medinah[['CaseId', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME']]
	gh_medinah.drop(['OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
					 'SHAPE_AREA', 'SHAPE_LEN', 'index_right'], axis=1, inplace=True)
	# retail_medinah.to_crs(epsg= 32637, inplace = True)
	gh_medinah.to_crs(epsg='32637', inplace=True)
	high = 'حرج'
	medium = 'متوسط'
	low = 'عادي'
	gh_medinah['priority_value'] = np.where(gh_medinah.PRIORITY == high, 3,
											np.where(gh_medinah.PRIORITY == medium, 2, 1))

	# Converting Satisfaction to numerical values
	gh_medinah.Satisfaction.unique()
	gh_medinah['satisfaction_level'] = np.where(gh_medinah.Satisfaction == 'Dissatisfied', -1,
												np.where(gh_medinah.Satisfaction == 'Satisfied', 1, 0))
	# Calculating Days elapsed since creation of the case
	gh_medinah['days_elapsed'] = (pd.to_datetime('now') - pd.to_datetime(gh_medinah['PXCREATEDATETIME'])).dt.days

	base_data = gh_medinah[
		['CaseId', 'geometry', 'LATITUDE', 'LONGITUDE', 'priority_value', 'satisfaction_level', 'days_elapsed']]
	# Loading gridwise population shape file
	# shpGrid = geopandas.read_file(r'C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\7. Group Housing\\0. Raw data\\Shape files\\Standard_grids\\Population_grids.shp')
	shpGrid = gdata.getPOPULATION()
	shpGrid = shpGrid.rename(columns={'CHECK': 'GridUniqueCode'})

	# #Overlay the grid layer on the Madinah layer to fetch the grids relevant to Madinah region
	join = geopandas.sjoin(shpGrid, Amana, how="inner", op="intersects")

	# Creating the Grid dataset with grid numbers and the corresponding geometries
	grid_data = join[['GridUniqueCode', 'GridNumber', 'geometry', 'DN']]
	grid_data.to_crs(epsg=32637, inplace=True)
	# grid_data.head()

	# #Joining base data and grid data to get population for each geometries
	pop_data = geopandas.sjoin(base_data, grid_data, how='left', op='intersects')
	base_data1 = pop_data.drop(columns=['index_right', 'GridNumber', 'GridUniqueCode'])

	# Adding buffer to geometries of CRM data
	base_data_buffer = base_data1.copy()
	base_data_buffer = pop_data.drop(columns=['index_right', 'GridNumber', 'GridUniqueCode'])
	# Adding buffer to geometries of CRM data
	base_data_buffer.to_crs(epsg='32637')
	base_data_buffer['geometry'] = base_data_buffer.geometry.buffer(100)
	base_data_buffer.head()
	# priority_areas  = pd.read_csv(direc + 'madina_priority_areas.csv')
	priority_areas = gdata.getPriorityAreasData()
	# priority_areas.drop(columns = ['Unnamed: 0'],inplace = True)
	priority_areas = gpd.convert_gpd(priority_areas)
	priority_areas.to_crs(epsg=32637, inplace=True)
	# Calculating the number of priority areas for each case
	prarea = geopandas.sjoin(base_data_buffer, priority_areas, how='left', op='intersects')
	prarea.drop(columns=['index_right'], inplace=True)
	# getting all the priority areas within the vicinity of a case
	print(prarea.columns)
	preadf = prarea.groupby('CaseId')['Name'].nunique().to_frame(name='no_of_priority_areas').reset_index()

	# preadf.to_csv("test_main.csv")
	# print("output generated")
	print(datetime.now())

	base_data2 = pd.merge(base_data1,preadf,on = 'CaseId')
	print('base_data2')
	print(base_data2.shape)
	base_data2.drop_duplicates(inplace = True)

	cwd = os.path.dirname(__file__)
	base_dir = os.path.join(cwd, '../data')
	target_file = os.path.join(base_dir, 'Region_Desc.shp')
	#print(target_file)
	shpRegions = geopandas.read_file(target_file)
	#print(shpRegions.head(2))
	shpRegions.to_crs(epsg=32637, inplace=True)
	landuse = shpRegions[['landuse','geometry']]
	# landuse.landuse.unique()
	landuse['landuse_priority'] = np.where(landuse.landuse == 'commercial',1,np.where(landuse.landuse == 'residential',2,np.where(landuse.landuse == 'industrial',3,4)))
	# landuse.head()

	landuse_join = geopandas.sjoin(base_data_buffer,landuse,how="left",predicate="intersects")
	print('landuse_join')
	print(landuse_join.shape)
	landuse_join.drop(columns =['index_right'],inplace = True)
	landuse_join.drop_duplicates(inplace= True)

	landuse_df = landuse_join[['CaseId','landuse_priority']]
	landuse_df.groupby('CaseId')['landuse_priority'].nunique().to_frame(name = 'count').sort_values(by = ['count'],ascending= False).reset_index()
	landuse_df1 = landuse_df.groupby('CaseId')['landuse_priority'].first().to_frame(name = 'landuse_priority').reset_index()
	print('landuse_df1')
	print(landuse_df1.shape)
	landuse_df1.drop_duplicates(inplace = True)
	# landuse_df1.head()

	base_data3 = pd.merge(base_data2,landuse_df1,on = 'CaseId')
	print('base_data3')
	print(base_data3.shape)
	base_data3.drop_duplicates(inplace = True)

	balady = GH_Licenses.copy()
	balady = balady.drop_duplicates()

	balady = balady.drop(columns = ['Order Number','Application Date','Beneficiary','Beneficiary ID','Applicant',
									'Applicant Mobile No',
									'Order Status','HR_Path_Type'])

	balady = balady.rename(columns={'x':'X', 'y':'Y','license_id':'LICENSE_ID','area' : 'AREA', 'estimated_capacity': 'ESTIMATED_CAPACITY', 'rooms_count':'ROOMS_COUNT', 'toilets_count':'TOILETS_COUNT'  })

	#Converting the dataframe into geodataframe
	balady_g = gpd.convert_gpd(balady,x = balady.X, y = balady.Y)
	balady_g = balady_g.to_crs(epsg = '32637')

	balady_g = balady_g[['LICENSE_ID','Amana','AREA','ESTIMATED_CAPACITY','ROOMS_COUNT','TOILETS_COUNT','geometry']]

	balady_g_m = balady_g

	print('balady_g_m')
	print(balady_g_m.shape)

	balady_crm = geopandas.sjoin(base_data_buffer, balady_g_m, how = 'inner', predicate = 'intersects')
	balady_crm = balady_crm.drop_duplicates()

	print('balady_crm')
	print(balady_crm.shape)

	#Identifying number of group housing licenses in vicinity
	df = balady_crm.groupby('CaseId')['LICENSE_ID'].nunique().to_frame(name = 'no_of_gh_licenses_vicinity').reset_index()
	print('df')
	print(df.shape)

	base_data4 = pd.merge(base_data3, df,how = 'left', on = 'CaseId')
	print('base_data4')
	print(base_data4.shape)
	#For feature - Area
	balady_crm['LICENSE_ID'] = balady_crm['LICENSE_ID'].astype(str)
	df1 = balady_crm[['CaseId','LICENSE_ID', 'AREA']]
	df1 = df1.rename(columns = {'AREA':'Area','LICENSE_ID':'LicenseId'})

	base_data5 = pd.merge(base_data4, df1, how = 'left',on = 'CaseId')
	print('base_data5')
	print(base_data5.shape)

	#Taking Inspections data into account
	momthathel = GH_Inspection.copy()
	momthathel.columns  = map(str.upper, momthathel.columns)

	#Filtering Completed Inspections
	Status = ['Resolved-Completed', 'Under Review and Approval',
		'Resolved-Withdrawn','Resolved-NoViolations']

	momthathel = momthathel[momthathel['STATUS_OF_WORK'].isin(Status)]

	#Filtering not null licenses
	momthathel = momthathel[pd.notnull(momthathel['LICENSE_NUMBER'])]

	#Renaming columns in Inspections and License data
	momthathel = momthathel.rename(columns = {'LICENSE_NUMBER':'LicenseId'})
	balady_g_m = balady_g_m.rename(columns = {'LICENSE_ID':'LicenseId'})
	balady_g_m['LicenseId'] = balady_g_m['LicenseId'].astype(str)
	momthathel['LicenseId'] = momthathel['LicenseId'].astype(str)


	#Joining Momthathel data with Balady data
	balady_mothathel = pd.merge(balady_g_m, momthathel,how = 'left',on = 'LicenseId')
	print('balady_mothathel')
	print(balady_mothathel.shape)
	balady_momththel = balady_mothathel.drop_duplicates()

	balady_mothathel = balady_mothathel.rename(columns = {'NUMBER_OF_CLAUSES':'Number of clauses', 'NUMBER_OF_COMPLIANT_CLAUSES': 'Number of compliant clauses'})

	balady_mothathel[['Number of compliant clauses','Number of clauses']] = balady_mothathel[['Number of compliant clauses','Number of clauses']].fillna(0)
	balady_mothathel['Number of compliant clauses'] = balady_mothathel['Number of compliant clauses'].astype(int)
	balady_mothathel['Number of clauses'] = balady_mothathel['Number of clauses'].astype(int)

	balady_mothathel = balady_mothathel.rename(columns = {'Number of compliant clauses':'Number_of_compliant_clauses'})
	balady_mothathel = balady_mothathel.rename(columns = {'Number of clauses':'Number_of_clauses'})

	balady_mothathel['proportion_of_compliant_clauses'] = balady_mothathel[['Number_of_compliant_clauses','Number_of_clauses']].apply(pd.to_numeric,errors ='coerce').eval('Number_of_compliant_clauses/Number_of_clauses').fillna(0).replace({np.inf : 0})
	print(balady_mothathel.shape)

	balady_mothathel = balady_mothathel.rename(columns = {'INSEPECTION_ID':'INSEPECTION ID','INSPECTION_DATE':'Inspection Date', 'ISSUED_FINE_AMOUNT':'Issued fine amount'})

	balady_momthathel = balady_mothathel[['LicenseId','AREA','geometry','INSEPECTION ID','Inspection Date','Issued fine amount','proportion_of_compliant_clauses']]
	print('balady_momthathel 2')
	print(balady_momthathel.shape)

	#For feature - days elapsed since last inspection
	insp_datedf = balady_momthathel.groupby('LicenseId')['Inspection Date'].max().to_frame(name = 'last_inspection_date').reset_index()
	insp_datedf['last_inspection_date'] = pd.to_datetime(insp_datedf['last_inspection_date']).dt.date
	insp_datedf['days_elapsed_last_inspection'] = (pd.to_datetime('now') - pd.to_datetime(insp_datedf['last_inspection_date'])).dt.days
	print('insp_datedf')
	print(insp_datedf.shape)

	balady_momthathel1 = pd.merge(balady_momthathel,insp_datedf, on = 'LicenseId')
	balady_momthathel1 = balady_momthathel1[['LicenseId','geometry','Issued fine amount',
											'proportion_of_compliant_clauses','days_elapsed_last_inspection']]
	balady_momthathel1 = balady_momthathel1.drop_duplicates()
	print('balady_momthathel1')
	print(balady_momthathel1.shape)

	base_data5['LicenseId'] = base_data5['LicenseId'].astype(str)
	balady_momthathel1['LicenseId'] = balady_momthathel1['LicenseId'].astype(str)

	balady_momthathel1 = balady_momthathel1.drop(columns = ['geometry'])

	base_data6 = pd.merge(base_data5, balady_momthathel1,how = 'left', on = 'LicenseId')
	print('base_data6')
	print(base_data6.shape)

	base_data7 = base_data6.copy()
	base_data7 = base_data7.drop(columns = 'LicenseId')
	base_data7 = base_data7.fillna(0)
	base_data7 = base_data7.drop_duplicates()
	base_data7.columns = base_data7.columns.str.lower()
	base_data7 = base_data7.rename (columns = {'dn':'population','Issued fine amount': 'issued_fine_amount'})
	print('base_data7')
	print(base_data7.shape)

	df = base_data7.copy()
	#df.to_csv("cleaned.csv")

	df = df.rename (columns = {'issued fine amount':'issued_fine_amount'})
	#Running a loop over all the numeric features for feature scoring
	features_list = ['days_elapsed', 'population', 'no_of_priority_areas',
		'landuse_priority','no_of_gh_licenses_vicinity', 'area', 'issued_fine_amount',
		'proportion_of_compliant_clauses', 'days_elapsed_last_inspection']

	for i in range(len(features_list)):
		feature_score(df,features_list[i])
		print(features_list[i])
	print('Done')

	#scoring for priority metric
	df['priority_value'] = df['priority_value'].fillna(1)
	df.loc[df['priority_value'] == 1, 'priority_value_score'] = 1
	df.loc[df['priority_value'] == 2, 'priority_value_score'] = 2
	df.loc[df['priority_value'] == 3, 'priority_value_score'] = 3

	#indexing the feature
	df['Priority_score']=(df['priority_value_score'])/statistics.mean(df['priority_value_score'])

	#For Satisfaction level feature
	#scoring for satisfaction_level metric

	df.loc[df['satisfaction_level'] == 1, 'satisfaction_score'] = 0.2
	df.loc[df['satisfaction_level'] == 0, 'satisfaction_score'] = 0.3
	df.loc[df['satisfaction_level'] == -1, 'satisfaction_score'] = 0.5

	#indexing the feature
	df['Customer_score']=(df['satisfaction_score'])/statistics.mean(df['satisfaction_score'])

	#filling NAs for blank score columns

	features_list = ['days_elapsed_score','population_score','no_of_priority_areas_score','landuse_priority_score',
					'no_of_gh_licenses_vicinity_score','area_score','issued_fine_amount_score',
					'proportion_of_compliant_clauses_score','days_elapsed_last_inspection_score','Priority_score','Customer_score']

	df[features_list] = df[features_list].fillna(0)
	# ----------------------------------------------------------------------------------------------------------------------
	#Feature scaling to standardize the range of each feature
	scaler = MinMaxScaler()
	df[features_list]= scaler.fit_transform(df[features_list])
	#df.head()

	#Final scoring of each case for risk

	#Weights for each feature as per their priority
	population_score = 2
	no_of_priority_areas_score = 1.5
	days_elapsed_score = 1.5
	priority_score = 2
	customer_score = 1.5
	landuse_priority_score = 1.5
	no_of_gh_licenses_vicinity_score = 1
	area_score = 1
	issued_fine_amount_score = 1
	proportion_of_compliant_clauses_score = 0.7
	days_elapsed_last_inspection_score = 0.5

	features_list = ['days_elapsed_score','population_score','no_of_priority_areas_score','landuse_priority_score',
					'no_of_gh_licenses_vicinity_score','area_score','issued_fine_amount_score',
					'proportion_of_compliant_clauses_score','days_elapsed_last_inspection_score','Priority_score','Customer_score']

	weights = {'population_score': population_score, 'no_of_priority_areas_score': no_of_priority_areas_score,
			'days_elapsed_score' : days_elapsed_score, 'landuse_priority_score': landuse_priority_score,
			'Priority_score' : priority_score , 'no_of_gh_licenses_vicinity_score' : no_of_gh_licenses_vicinity_score,
			'Customer_score' : customer_score, 'area_score' : area_score,
				'issued_fine_amount_score': issued_fine_amount_score,
			'proportion_of_compliant_clauses_score': proportion_of_compliant_clauses_score,
			'days_elapsed_last_inspection_score': days_elapsed_last_inspection_score}

	for i, j in weights.items():
		df[i] = j * df[i]

	#Bifurcating the features into two buckets - Visibility and Impact
	visibility_features = ['population_score','no_of_priority_areas_score','days_elapsed_score',
						'landuse_priority_score', 'no_of_gh_licenses_vicinity_score','area_score','issued_fine_amount_score',
					'proportion_of_compliant_clauses_score','days_elapsed_last_inspection_score']

	impact_features = ['Priority_score','Customer_score']

	#Calculating Final score for both Visibility and Impact Features
	df['Final_score_Visibility'] = df[visibility_features].sum(axis = 1)
	df['Final_score_Impact'] = df[impact_features].sum(axis = 1)

	#Scaling the final visibility and impact scores
	df[['Final_score_Visibility']] = scaler.fit_transform(df[['Final_score_Visibility']])
	df[['Final_score_Impact']] = scaler.fit_transform(df[['Final_score_Impact']])

	# BA team input to update the weights dynamically
	df_final = df.copy()
	df_final['Total_score']=df_final['Final_score_Impact']*.3 + df_final['Final_score_Visibility']*.7

	df_final['Total_score'] = ((df_final['Total_score'] - min(df_final['Total_score']))/(max(df_final['Total_score'])- min(df_final['Total_score'])))*100
	df_final = df_final.drop_duplicates()
	df_final=df_final.sort_values('Total_score', ascending=False)

	#Bringing all the score values to two decimal places
	scores_list = ['Total_score','Final_score_Visibility','Final_score_Impact','population_score','no_of_priority_areas_score',
				'days_elapsed_score', 'landuse_priority_score', 'no_of_gh_licenses_vicinity_score','area_score','issued_fine_amount_score',
					'proportion_of_compliant_clauses_score','days_elapsed_last_inspection_score',
				'Priority_score','Customer_score',
						]

	df_final[scores_list] = df_final[scores_list].astype('float').round(2)

	#Defining priroity classes based on Risk calculated
	df_final.loc[df_final['Total_score']>70, 'Total_score_classes']='Very High (70%-100%)'
	df_final.loc[df_final['Total_score']<=70, 'Total_score_classes']='High (40%-70%)'
	df_final.loc[df_final['Total_score']<=40, 'Total_score_classes']='Medium (10%-40%)'
	df_final.loc[df_final['Total_score']<=10, 'Total_score_classes']='Low (0%-10%)'

	df_final1 = df_final[['caseid', 'geometry', 'latitude', 'longitude',
			'priority_value', 'satisfaction_level', 'days_elapsed',
		'population', 'no_of_priority_areas', 'landuse_priority',
		'no_of_gh_licenses_vicinity', 'area', 'issued_fine_amount',
		'proportion_of_compliant_clauses', 'days_elapsed_last_inspection',
		'days_elapsed_score','population_score', 'no_of_priority_areas_score', 'landuse_priority_score', 'no_of_gh_licenses_vicinity_score',
		'area_score', 'issued_fine_amount_score', 'proportion_of_compliant_clauses_score', 'days_elapsed_last_inspection_score',
			'Priority_score', 'Customer_score', 'Final_score_Visibility', 'Final_score_Impact',
		'Total_score', 'Total_score_classes'

	]]

	df_final1=pd.merge(df_final1, config.meta_data, left_on='caseid', right_on='CaseId', how='left')
	df_final1.drop(['CaseId'], axis=1, inplace=True)
	df_final1.columns  = map(str.upper, df_final1.columns)
	Helper.backup(config.Output_table_name)
	#df_final1.to_csv("final.csv")
	Helper.insert_df_Batchwise( df_final1, config.Output_table_name,10000)
