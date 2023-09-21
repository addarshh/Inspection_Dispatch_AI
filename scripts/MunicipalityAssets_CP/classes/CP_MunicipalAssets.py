#!/usr/bin/env python
# coding: utf-8
#Importing libraries 
import pandas as pd 
import geopandas 
import numpy as np
import statistics
from classes import Database as data
DB=data.Database()
from classes import Convert_Gpd as gpd
from classes import MunicipalAssets_Deduplication as dedup
from classes import Score
import config
import glob
from shapely import wkt
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()

def crm_clean():
    # ***CRM Data***
    crm_data = DB.get_crm_data()
    print("got crm data municipal assets")
    crm_data.rename(columns={'latitude': 'LATITUDE', 'longitude': 'LONGITUDE','pxcreatedatetime':'PXCREATEDATETIME','interactiontype':'INTERACTIONTYPE','closure_date':'CLOSURE_DATE','short_status':'SHORT_STATUS','priority':'PRIORITY','submunic_3':'SUBMUNIC_3','pyid':'CaseId'}, inplace=True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace(' ', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace(' ', '', regex = True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace('\(', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace('\(', '', regex = True)
    crm_data['LATITUDE'] = crm_data['LATITUDE'].replace('\)', '', regex = True)
    crm_data['LONGITUDE'] = crm_data['LONGITUDE'].replace('\)', '', regex = True)
    
    crm_data.rename(columns={'PYID': 'CaseId'}, inplace=True)
    crm_data.drop_duplicates(inplace=True)
    crm_data = crm_data.dropna(subset = ['LATITUDE', 'LONGITUDE']) # 4328095
    crm_data = crm_data[crm_data['VISUAL POLLUTION CATEGORY'] != 'Not VP'] # 2821313
    crm_data = crm_data[crm_data['SHORT_STATUS'] != 'Close'] # 111577
    crm_data['PXCREATEDATETIME'] = pd.to_datetime(crm_data['PXCREATEDATETIME'])

    mapping = pd.read_excel(config.mapping_path)
    mapping[['BCG_rating_en']] = mapping[['BCG_rating_en']].apply(lambda x: x.str.strip())
    crm_mapped = pd.merge(crm_data, mapping, on = "SP_Classificaion", how='inner')
    
    crm_gc1 = gpd.convert_gpd(crm_mapped, x=crm_mapped.LONGITUDE, y=crm_mapped.LATITUDE)
    crm_gc1 = crm_gc1.to_crs(epsg='32637')
    # Filtering Duplicated Cases
    crm_data_sorted = crm_gc1.sort_values(by=['CaseId', 'Satisfaction'], ascending=[True, False])
    crm_data_sorted1 = crm_data_sorted.drop_duplicates(subset=['CaseId'], keep='last')
    
    crm_data_sorted1.columns = crm_data_sorted1.columns.str.replace(' ', '')
    crm_data_sorted1.rename(columns = {'BCG_rating_en':'VP_element'},inplace = True)
    
    crm_data_sorted1 = crm_data_sorted1[['CaseId','PXCREATEDATETIME','SHORT_STATUS', 'LATITUDE', 'LONGITUDE','PRIORITY','Satisfaction',
               'VP_element','Shock_wave','geometry']]
    print("finished crm municipal assets data transformation")
    return crm_data_sorted1

def clean_amana():
    crm_gc1=crm_clean()
    ma_ksa_cl = dedup.deduplication()
    #Filtering data further for Madinah 

    #Loading Amana information to get medina boundaries
    #Amana = geopandas.read_file(config.Amana_path)
    Amana=gdata.getAMANA()
    
    Amana_bound = Amana
    Amana_bound.to_crs(epsg = 32637, inplace = True)
    

    #Joining to extract all the CRM cases within medina
    ma_ksa = geopandas.sjoin(crm_gc1,Amana_bound, how = "inner", predicate = "intersects")
    ma_ksa.drop_duplicates(inplace = True)
    config.meta_data=ma_ksa[['CaseId', 'AMANAARNAM', 'AMANAENAME'
                            ]]
    # config.meta_data.rename(columns={'CaseId': 'CaseId'}, inplace=True)
    ma_ksa.drop(['OBJECTID', 'REGION', 'AMANAARNAM', 'AMANAENAME',
    'SHAPE_AREA', 'SHAPE_LEN','index_right'],axis = 1, inplace = True)
    ma_ksa.to_crs(epsg= 32637, inplace = True)

    ma_ksa = ma_ksa[['CaseId','Shock_wave','PXCREATEDATETIME','PRIORITY','Satisfaction', 'AMANACODE']]
    ksa_ma_cl = pd.merge(ma_ksa_cl,ma_ksa, on ='CaseId')
    ksa_ma_cl = ksa_ma_cl.drop(columns = ['LONGITUDE','LATITUDE'])
    ksa_ma_cl_g = gpd.convert_gpd(ksa_ma_cl, x = ksa_ma_cl.Cluster_Longitude, y = ksa_ma_cl.Cluster_Latitude)
    ksa_ma_cl_g.columns = ksa_ma_cl_g.columns.str.lower()
    ksa_ma_cl_g.rename(columns = {'pxcreatedatetime':'created_time'},inplace = True)
    ksa_ma_cl_g.drop_duplicates(inplace=True)
    ma_ksa = ksa_ma_cl_g.copy()
    
    high = 'حرج' 
    medium = 'متوسط'
    low = 'عادي'
    ma_ksa['priority_value'] = np.where(ma_ksa.priority == high,3,np.where(ma_ksa.priority == medium,2,1))
    
    #Converting Satisfaction catgeories to numerical values

    ma_ksa.satisfaction.unique() # Dissatisfied, Satisfied and No response
    ma_ksa['satisfaction_level'] = np.where(ma_ksa.satisfaction == 'Dissatisfied' ,-1,np.where(ma_ksa.satisfaction == 'Satisfied',1,0))
    
    #Calculating Days elapsed since creation of the case
    ma_ksa['days_elapsed'] = (pd.to_datetime('now') - pd.to_datetime(ma_ksa['created_time'])).dt.days
    ma_ksa['category_priority'] = np.where(ma_ksa.shock_wave == 1,3,np.where(ma_ksa.shock_wave == 2,2,1))
    
    base_data = ma_ksa[['amanacode','caseid','geometry','vp_element','cluster_latitude','cluster_longitude','cluster_label','priority_value','satisfaction_level','days_elapsed','category_priority']]


    return base_data


def features():
    base_data = clean_amana()
     
    ####Feature building for Popultaion Feature
    # Loading gridwise population shape file
    #shpGrid = geopandas.read_file(config.shpGrid_path)
    #shpGrid = shpGrid.rename(columns = {'CHECK':'GridUniqueCode'})
    shpGrid=gdata.getPopulationData()
    #Creating the Grid dataset with grid numbers and the corresponding geometries
    grid_data = shpGrid[['GridUniqueCode','geometry','DN']]
    # grid_data.to_crs(epsg = 32637, inplace = True)
    # grid_data.head()

    #Joining base data and grid data to get population for each geometries
    pop_data = geopandas.sjoin(base_data,grid_data,how = 'left', predicate = 'intersects')

    base_data1 = pop_data.drop(columns = ['index_right'])
    
    
    #Adding buffer to geometries of CRM data
    base_data_buffer = base_data1.copy()
    base_data_buffer = base_data_buffer.to_crs(epsg = '32637')
    base_data_buffer['geometry'] = base_data_buffer.geometry.buffer(200)



    ###Feature building for Priority areas
    # PRIORITYAREAS_shp = config.priority_areas_path
    # file_names = []
    # for file in glob.glob(PRIORITYAREAS_shp+r"\*.shp"):
    #     file_names.append(file)
    #
    # gpd_aggregated = pd.DataFrame()
    #
    # for file in file_names:
    #     temp = geopandas.read_file(file,  crs='epsg:32637')
    #     temp['file_name']=file
    #     gpd_aggregated = gpd_aggregated.append(temp)
    #
    # #Medina CSv Files
    # csv_files = []
    # for file in glob.glob(PRIORITYAREAS_shp+r"\*.csv"):
    #     csv_files.append(file)
    #
    # medina_areas = pd.read_csv(csv_files[0])
    # medina_areas['geometry'] = medina_areas['geometry'].apply(wkt.loads)
    # medina_areas = geopandas.GeoDataFrame(medina_areas, crs = 'EPSG:4326')
    # medina_areas['file_name'] ='Medina.csv'
    # medina_areas['PRIORITY'] =1
    #
    # gpd_aggregated = gpd_aggregated.append(medina_areas)
    # gpd_aggregated = gpd_aggregated.reset_index(drop=True)
    # gpd_aggregated = gpd_aggregated.to_crs('epsg:4326')
    # PriorityAreas = gpd_aggregated
    # PriorityAreas.to_crs(epsg=32637, inplace=True)
    #
    # PriorityAreas = PriorityAreas[(PriorityAreas.PRIORITY == 1)]
    PriorityAreas=gdata.getPriorityAreasData()


    # Calculating the number of priority areas for each case
    prarea = geopandas.sjoin(base_data_buffer,PriorityAreas,how = 'left', predicate= 'intersects')

    #getting all the priority areas within the vicinity of a case
    preadf = prarea.groupby('caseid')['index_right'].nunique().to_frame(name = 'no_of_priority_areas').reset_index()

    base_data2 = pd.merge(base_data1,preadf,on = 'caseid')
    base_data2.drop_duplicates(inplace = True)
    print("base data 2 :" + str(base_data2.shape))
 
    
    #Adding Landuse priority feature 

    #Load Shapefiles of landuse information obtained OSM analysis
    shpRegions = geopandas.read_file(config.shpRegions_path)
    shpRegions.to_crs(epsg=32637, inplace=True)
    landuse = shpRegions[['landuse','geometry']]
    landuse.landuse.unique()
    landuse['landuse_priority'] = np.where(landuse.landuse == 'commercial',1,np.where(landuse.landuse == 'residential',2,np.where(landuse.landuse == 'industrial',3,4)))


    landuse_join = geopandas.sjoin(base_data_buffer,landuse,how="left",predicate="intersects")
    landuse_join.drop(columns =['index_right'],inplace = True)
    landuse_join.drop_duplicates(inplace= True)


    landuse_df = landuse_join[['caseid','landuse_priority']]
    landuse_df.groupby('caseid')['landuse_priority'].nunique().to_frame(name = 'count').sort_values(by = ['count'],ascending= False).reset_index()
    landuse_df1 = landuse_df.groupby('caseid')['landuse_priority'].first().to_frame(name = 'landuse_priority').reset_index()
    landuse_df1.drop_duplicates(inplace = True)


    base_data3 = pd.merge(base_data2,landuse_df1,on = 'caseid')
    base_data3.drop_duplicates(inplace = True)
    print("base data 3 :" + str(base_data3.shape))
    
    
    # Adding count of repetitions feature  

    cluster_countdf=base_data3.groupby(['cluster_label','vp_element']).caseid.nunique().to_frame(name = 'count_of_cases').reset_index()

    #calculating only the repetitions
    cluster_countdf['count_of_repetitions'] = cluster_countdf['count_of_cases'] - 1
    cluster_countdf = cluster_countdf.drop(columns = ['count_of_cases'])


    base_data4 = pd.merge(base_data3,cluster_countdf, on = ['cluster_label','vp_element'])
    print("base data 4 :" + str(base_data4.shape))
    #Feature building for count of buildings
    bldg = pd.read_csv(config.bldg_path)
    bldg = bldg[['objectid', 'geometry']]
    bldg.rename(columns={'objectid':'OBJECTID'}, inplace=True)
    bldg=geopandas.GeoDataFrame(bldg , geometry=geopandas.GeoSeries.from_wkt(bldg.geometry, crs = 'epsg:4326'))

    bldg_crm = geopandas.sjoin(base_data_buffer, bldg, how = 'left', predicate = 'intersects')
    bldg_crm = bldg_crm.drop(columns = ['index_right'], axis = 1)

    bldg_df = bldg_crm.groupby(['cluster_label','vp_element'])['OBJECTID'].nunique().to_frame(name = 'count_of_buildings').reset_index()
    base_data5 = pd.merge(base_data4, bldg_df, on = ['cluster_label','vp_element'])
    print("base data 5 :"+str(base_data5.shape))
    #Feature building for Points of Interest data 

    poi = pd.read_excel(config.poi_path)

    poi_g = gpd.convert_gpd(poi, x = poi.longitude, y = poi.latitude)
    poi_g = poi_g.to_crs(epsg = 32637)
    poi_g = poi_g.drop_duplicates()

    poi_ksa = poi_g[['name', 'type', 'subtypes','category','latitude','longitude','geometry']]
    poi_ksa = poi_ksa.drop_duplicates()
    poi_ksa = poi_ksa.to_crs(epsg = 32637)

    poi_crm = geopandas.sjoin(base_data_buffer,poi_ksa,how = 'left', predicate = 'intersects')
    poi_crm = poi_crm.drop_duplicates()

    poi_df = poi_crm.groupby(['cluster_label','vp_element'])['name'].nunique().to_frame(name = 'count_of_pois').reset_index()

    base_data6 = pd.merge(base_data5, poi_df, on = ['cluster_label','vp_element'])
    
    base_data7 = base_data6.copy()
    base_data7.drop_duplicates(subset = ['caseid'],keep = 'first', inplace = True)
    base_data7 = base_data7.drop_duplicates()
    base_data7.fillna(0,inplace = True)
    base_data7.drop(columns = ['GridUniqueCode'],inplace = True)
    
    return base_data7, base_data_buffer


def featurebuilding_count():
    base_data7, base_data_buffer=features()
    df = base_data7
    print("feature building count")
    print(df.shape)
    print(df.columns)
    features_list = ['days_elapsed', 'DN', 'no_of_priority_areas',
       'count_of_repetitions','count_of_buildings',
       'count_of_pois']
    #print(df.isna.sum().sum())
    df[features_list].fillna(value=0, inplace=True)
    #df.to_csv("test.csv")
    for i in range(len(features_list)):
        col=features_list[i]
        #df[col]=pd.to_numeric(df[col])
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col]=df[col].fillna(value=0)
        Score.feature_score(df,features_list[i])


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
    
    df.loc[df['landuse_priority'] == 1, 'landuse_priority_score'] = 1
    df.loc[df['landuse_priority'] == 2, 'landuse_priority_score'] = 2
    df.loc[df['landuse_priority'] == 3, 'landuse_priority_score'] = 3
    df.loc[df['landuse_priority'] == 4, 'landuse_priority_score'] = 4
    df.loc[df['landuse_priority'] == 0, 'landuse_priority_score'] = 0
    #indexing the feature
    df['Landuse_score']=(df['landuse_priority_score'])/statistics.mean(df['landuse_priority_score'])
    
    #scoring for satisfaction_level metric

    df.loc[df['category_priority'] == 1, 'category_priority_score'] = 1
    df.loc[df['category_priority'] == 2, 'category_priority_score'] = 2
    df.loc[df['category_priority'] == 3, 'category_priority_score'] = 3
    df.loc[df['category_priority'] == 4, 'category_priority_score'] = 4
    df.loc[df['category_priority'] == 5, 'category_priority_score'] = 5
    #indexing the feature
    df['Category_score']=(df['category_priority_score'])/statistics.mean(df['category_priority_score'])
    
    
    #filling NAs for blank score columns

    features_list = ['days_elapsed_score','DN_score','no_of_priority_areas_score','Landuse_score',
                     'count_of_repetitions_score','count_of_pois_score','count_of_buildings_score',
                     'Category_score','Priority_score','Customer_score']

    df[features_list] = df[features_list].fillna(0)

    return df, features_list





