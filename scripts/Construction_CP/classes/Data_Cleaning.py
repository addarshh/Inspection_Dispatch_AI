from classes.Score import feature_score
import geopandas
import pandas as pd
import numpy as np
import statistics
from classes.Convert_Gpd import convert_gpd
from classes import Database as DB
from classes import GISDatabase as GDB
import config
gdata=GDB.GISDatabase()
data = DB.Database()
import os
def Clean_CRM():
    ##Importing entire CRM data and filtering for Retail inspection
    crm_data = data.get_crm_data()
    print('CRM data records:', crm_data.shape)
    # Cleaning the CRM data
    crm_data.columns = crm_data.columns.str.lower()
    # Renaming the necessary column names
    crm_data.rename(
        columns={'pyid': 'caseid', 'pxcreatedatetime': 'created_time', 'main_classificaion': 'main_classification',
                 'sub_classificaion': 'sub_classification', 'sp_classificaion': 'sp_classification'}, inplace=True)
    crm_data.drop_duplicates(inplace=True)
    # Dropping all the missing values present in Latitude and Longitude columns of the dataset
    crm_data = crm_data.dropna(subset=['latitude', 'longitude'])
    # Cleaning Config Data
    # Filtering for censorship type - construction Control
    ##Loading config file for classification types - which would help us filter data for construction inspection type
    class_config = pd.read_excel(config.translation_doc_path)
    # Filtering for censorship type - construction Control
    class_construction = class_config.loc[class_config['censorship type'] == 'الرقابة على المباني']
    # Filtering for all the Main classification, Subcategory and Specialty classification adjoining to that censorship type
    crm_construction = crm_data.loc[crm_data.main_classification.isin(class_construction['main classification'])
                                  & crm_data.sub_classification.isin(class_construction['Subcategory'])
                                  & crm_data.sp_classification.isin(class_construction['Specialty Classification'])]
    # Converting CRM construction into geopandas dataframe
    crm_construction = convert_gpd(crm_construction, x=crm_construction.longitude, y=crm_construction.latitude)
    crm_construction.to_crs(epsg=32637, inplace=True)
    print(crm_construction.shape)
    print("CRM completed"+"\n")
    return crm_construction

def Clean_Amana():
    # Cleaning Amana Data
    # Loading Amana information to get medina boundaries
    global Amana_bound
    crm_construction=Clean_CRM()
    #Amana_bound = geopandas.read_file(config.amana_shp_path)
    Amana_bound=gdata.getAMANA()
    Amana_bound = convert_gpd(Amana_bound)

    # filtering for medina
    #Amana_bound = Amana.loc[(Amana['AMANACODE'] == "003")]
    #Amana_bound=Amana.copy()
    Amana_bound.to_crs(epsg=32637, inplace=True)
    # Joining to extract all the CRM cases within medina
    construction_medina = geopandas.sjoin(crm_construction, Amana_bound, how="inner", predicate="intersects")
    construction_medina.drop_duplicates(inplace=True)
    config.meta_data=construction_medina[['caseid', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
                              ]]
    construction_medina.drop(['OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
                              'SHAPE_AREA', 'SHAPE_LEN', 'index_right'], axis=1, inplace=True)
    construction_medina.to_crs(epsg=32637, inplace=True)
    # Converting Priority to numerical values
    high = 'حرج'
    medium = 'متوسط'
    low = 'عادي'
    construction_medina['priority_value'] = np.where(construction_medina.priority == high, 3,np.where(construction_medina.priority == medium, 2, 1))
    # Converting Satisfaction to numerical values
    construction_medina['satisfaction_level'] = np.where(construction_medina.satisfaction == 'Dissatisfied', -1,np.where(construction_medina.satisfaction == 'Satisfied', 1, 0))
    # Calculating Days elapsed since creation of the case
    construction_medina['days_elapsed'] = (pd.to_datetime('now') - pd.to_datetime(construction_medina['created_time'])).dt.days
    base_data = construction_medina[['caseid', 'geometry', 'latitude', 'longitude', 'priority_value', 'satisfaction_level', 'days_elapsed']]
    print(base_data.shape)
    print("Amana completed"+"\n")
    return base_data

def Population_feature():
    ####Feature building for Population Feature
    # Loading gridwise population shape file
    global base_data_buffer
    base_data=Clean_Amana()
    #shpGrid = geopandas.read_file(config.population_grids_path)
    shpGrid = gdata.getPOPULATION()
    #shpGrid = convert_gpd(shpGrid)
    #shpGrid = shpGrid.dropna(axis=0, inplace=True)
    print('shpGrid.shape')
    print(shpGrid.shape)
    # Overlay the grid layer on the Madinah layer to fetch the grids relevant to Madinah region
    join = geopandas.sjoin(shpGrid, Amana_bound, how="inner", predicate="intersects")
    # Creating the Grid dataset with grid numbers and the corresponding geometries
    grid_data = join[['GridNumber', 'geometry', 'DN']]
    #grid_data.to_crs(epsg=32637, inplace=True)
    # Joining base data and grid data to get population for each geometries
    pop_data = geopandas.sjoin(base_data, grid_data, how='left', predicate='intersects')
    base_data1 = pop_data.drop(columns=['index_right', 'GridNumber'])
    # Adding buffer to geometries of CRM data
    base_data_buffer = base_data1.copy()
    base_data_buffer['geometry'] = base_data_buffer.geometry.buffer(200)
    print(base_data1.shape)
    print("population feature completed"+"\n")
    return base_data1

def Priorityareas_feature():

    ###Feature building for Priority areas
    # Importing Priority areas file
    base_data1=Population_feature()
    #priority_areas = pd.read_csv(config.madina_priority_path)
    priority_areas=gdata.getPriorityAreasData()
    priority_areas.columns = map(str.upper, priority_areas.columns)
    priority_areas = priority_areas.rename(columns={'GEOMETRY': 'geometry'})
    priority_areas=priority_areas.rename(columns={'AREANAME':'Name'})
    #priority_areas.drop(columns=['Unnamed: 0'], inplace=True)
    priority_areas = convert_gpd(priority_areas)
    priority_areas.to_crs(epsg=32637, inplace=True)
    # Calculating the number of priority areas for each case
    prarea = geopandas.sjoin(base_data_buffer, priority_areas, how='left', predicate='intersects')
    prarea.drop(columns=['index_right'], inplace=True)
    # getting all the priority areas within the vicinity of a case
    preadf = prarea.groupby('caseid')['Name'].nunique().to_frame(name='no_of_priority_areas').reset_index()
    base_data2 = pd.merge(base_data1, preadf, on='caseid')
    base_data2.drop_duplicates(inplace=True)
    print(base_data2.shape)
    print("priority areas completed"+"\n")
    return base_data2

def LandusePriority_Feature():
    # Adding Landuse priority feature
    # Load Shapefiles of landuse information obtained OSM analysis
    base_data2=Priorityareas_feature()
    shpRegions = geopandas.read_file(config.region_desc_path)
    shpRegions.to_crs(epsg=32637, inplace=True)
    landuse = shpRegions[['landuse', 'geometry']]
    landuse['landuse_priority'] = np.where(landuse.landuse == 'commercial', 1,np.where(landuse.landuse == 'residential', 2,np.where(landuse.landuse == 'industrial', 3, 4)))
    landuse_join = geopandas.sjoin(base_data_buffer, landuse, how="left", predicate="intersects")
    landuse_join.drop(columns=['index_right'], inplace=True)
    landuse_join.drop_duplicates(inplace=True)
    landuse_df = landuse_join[['caseid', 'landuse_priority']]
    landuse_df.groupby('caseid')['landuse_priority'].nunique().to_frame(name='count').sort_values(by=['count'],ascending=False).reset_index()
    landuse_df1 = landuse_df.groupby('caseid')['landuse_priority'].first().to_frame(name='landuse_priority').reset_index()
    landuse_df1.drop_duplicates(inplace=True)
    base_data3 = pd.merge(base_data2, landuse_df1, on='caseid')
    base_data3.drop_duplicates(inplace=True)
    print(base_data3.shape)
    print("landuse completed"+"\n")
    return base_data3

def License_information():
    # Joining the license information with Cases
    # Loading  Balady data to get license  information across cases
    base_data3=LandusePriority_Feature()
    balady_con = data.get_licencedata_constructions()
    balady_con = balady_con.dropna(subset=['lat', 'long'])
    # Cleaning Balady Latitude and Longitude values
    balady_con['lat'] = balady_con['lat'].str.replace(',', '.')
    balady_con['long'] = balady_con['long'].str.replace(',', '.')
    balady_con['lat'] = balady_con['lat'].str.replace('\n', '')
    balady_con['long'] = balady_con['long'].str.replace('\n', '')
    balady_con['lat'] = balady_con['lat'].str.replace('،', '')
    balady_con['long'] = balady_con['long'].str.replace('،', '')
    df1 = balady_con[balady_con['lat'].str.match('.*[\.].*[\.].*') == True]
    balady_con = balady_con.drop(df1.index, axis=0)
    df2 = balady_con[balady_con['long'].str.match('.*[\.].*[\.].*') == True]
    balady_con = balady_con.drop(df2.index, axis=0)

    balady_con['lat'] = pd.to_numeric(balady_con['lat'], errors='coerce').round(decimals=4)
    balady_con.dropna(axis=0, inplace=True)
    balady_con['long'] = pd.to_numeric(balady_con['long'], errors='coerce').round(decimals=4)
    balady_con.dropna(axis=0, inplace=True)

    balady_con_g = convert_gpd(balady_con, x=balady_con.lat, y=balady_con.long)
    # Filtering for outlier latitude and longitude values
    balady_con_g1 = balady_con_g[(balady_con_g['lat'] < 60) & (balady_con_g['lat'] > 0)]
    balady_con_g2 = balady_con_g1[(balady_con_g1['long'] < 60) & (balady_con_g1['long'] > 0)]
    balady_con_g2.to_crs(epsg=32637, inplace=True)
    balady_con_g2.drop_duplicates(inplace=True)
    balady_con_medina = geopandas.sjoin(balady_con_g2, Amana_bound, how="inner", predicate="intersects")
    balady_con_medina.drop(columns=['index_right'], inplace=True)
    balady_con_medina.drop_duplicates(inplace=True)
    balady_con_medina.columns = balady_con_medina.columns.str.lower()
    balady_crm = geopandas.sjoin(base_data_buffer, balady_con_medina, how="inner", predicate="intersects")
    balady_crm.drop_duplicates(inplace=True)
    balady_crm1 = balady_crm[['caseid', 'geometry', 'latitude', 'longitude', 'priority_value',
       'satisfaction_level', 'days_elapsed', 'DN','license id','issue_date', 'expiration_date','building type', 'parcel area',
        'building height'
         ]]
    # Adding volume, days left to expiry feature
    balady_crm1['days_left_to_expiry'] = (pd.to_datetime(balady_crm1['expiration_date']) - pd.to_datetime('now')).dt.days
    balady_crm1 = balady_crm1.rename({'parcel area': 'area'}, axis=1)
    residential = 'سكني'
    commercial = 'تجاري'
    govermental = 'حكومي='
    charity = 'أعمال خيرية'
    res_com = 'سكني تجاري'
    balady_crm1['building_type_score'] = np.where(balady_crm1['building type'] == govermental, 4,
                                                  np.where(balady_crm1['building type'] == commercial, 3,
                                                           np.where(balady_crm1['building type'] == residential, 2,
                                                                    np.where(balady_crm1['building type'] == 'res_com',
                                                                             2.5,
                                                                             np.where(balady_crm1[
                                                                                          'building type'] == 'charity',
                                                                                      1, 0)))))
    balady_crm1 = balady_crm1.drop(
        ['geometry', 'latitude', 'longitude', 'priority_value', 'satisfaction_level', 'days_elapsed', 'DN',
         'issue_date', 'expiration_date', 'building type', 'building height'], axis=1)
    base_data4 = pd.merge(base_data3, balady_crm1, on='caseid', how='left')
    base_data4.drop_duplicates(inplace=True)
    base_data4.drop_duplicates(subset=['caseid'], keep='first', inplace=True)
    print(base_data4.shape)
    print("license information completed"+"\n")
    return base_data4

def Failedcauses_proportion():
    base_data4=License_information()
    # Importing Drilling inspections data for proportion of failed causes
    bl_insp =data.get_inspectionsdata_construction()
    bl_insp.columns = bl_insp.columns.str.lower()
    bl_insp = bl_insp.dropna(subset=['lic_id'])
    bl_insp = bl_insp[['lic_id', 'inspection_date', 'numberoffailedclauses', 'complyingitems']]
    bl_insp['lic_id'] = bl_insp['lic_id'].astype(str)
    bl_insp['proportion_failed_causes'] = bl_insp['numberoffailedclauses'] / bl_insp['complyingitems']
    bl_insp.replace([np.inf, -np.inf],0, inplace= True)
    print("after converting infinity to 0 shape is", bl_insp.shape)
    # Feature building of days elapsed since last inspection
    bl_insp1 = bl_insp.copy()
    bl_insp1['inspection_date'] = pd.to_datetime(bl_insp1['inspection_date'])
    bl_inspdf = bl_insp1.groupby('lic_id')['inspection_date'].max().to_frame(name='last_inspection_date').reset_index()
    bl_inspdf['last_inspection_date'] = pd.to_datetime(bl_inspdf['last_inspection_date']).dt.date
    bl_inspdf['days_elapsed_last_inspection'] = (pd.to_datetime('now') - pd.to_datetime(bl_inspdf['last_inspection_date'])).dt.days
    bl_inspf = pd.merge(bl_insp, bl_inspdf, on=['lic_id'])
    base_data4['license id'] = base_data4['license id'].astype(str)
    base_data5 = pd.merge(base_data4, bl_inspf, left_on='license id', right_on='lic_id', how='left')
    base_data5.drop(columns=['lic_id', 'inspection_date', 'numberoffailedclauses', 'complyingitems', 'last_inspection_date'],
        inplace=True)
    base_data5.drop_duplicates(inplace=True)
    print(base_data5.shape)
    # loading the output of construction Case Prioritization - Feature creation
    df=base_data5.copy()
    df.drop_duplicates(subset=['caseid'], keep='first', inplace=True)
    df.fillna(0, inplace=True)
    df.drop(columns=['license id'], inplace=True)
    df.loc[df['days_left_to_expiry']< 0, 'days_left_to_expiry'] = 0
    print('Feature creation records:', df.shape)
    df1=df.copy()
    DB.save_final(df1, "CP_CONSTRUCTION_FEATURES")
    # Running a loop over all the numeric features for feature scoring
    features_list = ['days_elapsed', 'DN', 'no_of_priority_areas',
                     'landuse_priority', 'area', 'days_left_to_expiry', 'building_type_score',
                     'proportion_failed_causes', 'days_elapsed_last_inspection']
    for i in range(len(features_list)):
        print(i)
        print(features_list[i])
        feature_score(df, features_list[i])
    # scoring for priority metric
    df['priority_value'] = df['priority_value'].fillna(1)
    df.loc[df['priority_value'] == 1, 'priority_value_score'] = 1
    df.loc[df['priority_value'] == 2, 'priority_value_score'] = 2
    df.loc[df['priority_value'] == 3, 'priority_value_score'] = 3
    # indexing the feature
    df['Priority_score'] = (df['priority_value_score']) / statistics.mean(df['priority_value_score'])
    # scoring for satisfaction_level metric
    df.loc[df['satisfaction_level'] == 1, 'satisfaction_score'] = 0.2
    df.loc[df['satisfaction_level'] == 0, 'satisfaction_score'] = 0.3
    df.loc[df['satisfaction_level'] == -1, 'satisfaction_score'] = 0.5
    # indexing the feature-
    df['Customer_score'] = (df['satisfaction_score']) / statistics.mean(df['satisfaction_score'])
    features_list = ['days_elapsed_score', 'DN_score', 'no_of_priority_areas_score', 'landuse_priority_score',
                     'area_score', 'building_type_score_score', 'days_elapsed_last_inspection_score',
                     'days_left_to_expiry_score', 'proportion_failed_causes_score', 'Priority_score', 'Customer_score']
    df[features_list] = df[features_list].fillna(0)

    return df, features_list
