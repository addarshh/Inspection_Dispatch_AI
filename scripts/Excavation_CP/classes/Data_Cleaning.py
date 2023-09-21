from classes.Score import feature_score
import geopandas
import pandas as pd
import numpy as np
import statistics
from classes.Convert_Gpd import convert_gpd
from classes import Database as DB
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
import config
data=DB.Database()
def Clean_CRM():
    ##Importing entire CRM data and filtering for Retail inspection
    crm_data = data.get_crm_data()
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
    # Filtering for censorship type - Excavation Control
    ##Loading config file for classification types - which would help us filter data for excavation inspection type
    class_config = pd.read_excel(config.translation_doc_path)
    class_excavation = class_config.loc[class_config['censorship type'] == 'الرقابة على الحفريات']
    # Filtering for all the Main classification, Subcategory and Specialty classification adjoining to that censorship type
    crm_excavation = crm_data.loc[crm_data.main_classification.isin(class_excavation['main classification'])
                                  & crm_data.sub_classification.isin(class_excavation['Subcategory'])
                                  & crm_data.sp_classification.isin(class_excavation['Specialty Classification'])]
    # Converting CRM Excavation into geopandas dataframe
    crm_excavation = convert_gpd(crm_excavation, x=crm_excavation.longitude, y=crm_excavation.latitude)
    print(crm_excavation.shape)
    print("CRM completed:" +"\n")
    return crm_excavation

def Clean_Amana():
    # Cleaning Amana Data
    # filtering for qassim
    # Loading Amana information to get qassim boundaries
    global Amana_bound
    crm_excavation=Clean_CRM()
    #Amana = geopandas.read_file(config.amana_shp_path)
    Amana=gdata.getAMANA()
    Amana_bound = Amana.copy()#.loc[(Amana['AMANACODE'] == "008")]
    # Joining to extract all the CRM cases within qassim
    excavation_qassim = geopandas.sjoin(crm_excavation, Amana_bound, how="inner", predicate="intersects")
    excavation_qassim.drop_duplicates(inplace=True)
    config.meta_data=excavation_qassim[['caseid', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME'
                            ]]
    excavation_qassim.drop(['OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
                            'SHAPE_AREA', 'SHAPE_LEN', 'index_right'], axis=1, inplace=True)
    excavation_qassim.to_crs(epsg=32637, inplace=True)
    #excavation_qassim.columns = excavation_qassim.columns.str.lower()
    # Converting Priority to numerical values
    high = 'حرج'
    medium = 'متوسط'
    low = 'عادي'
    excavation_qassim['priority_value'] = np.where(excavation_qassim.priority == high, 3,
                                                   np.where(excavation_qassim.priority == medium, 2, 1))
    # Converting Satisfaction to numerical values
    excavation_qassim['satisfaction_level'] = np.where(excavation_qassim.satisfaction == 'Dissatisfied', -1,
                                                       np.where(excavation_qassim.satisfaction == 'Satisfied', 1, 0))
    # Calculating Days elapsed since creation of the case
    excavation_qassim['days_elapsed'] = (
                pd.to_datetime('now') - pd.to_datetime(excavation_qassim['created_time'])).dt.days
    
    base_data = excavation_qassim[
        ['caseid', 'geometry', 'latitude', 'longitude', 'priority_value', 'satisfaction_level', 'days_elapsed']]
    print(base_data.shape)
    print("Amana completed:" +"\n")
    return base_data

def Population_feature():
    ####Feature building for Population Feature
    # Loading gridwise population shape file
    global base_data_buffer
    base_data=Clean_Amana()
    #shpGrid = geopandas.read_file(config.population_grids_path)
    shpGrid = gdata.getPOPULATION()
    # Overlay the grid layer on the Madinah layer to fetch the grids relevant to Madinah region
    join = geopandas.sjoin(shpGrid, Amana_bound, how="inner", predicate="intersects")
    # Creating the Grid dataset with grid numbers and the corresponding geometries
    grid_data = join[['GridNumber', 'geometry', 'DN']]
    grid_data.to_crs(epsg=32637, inplace=True)
    # Joining base data and grid data to get population for each geometries
    pop_data = geopandas.sjoin(base_data, grid_data, how='left', predicate='intersects')
    base_data1 = pop_data.drop(columns=['index_right', 'GridNumber'])
    # Adding buffer to geometries of CRM data
    base_data_buffer = base_data1.copy()
    base_data_buffer['geometry'] = base_data_buffer.geometry.buffer(300)
    print(base_data1.shape)
    print("population feature completed"+"\n")
    return base_data1

def Priorityareas_feature():

    ###Feature building for Priority areas
    # Importing Priority areas file
    base_data1=Population_feature()
    #priority_areas = geopandas.read_file(config.priority_areas_path)
    priority_areas=gdata.getPriorityAreasData()
    priority_areas.to_crs(epsg=32637, inplace=True)
    # Calculating the number of priority areas for each case
    prarea = geopandas.sjoin(base_data_buffer, priority_areas, how='left', predicate='intersects')
    prarea.drop(columns=['index_right'], inplace=True)
    # getting all the priority areas within the vicinity of a case
    preadf = prarea.groupby('caseid')['AREANAME'].nunique().to_frame(name='no_of_priority_areas').reset_index()
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
    landuse['landuse_priority'] = np.where(landuse.landuse == 'commercial', 1,
                                           np.where(landuse.landuse == 'residential', 2,
                                                    np.where(landuse.landuse == 'industrial', 3, 4)))
    landuse_join = geopandas.sjoin(base_data_buffer, landuse, how="left", predicate="intersects")
    landuse_join.drop(columns=['index_right'], inplace=True)
    landuse_join.drop_duplicates(inplace=True)
    landuse_df = landuse_join[['caseid', 'landuse_priority']]
    landuse_df.groupby('caseid')['landuse_priority'].nunique().to_frame(name='count').sort_values(by=['count'],
                                                                                                  ascending=False).reset_index()
    landuse_df1 = landuse_df.groupby('caseid')['landuse_priority'].first().to_frame(
        name='landuse_priority').reset_index()
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
    balady_exc = data.get_datareq_excavations()
    balady_exc_g = convert_gpd(balady_exc, x=balady_exc['start_point_y'], y=balady_exc['start_point_x'])
    balady_exc_g.to_crs(epsg=32637, inplace=True)
    balady_exc_g.drop_duplicates(inplace=True)
    Amana_bound1 = Amana_bound.to_crs(epsg='32637')
    balady_exc_qassim = geopandas.sjoin(balady_exc_g, Amana_bound1, how="inner", predicate="intersects")
    balady_exc_qassim.drop(columns=['index_right'], inplace=True)
    balady_exc_qassim.drop_duplicates(inplace=True)
    balady_exc_qassim.to_crs(epsg=32637, inplace=True)
    balady_exc_qassim.columns = balady_exc_qassim.columns.str.lower()
    balady_crm = geopandas.sjoin(base_data_buffer, balady_exc_qassim, how="inner", predicate="intersects")
    balady_crm.drop_duplicates(inplace=True)
    balady_crm1 = balady_crm[['caseid', 'geometry', 'latitude', 'longitude', 'priority_value',
                              'satisfaction_level', 'days_elapsed', 'DN', 'lic_id', 'digging_start_date',
                              'digging_end_date',
                              'digging_duration',
                              'path_length_sum',
                              'length', 'width', 'depth']]
    # Adding volume, days left to expiry feature
    balady_crm1['volume'] = balady_crm1['length'] * balady_crm1['width'] * balady_crm1['depth']
    balady_crm1['days_left_to_expiry'] = (
                pd.to_datetime(balady_crm1['digging_end_date']) - pd.to_datetime('now')).dt.days
    balady_crm1 = balady_crm1[
        ['caseid', 'lic_id', 'digging_duration', 'path_length_sum', 'volume', 'days_left_to_expiry']]
    balady_crm1['lic_id'] = balady_crm1['lic_id'].astype(str)
    base_data4 = pd.merge(base_data3, balady_crm1, on='caseid', how='left')
    base_data4.drop_duplicates(inplace=True)
    base_data4.drop_duplicates(subset=['caseid'], keep='first', inplace=True)
    print(base_data4.shape)
    print("license information completed"+"\n")
    return base_data4

def Failedcauses_proportion():
    base_data4=License_information()
    # Importing Drilling inspections data for proportion of failed causes
    dr_insp =data.get_drilling_inspections()
    print("dataset loaded")
    dr_insp.columns = dr_insp.columns.str.lower()
    dr_insp = dr_insp[['lic_id', 'numberoffailedclauses', 'complyingitems']]
    dr_insp['lic_id'] = dr_insp['lic_id'].astype(str)
    dr_insp['proportion_failed_causes'] = dr_insp['numberoffailedclauses'] / dr_insp['complyingitems']
    dr_insp.replace([np.inf, -np.inf], 0, inplace=True)
    print("after converting infinity to 0 shape is", dr_insp.shape)
    base_data5 = pd.merge(base_data4, dr_insp, on='lic_id', how='left')
    base_data5.drop(columns=['numberoffailedclauses', 'complyingitems'], inplace=True)
    base_data5.drop_duplicates(inplace=True)
    base_data5.drop_duplicates(subset=['caseid'], keep='first', inplace=True)
    base_data5.fillna(0, inplace=True)
    base_data5.drop(columns=['lic_id'], inplace=True)
    # loading the output of Excavation Case Prioritization - Feature creation
    df=base_data5.copy()
    print('Final Feature records:',df.shape)
    df.loc[df['days_left_to_expiry']<0,'days_left_to_expiry']=0
    DB.save_final(df, "CP_EXCAVATION_FEATURES")
    # Running a loop over all the numeric features for feature scoring
    features_list = ['days_elapsed', 'DN', 'no_of_priority_areas',
                     'landuse_priority', 'digging_duration', 'path_length_sum', 'volume', 'days_left_to_expiry',
                     'proportion_failed_causes']
    for i in range(len(features_list)):
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
    # indexing the feature
    df['Customer_score'] = (df['satisfaction_score']) / statistics.mean(df['satisfaction_score'])
    features_list = ['days_elapsed_score', 'DN_score', 'no_of_priority_areas_score', 'landuse_priority_score',
                     'digging_duration_score', 'path_length_sum_score', 'volume_score',
                     'days_left_to_expiry_score', 'proportion_failed_causes_score', 'Priority_score', 'Customer_score']
    df[features_list] = df[features_list].fillna(0)
    return df,features_list
