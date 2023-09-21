#!/usr/bin/env python
# coding: utf-8
#Importing libraries
import sys
import pandas as pd
import geopandas 
import numpy as np
import statistics
from classes import Database as data
DB=data.Database()
from classes import Convert_Gpd as gpd
from classes import Cleaning_Deduplication as dedup
from classes import Score
import config
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
from classes.engines import Helper as Help
Helper=Help.Helper()

import logging
def crm_clean():
    # ***CRM Data***
    crm_data = DB.get_crm_data()
    logging.info("Extracted CRM data from database, shape of crm data is : "+str(crm_data.shape))
    crm_data.rename(columns={'pyid': 'CaseId'}, inplace=True)
    crm_data.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates, crm data shape is:"+str(crm_data.shape))
    # Dropping all the missing values present in Latitude and Longitude columns of the dataset
    crm_data.rename(columns={'latitude': 'LATITUDE', 'longitude': 'LONGITUDE','pxcreatedatetime':'PXCREATEDATETIME','interactiontype':'INTERACTIONTYPE','closure_date':'CLOSURE_DATE','short_status':'SHORT_STATUS','priority':'PRIORITY','submunic_3':'SUBMUNIC_3'}, inplace=True)
    crm_data = crm_data.dropna(subset=['LATITUDE', 'LONGITUDE'])
    logging.info("after dropping Na from lattitude and longitude shape is: "+str(crm_data.shape))
    crm_data['PXCREATEDATETIME'] = pd.to_datetime(crm_data['PXCREATEDATETIME'])
    # Filtering Duplicated Cases
    crm_data_sorted = crm_data.sort_values(by=['CaseId', 'Satisfaction'], ascending=[True, False])
    crm_data_sorted1 = crm_data_sorted.drop_duplicates(subset=['CaseId'], keep='last')
    crm_gc = crm_data_sorted1[(crm_data_sorted1['VISUAL POLLUTION CATEGORY'] == 'GeneralCleaning')]
    #*******************require for IN_CASES********************************************************************
    #crm_gc = crm_data_sorted1[(crm_data_sorted1['VISUAL POLLUTION CATEGORY'] == 'Cleanliness of public places')]
    logging.info("after filtering visual pollution category, shape is :"+str(crm_gc.shape))
    crm_gc = crm_gc[crm_gc['SHORT_STATUS'] != 'Close']
    logging.info("after filtering close status, shape is :"+str(crm_gc.shape))
    # Converting it into geopandas dataframe
    crm_gc1 = gpd.convert_gpd(crm_gc, x=crm_gc.LONGITUDE, y=crm_gc.LATITUDE)
    crm_gc1 = crm_gc1.to_crs(epsg='32637')
    print("crm completed")
    logging.info("After cleaning crm data, shape is "+str(crm_gc1.shape))
    return crm_gc1

def clean_amana():
    global Amana_bound
    crm_gc1=crm_clean()


    print("amana crm")
    print(crm_gc1.shape)
    if(crm_gc1.shape[0] ==0):
        #print("crm_gc1 is empty, Please check DB")
        logging.error("crm_gc1 is empty, Please check DB")
        # Helper.Engine_End_Metadata_Update_Failed()
        # sys.exit(1)
        raise Exception("CRM data is empty")


    # Filtering data further for Madinah
    # Loading Amana information to get medina boundaries
    logging.info("reading amana data")
    #Amana = geopandas.read_file(config.amana_path)
    Amana=gdata.getAMANA()
    logging.info("amana data shape is :"+str(Amana.shape))
    # filtering for medina
    Amana_bound=Amana.copy()
    #Amana_bound = Amana.loc[(Amana['AMANACODE'] == "003")]
    Amana_bound.to_crs(epsg=32637, inplace=True)
    # Joining to extract all the CRM cases within medina
    logging.info("Inner Joining amana and CRM data")
    gc_medina = geopandas.sjoin(crm_gc1, Amana_bound, how="inner", predicate="intersects")
    logging.info("after joining shape is :"+str(gc_medina.shape))
    gc_medina.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates, shape is :"+str(gc_medina.shape))
    # print("ASHISH : " , gc_medina.columns)
    config.meta_data=gc_medina[['CaseId', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME'
                            ]]
    gc_medina.drop(['OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
                    'SHAPE_AREA', 'SHAPE_LEN', 'index_right'], axis=1, inplace=True)
    gc_medina.to_crs(epsg=32637, inplace=True)
    # Loading CRM Cluster
    # gc_medina_cl = pd.read_excel(r'C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\5. General Cleaning\\0. Raw Data\\CRM_Cases_Clusters_25102022.xlsx')
    logging.info("cleaning deduplication")
    gc_medina_cl = dedup.deduplication()
    logging.info("cleaning deduplication completed")
    gc_medina = gc_medina[['CaseId', 'PXCREATEDATETIME', 'PRIORITY', 'Satisfaction']]
    logging.info("joining gc medina and clustered data on caseid")
    medina_gc_cl = pd.merge(gc_medina_cl, gc_medina, on='CaseId')
    logging.info("after joining shape is:"+str(medina_gc_cl.shape))
    medina_gc_cl = medina_gc_cl.drop(columns=['LONGITUDE', 'LATITUDE'])
    gc_medina = gpd.convert_gpd(medina_gc_cl, x=medina_gc_cl.Cluster_Longitude, y=medina_gc_cl.Cluster_Latitude)
    gc_medina = gc_medina.to_crs(epsg='32637')
    gc_medina.columns = gc_medina.columns.str.lower()
    # Renaming the necessary column names
    gc_medina.rename(columns={'caseid': 'CaseId', 'pxcreatedatetime': 'created_time'}, inplace=True)
    gc_medina.drop_duplicates(inplace=True)
    # Converting Priority to numerical values
    high = 'حرج'
    medium = 'متوسط'
    low = 'عادي'
    gc_medina['priority_value'] = np.where(gc_medina.priority == high, 3, np.where(gc_medina.priority == medium, 2, 1))
    # Converting Satisfaction catgeories to numerical values
    gc_medina.satisfaction.unique()  # Dissatisfied, Satisfied and No response
    gc_medina['satisfaction_level'] = np.where(gc_medina.satisfaction == 'Dissatisfied', -1,
                                               np.where(gc_medina.satisfaction == 'Satisfied', 1, 0))
    # Calculating Days elapsed since creation of the case
    gc_medina['days_elapsed'] = (pd.to_datetime('now') - pd.to_datetime(gc_medina['created_time'])).dt.days
    print("amana completed")
    logging.info("Amana completed")
    return gc_medina


def population_feature():
    logging.info("Population feature")
    gc_medina = clean_amana()
    print(gc_medina.shape)
    logging.info("cleaned amana data shape is: "+str(gc_medina.shape))
    base_data = gc_medina[
        ['CaseId', 'geometry', 'cluster_latitude', 'cluster_longitude', 'cluster_label', 'priority_value',
         'satisfaction_level', 'days_elapsed']]
    ####Feature building for Popultaion Feature
    # Loading gridwise population shape file
    #shpGrid = geopandas.read_file(config.population_grids_path)
    shpGrid = gdata.getPOPULATION()
    logging.info("population grids shape is :"+str(shpGrid.shape))
    #shpGrid = shpGrid.rename(columns={'CHECK': 'GridUniqueCode'})
    shpGrid = shpGrid.to_crs(epsg='32637')
    # #Overlay the grid layer on the Madinah layer to fetch the grids relevant to Madinah region
    logging.info("inner join of Amana_bound and population grids ")
    join = geopandas.sjoin(shpGrid, Amana_bound, how="inner", op="intersects")
    logging.info("after joining shape is :"+str(join.shape))
    # Creating the Grid dataset with grid numbers and the corresponding geometries
    grid_data = join[['GridUniqueCode', 'GridNumber', 'geometry', 'DN']]
    grid_data.to_crs(epsg=32637, inplace=True)
    # #Joining base data and grid data to get population for each geometries
    logging.info("inner join of base data and grid data")
    pop_data = geopandas.sjoin(base_data, grid_data, how='left', op='intersects')
    logging.info("after joining shape is :"+str(pop_data.shape))
    base_data1 = pop_data.drop(columns=['index_right', 'GridNumber', 'GridUniqueCode'])
    # Adding buffer to geometries of CRM data
    base_data_buffer = base_data1.copy()
    base_data_buffer['geometry'] = base_data_buffer.geometry.buffer(200)
    ###Feature building for Priority areas
    # Importing Priority areas file
    #priority_areas = pd.read_csv(config.direc + 'madina_priority_areas.csv')
    logging.info("getting priorities data")
    PriorityAreas=gdata.getPriorityAreasData()
    logging.info("priorities data shape is"+str(PriorityAreas.shape))
    # file_names = []
    # for file in glob.glob(os.path.join(config.PRIORITYAREAS_shp,r"\*.shp")):
    #     file_names.append(file)
    #
    # gpd_aggregated = pd.DataFrame()
    #
    # for file in file_names:
    #     temp = geopandas.read_file(file, crs='epsg:32637')
    #     temp['file_name'] = file
    #     gpd_aggregated = gpd_aggregated.append(temp)
    #
    # medina_areas = pd.read_csv(config.madina_priority_path)
    # medina_areas['geometry'] = medina_areas['geometry'].apply(wkt.loads)
    # medina_areas = geopandas.GeoDataFrame(medina_areas, crs='EPSG:4326')
    # medina_areas['file_name'] = 'Medina.csv'
    # medina_areas['PRIORITY'] = 1
    # gpd_aggregated = gpd_aggregated.append(medina_areas)
    # gpd_aggregated = gpd_aggregated.reset_index(drop=True)
    # gpd_aggregated = gpd_aggregated.to_crs('epsg:4326')
    # PriorityAreas = gpd_aggregated
    PriorityAreas = gpd.convert_gpd(PriorityAreas)
    PriorityAreas.to_crs(epsg=32637, inplace=True)
    #PriorityAreas = PriorityAreas[(PriorityAreas.PRIORITY == 1)]
    # Calculating the number of priority areas for each case
    logging.info("Calculating the number of priority areas for each case")
    logging.info("sjoin on priority areas and base data buffer")
    prarea = geopandas.sjoin(base_data_buffer, PriorityAreas, how='left', predicate='intersects')
    logging.info("after join shape is:"+str(prarea.shape))
    # getting all the priority areas within the vicinity of a case
    preadf = prarea.groupby('CaseId')['Name'].nunique().to_frame(name='no_of_priority_areas').reset_index()
    logging.info("after grouping on case id, shape is"+str(preadf.shape))
    # priority_areas.drop(columns=['Unnamed: 0'], inplace=True)
    # priority_areas = gpd.convert_gpd(priority_areas)
    # priority_areas.to_crs(epsg=32637, inplace=True)
    # priority_areas.head()
    # # Calculating the number of priority areas for each case
    # prarea = geopandas.sjoin(base_data_buffer, priority_areas, how='left', predicate='intersects')
    # prarea.drop(columns=['index_right'], inplace=True)
    # # getting all the priority areas within the vicinity of a case
    # preadf = prarea.groupby('CaseId')['Name'].nunique().to_frame(name='no_of_priority_areas').reset_index()
    logging.info("merging preadf and base_data1 on caseid")
    base_data2 = pd.merge(base_data1, preadf, on='CaseId')
    logging.info("after merger shape is:"+str(base_data2.shape))
    base_data2.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates shape is"+str(base_data2.shape))
    # Adding Landuse priority feature
    # Load Shapefiles of landuse information obtained OSM analysis
    shpRegions = geopandas.read_file(config.region_desc_path)
    shpRegions.to_crs(epsg=32637, inplace=True)
    logging.info("land use shape is"+str(shpRegions.shape))
    landuse = shpRegions[['landuse', 'geometry']]
    landuse.landuse.unique()
    landuse['landuse_priority'] = np.where(landuse.landuse == 'commercial', 1,
                                           np.where(landuse.landuse == 'residential', 2,
                                                    np.where(landuse.landuse == 'industrial', 3, 4)))
    # landuse.head()
    logging.info("left join base data buffer with landuse")
    landuse_join = geopandas.sjoin(base_data_buffer, landuse, how="left", predicate="intersects")
    logging.info("after join, shape is :"+str(landuse_join.shape))
    landuse_join.drop(columns=['index_right'], inplace=True)
    landuse_join.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates shape is :"+str(landuse_join.shape))
    # landuse_join.head()
    landuse_df = landuse_join[['CaseId', 'landuse_priority']]
    landuse_df.groupby('CaseId')['landuse_priority'].nunique().to_frame(name='count').sort_values(by=['count'],
                                                                                                  ascending=False).reset_index()
    logging.info("after grouping with caseid shape is :"+str(landuse_df.shape))
    landuse_df1 = landuse_df.groupby('CaseId')['landuse_priority'].first().to_frame(
        name='landuse_priority').reset_index()
    logging.info("after grouping with caseid and taking first values shape is :" + str(landuse_df1.shape))
    landuse_df1.drop_duplicates(inplace=True)
    logging.info("after droping duplicates shape is" + str(landuse_df1.shape))
    logging.info("merging basedata2 with landuse on caseid")
    base_data3 = pd.merge(base_data2, landuse_df1, on='CaseId')
    logging.info("after merge shape is "+str(base_data3.shape))
    base_data3.drop_duplicates(inplace=True)
    cluster_countdf = base_data3.groupby(['cluster_label']).CaseId.nunique().to_frame(
        name='count_of_cases').reset_index()
    logging.info("after grouping on cluster label shape is:"+str(cluster_countdf.shape))
    # calculating only the repetitions
    cluster_countdf['count_of_repetitions'] = cluster_countdf['count_of_cases'] - 1
    cluster_countdf = cluster_countdf.drop(columns=['count_of_cases'])
    base_data4 = pd.merge(base_data3, cluster_countdf, on='cluster_label')
    logging.info("after merging basedata3 and cluster count on cluster label shape is"+str(base_data4.shape))
    # Adding  Sewer manholes data
    sewer = geopandas.read_file(config.SewerManholes_path)
    # sewer = pd.read_csv(config.SewerManholes_path)
    # sewer = geopandas.GeoDataFrame(sewer, geometry=gpd.GeoSeries.from_wkt(sewer.geometry, crs='epsg:4326'))
    # sewer.rename(columns={"row":"ROW"}, inplace=True)

    logging.info("sewer manholes shape is"+str(sewer.shape))
    sewer = sewer[['ROW', 'geometry']]
    # sewer = sewer[sewer['ROW'] != 0 ]
    sewer = sewer.drop_duplicates()
    logging.info("after dropping duplicates from sewer shape is"+str(sewer.shape))
    sewer_crm = geopandas.sjoin(base_data_buffer, sewer, how='left', predicate='intersects')
    logging.info("after left join of basedata buffer and sewer shape is"+str(sewer_crm.shape))
    # sewerdf = sewerdf.drop(columns = ['index_right'])
    sewer_crm = sewer_crm.drop_duplicates()
    sewer_df = sewer_crm.groupby('cluster_label')['ROW'].nunique().to_frame(
        name='count_of_sewer_manholes').reset_index()
    logging.info("after grouping swere on cluster label shape is"+str(sewer_df.shape))
    base_data5 = pd.merge(base_data4, sewer_df, on='cluster_label')
    # Feature building for count of buildings
    logging.info("after merging basedata4 with sewer on cluster label shape is "+str(base_data5.shape))
    print("population feature completed")
    return base_data5, base_data_buffer


def featurebuilding_count():
    base_data5, base_data_buffer=population_feature()
    print(base_data5.shape)
    print("base_data_buffer")
    print(base_data_buffer.shape)
    #bldg = geopandas.read_file(config.BuildingFootPrint_path)
    bldg=pd.read_csv(config.BuildingFootPrint_path, dtype={"geometry":"str"})
    bldg=geopandas.GeoDataFrame(bldg, geometry=geopandas.GeoSeries.from_wkt(bldg.geometry, crs = 'epsg:4326'))

    bldg.rename(columns={"objectid":"OBJECTID"}, inplace=True)

    logging.info("building footprint data shape is"+str(bldg.shape))
    bldg = bldg[['OBJECTID', 'geometry']]
    bldg_crm = geopandas.sjoin(base_data_buffer, bldg, how='left', predicate='intersects')
    logging.info("after left joining basedata buffer with footprint shape is"+str(bldg_crm.shape))
    bldg_crm = bldg_crm.drop(columns=['index_right'], axis=1)
    bldg_df = bldg_crm.groupby('cluster_label')['OBJECTID'].nunique().to_frame(name='count_of_buildings').reset_index()
    base_data6 = pd.merge(base_data5, bldg_df, on='cluster_label')
    logging.info("after merging basedata5 and cluster group shape is"+str(base_data6.shape))
    # Feature building of count of Excavation licenses in vicinity
    # Joining the license information with Cases
    # Loading  Balady data to get license  information across cases
    #balady_exc = pd.read_excel(config.direc + 'DataRequest_Excavations.xlsx')
    balady_exc=DB.get_datareq_excavations()
    logging.info("excavations data shape is"+str(balady_exc.shape))
    balady_exc.rename(columns={"lic_id":"LIC_ID","req_id":"REQ_ID","start_point_x":"START_POINT_X","start_point_y":"START_POINT_Y","ammana":"AMMANA","Municipality":"Municipality","Sub-Municipality":"Sub-Municipality","district_id":"DISTRICT_ID","district_name":"DISTRICT_NAME","emergency_license":"EMERGENCY_LICENSE","issue_date":"ISSUE_DATE","expiration_gdate":"EXPIRATION_GDATE","digging_start_date":"DIGGING_START_DATE","digging_end_date":"DIGGING_END_DATE","digging_status":"DIGGING_STATUS","site_name":"SITE_NAME","project_name":"PROJECT_NAME","project_desc":"PROJECT_DESC","name":"NAME","project_start_date":"PROJECT_START_DATE","work_start_date":"WORK_START_DATE","digging_duration":"DIGGING_DURATION","project_end_date":"PROJECT_END_DATE","digging_method_id":"DIGGING_METHOD_ID","digging_method":"DIGGING_METHOD","work_nature_id":"WORK_NATURE_ID","work_nature":"WORK_NATURE","path_length_sum":"PATH_LENGTH_SUM","network_type_id":"NETWORK_TYPE_ID","network_type":"NETWORK_TYPE","map_no":"MAP_NO","heavy_equipment_permission":"HEAVY_EQUIPMENT_PERMISSION","camping_room_count":"CAMPING_ROOM_COUNT","consultant_name":"CONSULTANT_NAME","consultant_cr":"CONSULTANT_CR","contractor_name":"CONTRACTOR_NAME","contractor_cr":"CONTRACTOR_CR","path_code":"PATH_CODE","length":"LENGTH","width":"WIDTH","depth":"DEPTH","digging_late_status_id":"DIGGING_LATE_STATUS_ID","digging_late_status":"DIGGING_LATE_STATUS","digging_category_id":"DIGGING_CATEGORY_ID","digging_category":"DIGGING_CATEGORY","service_config":"SERVICE_CONFIG","request_type_id":"REQUEST_TYPE_ID","request_type":"REQUEST_TYPE"}, inplace=True)
    balady_exc_g = gpd.convert_gpd(balady_exc, x=balady_exc.START_POINT_Y, y=balady_exc.START_POINT_X)
    balady_exc_g.to_crs(epsg=32637, inplace=True)
    balady_exc_g.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates shape is"+str(balady_exc_g.shape))
    balady_exc_medina = geopandas.sjoin(balady_exc_g, Amana_bound, how="inner", predicate="intersects")
    logging.info("after joining with amana bound shape is"+str(balady_exc_medina.shape))
    balady_exc_medina.drop(columns=['index_right'], inplace=True)
    balady_exc_medina.drop_duplicates(inplace=True)
    logging.info("after dropping duplicates shape is"+str(balady_exc_medina.shape))
    balady_exc_medina.to_crs(epsg=32637, inplace=True)
    balady_exc_medina.columns = balady_exc_medina.columns.str.lower()
    balady_exc_medina['lic_id'] = balady_exc_medina['lic_id'].astype(str)
    balady_exc_medina = balady_exc_medina[['lic_id', 'geometry']]
    balady_crm = geopandas.sjoin(base_data_buffer, balady_exc_medina, how="left", predicate="intersects")
    logging.info("after left join base data buffer and balady exc medina shape is"+str(balady_crm.shape))
    balady_crm.drop_duplicates(inplace=True)
    exc_df = balady_crm.groupby('cluster_label')['lic_id'].nunique().to_frame(name='count_of_excavations').reset_index()
    base_data7 = pd.merge(base_data6, exc_df, on='cluster_label')
    logging.info("after merging basedata6 and exc_df shape is"+str(base_data7.shape))
    #Feature building for count of construction
    # Joining the license information with Cases
    # Loading  Balady data to get license  information across cases
    #balady_con = pd.read_excel(config.direc + 'DataRequest_Construction.xlsx')
    balady_con=DB.get_licencedata_constructions()
    print("balady_con")
    print(balady_con.shape)
    logging.info("licence data constructions shape is"+str(balady_con.shape))
    balady_con.rename(columns={"issue_date":"ISSUE_DATE","expiration_date":"EXPIRATION_DATE"}, inplace=True)
    balady_con = balady_con.dropna(subset = ['lat','long'])
    logging.info("after dropping na from lat and long shape is"+str(balady_con.shape))
    #Cleaning Balady Latitude and Longitude values
    balady_con['lat'] = balady_con['lat'].str.replace(',','.')
    balady_con['long'] = balady_con['long'].str.replace(',','.')

    balady_con['lat']= balady_con['lat'].str.replace('\n','')
    balady_con['long'] = balady_con['long'].str.replace('\n','')

    balady_con['lat'] = balady_con['lat'].str.replace('،','')
    balady_con['long'] = balady_con['long'].str.replace('،','')
    df1 = balady_con[balady_con['lat'].str.match('.*[\.].*[\.].*') == True]
    balady_con = balady_con.drop(df1.index,axis = 0)
    df2 = balady_con[balady_con['long'].str.match('.*[\.].*[\.].*') == True]
    balady_con = balady_con.drop(df2.index,axis = 0)
    balady_con['lat'] = balady_con['lat'].apply(pd.to_numeric, errors='coerce')
    balady_con['long'] = balady_con['long'].apply(pd.to_numeric, errors='coerce')
    balady_con_g = gpd.convert_gpd(balady_con, x  = balady_con.lat, y = balady_con.long)

    # Filtering for outlier latitude and longitude values
    balady_con_g1 = balady_con_g[(balady_con_g['lat'] < 60) & (balady_con_g['lat'] > 0)]
    balady_con_g2 = balady_con_g1[(balady_con_g1['long'] < 60) & (balady_con_g1['long'] > 0)]
    balady_con_g2.to_crs(epsg=32637, inplace=True)
    balady_con_g2.drop_duplicates(inplace = True)
    balady_con_medina = geopandas.sjoin(balady_con_g2,Amana_bound, how = "inner", predicate = "intersects")
    logging.info("after inner joining balady con2 and amana bound shape is"+str(balady_con_medina.shape))
    balady_con_medina.drop(columns = ['index_right'],inplace = True)
    balady_con_medina.drop_duplicates(inplace = True)

    # balady_con_medina.to_crs(epsg=32637, inplace=True)
    balady_con_medina.columns = balady_con_medina.columns.str.lower()
    balady_con_medina = balady_con_medina[['license id','geometry']]
    balady_crm_c = geopandas.sjoin(base_data_buffer,balady_con_medina,how="left",predicate="intersects")
    logging.info("after joining base_data_buffer and balady_con_medina shape is "+str(balady_crm_c.shape))
    balady_crm_c.drop_duplicates(inplace = True)
    con_df = balady_crm_c.groupby('cluster_label')['license id'].nunique().to_frame(name = 'count_of_constructions').reset_index()
    df = pd.merge(base_data7, con_df, on = 'cluster_label')
    logging.info("after merging basedata7 and clustered con_df shape is"+str(df.shape))
    df.drop_duplicates(subset = ['CaseId'],keep = 'first', inplace = True)
    df.fillna(0,inplace = True)
    print(df.shape)
    #General Cleaning Case Prioritization - Model Scoring
    #loading the output of Excavation Case Prioritization - Feature creation
    #Running a loop over all the numeric features for feature scoring
    features_list = ['days_elapsed', 'DN', 'no_of_priority_areas',
           'landuse_priority','count_of_repetitions','count_of_sewer_manholes','count_of_buildings',
           'count_of_excavations', 'count_of_constructions']
    for i in range(len(features_list)):
        if(df.shape[0] ==0):
            print("Dataframe is empty")
            logging.error("Dataframe is empty")
            sys.exit(1)
        col=features_list[i]
        df[col]=pd.to_numeric(df[col])
        Score.feature_score(df,features_list[i])
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

    features_list = ['days_elapsed_score','DN_score','no_of_priority_areas_score','landuse_priority_score',
                     'count_of_repetitions_score','count_of_sewer_manholes_score','count_of_buildings_score',
                     'count_of_excavations_score','count_of_constructions_score','Priority_score','Customer_score']

    df[features_list] = df[features_list].fillna(0)
    print("feature building completed")
    logging.info("final output of featuredf shape is"+str(df.shape))
    return df, features_list
    # df_final.to_excel('C:\\Environment\\VM_Files\\MOMRAH_WORKING\\7. CV expansion\\5. General Cleaning\\1.Case Prioritization\\Output\\CP_GeneralCleaning_ModelScoring_21092022.xlsx',index = False)




