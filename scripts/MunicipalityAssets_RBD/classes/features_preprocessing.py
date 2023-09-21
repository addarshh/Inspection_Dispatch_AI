#Importing libraries without acronyms
import os
import glob
import warnings
from abc import ABC
#Importing libraries with acronyms
import numpy as np
import pandas as pd
import geopandas as gpd
from classes import Database as Data
import config
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
#Importing Specific Functions and Modules
from shapely import wkt

import datetime
DB=Data.Database()
shp=Data.shapefiles()
warnings.filterwarnings("ignore")
"""
The following set of variables are dynamic in nature and will be updated based on user input. We write these into a config file which can later be updated by team and integrated into their systems
"""
class cleaning(ABC):
    def __init__(self):
        # Initialising the flag to use 2 week old momthatel cases instead of historic cases
        #self.SET_MOMTATHEL = False
        #self.planned_inspections = pd.read_excel(config.planned_inspection_path)
        self.planned_inspections=DB.get_visualdistortionsinspections()
        self.now = pd.Timestamp.now()
        self.mapping_file = pd.read_excel(config.vp_category_mapping_path)
        self.clause_mapping_file = pd.read_excel(config.vp_category_clause_mapping_path)
        # #Load updated Shapefiles of the population data with Grid Number
        #self.shpGrid = gpd.read_file(config.population_grids_path)
        self.shpGrid=gdata.getPopulationData()
        print('self.shpGrid.shape')
        print(self.shpGrid.shape)
        # #Import OSM analysis output to categorize the areas into commercial, residential, industrial and forest
        self.shpRegions = gpd.read_file(config.Region_Desc_path)
        # #Input file to get the geographical boundaries of medinah
        #self.Amana = gpd.read_file(config.amana_shp_path)
        self.Amana = gdata.getAMANA()
        print('self.Amana.shape')
        print(self.Amana.shape)
        self.street_grids = pd.read_csv(config.street_grids_path)

    # def grid_preprocessing(self):

        # Lets take the grids which have population and generate 0 for the rest of the grids. Addtionally, the base grids should also have the general static details which are listed below:
        # 1. POIs
        # 2. Priority Areas
        # 3. Landuse

        self.shpGrids_usable = self.shpGrid[self.shpGrid['DN']>0][['GridNumber','AMANACODE','MUNICIPA_1','DN','geometry']]

        #self.shpGrids_usable.rename(columns={"geometry":"GEOMETRY"}, inplace=True)
        # LandUse Feature
        # Classification of Grids by Type of Use
        LandUse = self.shpRegions[['landuse', 'geometry']]

        # joining landuse with the grid dataset
        LandUse_join = gpd.sjoin(self.shpGrids_usable, LandUse, how="left", op="intersects")
        print('LandUse_join')
        print(LandUse_join.shape)

        LandUse_join.drop('index_right', axis=1, inplace=True)
        LandUse_join.drop_duplicates(inplace=True)

        # converting landuse types to numbers using the precedence/importance heuristic
        # creating precedence for land use type
        data = [['commercial', 1], ['residential', 2], ['industrial', 3], ['forest', 4]]
        order_landuse = pd.DataFrame(data, columns=['landuse', 'Precedence'])
        # joining the precedence rank with land use data
        LandUse_join1 = pd.merge(LandUse_join, order_landuse, how='left', on='landuse')

        LandUse_Distinct = LandUse_join1[['GridNumber', 'landuse', 'Precedence']]
        #print(LandUse_Distinct)

        # Sort the Grids using the precedence scores and keep only the first record per grid
        LandUse_Distinct = LandUse_Distinct.sort_values(by=['GridNumber', 'Precedence'], ascending=[True, True],
                                                        na_position='last')
        LandUse_Distinct.drop_duplicates(subset=['GridNumber'], keep='first', inplace=True)
        LandUse_Distinct.drop('Precedence', axis=1, inplace=True)

        # #joining the landuse type with the base data file
        shpGrids_usable_land = self.shpGrids_usable.copy()
        shpGrids_usable_land = pd.merge(shpGrids_usable_land, LandUse_Distinct, on='GridNumber')

        shpGrids_usable_land['landuse'].fillna(0, inplace=True)
        shpGrids_usable_land['landuse'] = shpGrids_usable_land['landuse'].replace(['commercial'], 4)
        shpGrids_usable_land['landuse'] = shpGrids_usable_land['landuse'].replace(['residential'], 3)
        shpGrids_usable_land['landuse'] = shpGrids_usable_land['landuse'].replace(['industrial'], 2)
        shpGrids_usable_land['landuse'] = shpGrids_usable_land['landuse'].replace(['forest'], 1)
        # POIs
        POI_1 = pd.read_excel(config.POI_1_path)
        POI_2 = pd.read_excel(config.POI_2_path)
        List_of_POIs = POI_1.append(POI_2)
        # Converting the POIs data into a geodataframe so that it can be merged with base data
        List_of_POIs.drop_duplicates(inplace=True)

        GDF_POIs = gpd.GeoDataFrame(List_of_POIs,
                                    geometry=gpd.points_from_xy(List_of_POIs.longitude, List_of_POIs.latitude),
                                    crs='epsg:4326')
        POI_join = gpd.sjoin(shpGrids_usable_land, GDF_POIs, how="left", op="intersects")

        # logic to count number of POIs by grid
        count = POI_join.groupby(['GridNumber'])['place_id'].count()
        CountDf = count.to_frame().reset_index()
        CountDf.columns.values[1] = "Count of POIs"

        # joining the number of POIs at grid level feature with the base data
        shpGrids_usable_POI = shpGrids_usable_land.copy()
        shpGrids_usable_POI = pd.merge(shpGrids_usable_POI, CountDf, on='GridNumber')

        shpGrids_usable_POI['Count of POIs'].fillna(0, inplace=True)
        # Calculating Index
        average_cases = CountDf['Count of POIs'].sum() / len(shpGrids_usable_POI)
        shpGrids_usable_POI['Average POI'] = average_cases
        shpGrids_usable_POI['POI_index'] = shpGrids_usable_POI['Count of POIs'] / shpGrids_usable_POI['Average POI']
        shpGrids_usable_POI['POI_index'].fillna(0, inplace=True)
        shpGrids_usable_POI['POI_index'] = round(shpGrids_usable_POI['POI_index'], 2)
        # Priority Areas
        # file_names = []
        # for file in glob.glob(os.path.join(config.PRIORITYAREAS_shp,r"*.shp")):
        #     file_names.append(file)
        # gpd_aggregated = pd.DataFrame()
        #
        # for file in file_names:
        #     temp = gpd.read_file(file, crs='epsg:32637')
        #     temp['file_name'] = file
        #     gpd_aggregated = gpd_aggregated.append(temp)
        # # Medina CSv Files
        # csv_files = []
        # for file in glob.glob(os.path.join(config.PRIORITYAREAS_shp,r"*.csv")):
        #     csv_files.append(file)
        # medina_areas = pd.read_csv(csv_files[0])
        # medina_areas['geometry'] = medina_areas['geometry'].apply(wkt.loads)
        # medina_areas = gpd.GeoDataFrame(medina_areas, crs='EPSG:4326')
        # medina_areas['file_name'] = 'Medina.csv'
        # medina_areas['PRIORITY'] = 1
        # gpd_aggregated = gpd_aggregated.append(medina_areas)
        # gpd_aggregated = gpd_aggregated.reset_index(drop=True)
        # gpd_aggregated = gpd_aggregated.to_crs('epsg:4326')
        PriorityAreas = gdata.getPriorityAreasData()
        # PriorityAreas.to_crs(epsg=32637, inplace=True)
        # Mapping priority areas
        self.shpGrids_usable = shpGrids_usable_POI.copy()
        # shpGrids_usable.to_crs(epsg=32637,inplace=True)
        ##checkk this condition below
        #PriorityAreas = PriorityAreas[(PriorityAreas.PRIORITY == 1)]
        Grid_priority_areas = gpd.sjoin(self.shpGrids_usable, PriorityAreas, how="inner", op="intersects")
        Areas_per_grid = Grid_priority_areas.groupby(['GridNumber']).index_right.nunique()
        Grid_priority_AreasDF = Areas_per_grid.to_frame().reset_index()
        Grid_priority_AreasDF.columns.values[1] = "Number of priority Areas"
        Grid_priority_AreasDF['Number of priority Areas'].fillna(0, inplace=True)

        # merging both priority areas count back to base data
        shpGrids_usable_POI = pd.merge(self.shpGrids_usable, Grid_priority_AreasDF, on='GridNumber', how='left')
        # Calculating Index
        average_cases = shpGrids_usable_POI['Number of priority Areas'].sum() / len(shpGrids_usable_POI)
        shpGrids_usable_POI['Average Priority areas'] = average_cases
        shpGrids_usable_POI['Priority_Areas_Index'] = shpGrids_usable_POI['Number of priority Areas'] / shpGrids_usable_POI['Average Priority areas']
        shpGrids_usable_POI['Priority_Areas_Index'].fillna(0, inplace=True)
        shpGrids_usable_POI['Priority_Areas_Index'] = round(shpGrids_usable_POI['Priority_Areas_Index'], 2)
        shpGrids_usable_POI['Number of priority Areas'] = shpGrids_usable_POI['Number of priority Areas'].replace(np.nan, 0)
        self.shpGrids_usable = shpGrids_usable_POI.copy()
        self.CRM_full = DB.get_crm_data()
        print(self.CRM_full.columns)
        self.CRM_full.rename(columns={"category":"Category","pyid":"PYID", "interactiontype":"INTERACTIONTYPE", "pxcreatedatetime":"PXCREATEDATETIME",
            "closure_date":"CLOSURE_DATE" , "short_status":"SHORT_STATUS" , "latitude":"LATITUDE",
            "longitude":"LONGITUDE", "priority":"PRIORITY", "submunic_3":"SUBMUNIC_3"}, inplace=True)
        self.CRM_full['LATITUDE'] = self.CRM_full['LATITUDE'].replace(' ', '', regex = True)
        self.CRM_full['LONGITUDE'] = self.CRM_full['LONGITUDE'].replace(' ', '', regex = True)
        self.CRM_full['LATITUDE'] = self.CRM_full['LATITUDE'].replace('\(', '', regex = True)
        self.CRM_full['LONGITUDE'] = self.CRM_full['LONGITUDE'].replace('\(', '', regex = True)
        self.CRM_full['LATITUDE'] = self.CRM_full['LATITUDE'].replace('\)', '', regex = True)
        self.CRM_full['LONGITUDE'] = self.CRM_full['LONGITUDE'].replace('\)', '', regex = True)
        self.CRM_VP = self.CRM_full.loc[(self.CRM_full['VISUAL POLLUTION CATEGORY'] != "NaN")]
        self.CRM_VP = self.CRM_VP[['PYID', 'INTERACTIONTYPE', 'PXCREATEDATETIME', 'LATITUDE', 'LONGITUDE', 'VISUAL POLLUTION CATEGORY','SP_Classificaion', 'Category']]
        self.CRM_VP = self.CRM_VP.dropna(subset=['LATITUDE', 'LONGITUDE', 'VISUAL POLLUTION CATEGORY'])
        self.CRM_VP = self.CRM_VP.drop_duplicates()
        self.GDF_VP_cases = gpd.GeoDataFrame(self.CRM_VP, geometry=gpd.points_from_xy(self.CRM_VP['LONGITUDE'],self.CRM_VP['LATITUDE']),crs='epsg:4326')
        self.GDF_VP_cases = self.GDF_VP_cases[self.GDF_VP_cases['VISUAL POLLUTION CATEGORY'] != 'Not VP']
        print("VP cases")
        print(self.GDF_VP_cases.shape)
        self.GDF_VP_cases['PXCREATEDATETIME'] = pd.to_datetime(self.GDF_VP_cases['PXCREATEDATETIME'],
                                                               format='%m/%d/%Y %I:%M:%S.%f %p')
        self.GDF_VP_cases[self.GDF_VP_cases['VISUAL POLLUTION CATEGORY'] != 'Not VP']['VISUAL POLLUTION CATEGORY'].value_counts()

        self.mapping_file = self.mapping_file.rename(
            columns={"التصنيف التخصصي الحالي في تحسين المشهد الحضري": "SP_Classificaion"})
        self.GDF_VP_cases = pd.merge(self.GDF_VP_cases, self.mapping_file, on="SP_Classificaion", how='left')
        #print(self.GDF_VP_cases.columns)
        self.last_data = self.GDF_VP_cases['PXCREATEDATETIME'].max()
        self.GDF_VP_cases_last_two = self.GDF_VP_cases[self.GDF_VP_cases['PXCREATEDATETIME'] > (self.last_data - datetime.timedelta(days=7))]
        #print(self.shpGrids_usable.columns)
        #self.shpGrids_usable.to_file(
        #    r"C:\Environment\MOMRAH_WORKING\7. CV expansion\5. Municipality Assets Inspection\FEATURE_ENGINEERING\base_features\base_features.shp")


    def buildings(self):

        #Creating the Clustering Model for the Buildings VP Category. For this Category, we have the following Model features:
            # 1. Municipal Assets:
            #     - Number of Buildings
            #     - Number of Restaurants
            #     - Number of People
            #     - Priority area flag
            # 2. Volume of cases:
            #     - Number of CRM cases observed in the last 2 weeks
            #     - Number of Inspector led cases observed in the last 2 weeks

        dfRetail=DB.get_licencedata_retail()
        dfRetail.columns=dfRetail.columns.str.upper()
        restaurants = dfRetail[dfRetail['FACILITY_TYPE'] == "المطابخ و المطاعم و ما في حكمها"]
        restaurants_active = restaurants[
            (restaurants['LICENSE_START_DATE'] <= self.now) & (restaurants['LICENSE_EXPIRY_DATE'] >= self.now)]
        comm_lic_short = restaurants_active[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'SUB_MUNICIPALITY', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
        comm_lic_short.rename(columns={'LATITUDE': 'LONGITUDE', 'LONGITUDE': 'LATITUDE'}, inplace=True)
        comm_lic_short['LICENSE_START_DATE'] = pd.to_datetime(comm_lic_short['LICENSE_START_DATE'], format='%Y-%m-%d')
        comm_lic_short['LICENSE_EXPIRY_DATE'] = pd.to_datetime(comm_lic_short['LICENSE_EXPIRY_DATE'], format='%Y-%m-%d')
        comm_lic_short['LATITUDE'] = comm_lic_short['LATITUDE'].apply(pd.to_numeric, errors='coerce')
        comm_lic_short['LONGITUDE'] = comm_lic_short['LONGITUDE'].apply(pd.to_numeric, errors='coerce')
        comm_lic_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
        GDF_comm_lic_short = gpd.GeoDataFrame(comm_lic_short, geometry=gpd.points_from_xy(comm_lic_short['LONGITUDE'],
                                                                                          comm_lic_short['LATITUDE']),
                                              crs='epsg:4326')
        base_data_grids = self.shpGrids_usable.sjoin(GDF_comm_lic_short, how='left', predicate='intersects')
        base_data_grids['active_rest'] = base_data_grids.groupby(['GridNumber', 'AMANACODE'])['LICENCES_ID'].transform(
            'nunique')
        base_data_grids = base_data_grids[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'landuse',
                                           'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                           'Average Priority areas', 'Priority_Areas_Index', 'geometry', 'active_rest']]
        base_data_grids = base_data_grids.drop_duplicates()
        #base_data_grids.to_csv("base_data.csv")
        buildingdata=shp.get_buildingdata()
        buildingdata = buildingdata.rename(columns={"GEOMETRY": 'geometry'})
        # Converting the dataframe into a geopandas dataframe
        buildingdata = buildingdata[buildingdata['geometry'].notna()]
        buildingdata['geometry'] = buildingdata['geometry'].astype(str)
        buildingdata['geometry'] = buildingdata['geometry'].apply(wkt.loads)
        buildingdata_gpd = gpd.GeoDataFrame(buildingdata, geometry='geometry', crs='epsg:4326')
        base_data_grids = base_data_grids.sjoin(buildingdata_gpd, how='left', predicate='intersects')
        base_data_grids['num_buildings'] = base_data_grids.groupby(['GridNumber', 'AMANACODE'])['OBJECTID'].transform(
            'nunique')
        base_data_grids = base_data_grids[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'landuse',
                                           'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                           'Average Priority areas', 'Priority_Areas_Index', 'geometry', 'active_rest',
                                           'num_buildings']]
        base_data_grids = base_data_grids.drop_duplicates()
        GDF_VP_cases_last_two_buildings = self.GDF_VP_cases_last_two[self.GDF_VP_cases_last_two['BCG_en_Categories'] == "Buildings"]
        print("GDF_VP_cases_last_two_buildings shape is :", GDF_VP_cases_last_two_buildings.shape)
        base_data_grids_crm = base_data_grids.sjoin(GDF_VP_cases_last_two_buildings, how='left', predicate='intersects')
        base_data_grids_crm['num_cases'] = base_data_grids_crm.groupby(['GridNumber', 'AMANACODE'])['PYID'].transform(
            'nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'landuse',
                                                   'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index', 'geometry',
                                                   'active_rest', 'num_buildings', 'num_cases']]
        base_data_grids_crm = base_data_grids_crm.drop_duplicates()

        #Momtathel

        self.planned_inspections['Inspection Date'] = pd.to_datetime(self.planned_inspections['Inspection Date'],
                                                                format=r'%m/%d/%Y %I:%M:%S.%f %p')
        if config.SET_MOMTATHEL:
            last_mom_data = self.planned_inspections['Inspection Date'].max()
            self.planned_inspections = self.planned_inspections[
                self.planned_inspections['Inspection Date'] > (last_mom_data - datetime.timedelta(days=7))]
        self.mapping_file = self.mapping_file.rename(columns={'الفئة الحالية في تحسين المشهد الحضري': 'CLAUSE NAME'})
        
        self.planned_inspections = pd.merge(self.planned_inspections, self.mapping_file, on="CLAUSE NAME", how='left')
        self.planned_inspections['BCG_en_Categories'] = self.planned_inspections['BCG_en_Categories'].replace(np.nan, "0")
        
        # ADDED NEW MAPPING FILE>> REVANTH
        #self.clause_mapping_file = self.clause_mapping_file.rename(columns={'الفئة الحالية في تحسين المشهد الحضري': 'CLAUSE NAME'})
        print("clause_mapping_file", self.clause_mapping_file.shape)
        self.clause_mapping_file['CLAUSE NO'] = self.clause_mapping_file['CLAUSE NO'].replace(',','').astype(int)
        
        # bcg_map = {
        #     "Buildings": [4400, 4397, 4402, 4178, 8118, 9253],
        #     "Construction": [7456, 7455, 9678, 9672, 9674],
        #     "Streets": [9675],
        #     "Signage": [9676]}

        # def bcg_cat_map(row):
        #     if row['CLAUSE NO'] in bcg_map['Buildings']:
        #         return "Buildings"
        #     if row['CLAUSE NO'] in bcg_map['Construction']:
        #         return "Construction"
        #     if row['CLAUSE NO'] in bcg_map['Streets']:
        #         return "Roads and Streets"
        #     if row['CLAUSE NO'] in bcg_map['Signage']:
        #         return "Lighting, utilities, and\nsignage "
        #     if row['BCG_en_Categories'] == "0":
        #         return "Not VP"
        #     else:
        #         return row['BCG_en_Categories']

        # self.planned_inspections['BCG_en_Categories'] = self.planned_inspections.apply(lambda x: bcg_cat_map(x), axis=1)

        print("planned_inspections_before merge", self.planned_inspections.shape)
        self.planned_inspections = pd.merge(self.planned_inspections, self.clause_mapping_file, on="CLAUSE NO", how='left')
        print("planned_inspections_after merge", self.planned_inspections.shape)
        
        print(self.planned_inspections.columns)
        print(self.planned_inspections['BCG_en_Categories_y'].nunique())
        print(self.planned_inspections['BCG_en_Categories_x'].nunique())
        self.planned_inspections['BCG_en_Categories'] = self.planned_inspections['BCG_en_Categories_y']
        
        
        self.planned_inspections = self.planned_inspections[['INSEPECTION ID', 'LATITUDE', 'LONGITUDE', 'BCG_en_Categories']]
        self.planned_inspections_gdf = gpd.GeoDataFrame(self.planned_inspections,
                                                   geometry=gpd.points_from_xy(self.planned_inspections['LONGITUDE'],
                                                                               self.planned_inspections['LATITUDE']),
                                                   crs='epsg:4326')
        self.planned_inspections_gdf = self.planned_inspections_gdf[self.planned_inspections_gdf['BCG_en_Categories'] != 'Not VP']
        self.planned_inspections_gdf = self.planned_inspections_gdf.groupby('INSEPECTION ID').first().reset_index()
        self.planned_inspections_gdf = self.planned_inspections_gdf.groupby('INSEPECTION ID').first().reset_index()
        base_data_grids_crm = base_data_grids_crm.sjoin(
            self.planned_inspections_gdf[self.planned_inspections_gdf['BCG_en_Categories'] == "Buildings"], how='left',
            predicate='intersects')
        base_data_grids_crm['num_momthat_cases'] = \
        base_data_grids_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['INSEPECTION ID'].transform(
            'count')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'landuse',
                                                   'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index', 'geometry',
                                                   'active_rest', 'num_buildings', 'num_cases',
                                                   'num_momthat_cases']].drop_duplicates()
        # base_data_grids_crm.to_csv(
        #    r"C:\Users\acioopr46\Downloads\test\Buildings_Features.csv",
        #    index=False)
        return self.shpGrid, base_data_grids_crm


    def street_cleaning(self):

            # Creating the Clustering Model for the Street VP Category. For this Category, we have the following Model features:
            # 1. Municipal Assets:
            #     - Number of streets
            #     - Length of streets
            #     - Number of pavements
            #     - Number of parking areas
            #
            # 2. Volume of cases:
            #     - Number of CRM cases observed in the last 2 weeks
            #     - Number of Inspector led cases observed in the last 2 weeks
            # 3. Contractors

        self.street_grids['num_roads'] = self.street_grids.groupby(['GridNumber', 'AMANACODE'])['index'].transform('nunique')
        self.street_grids['road_length'] = self.street_grids.groupby(['GridNumber', 'AMANACODE'])['RoadLength_New'].transform('sum')
        self.shpGrids_usable['common_key'] = self.shpGrid['AMANACODE'].astype(str) +self.shpGrids_usable['GridNumber']

        def create_padding(row):
            if len(row) == 4:
                return "00" + row
            if len(row) == 5:
                return "0" + row

        self.street_grids['AMANACODE'] = self.street_grids['AMANACODE'].apply(lambda x: create_padding(str(x)))
        self.street_grids['common_key'] =  self.shpGrid['AMANACODE'].astype(str) +self.street_grids['GridNumber']
        self.shpGrid['common_key'] = self.shpGrid['AMANACODE'].astype(str) + self.shpGrid['GridNumber']
        base_grids_roads = pd.merge(self.shpGrids_usable,
                                    self.street_grids[['common_key', 'num_roads', 'road_length']].drop_duplicates(),
                                    on='common_key', how='left')
        base_grids_roads[['num_roads', 'road_length']] = base_grids_roads[['num_roads', 'road_length']].replace(np.nan,0)

        #Pavements

        pavementsdata = shp.get_pavementsdata()
        pavementsdata = pavementsdata.rename(columns={"GEOMETRY": "geometry"})
        pavementsdata = pavementsdata[pavementsdata['geometry'].notna()]
        # pavementsdata['geometry'] = pavementsdata['geometry'].astype(str)
        pavementsdata['geometry'] = pavementsdata['geometry'].apply(wkt.loads)
        pavementsdata_gpd = gpd.GeoDataFrame(pavementsdata, geometry='geometry', crs='epsg:4326')
        pavements_grid_length = pavementsdata_gpd.overlay(base_grids_roads, how='intersection')
        ECKERT_IV_PROJ4_STRING = "+proj=eck4 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
        agg_pavement_grids = pavements_grid_length[
            ['geometry', 'OBJECTID', 'GridNumber', 'AMANACODE', 'common_key']].to_crs(ECKERT_IV_PROJ4_STRING)

        agg_pavement_grids['Pavement_length'] = agg_pavement_grids['geometry'].length
        agg_pavement_grids['num_pavements'] = agg_pavement_grids.groupby(['GridNumber', 'AMANACODE'])[
            'OBJECTID'].transform('nunique')
        agg_pavement_grids['pavement_length'] = agg_pavement_grids.groupby(['GridNumber', 'AMANACODE'])[
            'Pavement_length'].transform('sum')
        base_grids_roads_pavements = base_grids_roads.merge(
            agg_pavement_grids[['common_key', 'num_pavements', 'pavement_length']], on='common_key', how='left')
        base_grids_roads_pavements[['num_pavements', 'pavement_length']] = base_grids_roads_pavements[
            ['num_pavements', 'pavement_length']].replace(np.nan, 0)
        base_grids_roads_pavements = base_grids_roads_pavements.drop_duplicates()

        #Parkingareas

        parking_areas = pd.read_csv(config.parking_areas_path,dtype={"geometry":"str"})
        parking_areas=gpd.GeoDataFrame(parking_areas, geometry=gpd.GeoSeries.from_wkt(parking_areas.geometry, crs = 'epsg:4326'))
        parking_areas = parking_areas.reset_index()
        parking_areas = parking_areas.rename(columns={"index": "POBJECT_ID"})
        parking_areas_grids = base_grids_roads_pavements.sjoin(parking_areas, how='left', predicate='intersects')
        parking_areas_grids = parking_areas_grids[['common_key', 'POBJECT_ID', 'geometry']]
        parking_areas_grids['num_parking_areas'] = parking_areas_grids.groupby('common_key')['POBJECT_ID'].transform('nunique')
        base_grids_final = base_grids_roads_pavements.merge(
        parking_areas_grids[['common_key', 'num_parking_areas']].drop_duplicates(), how='left', on='common_key')
        base_grids_final['num_parking_areas'] = base_grids_final['num_parking_areas'].replace(np.nan, 0)

        #CRM Cases

        GDF_VP_cases_last_two_ligh = self.GDF_VP_cases_last_two[self.GDF_VP_cases_last_two['BCG_en_Categories'] == "Roads and Streets"]
        base_data_grids_crm = base_grids_final.sjoin(GDF_VP_cases_last_two_ligh, how='left', predicate='intersects')
        base_data_grids_crm['num_cases'] = base_data_grids_crm.groupby(['GridNumber', 'AMANACODE'])['PYID'].transform(
            'nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                   'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index', 'common_key',
                                                   'num_roads', 'road_length', 'num_pavements', 'pavement_length',
                                                   'num_parking_areas', 'num_cases']]
        base_data_grids_crm = base_data_grids_crm.drop_duplicates()

        #Momtathel cases

        base_data_grids_crm = base_data_grids_crm.sjoin(
            self.planned_inspections_gdf[self.planned_inspections_gdf['BCG_en_Categories'] == "Roads and Streets"], how='left',
            predicate='intersects')
        base_data_grids_crm['num_momtathel_cases'] = \
        base_data_grids_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])['INSEPECTION ID'].transform('nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                   'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index', 'common_key',
                                                   'num_roads', 'road_length', 'num_pavements', 'pavement_length',
                                                   'num_parking_areas', 'num_cases',
                                                   'num_momtathel_cases']].drop_duplicates()

        ## Generating Bad Contractor Data
            # Generating Active Construction Licenses
                # preparing data for construction licenses and cleaning up dates in Hijri calendar
        self.constr_licenses=DB.get_licencedata_constructions()
        self.constr_licenses.columns=self.constr_licenses.columns.str.upper()
        constr_licenses_short = self.constr_licenses[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
        constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE'],
                                     inplace=True)
        constr_lic_lat = list(constr_licenses_short['LONGITUDE'])
        constr_lic_long = list(constr_licenses_short['LATITUDE'])

        # cleaning lat/logs
        for idx, x in enumerate(constr_lic_lat):
            y = constr_lic_long[idx]
            try:
                if (float(x) > 50 or float(y) > 50):
                    constr_lic_lat[idx] = np.NaN
                    constr_lic_long[idx] = np.NaN
                    continue
                if (float(x) > float(y)):
                    constr_lic_lat[idx] = y
                    constr_lic_long[idx] = x
            except ValueError:
                constr_lic_lat[idx] = np.NaN
                constr_lic_long[idx] = np.NaN

        constr_licenses_short['LATITUDE'] = constr_lic_lat
        constr_licenses_short['LONGITUDE'] = constr_lic_long
        constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
        self.constr_licenses_gdf = gpd.GeoDataFrame(constr_licenses_short,
                                               geometry=gpd.points_from_xy(constr_licenses_short['LONGITUDE'],
                                                                           constr_licenses_short['LATITUDE']),
                                               crs='epsg:4326')
        constr_licenses_gdf_active = self.constr_licenses_gdf[
            (self.constr_licenses_gdf['LICENSE_START_DATE'] <= self.now) & (self.constr_licenses_gdf['LICENSE_EXPIRY_DATE'] >= self.now)]
        constr_licenses_gdf_active = constr_licenses_gdf_active.to_crs("epsg:32637")
        constr_licenses_gdf_active['geometry'] = constr_licenses_gdf_active.buffer(500)
        constr_licenses_gdf_active = constr_licenses_gdf_active.to_crs("epsg:4326")
        #print(self.GDF_VP_cases.columns)
        GDF_VP_cases_streets = self.GDF_VP_cases[self.GDF_VP_cases['BCG_en_Categories'] == 'Roads and Streets']
        GDF_VP_cases_last_six = GDF_VP_cases_streets[
            GDF_VP_cases_streets['PXCREATEDATETIME'] > (self.last_data - datetime.timedelta(days=180))]
        constr_licenses_gdf_active_crm = constr_licenses_gdf_active.sjoin(GDF_VP_cases_last_six, how='left',
                                                                          predicate='intersects')
        constr_licenses_gdf_active_crm['crm_cases'] = constr_licenses_gdf_active_crm.groupby('LICENCES_ID')[
            'PYID'].transform('nunique')
        constr_licenses_gdf_active = constr_licenses_gdf_active.merge(
            constr_licenses_gdf_active_crm[['LICENCES_ID', 'crm_cases']].drop_duplicates(), on='LICENCES_ID',
            how='left')
        constr_licenses_gdf_active['bad_contractors'] = constr_licenses_gdf_active['crm_cases'].apply(
            lambda x: 1 if x > 0 else 0)
        base_data_grids_contract = base_data_grids_crm.sjoin(constr_licenses_gdf_active, how='left',
                                                             predicate='intersects')
        base_data_grids_contract['total_bad_construction_contractors'] = \
        base_data_grids_contract.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['bad_contractors'].transform(
            'sum')
        base_data_grids_contract = base_data_grids_contract[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                             'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                             'Number of priority Areas',
                                                             'Average Priority areas', 'Priority_Areas_Index',
                                                             'common_key',
                                                             'num_roads', 'road_length', 'num_pavements',
                                                             'pavement_length',
                                                             'num_parking_areas', 'num_cases', 'num_momtathel_cases',
                                                             'total_bad_construction_contractors']].drop_duplicates()

        #EXCAVATION
        # preparing data for construction licenses and cleaning up dates in Hijri calendar
        excav_licenses=DB.get_licencedata_excavations()
        excav_licenses.columns=excav_licenses.columns.str.upper()
        exca_licenses_short = excav_licenses[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'CONTRACTOR_CR']]
        exca_licenses_short.dropna(
            subset=['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE',
                    'CONTRACTOR_CR'], inplace=True)
        exca_lic_lat = list(exca_licenses_short['LATITUDE'])
        exca_lic_long = list(exca_licenses_short['LONGITUDE'])

        # cleaning lat/logs
        for idx, x in enumerate(exca_lic_lat):
            y = exca_lic_long[idx]
            try:
                if (float(x) > 50 or float(y) > 50):
                    exca_lic_lat[idx] = np.NaN
                    exca_lic_long[idx] = np.NaN
                    continue
                if (float(x) > float(y)):
                    exca_lic_lat[idx] = y
                    exca_lic_long[idx] = x
            except ValueError:
                exca_lic_lat[idx] = np.NaN
                exca_lic_long[idx] = np.NaN

        exca_licenses_short['LATITUDE'] = exca_lic_lat
        exca_licenses_short['LONGITUDE'] = exca_lic_long
        exca_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

        # preparing data for commercial licenses
        exca_licenses_short['LICENSE_START_DATE'] = pd.to_datetime(exca_licenses_short['LICENSE_START_DATE'],
                                                                   format='%Y-%m-%d')
        exca_licenses_short['LICENSE_EXPIRY_DATE'] = pd.to_datetime(exca_licenses_short['LICENSE_EXPIRY_DATE'],
                                                                    format='%Y-%m-%d')
        exca_licenses_short['LATITUDE'] = exca_licenses_short['LATITUDE'].apply(pd.to_numeric, errors='coerce')
        exca_licenses_short['LONGITUDE'] = exca_licenses_short['LONGITUDE'].apply(pd.to_numeric, errors='coerce')
        exca_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
        exca_lic_gpd = gpd.GeoDataFrame(exca_licenses_short,
                                        geometry=gpd.points_from_xy(exca_licenses_short['LONGITUDE'],
                                                                    exca_licenses_short['LATITUDE']), crs='epsg:4326')
        exca_lic_gpd = exca_lic_gpd[
            ['LICENCES_ID', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'LATITUDE', 'LONGITUDE', 'geometry',
             'CONTRACTOR_CR']]
        exca_lic_gpd_active = exca_lic_gpd[
            (exca_lic_gpd['LICENSE_START_DATE'] <= self.now) & (exca_lic_gpd['LICENSE_EXPIRY_DATE'] >= self.now)]
        exca_lic_gpd_active = exca_lic_gpd_active.to_crs("epsg:32637")
        exca_lic_gpd_active['geometry'] = exca_lic_gpd_active.buffer(500)
        exca_lic_gpd_active = exca_lic_gpd_active.to_crs("epsg:4326")

        exca_lic_gpd_active_crm = exca_lic_gpd_active.sjoin(GDF_VP_cases_last_six, how='left', predicate='intersects')
        exca_lic_gpd_active_crm['crm_cases_excav'] = exca_lic_gpd_active_crm.groupby('LICENCES_ID')['PYID'].transform(
            'nunique')
        exca_lic_gpd_active = exca_lic_gpd_active.merge(
            exca_lic_gpd_active_crm[['LICENCES_ID', 'crm_cases_excav']].drop_duplicates(), on='LICENCES_ID', how='left')
        exca_lic_gpd_active['contractor_vp_cases'] = exca_lic_gpd_active.groupby('CONTRACTOR_CR')[
            'crm_cases_excav'].transform('sum')
        exca_lic_gpd_active['bad_contractors'] = exca_lic_gpd_active['contractor_vp_cases'].apply(
            lambda x: 1 if x > 0 else 0)
        base_data_grids_excav = base_data_grids_contract.sjoin(exca_lic_gpd_active, how='left', predicate='intersects')
        base_data_grids_excav['total_bad_excavation_contractors'] = \
        base_data_grids_excav.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['bad_contractors'].transform(
            'sum')
        base_data_grids_excav = base_data_grids_excav[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                       'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                       'Number of priority Areas',
                                                       'Average Priority areas', 'Priority_Areas_Index', 'common_key',
                                                       'num_roads', 'road_length', 'num_pavements', 'pavement_length',
                                                       'num_parking_areas', 'num_cases', 'num_momtathel_cases',
                                                       'total_bad_construction_contractors',
                                                       'total_bad_excavation_contractors']].drop_duplicates()
        # base_data_grids_excav.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\Street_Features.csv",
        #     index=False)
        return base_data_grids_excav

    def lights_utilities(self):
        ## Lighting, Utilities, & Signage
            # Creating the Clustering Model for the Street VP Category. For this Category, we have the following Model features:
            # 1. Municipal Assets:
            #     - Number of commercial stores (which are not restaurants)
            #     - Number of street lights
            #     - Number of traffic lights
            #
            # 2. Volume of cases:
            #     - Number of CRM cases observed in the last 2 weeks
        # preparing data for commercial licenses
        dfRetail=DB.get_licencedata_retail()
        dfRetail.columns=dfRetail.columns.str.upper()
        stores = dfRetail[dfRetail['FACILITY_TYPE'] != "المطابخ و المطاعم و ما في حكمها"]
        stores_active = stores[(stores['LICENSE_START_DATE'] <= self.now) & (stores['LICENSE_EXPIRY_DATE'] >= self.now)]
        comm_lic_stores = stores_active[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'SUB_MUNICIPALITY', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
        comm_lic_stores.rename(columns={'LATITUDE': 'LONGITUDE', 'LONGITUDE': 'LATITUDE'}, inplace=True)
        comm_lic_stores['LICENSE_START_DATE'] = pd.to_datetime(comm_lic_stores['LICENSE_START_DATE'], format='%Y-%m-%d')
        comm_lic_stores['LICENSE_EXPIRY_DATE'] = pd.to_datetime(comm_lic_stores['LICENSE_EXPIRY_DATE'],
                                                                format='%Y-%m-%d')
        comm_lic_stores['LATITUDE'] = comm_lic_stores['LATITUDE'].apply(pd.to_numeric, errors='coerce')
        comm_lic_stores['LONGITUDE'] = comm_lic_stores['LONGITUDE'].apply(pd.to_numeric, errors='coerce')
        comm_lic_stores.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
        GDF_comm_lic_stores = gpd.GeoDataFrame(comm_lic_stores,
                                               geometry=gpd.points_from_xy(comm_lic_stores['LONGITUDE'],
                                                                           comm_lic_stores['LATITUDE']),
                                               crs='epsg:4326')
        base_data_grids = self.shpGrids_usable.sjoin(GDF_comm_lic_stores, how='left', predicate='intersects')
        base_data_grids['active_stores'] = base_data_grids.groupby(['GridNumber', 'AMANACODE'])[
            'LICENCES_ID'].transform('nunique')
        base_data_grids = base_data_grids[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
                                           'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                           'Average Priority areas', 'Priority_Areas_Index', 'common_key',
                                           'active_stores']].drop_duplicates()

        ##We need to read this from the Database
        street_lights = pd.read_csv(config.street_lights_path,dtype={"geometry":"str"})
        street_lights=gpd.GeoDataFrame(street_lights, geometry=gpd.GeoSeries.from_wkt(street_lights.geometry, crs = 'epsg:4326'))
        traffic_lights = pd.read_csv(config.traffic_lights_path,dtype={"geometry":"str"})
        traffic_lights=gpd.GeoDataFrame(traffic_lights, geometry=gpd.GeoSeries.from_wkt(traffic_lights.geometry, crs = 'epsg:4326'))
        street_lights = street_lights.reset_index()
        street_lights = street_lights.rename(columns={"index": "SLOBJECT_ID"})

        traffic_lights = traffic_lights.reset_index()
        traffic_lights = traffic_lights.rename(columns={"index": "TLOBJECT_ID"})

        street_light_grids = base_data_grids.sjoin(street_lights, how='left', predicate='intersects')
        traffic_light_grids = base_data_grids.sjoin(traffic_lights, how='left', predicate='intersects')

        street_light_grids = street_light_grids[['common_key', 'SLOBJECT_ID', 'geometry']]
        traffic_light_grids = traffic_light_grids[['common_key', 'TLOBJECT_ID', 'geometry']]

        street_light_grids['num_street_lights'] = street_light_grids.groupby('common_key')['SLOBJECT_ID'].transform(
            'nunique')
        traffic_light_grids['num_traffic_lights'] = traffic_light_grids.groupby('common_key')['TLOBJECT_ID'].transform(
            'nunique')

        base_grids_slights = base_data_grids.merge(
            street_light_grids[['common_key', 'num_street_lights']].drop_duplicates(), how='left', on='common_key')
        base_grids_final = base_grids_slights.merge(
            traffic_light_grids[['common_key', 'num_traffic_lights']].drop_duplicates(), how='left', on='common_key')

        #CRM Cases
        GDF_VP_cases_last_two_streets = self.GDF_VP_cases_last_two[
            self.GDF_VP_cases_last_two['BCG_en_Categories'] == "Lighting, utilities, and\nsignage "]
        base_data_grids_crm = base_grids_final.sjoin(GDF_VP_cases_last_two_streets, how='left', predicate='intersects')
        base_data_grids_crm['num_cases'] = base_data_grids_crm.groupby(['GridNumber', 'AMANACODE'])['PYID'].transform(
            'nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
                                                   'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index', 'common_key',
                                                   'active_stores', 'num_street_lights', 'num_traffic_lights', 'num_cases']]
        base_data_grids_crm = base_data_grids_crm.drop_duplicates()

        #Momtathel cases

        base_data_grids_crm = base_data_grids_crm.sjoin(self.planned_inspections_gdf[self.planned_inspections_gdf[
                                                                                    'BCG_en_Categories'] == "Lighting, utilities, and\nsignage "],
                                                        how='left', predicate='intersects')
        base_data_grids_crm['num_momtathel_cases'] = \
        base_data_grids_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])['INSEPECTION ID'].transform('nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'landuse',
                                                   'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index',
                                                   'active_stores', 'num_street_lights', 'num_traffic_lights',
                                                   'num_cases', 'num_momtathel_cases']].drop_duplicates()
        # base_data_grids_crm.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\Lighting_Features.csv",
        #     index=False)
        return base_data_grids_crm

    def construction(self):
        # preparing data for construction licenses and cleaning up dates in Hijri calendar

        constr_licenses_short = self.constr_licenses[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE']]
        constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE'],
                                     inplace=True)
        constr_lic_lat = list(constr_licenses_short['LONGITUDE'])
        constr_lic_long = list(constr_licenses_short['LATITUDE'])
        # cleaning lat/logs
        for idx, x in enumerate(constr_lic_lat):
            y = constr_lic_long[idx]
            try:
                if (float(x) > 50 or float(y) > 50):
                    constr_lic_lat[idx] = np.NaN
                    constr_lic_long[idx] = np.NaN
                    continue
                if (float(x) > float(y)):
                    constr_lic_lat[idx] = y
                    constr_lic_long[idx] = x
            except ValueError:
                constr_lic_lat[idx] = np.NaN
                constr_lic_long[idx] = np.NaN

        constr_licenses_short['LATITUDE'] = constr_lic_lat
        constr_licenses_short['LONGITUDE'] = constr_lic_long
        constr_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

        constr_licenses_gdf_active = self.constr_licenses_gdf[
            (self.constr_licenses_gdf['LICENSE_START_DATE'] <= self.now) & (self.constr_licenses_gdf['LICENSE_EXPIRY_DATE'] >= self.now)]
        base_grid_file = self.shpGrids_usable.sjoin(constr_licenses_gdf_active, how='left', predicate='intersects')
        base_grid_file["num_const_licenses"] = base_grid_file.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])[
            'LICENCES_ID'].transform('nunique')
        # preparing data for construction licenses and cleaning up dates in Hijri calendar
        excav_licenses=DB.get_licencedata_excavations()
        excav_licenses.columns=excav_licenses.columns.str.upper()
        exca_licenses_short = excav_licenses[
            ['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'CONTRACTOR_CR']]
        exca_licenses_short.dropna(
            subset=['LICENCES_ID', 'LATITUDE', 'LONGITUDE', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE',
                    'CONTRACTOR_CR'], inplace=True)
        exca_lic_lat = list(exca_licenses_short['LATITUDE'])
        exca_lic_long = list(exca_licenses_short['LONGITUDE'])
        # cleaning lat/logs
        for idx, x in enumerate(exca_lic_lat):
            y = exca_lic_long[idx]
            try:
                if (float(x) > 50 or float(y) > 50):
                    exca_lic_lat[idx] = np.NaN
                    exca_lic_long[idx] = np.NaN
                    continue
                if (float(x) > float(y)):
                    exca_lic_lat[idx] = y
                    exca_lic_long[idx] = x
            except ValueError:
                exca_lic_lat[idx] = np.NaN
                exca_lic_long[idx] = np.NaN

        exca_licenses_short['LATITUDE'] = exca_lic_lat
        exca_licenses_short['LONGITUDE'] = exca_lic_long
        exca_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

        # preparing data for commercial licenses
        exca_licenses_short['LICENSE_START_DATE'] = pd.to_datetime(exca_licenses_short['LICENSE_START_DATE'],
                                                                   format='%Y-%m-%d')
        exca_licenses_short['LICENSE_EXPIRY_DATE'] = pd.to_datetime(exca_licenses_short['LICENSE_EXPIRY_DATE'],
                                                                    format='%Y-%m-%d')
        exca_licenses_short['LATITUDE'] = exca_licenses_short['LATITUDE'].apply(pd.to_numeric, errors='coerce')
        exca_licenses_short['LONGITUDE'] = exca_licenses_short['LONGITUDE'].apply(pd.to_numeric, errors='coerce')
        exca_licenses_short.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

        exca_lic_gpd = gpd.GeoDataFrame(exca_licenses_short,
                                        geometry=gpd.points_from_xy(exca_licenses_short['LONGITUDE'],
                                                                    exca_licenses_short['LATITUDE']), crs='epsg:4326')
        exca_lic_gpd = exca_lic_gpd[
            ['LICENCES_ID', 'LICENSE_START_DATE', 'LICENSE_EXPIRY_DATE', 'LATITUDE', 'LONGITUDE', 'geometry',
             'CONTRACTOR_CR']]
        exca_lic_gpd_active = exca_lic_gpd[
            (exca_lic_gpd['LICENSE_START_DATE'] <= self.now) & (exca_lic_gpd['LICENSE_EXPIRY_DATE'] >= self.now)]
        base_grid_file = base_grid_file[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse', 'Count of POIs', 'Average POI',
             'POI_index', 'Number of priority Areas', 'Average Priority areas', 'Priority_Areas_Index',
             'num_const_licenses']].drop_duplicates()
        base_grid_file_lic = base_grid_file.sjoin(exca_lic_gpd_active, how='left', predicate='intersects')
        base_grid_file_lic["num_excav_licenses"] = \
        base_grid_file_lic.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])['LICENCES_ID'].transform('nunique')
        base_grid_file_lic = base_grid_file_lic[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse', 'Count of POIs', 'Average POI',
             'POI_index', 'Number of priority Areas', 'Average Priority areas', 'Priority_Areas_Index',
             'num_const_licenses', 'num_excav_licenses']].drop_duplicates()
        GDF_VP_cases_last_two_ligh = self.GDF_VP_cases_last_two[self.GDF_VP_cases_last_two['BCG_en_Categories'] == "Construction"]
        base_data_grids_crm = base_grid_file_lic.sjoin(GDF_VP_cases_last_two_ligh, how='left', predicate='intersects')
        base_data_grids_crm['num_cases'] = base_data_grids_crm.groupby(['GridNumber', 'AMANACODE'])['PYID'].transform(
            'nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                   'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index',
                                                   'num_const_licenses',
                                                   'num_excav_licenses', 'num_cases']]
        base_data_grids_crm = base_data_grids_crm.drop_duplicates()
        # base_data_grids_crm
        base_data_grids_crm = base_data_grids_crm.sjoin(
            self.planned_inspections_gdf[self.planned_inspections_gdf['BCG_en_Categories'] == "Construction"], how='left',
            predicate='intersects')
        base_data_grids_crm['num_momtathel_cases'] = \
        base_data_grids_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])['INSEPECTION ID'].transform('nunique')
        base_data_grids_crm = base_data_grids_crm[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry',
                                                   'landuse', 'Count of POIs', 'Average POI', 'POI_index',
                                                   'Number of priority Areas',
                                                   'Average Priority areas', 'Priority_Areas_Index',
                                                   'num_const_licenses',
                                                   'num_excav_licenses', 'num_cases', 'num_momtathel_cases']]
        base_data_grids_crm = base_data_grids_crm.drop_duplicates()
        constr_licenses_gdf_active = constr_licenses_gdf_active.to_crs("epsg:32637")
        constr_licenses_gdf_active['geometry'] = constr_licenses_gdf_active.buffer(500)
        constr_licenses_gdf_active = constr_licenses_gdf_active.to_crs("epsg:4326")
        GDF_VP_cases_construction = self.GDF_VP_cases[self.GDF_VP_cases['BCG_en_Categories'] == 'Construction']
        GDF_VP_cases_last_six = GDF_VP_cases_construction[
            GDF_VP_cases_construction['PXCREATEDATETIME'] > (self.last_data - datetime.timedelta(days=180))]
        constr_licenses_gdf_active_crm = constr_licenses_gdf_active.sjoin(GDF_VP_cases_last_six, how='left',
                                                                          predicate='intersects')
        # constr_licenses_gdf_active_crm
        constr_licenses_gdf_active_crm['crm_cases'] = constr_licenses_gdf_active_crm.groupby('LICENCES_ID')[
            'PYID'].transform('nunique')
        constr_licenses_gdf_active = constr_licenses_gdf_active.merge(
            constr_licenses_gdf_active_crm[['LICENCES_ID', 'crm_cases']].drop_duplicates(), on='LICENCES_ID',
            how='left')
        # constr_licenses_gdf_active['contractor_vp_cases'] = constr_licenses_gdf_active.groupby('CONTRACTOR_LICENSE_ID')['crm_cases'].transform('sum')
        constr_licenses_gdf_active['bad_contractors'] = constr_licenses_gdf_active['crm_cases'].apply(
            lambda x: 1 if x > 0 else 0)
        base_data_grids_contract = base_data_grids_crm.sjoin(constr_licenses_gdf_active, how='left',
                                                             predicate='intersects')
        base_data_grids_contract['total_bad_construction_contractors'] = \
        base_data_grids_contract.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['bad_contractors'].transform(
            'sum')
        base_data_grids_contract = base_data_grids_contract[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
             'Average Priority areas', 'Priority_Areas_Index', 'num_const_licenses',
             'num_excav_licenses', 'num_cases', 'num_momtathel_cases',
             'total_bad_construction_contractors']].drop_duplicates()
        exca_lic_gpd_active = exca_lic_gpd_active.to_crs("epsg:32637")
        exca_lic_gpd_active['geometry'] = exca_lic_gpd_active.buffer(500)
        exca_lic_gpd_active = exca_lic_gpd_active.to_crs("epsg:4326")
        GDF_VP_cases_construction = self.GDF_VP_cases[self.GDF_VP_cases['BCG_en_Categories'] == 'Construction']
        GDF_VP_cases_last_six = GDF_VP_cases_construction[
            GDF_VP_cases_construction['PXCREATEDATETIME'] > (self.last_data - datetime.timedelta(days=180))]
        exca_lic_gpd_active_crm = exca_lic_gpd_active.sjoin(GDF_VP_cases_last_six, how='left', predicate='intersects')
        # constr_licenses_gdf_active_crm
        exca_lic_gpd_active_crm['crm_cases'] = exca_lic_gpd_active_crm.groupby('LICENCES_ID')['PYID'].transform(
            'nunique')
        exca_lic_gpd_active = exca_lic_gpd_active.merge(
            exca_lic_gpd_active_crm[['LICENCES_ID', 'crm_cases']].drop_duplicates(), on='LICENCES_ID', how='left')
        exca_lic_gpd_active['contractor_vp_cases'] = exca_lic_gpd_active.groupby('CONTRACTOR_CR')[
            'crm_cases'].transform('sum')
        exca_lic_gpd_active['bad_contractors'] = exca_lic_gpd_active['contractor_vp_cases'].apply(
            lambda x: 1 if x > 0 else 0)
        base_data_grids_excav = base_data_grids_contract.sjoin(exca_lic_gpd_active, how='left', predicate='intersects')
        base_data_grids_excav['total_bad_excav_contractors'] = \
        base_data_grids_excav.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['bad_contractors'].transform(
            'sum')
        base_data_grids_excav = base_data_grids_excav[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
             'Average Priority areas', 'Priority_Areas_Index', 'num_const_licenses',
             'num_excav_licenses', 'num_cases', 'num_momtathel_cases',
             'total_bad_construction_contractors', 'total_bad_excav_contractors']].drop_duplicates()
        # base_data_grids_excav.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\Construction_Features.csv",
        #     index=False)
        return base_data_grids_excav

    def publicspaces_parks(self):
        publicfacilities=shp.get_publicfacilities()
        publicfacilities = publicfacilities.rename(columns={"GEOMETRY": "geometry"})
        publicfacilities['geometry'] = publicfacilities['geometry'].astype(str)
        publicfacilities['geometry'] = publicfacilities['geometry'].apply(wkt.loads)
        publicfacilities = gpd.GeoDataFrame(publicfacilities, geometry='geometry', crs='epsg:4326')
        base_grids = self.shpGrids_usable.sjoin(publicfacilities, how="left", predicate="intersects")
        base_grids['num_of_facilities'] = base_grids.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])[
            'OBJECTID'].transform('count')
        base_grids[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
                    'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                    'Average Priority areas', 'Priority_Areas_Index', 'num_of_facilities']].drop_duplicates()
        base_grids = base_grids[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
                                 'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                 'Average Priority areas', 'Priority_Areas_Index',
                                 'num_of_facilities']].drop_duplicates()
        parksandrec=shp.get_parksandrec()
        parksandrec = parksandrec.rename(columns={"GEOMETRY": "geometry"})
        parksandrec['geometry'] = parksandrec['geometry'].astype(str)
        parksandrec['geometry'] = parksandrec['geometry'].apply(wkt.loads)
        parksandrec = gpd.GeoDataFrame(parksandrec, geometry='geometry', crs='epsg:4326')
        base_grids_parks = base_grids.sjoin(parksandrec, how="left", predicate="intersects")
        base_grids_parks['num_of_parks'] = base_grids_parks.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])[
            'OBJECTID'].transform('count')
        base_grids_parks = base_grids_parks[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
                                             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                             'Average Priority areas', 'Priority_Areas_Index', 'num_of_facilities',
                                             "num_of_parks"]].drop_duplicates()
        GDF_VP_cases_last_two_parks = self.GDF_VP_cases_last_two[
            self.GDF_VP_cases_last_two['BCG_en_Categories'] == "Public spaces and parks"]
        base_grids_parks_crm = base_grids_parks.sjoin(GDF_VP_cases_last_two_parks, how='left', predicate='intersects')
        base_grids_parks_crm['num_cases'] = \
        base_grids_parks_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN'])['PYID'].transform('nunique')
        base_grids_parks_crm = base_grids_parks_crm[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
             'Average Priority areas', 'Priority_Areas_Index', 'num_of_facilities',
             'num_of_parks', 'num_cases']].drop_duplicates()
        base_grids_parks_crm = base_grids_parks_crm.sjoin(
            self.planned_inspections_gdf[self.planned_inspections_gdf['BCG_en_Categories'] == "Public spaces and parks"],
            how='left', predicate='intersects')
        base_grids_parks_crm['num_momtathel_cases'] = \
        base_grids_parks_crm.groupby(['AMANACODE', 'MUNICIPA_1', 'GridNumber'])['INSEPECTION ID'].transform('nunique')
        base_grids_parks_crm = base_grids_parks_crm[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'geometry', 'landuse',
             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
             'Average Priority areas', 'Priority_Areas_Index', 'num_of_facilities',
             'num_of_parks', 'num_cases', 'num_momtathel_cases']].drop_duplicates()
        # base_grids_parks_crm.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\Parks_Features.csv",
        #     index=False)
        return base_grids_parks_crm