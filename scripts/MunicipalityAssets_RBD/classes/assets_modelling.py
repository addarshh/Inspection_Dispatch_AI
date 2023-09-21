# Import Librariesb
import numpy as np
import pandas as pd
import geopandas as gpd
import config
from classes import scores
from sklearn.mixture import GaussianMixture
from abc import ABC
from classes import features_preprocessing as features
import warnings
from classes import Database as DB
data=DB.Database()
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
import os
from datetime import datetime
warnings.filterwarnings('ignore')
from classes.engines import Helper
preprocess=features.cleaning()
class clustering(ABC):
    def __init__(self):
        pass
    def buildings_model(self):
        print("buildings model")
        ### Buildings
    # Creating the Clustering Model for the Buildings VP Category. For this Category, we have the following Model features:
    # 1. Municipal Assets:
    #     - Number of Buildings
    #     - Number of Restaurants
    #     - Number of People
    #     - Priority area flag
    # 2. Volume of cases:
    #     - Number of CRM cases observed in the last 2 weeks
    #     - Number of Inspector led cases observed in the last 2 weeks


        #buildings_features = pd.read_csv(r"C:\Users\acioopr46\Downloads\test\Buildings_Features.csv")
        self.shpGrid, buildings_features=preprocess.buildings()
        model_ma = GaussianMixture(n_components=2, covariance_type='spherical', random_state=143)
        model_cases = GaussianMixture(n_components=2, covariance_type='spherical', random_state=143)
        final_grids = buildings_features.copy()
        ### Municipal Assets Cluster Engine
        gm = model_ma.fit(buildings_features[['landuse',
                                              'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                              'Average Priority areas', 'Priority_Areas_Index',
                                              'active_rest', 'num_buildings', 'DN']])
        final_grids['ma_clusters_buildings'] = gm.predict(buildings_features[['landuse',
                                                                              'Count of POIs', 'Average POI', 'POI_index',
                                                                              'Number of priority Areas',
                                                                              'Average Priority areas',
                                                                              'Priority_Areas_Index',
                                                                              'active_rest', 'num_buildings', 'DN']])
        ### CRM Cases Engine
        gm_crm = model_cases.fit(buildings_features[['num_cases', 'num_momthat_cases']])
        final_grids['cases_clusters_buildings'] = gm_crm.predict(buildings_features[['num_cases', 'num_momthat_cases']])
        final_grids[final_grids['cases_clusters_buildings'] == 1]['num_momthat_cases'].sum()

        #### Hardcoding Cluster Identities
        #To do this, we will take 3 features and compare the total values in each class. The class with the higher total in all the three cases will be tagged to 1
        switch_flag = 0
        if (final_grids[final_grids['ma_clusters_buildings'] == 1]['DN'].sum() >=
            final_grids[final_grids['ma_clusters_buildings'] == 0]['DN'].sum()) or \
                (final_grids[final_grids['ma_clusters_buildings'] == 1]['num_buildings'].sum() >=
                 final_grids[final_grids['ma_clusters_buildings'] == 0]['num_buildings'].sum()) or \
                (final_grids[final_grids['ma_clusters_buildings'] == 1]['active_rest'].sum() >=
                 final_grids[final_grids['ma_clusters_buildings'] == 0]['active_rest'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['ma_clusters_buildings'] = final_grids['ma_clusters_buildings'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['cases_clusters_buildings'] == 1]['num_cases'].sum() >=
            final_grids[final_grids['cases_clusters_buildings'] == 0]['num_cases'].sum()) and \
                (final_grids[final_grids['cases_clusters_buildings'] == 1]['num_momthat_cases'].sum() >=
                 final_grids[final_grids['cases_clusters_buildings'] == 0]['num_momthat_cases'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['cases_clusters_buildings'] = final_grids['cases_clusters_buildings'].replace({0: 1, 1: 0})
        #final_grids.to_csv(r"C:\Users\acioopr23\temp\Buildings_Scores.csv")
        self.building_scores = final_grids.copy()
        if config.scores_backup==True:
            DB.save_final(final_grids, "MUNICIPALITY_ASSETS_BUILDING_SCORES")


    def street_model(self):
        print("street models")
        ## Street Clustering
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

        #street_features = pd.read_csv(r"C:\Users\acioopr46\Downloads\test\Street_Features.csv")
        street_features=preprocess.street_cleaning()
        street_features['total_bad_contractors'] = street_features['total_bad_construction_contractors'] + street_features[
            'total_bad_excavation_contractors']
        model_ma = GaussianMixture(n_components=2, random_state=143)
        model_cases = GaussianMixture(n_components=2, random_state=143)
        model_contract = GaussianMixture(n_components=2, random_state=143)

        final_grids = street_features[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'DN', 'Average POI',
                                       'Average Priority areas',
                                       'num_roads', 'road_length', 'num_pavements', 'pavement_length',
                                       'num_parking_areas', 'num_cases', 'num_momtathel_cases',
                                       'total_bad_contractors']].copy()
        ### Municipal Assets Cluster Engine
        gm = model_ma.fit(street_features[[
            'Average POI',
            'Average Priority areas', 'num_roads', 'road_length', 'num_pavements', 'pavement_length',
            'num_parking_areas', 'DN']])
        final_grids['ma_clusters_street'] = gm.predict(street_features[[
            'Average POI',
            'Average Priority areas', 'num_roads', 'road_length', 'num_pavements', 'pavement_length',
            'num_parking_areas', 'DN']])
        ### CRM Cases Engine
        gm_crm = model_cases.fit(street_features[['num_cases', 'num_momtathel_cases']])
        final_grids['cases_clusters_street'] = gm_crm.predict(street_features[['num_cases', 'num_momtathel_cases']])
        ### Contractor Engine
        gm_contr = model_contract.fit(street_features['total_bad_contractors'].values.reshape(-1, 1))
        final_grids['contractor_clusters_street'] = gm_contr.predict(
            street_features['total_bad_contractors'].values.reshape(-1, 1))

        #### Hardcoding Cluster Identities
        #To do this, we will take 3 features and compare the median values. The class with the higher median in all the three cases will be tagged to 1
        switch_flag = 0
        if (final_grids[final_grids['ma_clusters_street'] == 1]['num_roads'].sum() >=
            final_grids[final_grids['ma_clusters_street'] == 0]['num_roads'].sum()) or \
                (final_grids[final_grids['ma_clusters_street'] == 1]['num_pavements'].sum() >=
                 final_grids[final_grids['ma_clusters_street'] == 0]['num_pavements'].sum()) or \
                (final_grids[final_grids['ma_clusters_street'] == 1]['num_parking_areas'].sum() >=
                 final_grids[final_grids['ma_clusters_street'] == 0]['num_parking_areas'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['ma_clusters_street'] = final_grids['ma_clusters_street'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['cases_clusters_street'] == 1]['num_cases'].sum() >=
            final_grids[final_grids['cases_clusters_street'] == 0]['num_cases'].sum()) and \
                (final_grids[final_grids['cases_clusters_street'] == 1]['num_momtathel_cases'].sum() >=
                 final_grids[final_grids['cases_clusters_street'] == 0]['num_momtathel_cases'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['cases_clusters_street'] = final_grids['cases_clusters_street'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['contractor_clusters_street'] == 1]['total_bad_contractors'].sum() >=
                final_grids[final_grids['contractor_clusters_street'] == 0]['total_bad_contractors'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['contractor_clusters_street'] = final_grids['contractor_clusters_street'].replace({0: 1, 1: 0})
        # final_grids.to_csv(r"C:\Users\acioopr46\Downloads\test\scores\Street_Scores.csv")
        self.street_scores = final_grids
        if config.scores_backup==True:
            DB.save_final(final_grids, "MUNICIPALITY_ASSETS_STREET_SCORES")


    def lighting_model(self):
        print("lighting models")
        #lighting_features = pd.read_csv(r"C:\Users\acioopr46\Downloads\test\Lighting_Features.csv")
        lighting_features=preprocess.lights_utilities()
        model_ma = GaussianMixture(n_components=2, random_state=143)
        model_cases = GaussianMixture(n_components=2, random_state=143)
        final_grids = lighting_features.copy()
        ### Municipal Assets Cluster Engine
        gm = model_ma.fit(lighting_features[['DN', 'landuse',
                                             'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                             'Average Priority areas', 'Priority_Areas_Index', 'active_stores',
                                             'num_street_lights', 'num_traffic_lights']])
        final_grids['ma_clusters_light'] = gm.predict(lighting_features[['DN', 'landuse',
                                                                         'Count of POIs', 'Average POI', 'POI_index',
                                                                         'Number of priority Areas',
                                                                         'Average Priority areas', 'Priority_Areas_Index',
                                                                         'active_stores',
                                                                         'num_street_lights', 'num_traffic_lights']])
        ### CRM Cases Engine
        gm_crm = model_cases.fit(lighting_features[['num_cases',
                                                    'num_momtathel_cases']])
        final_grids['cases_clusters_light'] = gm_crm.predict(lighting_features[['num_cases',
                                                                                'num_momtathel_cases']])
        #### Hardcoding Cluster Identities
        #To do this, we will take 3 features and compare the total values in each class. The class with the higher total in all the three cases will be tagged to 1
        switch_flag = 0
        if (final_grids[final_grids['ma_clusters_light'] == 1]['active_stores'].sum() >=
            final_grids[final_grids['ma_clusters_light'] == 0]['active_stores'].sum()) or \
                (final_grids[final_grids['ma_clusters_light'] == 1]['num_street_lights'].sum() >=
                 final_grids[final_grids['ma_clusters_light'] == 0]['num_street_lights'].sum()) or \
                (final_grids[final_grids['ma_clusters_light'] == 1]['num_traffic_lights'].sum() >=
                 final_grids[final_grids['ma_clusters_light'] == 0]['num_traffic_lights'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['ma_clusters_light'] = final_grids['ma_clusters_light'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['cases_clusters_light'] == 1]['num_cases'].sum() >=
            final_grids[final_grids['cases_clusters_light'] == 0]['num_cases'].sum()) or \
                (final_grids[final_grids['cases_clusters_light'] == 1]['num_momtathel_cases'].sum() >=
                 final_grids[final_grids['cases_clusters_light'] == 0]['num_momtathel_cases'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['cases_clusters_light'] = final_grids['cases_clusters_light'].replace({0: 1, 1: 0})
        # final_grids.to_csv(r"C:\Users\acioopr46\Downloads\test\scores\Lighting_Scores.csv")
        self.light_scores = final_grids
        if config.scores_backup==True:
            DB.save_final(final_grids, "MUNICIPALITY_ASSETS_LIGHTING_SCORES")


    def construction_model(self):
        print("construction model")
        ## Construction
        # Creating the Clustering Model for the Street VP Category. For this Category, we have the following Model features:
        # 1. Municipal Assets:
        #     - Number of Active Construction licenses
        #     - Number of Construction Contractors with VP History
        #     - Number of Active Excavation licenses
        #     - Number of Excavation Contractors with VP History
        #
        # 2. Volume of cases:
        #     - Number of CRM cases observed in the last 2 weeks,
        self.construction_features=preprocess.construction()
        #self.construction_features = pd.read_csv(r"C:\Users\acioopr46\Downloads\test\Construction_Features.csv")
        self.construction_features['total_bad_contractors'] = self.construction_features['total_bad_construction_contractors'] + \
                                                         self.construction_features['total_bad_excav_contractors']
        model_ma = GaussianMixture(n_components=2, random_state=143)
        model_cases = GaussianMixture(n_components=2, random_state=143)
        model_contrac = GaussianMixture(n_components=2, random_state=143)

        final_grids = self.construction_features.copy()
        ### Municipal Assets Cluster Engine
        gm = model_ma.fit(self.construction_features[['landuse',
                                                 'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                                 'Average Priority areas', 'Priority_Areas_Index', 'num_const_licenses',
                                                 'num_excav_licenses', 'DN']])
        final_grids['ma_clusters_construction'] = gm.predict(self.construction_features[['landuse',
                                                                                    'Count of POIs', 'Average POI',
                                                                                    'POI_index', 'Number of priority Areas',
                                                                                    'Average Priority areas',
                                                                                    'Priority_Areas_Index',
                                                                                    'num_const_licenses',
                                                                                    'num_excav_licenses', 'DN']])
        ### CRM Cases Engine
        gm_crm = model_cases.fit(self.construction_features[['num_cases', 'num_momtathel_cases']])
        final_grids['cases_clusters_construction'] = gm_crm.predict(
            self.construction_features[['num_cases', 'num_momtathel_cases']])
        ### Contractor Engine
        gm_contra = model_contrac.fit(self.construction_features['total_bad_contractors'].values.reshape(-1, 1))
        final_grids['contract_clusters_construction'] = gm_contra.predict(
            self.construction_features['total_bad_contractors'].values.reshape(-1, 1))

        #### Hardcoding Cluster Identities
        # To do this, we will take 3 features and compare the total values in each class. The class with the higher total in all the three cases will be tagged to 1
        switch_flag = 0
        if (final_grids[final_grids['ma_clusters_construction'] == 1]['num_const_licenses'].sum() >=
            final_grids[final_grids['ma_clusters_construction'] == 0]['num_const_licenses'].sum()) or \
                (final_grids[final_grids['ma_clusters_construction'] == 1]['num_excav_licenses'].sum() >=
                 final_grids[final_grids['ma_clusters_construction'] == 0]['num_excav_licenses'].sum()):
            pass
        else:
            switch_flag = 1

        if switch_flag:
            final_grids['ma_clusters_construction'] = final_grids['ma_clusters_construction'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['contract_clusters_construction'] == 1]['num_cases'].sum() >=
            final_grids[final_grids['contract_clusters_construction'] == 0]['num_cases'].sum()) or \
                (final_grids[final_grids['contract_clusters_construction'] == 1]['num_momtathel_cases'].sum() >=
                 final_grids[final_grids['contract_clusters_construction'] == 0]['num_momtathel_cases'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['cases_clusters_construction'] = final_grids['cases_clusters_construction'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['contract_clusters_construction'] == 1]['total_bad_contractors'].sum() >=
                final_grids[final_grids['contract_clusters_construction'] == 0]['total_bad_contractors'].sum()):
            pass
        else:
            switch_flag = 1
        # final_grids.to_csv(r"C:\Users\acioopr46\Downloads\test\scores\Construction_Scores.csv")
        self.const_scores = final_grids
        if config.scores_backup==True:
            DB.save_final(final_grids, "MUNICIPALITY_ASSETS_CONSTRUCTION_SCORES")


    def parks_model(self):
        print("parks models")
        ## Public Spaces and Parks
        # Creating the Clustering Model for the Street VP Category. For this Category, we have the following Model features:
        # 1. Municipal Assets:
        #     - Number of Public Toilets
        #     - Number of Public Parks
        #
        # 2. Volume of cases:
        #     - Number of CRM cases observed in the last 2 weeks
        parks_features=preprocess.publicspaces_parks()
        #parks_features = pd.read_csv(
        #   r"C:\Users\acioopr46\Downloads\test\Parks_Features.csv")
        model_ma = GaussianMixture(n_components=2, random_state=143)
        model_cases = GaussianMixture(n_components=2, random_state=143)
        final_grids = parks_features.copy()
        gm = model_ma.fit(parks_features[['landuse',
                                          'Count of POIs', 'Average POI', 'POI_index', 'Number of priority Areas',
                                          'Average Priority areas', 'Priority_Areas_Index', 'num_of_facilities',
                                          'num_of_parks', 'DN']])
        final_grids['ma_clusters_parks'] = gm.predict(parks_features[['landuse',
                                                                      'Count of POIs', 'Average POI', 'POI_index',
                                                                      'Number of priority Areas',
                                                                      'Average Priority areas', 'Priority_Areas_Index',
                                                                      'num_of_facilities',
                                                                      'num_of_parks', 'DN']])
        gm_crm = model_cases.fit(self.construction_features[['num_cases', 'num_momtathel_cases']])
        final_grids['cases_clusters_parks'] = gm_crm.predict(self.construction_features[['num_cases', 'num_momtathel_cases']])
        switch_flag = 0
        if (final_grids[final_grids['ma_clusters_parks'] == 1]['num_of_facilities'].sum() >=
            final_grids[final_grids['ma_clusters_parks'] == 0]['num_of_facilities'].sum()) or \
                (final_grids[final_grids['ma_clusters_parks'] == 1]['num_of_parks'].sum() >=
                 final_grids[final_grids['ma_clusters_parks'] == 0]['num_of_parks'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['ma_clusters_parks'] = final_grids['ma_clusters_parks'].replace({0: 1, 1: 0})
        switch_flag = 0
        if (final_grids[final_grids['cases_clusters_parks'] == 1]['num_cases'].sum() >=
            final_grids[final_grids['cases_clusters_parks'] == 0]['num_cases'].sum()) or \
                (final_grids[final_grids['cases_clusters_parks'] == 1]['num_momtathel_cases'].sum() >=
                 final_grids[final_grids['cases_clusters_parks'] == 0]['num_momtathel_cases'].sum()):
            pass
        else:
            switch_flag = 1
        if switch_flag:
            final_grids['cases_clusters_parks'] = final_grids['cases_clusters_parks'].replace({0: 1, 1: 0})
        # final_grids.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\scores\Park_Scores.csv")
        self.park_scores = final_grids
        if config.scores_backup==True:
            DB.save_final(final_grids, "MUNICIPALITY_ASSETS_PARK_SCORES")


    def score_generation(self):
        ## Score Generation
        print("score generation")
        print(self.building_scores.columns)
        self.building_scores.rename(columns={'GRIDNUMBER':'GridNumber','MA_CLUSTERS_BUILDING':'ma_clusters_buildings','CASES_CLUSTERS_BUILDINGS':'cases_clusters_buildings'})
        building_scores = self.building_scores[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'ma_clusters_buildings', 'cases_clusters_buildings']]
        street_scores = self.street_scores[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'ma_clusters_street', 'cases_clusters_street',
             'contractor_clusters_street']]
        light_scores = self.light_scores[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'ma_clusters_light', 'cases_clusters_light']]
        const_scores = self.const_scores[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'ma_clusters_construction', 'cases_clusters_construction',
             'contract_clusters_construction']]
        park_scores = self.park_scores[['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'ma_clusters_parks', 'cases_clusters_parks']]
        final_scores = building_scores.merge(street_scores, how='outer').merge(light_scores, how='outer').merge(
            const_scores, how='outer').merge(park_scores, how='outer')

        final_scores['building_scores'] = final_scores[['ma_clusters_buildings', 'cases_clusters_buildings']].apply(
            lambda x: scores.generate_build(x), axis=1)

        final_scores['streets_scores'] = final_scores[
            ['ma_clusters_street', 'cases_clusters_street', 'contractor_clusters_street']].apply(
            lambda x: scores.generate_roads(x), axis=1)

        final_scores['light_scores'] = final_scores[['ma_clusters_light', 'cases_clusters_light']].apply(
            lambda x: scores.generate_light(x), axis=1)

        final_scores['parks_scores'] = final_scores[['ma_clusters_parks', 'cases_clusters_parks']].apply(
            lambda x: scores.generate_park(x), axis=1)

        final_scores['construction_scores'] = final_scores[
            ['ma_clusters_construction', 'cases_clusters_construction', 'contract_clusters_construction']].apply(
            lambda x: scores.generate_const(x), axis=1)
        final_scores['final_score'] = 0.25 * (
                    final_scores['construction_scores'] + final_scores['streets_scores']) + 0.20 * (
                                                  final_scores['building_scores'] + final_scores['light_scores']) + 0.1 * (
                                      final_scores['parks_scores'])
        final_score_df = final_scores[
            ['AMANACODE', 'MUNICIPA_1', 'GridNumber', 'final_score', 'building_scores', 'streets_scores',
             'construction_scores', 'light_scores', 'parks_scores']]
        # #Load updated Shapefiles of the population data with Grid Number
        # print("reading population shape files")
        # #shpGrid = gpd.read_file(config.population_grids_path)
        #
        # print("reading completed")
        shpGrid_nopop = self.shpGrid[self.shpGrid['DN'] <= 0]
        final_score_df_all = pd.concat([final_score_df, shpGrid_nopop[['AMANACODE', 'MUNICIPA_1', 'GridNumber']]])
        # final_score_df_all.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\scores\final_scores_all.csv",
        #     index=False)
        final_score_df_all = final_score_df_all.replace(np.nan, 0)
        percentile_value=config.percentile_value
        if (len(final_score_df_all[final_score_df_all['final_score'] >= (
        final_score_df_all['final_score'].quantile(q=config.percentile_value))]) / len(final_score_df_all)) > config.thresh_percentage:
            while True:
                percentile_value = percentile_value + config.incremental_value
                if (len(final_score_df_all[final_score_df_all['final_score'] >= (final_score_df_all['final_score'].quantile(q=percentile_value))]) / len(final_score_df_all)) > config.thresh_percentage:
                    continue
                else:
                    break
        medium_thresh = final_score_df_all['final_score'].quantile(q=percentile_value)

        def get_cadence(score):
            if score >= config.very_high_thresh:
                return "Very High Risk"
            if score >= config.high_thresh:
                return "High Risk"
            if score >= medium_thresh:
                return "Medium Risk"
            return "Low Risk"

        final_score_df_all['Riskiness'] = final_score_df_all['final_score'].apply(lambda x: get_cadence(x))
        print("completed")
        zone_df=data.get_grid_zone()
        final_score_df_all=final_score_df_all.merge(zone_df, how="left", left_on="GridNumber", right_on="griduniquecode")
        # print(final_score_df_all.columns)
        # grid_geo=pd.read_csv(config.population_grids_path, dtype={"geometry":"str"})
        # grid_geo=grid_geo[grid_geo["griduniquecode","geometry"]]
        # print(grid_geo.columns)
        #final_score_df_all=final_score_df_all.merge(grid_geo, how="left", left_on="GRIDUNIQUECODE", right_on="griduniquecode")
        DB.save_final(final_score_df_all,"MUNICIPALITY_ASSETS_FINAL_SCORE")
        # final_score_df_all.to_csv(
        #     r"C:\Users\acioopr46\Downloads\test\scores\final_scores_all.csv",index=False)