import pandas as pd
from xgboost import XGBClassifier
import time
import datetime
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import classification_report,confusion_matrix
from sklearn.preprocessing import MinMaxScaler, PowerTransformer, QuantileTransformer
from imblearn.pipeline import make_pipeline as imbalanced_make_pipeline
from imblearn.over_sampling import SMOTE
from decimal import Decimal
import numpy as np
import geopandas as gpd
from shapely import wkt
import os.path as path

import config
from ..FileDatabase import fileDatabase


class RiskBasedDispatch ():
    def __init__(self):
       self.model = None
       self.scaler = None

    def predict_descrete(self, features : pd.DataFrame) -> pd.DataFrame:
        xgb_predict = self.model.predict_proba(features)
        xgb_predict[(xgb_predict > 0.5)] = 1
        xgb_predict[(xgb_predict < 0.5)] =0
        xgb_predict_df = pd.DataFrame(xgb_predict, columns=['probability of no violation','probability of violation'])
        return xgb_predict_df

    def predict(self, features : pd.DataFrame) -> pd.DataFrame:
        xgb_predict = self.model.predict_proba(features)
        xgb_predict_df = pd.DataFrame(xgb_predict, columns=['probability of no violation','probability of violation'])
        return xgb_predict_df

    def saveModel(self) -> None:
        self.model.save_model(path.join(path.dirname(__file__), config.MODEL_SCORING_OUTPUT_FOLDER + '/model.bin'))
    
    def loadModel(self) -> None:
        self.model.load_model(path.join(path.dirname(__file__), config.MODEL_SCORING_OUTPUT_FOLDER + '/model.bin'))
    
    def trainTestModel(self, data : pd.DataFrame, features_columns : list, labels_column : list) -> None:
        print(data.columns)
        x = data[features_columns]
        y = data[labels_column]
        
        self.model =XGBClassifier(objective='binary:logistic', scale_pos_weight=2, use_label_encoder=False)

        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.33, random_state=42, stratify=y)
        self.model.fit(x_train, y_train)

        xgb_predict_df = self.predict_descrete(x_train)

        # #print(confusion_matrix(y_train,xgb_predict_df['probability of violation']))
        # #print(classification_report(y_train,xgb_predict_df['probability of violation']))

        xgb_predict_df = self.predict_descrete(x_test)

        # #print(confusion_matrix(y_test,xgb_predict_df['probability of violation']))
        # #print(classification_report(y_test,xgb_predict_df['probability of violation']))

    def trainTestModelExcavation(self, data : pd.DataFrame, features_columns : list, labels_column : list):
        X = data[features_columns].copy()
        y = data[labels_column].copy()
        y['ISVIOLATION'] = y['ISVIOLATION'].apply(lambda x: 1 if x else 0 )
        print(y['ISVIOLATION'].unique())
        # print(X.shape)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, shuffle=True, stratify=y, random_state=21)
        X.fillna(method="bfill",inplace=True)
        X.fillna(method="ffill",inplace=True)

        sk = StratifiedKFold(n_splits=16, random_state=143, shuffle=True)
        for train_index, test_index in sk.split(X_train, y_train):
        #     #print("Train:", train_index, "Test:", test_index)
            unsampled_Xtrain, unsampled_Xtest = X.iloc[train_index], X.iloc[test_index]
            unsampled_ytrain, unsampled_ytest = y.iloc[train_index], y.iloc[test_index]

        unsampled_Xtrain = unsampled_Xtrain.values
        unsampled_Xtest = unsampled_Xtest.values
        unsampled_ytrain = unsampled_ytrain.values
        unsampled_ytest = unsampled_ytest.values

        self.model = XGBClassifier(random_state=143)
        self.scaler = QuantileTransformer(output_distribution='normal', random_state=1975)

        unsampled_Xtrain = self.scaler.fit_transform(unsampled_Xtrain)


        for train, test in sk.split(unsampled_Xtrain, unsampled_ytrain):
            sampled_pipeline = imbalanced_make_pipeline(SMOTE(sampling_strategy='minority', random_state=143), self.model)
            sampled_model = sampled_pipeline.fit(unsampled_Xtrain[train], unsampled_ytrain[train])
            sampled_prediction = self.model.predict(unsampled_Xtrain[test])
        
        X_test_transformed = self.scaler.transform(X_test)

        xgb_predict_df = self.predict_descrete(X_test_transformed)
        #print(confusion_matrix(y_test,xgb_predict_df['probability of violation']))
        #print(classification_report(y_test,xgb_predict_df['probability of violation']))

    def processModelExcavation(self, data : pd.DataFrame, features_columns : list, labels_column : list) -> None:
        X = data[features_columns]
        X = self.scaler.transform(X)
        data = data.reset_index(drop=True)

        xgb_predict_df = self.predict(X)
        predictions_df = data.merge(xgb_predict_df, left_index=True, right_index=True)
        return predictions_df

    def processModel(self, data : pd.DataFrame, features_columns : list, labels_column : list) -> None:
        x = data[features_columns]

        data = data.reset_index(drop=True)
        xgb_predict_df = self.predict(x)
        predictions_df = data.merge(xgb_predict_df, left_index=True, right_index=True)
        return predictions_df

    def scale(self, X):
        scaler = MinMaxScaler()
        try:
            X_ = np.atleast_2d(X)
            return pd.DataFrame(scaler.fit_transform(X_), X.index)
        except ValueError:
            return pd.DataFrame((X_), X.index)

    def getModelResults(self, data : fileDatabase, calculated_data : pd.DataFrame, inspectionType, licensesData : pd.DataFrame,) -> None:
        impact_weight = 0.5
        visibility_weight = 0.5
        unlicensed_weight = 10

        powerScaler = PowerTransformer(method="box-cox")
        # quantileScaler = QuantileTransformer()
        scaler = MinMaxScaler()

        licenses_df = licensesData

        predictions_df=calculated_data.merge(licenses_df[['License ID (MOMRAH)','Area of the retail outlet']], on='License ID (MOMRAH)', how='left')
        predictions_df[['Scaled area of the retail outlet']] = predictions_df.groupby('Business activity')[['Area of the retail outlet']].apply(lambda x : self.scale(x))+1

        n_priority_areas_per_grid=gpd.sjoin(data.amanaPopulationOverlay, data.priorityAreas, how='inner', predicate='intersects').groupby('GridNumber')['geometry'].count().reset_index().rename(columns={'geometry':'n_priority_intersection'})
        n_priority_areas_per_grid=n_priority_areas_per_grid.merge(data.amanaPopulationOverlay[['GridNumber']], on='GridNumber', how='right')
        n_priority_areas_per_grid['n_priority_intersection']=n_priority_areas_per_grid['n_priority_intersection'].fillna(0)
        n_priority_areas_per_grid=n_priority_areas_per_grid.merge(data.populationData[['GridNumber','DN']])

        #why add 1?
        n_priority_areas_per_grid[['impact_risk']]=scaler.fit_transform(n_priority_areas_per_grid[['n_priority_intersection']])+1
        #why add 1?
        n_priority_areas_per_grid[['visibility_risk']]=scaler.fit_transform(n_priority_areas_per_grid[['DN']])+1
        n_priority_areas_per_grid['location_risk']=n_priority_areas_per_grid['visibility_risk']*visibility_weight+n_priority_areas_per_grid['impact_risk']*impact_weight

        predictions_df['Business Activity Weight']=predictions_df['Business Activity Weight'].fillna(2)

        predictions_df.loc[predictions_df['probability of violation']>0.75, 'probability of violation (discrete)']='high (75% - 100%)'
        predictions_df.loc[predictions_df['probability of violation']<=0.75, 'probability of violation (discrete)']='medium (50% - 75%)'
        predictions_df.loc[predictions_df['probability of violation']<=0.5, 'probability of violation (discrete)']='low (0% - 50%)'

        predictions_df = gpd.GeoDataFrame(
            predictions_df, geometry=gpd.points_from_xy(predictions_df.Latitude, predictions_df.Longitude), crs="EPSG:4326")

        grid_level_probabilities = gpd.sjoin(data.amanaPopulationOverlay, predictions_df, how='left').drop_duplicates(['GridNumber','License ID (MOMRAH)'])
        grid_level_probabilities['License risk']=grid_level_probabilities['Business Activity Weight']*grid_level_probabilities['probability of violation']*grid_level_probabilities['Scaled area of the retail outlet']
        grid_level_probabilities['probability of violation (discrete)'].fillna(0, inplace = True)
        
        grid_level_license_risks=grid_level_probabilities.groupby(['GridNumber','probability of violation (discrete)'])['License ID (MOMRAH)'].nunique().reset_index()
        grid_level_license_risks=pd.pivot_table(grid_level_license_risks, index='GridNumber', values='License ID (MOMRAH)', columns='probability of violation (discrete)').reset_index().fillna(0)

        grid_level_license_risks['high-risk license %']=100*grid_level_license_risks['high (75% - 100%)']/(grid_level_license_risks['high (75% - 100%)'] + grid_level_license_risks['medium (50% - 75%)'] + grid_level_license_risks['low (0% - 50%)'])
        grid_level_license_risks['medium-risk license %']=100*grid_level_license_risks['medium (50% - 75%)']/(grid_level_license_risks['high (75% - 100%)'] +grid_level_license_risks['medium (50% - 75%)']+ grid_level_license_risks['low (0% - 50%)'])
        grid_level_license_risks['low-risk license %']=100*grid_level_license_risks['low (0% - 50%)']/(grid_level_license_risks['high (75% - 100%)'] +grid_level_license_risks['medium (50% - 75%)']+ grid_level_license_risks['low (0% - 50%)'])

        grid_level_license_risks=grid_level_license_risks.rename(columns={'high (75% - 100%)':'number of high (75% - 100%) licenses',
                                                                        'medium (50% - 75%)':'number of medium (50% - 75%) licenses',
                                                                        'low (0% - 50%)':'number of low (0% - 50%) licenses'})
        grid_level_new_businesses=grid_level_probabilities.groupby('GridNumber')['new_business'].sum().reset_index().rename(columns={'new_business':'# of new businesses'})

        grid_level_license_risks=grid_level_license_risks.merge(grid_level_new_businesses, how='left', on='GridNumber')
        grid_level_license_risks=grid_level_license_risks.fillna(0)

        licenses_per_grid=grid_level_probabilities.groupby('GridNumber')['License ID (MOMRAH)'].nunique().reset_index().rename(columns={'License ID (MOMRAH)':'Number of Licenses'})
        population_per_grid=data.amanaPopulationOverlay[['GridNumber','DN']].rename(columns={'DN':'Population'})
        licenses_per_grid['potential unlicensed']= 0.0122473 * licenses_per_grid['Number of Licenses'] + 0.0372244

        #Following was changed from "left" to "inner"
        licenses_per_grid=licenses_per_grid.merge(population_per_grid, how='inner',on='GridNumber')
        licenses_per_grid['Population percentile']=100*licenses_per_grid['Population'].rank(pct=True)

        licensed_risk_per_grid=grid_level_probabilities.groupby('GridNumber')['License risk'].sum().reset_index()
        risk_from_businesses=licensed_risk_per_grid.merge(licenses_per_grid, on='GridNumber', how='left')
        risk_from_businesses=risk_from_businesses.merge(data.amanaPopulationOverlay[['GridNumber', 'geometry']], on='GridNumber')

        total_risk_df=risk_from_businesses.merge(n_priority_areas_per_grid, on='GridNumber')

        total_risk_df['Risk from businesses']=total_risk_df['License risk']+unlicensed_weight*total_risk_df['potential unlicensed']
        total_risk_df['Total risk']=total_risk_df['Risk from businesses']*total_risk_df['location_risk']

        total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=powerScaler.fit_transform(total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']])+4
        # total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=np.power((total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']]),50)
        # total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=np.sqrt((total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']]))

        total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=scaler.fit_transform(total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']])*100
        total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=np.power((total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']]),3)
        total_risk_df.loc[total_risk_df['Number of Licenses']>=1,'Total risk']=scaler.fit_transform(total_risk_df[total_risk_df['Number of Licenses']>=1][['Total risk']])*100



        total_risk_df['geometry'] = total_risk_df['geometry'].astype(str).apply(wkt.loads)
        total_risk_df = gpd.GeoDataFrame(total_risk_df, geometry='geometry',crs='epsg:4326')
        total_risk_df.set_index('GridNumber', inplace=True)

        total_risk_df.loc[total_risk_df['Total risk']>70, 'Total risk (discrete)']='very high (70%-100%)'
        total_risk_df.loc[total_risk_df['Total risk']<=70, 'Total risk (discrete)']='high (40%-70%)'
        total_risk_df.loc[total_risk_df['Total risk']<=40, 'Total risk (discrete)']='medium (10%-40%)'
        total_risk_df.loc[total_risk_df['Total risk']<=10, 'Total risk (discrete)']='low (0%-10%)'


        points_for_qgis = predictions_df[
            ['License ID (MOMRAH)','Latitude', 'Longitude',
            'Business activity', 'Business Activity Weight', 'Facility type','previously issued fines amount',
            'cumulative_paid_fines', 'previously issued fines count',
            'days_since_last_inspection', 'days_since_establishment',
            'last_inspection_compliance', 'last_3_inspections_average_compliance',
            'paid_fines_percentage_amount', 'new_business',
            'last_inspection_high_risk_violations', 'last_inspection_fine_issued',
            'last_3_inspections_percentage_of_compliance',
            'last_inspection_clauses_non_compliance_percentage','probability of no violation', 'Facility type (English)', 'probability of violation',
            'Area of the retail outlet','Scaled area of the retail outlet', 'probability of violation (discrete)', 'inspection number',
            'geometry']
        ].copy()

        points_for_qgis['Business risk']=points_for_qgis['probability of violation']*points_for_qgis['Scaled area of the retail outlet']*points_for_qgis['Business Activity Weight']

        points_for_qgis=grid_level_probabilities[~grid_level_probabilities['License ID (MOMRAH)'].isna()][['GridNumber','License ID (MOMRAH)']].merge(points_for_qgis, how='right', on='License ID (MOMRAH)')
       
        points_for_qgis.loc[points_for_qgis['new_business']==1, 'Never inspected']=True
        points_for_qgis['Never inspected']=points_for_qgis['Never inspected'].fillna(False)
        points_for_qgis['Number of past inspections']=points_for_qgis['inspection number']-1
        # points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry='geometry',crs='epsg:4326')

        # points_for_qgis.to_csv(path.join(path.dirname(__file__),config.MODEL_SCORING_OUTPUT_FOLDER + '/rbd/' + inspectionType +'_points_for_qgis.csv'), index = False)
        
        licenses_per_facility=points_for_qgis.groupby(['GridNumber', 'Facility type (English)'])['License ID (MOMRAH)'].count().reset_index().rename(columns={'License ID (MOMRAH)':'Number of Licenses'})
        unique_facilities=licenses_per_facility['Facility type (English)'].unique()
        n_licenses_per_facility=points_for_qgis.groupby(['GridNumber'])['License ID (MOMRAH)'].count().reset_index().rename(columns={'License ID (MOMRAH)':'Total Number of Licenses'})
        licenses_per_facility=pd.pivot_table(licenses_per_facility, index='GridNumber', values='Number of Licenses', columns='Facility type (English)').reset_index()
        licenses_per_facility=licenses_per_facility.merge(n_licenses_per_facility, how='left', on='GridNumber')
        # licenses_per_facility['']

        for col in unique_facilities:
            licenses_per_facility.loc[licenses_per_facility[col]>=0.5*licenses_per_facility['Total Number of Licenses'], 'Area facility type']=col

        tooltip_df=total_risk_df.merge(grid_level_license_risks, on='GridNumber', how='left')
        #change following line to filter out the empty grids

        # tooltip_df=tooltip_df[tooltip_df['Number of Licenses']>0]


        tooltip_df['% new businesses']=100*tooltip_df['# of new businesses']/tooltip_df['Number of Licenses']
        tooltip_df['% new businesses']=tooltip_df['% new businesses'].fillna(0)
        tooltip_df=tooltip_df.merge(licenses_per_facility[['GridNumber','Area facility type']], how='left', on='GridNumber')
        # tooltip_df = gpd.GeoDataFrame(tooltip_df, geometry='geometry',crs='epsg:4326')
        tooltip_df=tooltip_df.round(2)
        tooltip_df=tooltip_df.sort_values('Total risk')

        # tooltip_df.to_csv(path.join(path.dirname(__file__),config.MODEL_SCORING_OUTPUT_FOLDER + '/rbd/' + inspectionType +'_qgis_grids.csv'), index = False)

        tooltip_df.rename (columns = {'GridNumber': 'GRID_NUMBER', 'License risk' : 'LICENSE_RISK', 'Number of Licenses' : 'NUMBER_OF_LICENSES' , 'potential unlicensed' : 'POTENTIAL_UNLICENSED', 'Population' : 'POPULATION', 
        'Population percentile' : 'POPULATION_PERCENTILE', 'geometry': 'GEOMETRY' , 'n_priority_intersection' : 'N_PRIORITY_INTERSECTION', 'impact_risk' : 'IMPACT_RISK',
        'visibility_risk' : 'VISIBILITY_RISK', 'location_risk' : 'LOCATION_RISK', 'Risk from businesses' : 'RISK_FROM_BUSINESSES', 'Total risk' : 'TOTAL_RISK',
        'Total risk (discrete)': 'TOTAL_RISK_DISCRETE', 'number of high (75% - 100%) licenses': 'NUMBER_OF_HIGH_LICENSES' , 'number of low (0% - 50%) licenses' : 'NUMBER_OF_LOW_LICENSES', 'number of medium (50% - 75%) licenses' : 'NUMBER_OF_MEDIUM_LICENSES' ,
        'high-risk license %' : 'HIGH_RISK_LICENSE_PERCENTAGE', 'medium-risk license %' : 'MEDIUM-RISK_LICENSE_PERCENTAGE', 'low-risk license %' : 'LOW_RISK_LICENSE_PERCENTAGE',
        '# of new businesses' : 'NUMBER_OF_NEW_BUSINESSES', '% new businesses' : 'PERCENTAGE_NEW_BUSINESSES', 'Area facility type' : 'AREA_FACILITY_TYPE' }, inplace = True)
        tooltip_df.drop(columns={0}, inplace=True)
        tooltip_df['INSPECTION_TYPE'] = inspectionType
        tooltip_df['GEOMETRY'] = tooltip_df.apply(lambda row : row['GEOMETRY'].wkt, axis = 1)

        # tooltip_df.to_sql(name = 'inspections_rbd_output_grids', con =  data.engine, schema = 'C##ACIO', if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : types.CLOB })

        points_for_qgis.rename(columns={'GridNumber' :  "GRID_NUMBER" , 'License ID (MOMRAH)' :  "LICENSE_ID" , 'Latitude' : "LATITUDE"  , 'Longitude' :  "LONGITUDE" , 'Business activity' : "BUSINESS_ACTIVITY"  , 'Business Activity Weight' : "BUSINESS_ACTIVITY_WEIGHT"  , 'Facility type' : "FACILITY_TYPE"  , 'previously issued fines amount' : "PREVIOUSLY_ISSUED_FINES_AMOUNT"  , 
            'cumulative_paid_fines' : "CUMULATIVE_PAID_FINES"  , 'previously issued fines count' : "PREVIOUSLY_ISSUED_FINES_COUNT"  , 'days_since_last_inspection' : "DAYS_SINCE_LAST_INSPECTION" , 'days_since_establishment' : "DAYS_SINCE_ESTABLISHMENT"  , 'last_inspection_compliance' : "LAST_INSPECTION_COMPLIANCE"  , 'last_3_inspections_average_compliance' : "LAST_3_INSPECTIONS_AVERAGE_COMPLIANCE"  , 
            'paid_fines_percentage_amount' : "PAID_FINES_PERCENTAGE_AMOUNT"  , 'new_business' : "NEW_BUSINESS"  , 'last_inspection_high_risk_violations' : "LAST_INSPECTION_HIGH_RISK_VIOLATIONS"  , 'last_inspection_fine_issued' : "LAST_INSPECTION_FINE_ISSUED" , 'last_3_inspections_percentage_of_compliance' : "LAST_3_INSPECTIONS_PERCENTAGE_OF_COMPLIANCE"  , 'last_inspection_clauses_non_compliance_percentage' : "LAST_INSPECTION_CLAUSES_NON_COMPLIANCE_PERCENTAGE"  , 
            'probability of no violation' : "PROBABILITY_OF_NO_VIOLATION"  , 'Facility type (English)' : "FACILITY_TYPE_ENGLISH" , 'probability of violation' : "PROBABILITY_OF_VIOLATION"  , 'Area of the retail outlet' : "AREA_OF_THE_RETAIL_OUTLET"  , 'Scaled area of the retail outlet' : "SCALED_AREA_OF_THE_RETAIL_OUTLET"  , 'probability of violation (discrete)' : "PROBABILITY_OF_VIOLATION_DISCRETE"  , 'inspection number' : "INSPECTION_NUMBER"  , 'geometry' :  "GEOMETRY" , 
            'Business risk' :  "BUSINESS_RISK" , 'Never inspected' : "NEVER_INSPECTED"  , 'Number of past inspections' : "NUMBER_OF_PAST_INSPECTIONS"}, inplace =True)
        points_for_qgis['INSPECTION_TYPE'] = inspectionType
        points_for_qgis['GEOMETRY'] = points_for_qgis.apply(lambda row : row['GEOMETRY'].wkt, axis = 1)

        # points_for_qgis.to_sql(name = 'inspections_output_rbd_licenses', con =  data.engine, schema = 'C##ACIO', if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : types.CLOB, 'LONGITUDE' : types.FLOAT, 'LATITUDE' : types.FLOAT })
        return tooltip_df, points_for_qgis


    def getModelResultsExcavation(self, data : fileDatabase, calculated_data : pd.DataFrame, inspectionType, licensesData : pd.DataFrame,) -> None:
        
        predictions_df = calculated_data.copy()
  
        predictions_df['ROAD_TYPE']=predictions_df['ROAD_TYPE'].fillna('طريق محلي')
        predictions_df.loc[predictions_df['ROAD_TYPE']=='طريق محلي', 'ROAD_RISK']=1
        predictions_df.loc[predictions_df['ROAD_TYPE']=='طريق تجميعي', 'ROAD_RISK']=1.01
        predictions_df.loc[predictions_df['ROAD_TYPE']=='طريق شريان', 'ROAD_RISK']=1.03
        predictions_df.loc[predictions_df['ROAD_TYPE']=='طريق سريع', 'ROAD_RISK']=1.05

        predictions_df['STREET_CLOSE_TYPE']=predictions_df['STREET_CLOSE_TYPE'].fillna('بدون إغلاق')
        predictions_df.loc[predictions_df['STREET_CLOSE_TYPE']=='بدون إغلاق', 'STREET_CLOSURE_RISK']=1.00
        predictions_df.loc[predictions_df['STREET_CLOSE_TYPE']=='إغلاق جزئي', 'STREET_CLOSURE_RISK']=1.02
        predictions_df.loc[predictions_df['STREET_CLOSE_TYPE']=='إغلاق كلي', 'STREET_CLOSURE_RISK']=1.05





        # print(predictions_df['ROAD_TYPE'].value_counts())
        predictions_df[['VOLUME_SCALED']]=np.power((predictions_df[['VOLUME_sum']]),1/10)
        predictions_df[['VOLUME_SCALED']] = 0.2*(predictions_df.groupby(['AMMANA','Municipality','Sub-Municipality'])[['VOLUME_SCALED']].apply(lambda x : self.scale(x)))+1
        predictions_df['LICENSE_RISK']=(np.power(predictions_df['probability of violation']+2,5)*(predictions_df['EMERGENCY_LICENSE'])+1)*predictions_df['VOLUME_SCALED']
        predictions_df[['LICENSE_RISK']]=100*(predictions_df.groupby(['AMMANA','Municipality','Sub-Municipality'])[['LICENSE_RISK']].apply(lambda x : self.scale(x)))
        predictions_df['LICENSE_RISK']=predictions_df['LICENSE_RISK']*predictions_df['ROAD_RISK']
        predictions_df['LICENSE_RISK']=predictions_df['LICENSE_RISK']*predictions_df['STREET_CLOSURE_RISK']
        predictions_df[['LICENSE_RISK']]=100*(predictions_df.groupby(['AMMANA','Municipality','Sub-Municipality'])[['LICENSE_RISK']].apply(lambda x : self.scale(x)))


        predictions_df[['LICENSE_RISK_points_size']]=2*(predictions_df.groupby(['AMMANA','Municipality','Sub-Municipality'])[['LICENSE_RISK']].apply(lambda x : self.scale(x)))+1
        #print(predictions_df.groupby(['AMMANA','Municipality','Sub-Municipality'])['LICENSE_RISK'].describe())

        predictions_df.loc[predictions_df['probability of violation']>0.75, 'probability of violation discrete']='high (75% - 100%)'
        predictions_df.loc[predictions_df['probability of violation']<=0.75, 'probability of violation discrete']='medium (50% - 75%)'
        predictions_df.loc[predictions_df['probability of violation']<=0.5, 'probability of violation discrete']='low (0% - 50%)'

        predictions_df = gpd.GeoDataFrame(
            predictions_df, geometry=gpd.points_from_xy(predictions_df.START_POINT_X,  predictions_df.START_POINT_Y, crs="EPSG:4326"), crs="EPSG:4326")

        #print(predictions_df['AMMANA'].value_counts())
        predictions_df['INSPECTION_TYPE'] = inspectionType
        predictions_df.rename(columns={'geometry' :  "GEOMETRY" }, inplace =True)
        predictions_df['GEOMETRY'] = predictions_df.GEOMETRY.apply(lambda x : x.wkt)
        # predictions_df = predictions_df.astype({'LIC_ID' : n})
        predictions_df.columns = [i.upper().replace("-", "_").replace(" ", "_") for i in predictions_df.columns]
        return None, predictions_df
