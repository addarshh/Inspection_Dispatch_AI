## Defining a function to score all the numeric features
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import statistics
import config
def feature_score(df, col):
    # defining bin labels
    bins_label1 = [1, 2, 3, 4]
    bins_label2 = [2, 3, 4]
    bins_label3 = [2, 4]
    bins_label4 = [4]
    df[col] = df[col].astype('float')
    col_bins1 = [-1, np.percentile(df[col], 25), np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins2 = [-1, np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins3 = [-1, np.percentile(df[col], 75), df[col].max()]
    col_bins4 = [-1, df[col].max()]

    if np.array_equal(df[col].unique(), df[col].unique().astype(bool)) is False:
        #print('df[col]')
        #print(df[col])
        bins_count = pd.qcut(df[col], q=4, duplicates='drop')
        bins_count_value = len(bins_count.unique())
        col_bins = col_bins1 if bins_count_value == 4 else col_bins2 if bins_count_value == 3 else col_bins3 if bins_count_value == 2 else col_bins4
        # conditional statement for labels
        col_labels = bins_label1 if bins_count_value == 4 else bins_label2 if bins_count_value == 3 else bins_label3 if bins_count_value == 2 else bins_label4
        # creating bins for population feature
        df[col + '_bins'] = pd.cut(df[col], bins=col_bins, labels=col_labels, duplicates='drop').astype(int)
        df[col + '_score'] = (df[col + '_bins']) * df[col] / statistics.mean(df[col])
    else:
        df.loc[df[col] == 1, col + '_score'] = 1
        df.loc[df[col] == 0, col + '_score'] = 0
        df[col + '_score'] = (df[col + '_score']) / statistics.mean(df[col + '_score'])

def Risk_score(df, features_list):
    # Feature scaling to standardize the range of each feature
    scaler = MinMaxScaler()
    df[features_list] = scaler.fit_transform(df[features_list])
    # Final scoring of each case for risk
    # Weights for each feature as per their priority
    weights=config.weights
    for i, j in weights.items():
        df[i] = j * df[i]
    # Bifurcating the features into two buckets - Visibility and Impact
    visibility_features = ['DN_score','no_of_priority_areas_score','days_elapsed_score','landuse_priority_score', 'building_type_score_score','area_score']
    impact_features = ['Priority_score','Customer_score','proportion_failed_causes_score','days_left_to_expiry_score','days_elapsed_last_inspection_score']
    # Calculating Final score for both Visibility and Impact Features
    df['Final_score_Visibility'] = df[visibility_features].sum(axis=1)
    df['Final_score_Impact'] = df[impact_features].sum(axis=1)
    # Scaling the final visibility and impact scores
    df[['Final_score_Visibility']] = scaler.fit_transform(df[['Final_score_Visibility']])
    df[['Final_score_Impact']] = scaler.fit_transform(df[['Final_score_Impact']])
    # BA team input to update the weights dynamically
    df_final = df.copy()
    df_final['Total_score'] = df_final['Final_score_Impact'] * .4 + df_final['Final_score_Visibility'] * .6
    df_final['Total_score'] = ((df_final['Total_score'] - min(df_final['Total_score'])) / (
            max(df_final['Total_score']) - min(df_final['Total_score']))) * 100
    df_final = df_final.drop_duplicates()
    df_final = df_final.sort_values('Total_score', ascending=False)
    # Bringing all the score values to two decimal places
    scores_list = ['Total_score', 'Final_score_Visibility', 'Final_score_Impact', 'DN_score',
                   'no_of_priority_areas_score', 'days_elapsed_score',
                   'landuse_priority_score', 'building_type_score_score', 'area_score',
                   'Priority_score', 'Customer_score', 'proportion_failed_causes_score',
                   'days_left_to_expiry_score', 'days_elapsed_last_inspection_score']
    df_final[scores_list] = df_final[scores_list].astype('float').round(2)
    # Defining priroity classes based on Risk calculated
    df_final.loc[df_final['Total_score'] > 70, 'Total_score_classes'] = 'Very High (70%-100%)'
    df_final.loc[df_final['Total_score'] <= 70, 'Total_score_classes'] = 'High (40%-70%)'
    df_final.loc[df_final['Total_score'] <= 40, 'Total_score_classes'] = 'Medium (10%-40%)'
    df_final.loc[df_final['Total_score'] <= 10, 'Total_score_classes'] = 'Low (0%-10%)'
    df_final=df_final[['caseid', 'geometry', 'latitude', 'longitude', 'priority_value',
       'satisfaction_level', 'days_elapsed', 'DN', 'no_of_priority_areas',
       'landuse_priority', 'days_elapsed_last_inspection', 'building_type_score', 'area',
       'days_left_to_expiry', 'proportion_failed_causes', 'DN_score','no_of_priority_areas_score','days_elapsed_score',
                       'landuse_priority_score', 'building_type_score_score','area_score',
                     'Priority_score','Customer_score','proportion_failed_causes_score',
                   'days_left_to_expiry_score','days_elapsed_last_inspection_score',
                     'Final_score_Visibility', 'Final_score_Impact', 'Total_score',
       'Total_score_classes']]
    df_final=pd.merge(df_final,config.meta_data, on='caseid', how="left")
    print('Model scoring records:', df_final.shape)
    return df_final
