#Defining a function to score all the numeric features
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import statistics
def feature_score(df, col):
    bins_label1 = [1, 2, 3, 4]
    bins_label2 = [2, 3, 4]
    bins_label3 = [2, 4]
    bins_label4 = [4]
    print(df.shape)

    #df[col] = df[col].astype(float)

    col_bins1 = [-1, np.percentile(df[col], 25), np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins2 = [-1, np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins3 = [-1, np.percentile(df[col], 75), df[col].max()]
    col_bins4 = [-1, df[col].max()]

    if np.array_equal(df[col].unique(), df[col].unique().astype(bool)) is False:
        bins_count = pd.qcut(df[col], q=4, duplicates='drop')
        bins_count_value = len(bins_count.unique())
        col_bins = col_bins1 if bins_count_value == 4 else col_bins2 if bins_count_value == 3 else col_bins3 if bins_count_value == 2 else col_bins4
        # conditional statement for labels
        col_labels = bins_label1 if bins_count_value == 4 else bins_label2 if bins_count_value == 3 else bins_label3 if bins_count_value == 2 else bins_label4

        # creating bins for population feature
        df[col + '_bins'] = pd.cut(df[col], bins=col_bins, labels=col_labels, duplicates='drop').astype(int)

        # indexing the feature
        df[col + '_score'] = (df[col + '_bins']) * df[col] / statistics.mean(df[col])
        # print(df[col+'_score'])
    else:
        df.loc[df[col] == 1, col + '_score'] = 1
        df.loc[df[col] == 0, col + '_score'] = 0
        df[col + '_score'] = (df[col + '_score']) / statistics.mean(df[col + '_score'])
    return df

def Risk_score(df, features_list):
    # Feature scaling to standardize the range of each feature
    scaler = MinMaxScaler()
    df[features_list] = scaler.fit_transform(df[features_list])
    df.head()

    # Final scoring of each case for risk

    # Weights for each feature as per their priority
    dn_score = 1
    no_of_priority_areas_score = 1
    days_elapsed_score = 1
    priority_score = 1
    customer_score = 1
    landuse_priority_score = 1
    count_of_repetitions_score = 1
    count_of_sewer_manholes_score = 1
    count_of_buildings_score = 1
    count_of_excavations_score = 1
    count_of_constructions_score = 1

    features_list = ['days_elapsed_score', 'DN_score', 'no_of_priority_areas_score', 'landuse_priority_score',
                     'count_of_repetitions_score', 'count_of_sewer_manholes_score', 'count_of_buildings_score',
                     'count_of_excavations_score', 'count_of_constructions_score', 'Priority_score', 'Customer_score']

    weights = {'DN_score': dn_score, 'no_of_priority_areas_score': no_of_priority_areas_score,
               'days_elapsed_score': days_elapsed_score, 'landuse_priority_score': landuse_priority_score,
               'Priority_score': priority_score, 'count_of_repetitions_score': count_of_repetitions_score,
               'Customer_score': customer_score, 'count_of_sewer_manholes_score': count_of_sewer_manholes_score,
               'count_of_buildings_score': count_of_buildings_score,
               'count_of_excavations_score': count_of_excavations_score,
               'count_of_constructions_score': count_of_constructions_score}

    for i, j in weights.items():
        df[i] = j * df[i]

    # Bifurcating the features into two buckets - Visibility and Impact
    visibility_features = ['DN_score', 'no_of_priority_areas_score', 'days_elapsed_score',
                           'landuse_priority_score', 'count_of_repetitions_score', 'count_of_sewer_manholes_score',
                           'count_of_excavations_score', 'count_of_constructions_score']

    impact_features = ['Priority_score', 'Customer_score']

    # Calculating Final score for both Visibility and Impact Features
    df['Final_score_Visibility'] = df[visibility_features].sum(axis=1)
    df['Final_score_Impact'] = df[impact_features].sum(axis=1)

    # Scaling the final visibility and impact scores
    df[['Final_score_Visibility']] = scaler.fit_transform(df[['Final_score_Visibility']])
    df[['Final_score_Impact']] = scaler.fit_transform(df[['Final_score_Impact']])

    # BA team input to update the weights dynamically
    df['Total_score'] = df['Final_score_Impact'] * .4 + df['Final_score_Visibility'] * .6

    df['Total_score'] = ((df['Total_score'] - min(df['Total_score'])) / (
                max(df['Total_score']) - min(df['Total_score']))) * 100
    df.drop_duplicates(inplace=True)
    df = df.sort_values('Total_score', ascending=False)

    # Bringing all the score values to two decimal places
    scores_list = ['Total_score', 'Final_score_Visibility', 'Final_score_Impact', 'DN_score',
                   'no_of_priority_areas_score',
                   'days_elapsed_score', 'landuse_priority_score', 'count_of_repetitions_score',
                   'count_of_sewer_manholes_score',
                   'count_of_excavations_score', 'count_of_constructions_score',
                   'Priority_score', 'Customer_score']

    df[scores_list] = df[scores_list].astype('float').round(2)

    # Defining priroity classes based on Risk calculated
    df.loc[df['Total_score'] > 70, 'Total_score_classes'] = 'Very High (70%-100%)'
    df.loc[df['Total_score'] <= 70, 'Total_score_classes'] = 'High (40%-70%)'
    df.loc[df['Total_score'] <= 40, 'Total_score_classes'] = 'Medium (10%-40%)'
    df.loc[df['Total_score'] <= 10, 'Total_score_classes'] = 'Low (0%-10%)'

    df = df[['CaseId', 'geometry', 'cluster_latitude', 'cluster_longitude',
                          'cluster_label', 'priority_value', 'satisfaction_level', 'days_elapsed',
                          'DN', 'no_of_priority_areas', 'landuse_priority',
                          'count_of_repetitions', 'count_of_sewer_manholes', 'count_of_buildings',
                          'count_of_excavations', 'count_of_constructions', 'days_elapsed_bins',
                          'days_elapsed_score', 'DN_score', 'no_of_priority_areas_score', 'landuse_priority_score',
                          'count_of_repetitions_score',
                          'count_of_sewer_manholes_score', 'count_of_buildings_score', 'count_of_excavations_score',
                          'count_of_constructions_score',
                          'Priority_score', 'Customer_score', 'Final_score_Visibility', 'Final_score_Impact',
                          'Total_score', 'Total_score_classes']]
    return df