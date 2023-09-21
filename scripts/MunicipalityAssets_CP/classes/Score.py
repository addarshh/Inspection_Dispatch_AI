#Defining a function to score all the numeric features
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import statistics

def feature_score(df,col):
    #defining bin labels
    bins_label1=[1,2,3,4]
    bins_label2=[2,3,4]
    bins_label3=[2,4]
    bins_label4=[4]
    
    col_bins1 = [-1, np.percentile(df[col], 25), np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins2 = [-1, np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
    col_bins3 = [-1, np.percentile(df[col], 75), df[col].max()]
    col_bins4 = [-1, df[col].max()] 
    
#     print(col_bins1)
#     print(col_bins2)
#     print(col_bins3)
#     print(col_bins4)
    
    
    if np.array_equal(df[col].unique(), df[col].unique().astype(bool)) is False:
        bins_count = pd.qcut(df[col],q=4,duplicates='drop')
#         print('bins count')
#         print(bins_count.unique())
        bins_count_value = len(bins_count.unique())
#         print('bins count value')
#         print(bins_count_value)
        col_bins = col_bins1 if bins_count_value == 4 else col_bins2 if bins_count_value == 3 else col_bins3 if bins_count_value == 2 else col_bins4
#         print('col bins')
#         print(col_bins)
        #conditional statement for labels
        col_labels = bins_label1 if bins_count_value == 4 else bins_label2 if bins_count_value == 3 else bins_label3 if bins_count_value == 2 else bins_label4
#         print('col labels')
#         print(col_labels)
        #creating bins for population feature
        #df[col+'_bins'] = pd.cut(df[col], bins= col_bins, labels= col_labels, duplicates='drop').astype(int)
        df[col + '_bins'] = pd.cut(df[col], bins=col_bins, labels=col_labels, duplicates='drop')
        df[col + '_bins'] = pd.to_numeric(df[col + '_bins'], errors='coerce')
        #df[col + '_bins'].dropna(axis=0, inplace=True)
        df[col + '_bins'].fillna(value=0, inplace=True)
#         print('col _bins')
#         print(df[col+'_bins'].unique())
#         print(type(df[col+'_bins']))
#         print(df[col+'_bins'].dropna(axis = 0))
#         df[col+'_bins']  = df[col+'_bins'].astype(int)
        #indexing the feature
        df[col+'_score']=(df[col+ '_bins'])*df[col]/statistics.mean(df[col])
        #print(df[col+'_score'])
    else:
        df.loc[df[col] == 1, col+'_score'] = 1
        df.loc[df[col] == 0, col+'_score'] = 0
        df[col+'_score']=(df[col+'_score'])/statistics.mean(df[col+'_score'])
    return df

def Risk_score(df, features_list):
    # Feature scaling to standardize the range of each feature
    scaler = MinMaxScaler()
    df[features_list] = scaler.fit_transform(df[features_list])

    # Final scoring of each case for risk

    # Weights for each feature as per their priority
    dn_score = 1
    no_of_priority_areas_score = 1
    days_elapsed_score = 1
    priority_score = 1
    customer_score = 1
    landuse_priority_score = 1
    count_of_repetitions_score = 1
    count_of_pois_score = 1
    count_of_buildings_score = 1
    category_score = 1

    
    features_list = ['days_elapsed_score','DN_score','no_of_priority_areas_score','Landuse_score',
                 'count_of_repetitions_score','count_of_pois_score','count_of_buildings_score',
                 'Category_score','Priority_score','Customer_score']

    
    weights = {'DN_score': dn_score, 'no_of_priority_areas_score': no_of_priority_areas_score,
           'days_elapsed_score' : days_elapsed_score, 'Landuse_score': landuse_priority_score,
           'Priority_score' : priority_score , 'count_of_repetitions_score' : count_of_repetitions_score,
           'Customer_score' : customer_score, 'count_of_pois_score' : count_of_pois_score,
            'count_of_buildings_score': count_of_buildings_score,
          'Category_score': category_score}

    for i, j in weights.items():
        df[i] = j * df[i]

    # Bifurcating the features into two buckets - Visibility and Impact
    visibility_features = ['days_elapsed_score','DN_score','no_of_priority_areas_score','Landuse_score',
                 'count_of_repetitions_score','count_of_pois_score','count_of_buildings_score',
                 'Category_score']


    impact_features = ['Priority_score', 'Customer_score']

    # Calculating Final score for both Visibility and Impact Features
    df['Final_score_Visibility'] = df[visibility_features].sum(axis = 1)
    df['Final_score_Impact'] = df[impact_features].sum(axis = 1)

    # Scaling the final visibility and impact scores
    df[['Final_score_Visibility']] = scaler.fit_transform(df[['Final_score_Visibility']])
    df[['Final_score_Impact']] = scaler.fit_transform(df[['Final_score_Impact']])

    # BA team input to update the weights dynamically
    df_final = df.copy()
    df_final['Total_score']=df_final['Final_score_Impact']*.4 + df_final['Final_score_Visibility']*.6
    

    df_final['Total_score'] = ((df_final['Total_score'] - min(df_final['Total_score']))/(max(df_final['Total_score'])-     min(df_final['Total_score'])))*100
    df_final.drop_duplicates(inplace=True)
    df_final = df_final.sort_values('Total_score', ascending=False)

    # Bringing all the score values to two decimal places
    scores_list = ['Total_score','Final_score_Visibility','Final_score_Impact','DN_score','no_of_priority_areas_score',
               'days_elapsed_score', 'Landuse_score', 'count_of_repetitions_score','count_of_pois_score',
                       'count_of_buildings_score','Category_score',
               'Priority_score','Customer_score',
                    ]

    df_final[scores_list] = df_final[scores_list].astype('float').round(2)

    # Defining priroity classes based on Risk calculated
    df_final.loc[df_final['Total_score']>70, 'Total_score_classes']='Very High (70%-100%)'
    df_final.loc[df_final['Total_score']<=70, 'Total_score_classes']='High (40%-70%)'
    df_final.loc[df_final['Total_score']<=40, 'Total_score_classes']='Medium (10%-40%)'
    df_final.loc[df_final['Total_score']<=10, 'Total_score_classes']='Low (0%-10%)'

    df_final1 = df_final[['caseid', 'vp_element' ,'geometry', 'cluster_latitude',
       'cluster_longitude', 'cluster_label', 'priority_value',
       'satisfaction_level', 'days_elapsed', 'category_priority', 'DN',
       'no_of_priority_areas', 'landuse_priority', 'count_of_repetitions',
       'count_of_buildings', 'count_of_pois','days_elapsed_score','DN_score', 
        'no_of_priority_areas_score', 'Landuse_score', 'count_of_repetitions_score',
        'count_of_buildings_score', 'count_of_pois_score', 'Category_score',
        'Priority_score', 'Customer_score', 'Final_score_Visibility', 'Final_score_Impact',
       'Total_score', 'Total_score_classes','amanacode']]

    return df_final1