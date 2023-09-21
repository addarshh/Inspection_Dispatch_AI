import warnings
import gc
gc.enable()

import pickle
import cx_Oracle
import numpy as np
import pandas as pd
import os
import config
import sqlalchemy 
from sqlalchemy import create_engine
from datetime import datetime
import plotly.express as px
import geopandas as gpd
#import pandas as pd
#import GISDatabase as GDB
import FetchFromDB

# from ../ import FetchFromDB as FetchFromDB
import logging
warnings.filterwarnings('ignore')
#gdata = GDB.GISDatabase()
def Perform_Predictions_Risk_Calculations(Retail_Licenses_medina_df, Inspections_df,Health_df,model,transformer, pois_licenses_comparison, population_grids, amana):

    Retail_Licenses_medina_df = Retail_Licenses_medina_df

    Inspections_df = Inspections_df
    Health_df = Health_df
    Health_df['Business Activity Description'] = Health_df['D_ACTIVITIES_NAME']

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations df1:Inspections_df Shape: {}
    	and df2:Health_df Shape: {}  on='Business Activity Description', how='left'
        """.format(Inspections_df.shape, Health_df.shape))
    Inspections_df = pd.merge(left=Inspections_df, right = 
                    Health_df, on='Business Activity Description', how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:Inspections_df {}
        """.format(Inspections_df.shape))

    df = Inspections_df
    df = df.drop(df[df["Degree of Compliance"].isna()].index)
    df = df.drop(df[df["LICENSE NUMBER"].isna()].index)


    df['Inspection Date'] = pd.to_datetime(df['Inspection Date'])

    df['LICENSE NUMBER'] = df['LICENSE NUMBER'].astype('int64')

    df = df.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])

    df = df.reset_index(drop=True)
    df_inspections = df

    Retail_Licenses_medina_df['License ID (MOMRAH) INT'] = Retail_Licenses_medina_df['License ID (MOMRAH)']

    Retail_Licenses_medina_df['LICENSE NUMBER'] = Retail_Licenses_medina_df['License ID (MOMRAH) INT'].astype('int64')

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations df1:df_inspections Shape: {}
    	and df2:Retail_Licenses_medina_df Shape: {}  on='LICENSE NUMBER',how='right' 
        """.format(df_inspections.shape, Retail_Licenses_medina_df.shape))
    df_all = pd.merge(left=df_inspections, right = Retail_Licenses_medina_df, on='LICENSE NUMBER',how='right')
    logging.info("""CHECK:MERGE_OUTPUT df:df_all {}
        """.format(df_all.shape))

    print(df_all.columns)
    #df_all['Business Activity Description'] = df_all['Business activity_x']
    #df_all['Business Activity Description'] = df_all['BUSINESS ACTIVITY']

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations df1:df_all Shape: {}
    	and df2:Health_df Shape: {}  on='Business Activity Description', how='left'
        """.format(df_all.shape, Health_df.shape))
    df_all = pd.merge(left=df_all, right = 
                    Health_df, on='Business Activity Description', how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:df_all {}
        """.format(df_all.shape))

    #df_all['LICENSE NUMBER'].nunique()

    df_all = df_all.loc[df_all['ACTIVITIE_TYPE_ID_y'] == 1]
    logging.info("""CHECK:ONLY HEALTH LICENCES df:df_all {}
        """.format(df_all.shape))
    df = df_all.copy()

    df = df.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])
    df = df.reset_index(drop=True)

    df_artificial = df.drop_duplicates(subset = ['LICENSE NUMBER'], keep='first')

    df_artificial['Inspection Date'] = pd.to_datetime('today').normalize()

    df = df.append(df_artificial)

    df = df.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])
    df = df.reset_index(drop=True)

    df['count'] = df.groupby('LICENSE NUMBER').cumcount() + 1

    df['Degree of Compliance'] = df['Degree of Compliance'].fillna(0)

    df['Degree of Compliance'] = abs(df['Degree of Compliance'].astype('int'))

    df['Degree of Compliance_FLAG'] = df['Degree of Compliance'].apply(lambda x: 1 if x ==100 else 0 )

    df['countall'] = df.groupby('LICENSE NUMBER')['count'].transform('count')

    df1_new = df[df['count'] == df['countall']]

    df1_new = pd.DataFrame(df1_new)

    df = df.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])
    df = df.reset_index(drop=True)


    df = df.sort_values(['LICENSE NUMBER','Inspection Date'], ascending = [True, True])
    df['Max_Degree of Compliance'] = df.groupby('LICENSE NUMBER')['Degree of Compliance'].cummax()

    df = df.sort_values(['LICENSE NUMBER','Inspection Date'], ascending = [True, True])
    df['Min_Degree of Compliance'] = df.groupby('LICENSE NUMBER')['Degree of Compliance'].cummin()

    df = df.sort_values(['LICENSE NUMBER','Inspection Date'], ascending = [True, True])
    df['LicInspectioncount'] = df.groupby('LICENSE NUMBER').cumcount() + 1
    df['Mean_Degree of Compliance'] = df.groupby('LICENSE NUMBER')['Degree of Compliance'].cumsum()/df['LicInspectioncount']

    df = df.sort_values(['LICENSE NUMBER','Inspection Date'], ascending = [True, True])
    df['LicInspectioncount'] = df.groupby('LICENSE NUMBER').cumcount() + 1
    df['Mean_Number of non-compliant clauses'] = df.groupby('LICENSE NUMBER')[
        'Number of non-compliant clauses'].cumsum()/df['LicInspectioncount']

    #df['Prev_count'] = df.loc[df['count'].shift(-1)>1, 'count']
    #df['Prev_count'] = df['count'].shift()

    #Type of Min_Degree of Compliance
    df['Prev_Max_Degree of Compliance'] = df.loc[df['count'].shift(-1)>1, 'Max_Degree of Compliance']
    df['Prev_Max_Degree of Compliance'] = df['Prev_Max_Degree of Compliance'].shift()

    #Type of Max_Degree of Compliance
    df['Prev_Min_Degree of Compliance'] = df.loc[df['count'].shift(-1)>1, 'Min_Degree of Compliance']
    df['Prev_Min_Degree of Compliance'] = df['Prev_Min_Degree of Compliance'].shift()

    #Type of Mean_Degree of Compliance
    df['Prev_Mean_Degree of Compliance'] = df.loc[df['count'].shift(-1)>1, 'Mean_Degree of Compliance']
    df['Prev_Mean_Degree of Compliance'] = df['Prev_Mean_Degree of Compliance'].shift()

    #Type of Mean_Number of non-compliant clauses
    df['Prev_Mean_Number of non-compliant clauses'] = df.loc[df['count'].shift(-1)>1, 'Number of non-compliant clauses']
    df['Prev_Mean_Number of non-compliant clauses'] = df['Prev_Mean_Number of non-compliant clauses'].shift()


    #Type of inspection id
    df['Prev_INSPECTYPE ID'] = df.loc[df['count'].shift(-1)>1, 'INSPECTYPE ID']
    df['Prev_INSPECTYPE ID'] = df['Prev_INSPECTYPE ID'].shift()

    #status of work
    df['Prev_Status of Work'] = df.loc[df['count'].shift(-1)>1, 'Status of Work']
    df['Prev_Status of Work'] = df['Prev_Status of Work'].shift()

    #Type of visit
    df['Prev_TYPE_OF_VISIT'] = df.loc[df['count'].shift(-1)>1, 'TYPE OF VISIT']
    df['Prev_TYPE_OF_VISIT'] = df['Prev_TYPE_OF_VISIT'].shift()

    #Inspection Date
    df['Prev_Inspection_Date'] = df.loc[df['count'].shift(-1)>1, 'Inspection Date']
    df['Prev_Inspection_Date'] = df['Prev_Inspection_Date'].shift()

    #Degree of Compliance
    df['Prev_Degree of Compliance'] = df.loc[df['count'].shift(-1)>1, 'Degree of Compliance']
    df['Prev_Degree of Compliance'] = df['Prev_Degree of Compliance'].shift()

    #Number of non-compliant clauses
    df['Prev_Number of non-compliant clauses'] = df.loc[df['count'].shift(-1)>1, 'Number of non-compliant clauses']
    df['Prev_Number of non-compliant clauses'] = df['Prev_Number of non-compliant clauses'].shift()

    #Number of Number of non-compliant clauses and High risk
    df['Prev_Number of non-compliant clauses and High risk'] = df.loc[df['count'].shift(-1)>1, 'Number of non-compliant clauses']
    df['Prev_Number of non-compliant clauses and High risk'] = df['Prev_Number of non-compliant clauses'].shift()

    #Number of Number of non-compliant clauses and Medium risk
    df['Prev_Number of non-compliant clauses and medium risk'] = df.loc[df['count'].shift(-1)>1, 'Number of non-compliant clauses and medium risk']
    df['Prev_Number of non-compliant clauses and medium risk'] = df['Prev_Number of non-compliant clauses and medium risk'].shift()

    #Number of non-compliant clauses
    df['Prev_Number of non-compliant clauses'] = df.loc[df['count'].shift(-1)>1, 'Number of non-compliant clauses']
    df['Prev_Number of non-compliant clauses'] = df['Prev_Number of non-compliant clauses'].shift()

    #Issued fine amount
    df['Prev_Issued fine amount'] = df.loc[df['count'].shift(-1)>1, 'Issued fine amount']
    df['Prev_Issued fine amount'] = df['Prev_Issued fine amount'].shift()

    df['Prev_INSPECTION NAME'] = df.loc[df['count'].shift(-1)>1, 'INSPECTION NAME']
    df['Prev_INSPECTION NAME'] = df['Prev_INSPECTION NAME'].shift()

    df['Prev_Inspector_Action'] = df.loc[df['count'].shift(-1)>1, 'Inspector_Action']
    df['Prev_Inspector_Action'] = df['Prev_Inspector_Action'].shift()

    df['Prev_NO LICENSE'] = df.loc[df['count'].shift(-1)>1, 'NO LICENSE']
    df['Prev_NO LICENSE'] = df['Prev_NO LICENSE'].shift()

    df['Prev_Degree of Compliance_FLAG'] = df.loc[df['count'].shift(-1)>1, 'Degree of Compliance_FLAG']
    df['Prev_Degree of Compliance_FLAG'] = df['Prev_Degree of Compliance_FLAG'].shift()

    df['Prev_Number of compliant clauses'] = df.loc[df['count'].shift(-1)>1, 'Number of compliant clauses']
    df['Prev_Number of compliant clauses'] = df['Prev_Number of compliant clauses'].shift()

    df['Prev_Number of clauses'] = df.loc[df['count'].shift(-1)>1, 'Number of clauses']
    df['Prev_Number of clauses'] = df['Prev_Number of clauses'].shift()


    df['Inspection Date_Max'] = df.groupby(['LICENSE NUMBER'])['Inspection Date'].transform('max')

    df['Degree of Compliance_Max'] = df.groupby(['LICENSE NUMBER'])['Degree of Compliance'].transform('max')

    df['Degree of Compliance_Min'] = df.groupby(['LICENSE NUMBER'])['Degree of Compliance'].transform('min')
    df1 = df

    df1['SADAD PAYMENT DATE'] = pd.to_datetime(df1['SADAD PAYMENT DATE'])
    df1['License Start Date'] = pd.to_datetime(df1['License Start Date'])
    df1['License Expiry Date'] = pd.to_datetime(df1['License Expiry Date'])
    df1['Last License renewal date'] = pd.to_datetime(df1['Last License renewal date'])
    df1['Inspection Date'] = pd.to_datetime(df1['Inspection Date'])
    df1['Inspection Date_Max'] = pd.to_datetime(df1['Inspection Date_Max'])
    df1['Prev_Inspection_Date'] = pd.to_datetime(df1['Prev_Inspection_Date'])


    df1['Violated'] = (df1['Degree of Compliance'] < 100) | (df1['Number of non-compliant clauses'] > 0)
    df1.loc[df1['Status of Work'].isin(['Resolved-NoViolations','Resolved-Rejected','Resolved-Cancelled']),'Violated']= False
    # CALCULATE HISTORICAL Features

    for item in ['Prev_INSPECTION NAME',
                
                'Prev_TYPE_OF_VISIT','Prev_NO LICENSE',
                'Facility type', 'Business Activity Number',
            'Prev_INSPECTION NAME','Prev_Inspector_Action','Prev_Status of Work',
                'Prev_TYPE_OF_VISIT', 'AMANA',
            'Tenancy (Own/Rented)']:
        df1 = df1.sort_values(
        [item, 'Inspection Date'], ascending=[True, True])
        print(item)
        var1 = item+'_InspectionCount'
        
        var2temp = item+'_viorate'
        var2 = item+'_AvgViolationRate'
        var2final = 'Prev_' + var2
        
        var3temp = item+'_compscore'
        var3 = item+'_AvgComplianceScore'
        var3final = 'Prev_' + var3

        var4temp = item+'_failedclauses'
        var4 = item+'_AvgFailedClauses'
        var4final = 'Prev_' + var4
        
        df1[var1] = df1.groupby(item).cumcount() + 1

        df1.loc[df1[item].isna(),var1]=1

        df1[var2] = df1.groupby(item)['Violated'].cumsum()/df1[var1]
        df1[var3] = df1.groupby(item)['Degree of Compliance'].cumsum()/df1[var1]
        df1[var4] = df1.groupby(item)['Number of non-compliant clauses'].cumsum()/df1[var1]

    #     df1[var2] = df1.apply(
    #         lambda x: -1 if pd.isna(x[item]) else x[var2temp], axis=1)
    #     df1[var3] = df1.apply(
    #         lambda x: -1 if pd.isna(x[item]) else x[var3temp], axis=1)
    #     df1[var4] = df1.apply(
    #         lambda x: -1 if pd.isna(x[item]) else x[var4temp], axis=1)
        
        df1[var2final] = df1.loc[df1[var1].shift(-1)>1, var2]
        df1[var2final] = df1[var2final].shift()
        df1.loc[df1[item].isna(),var2final]=-1

        df1[var3final] = df1.loc[df1[var1].shift(-1)>1, var3]
        df1[var3final] = df1[var3final].shift()
        df1.loc[df1[item].isna(),var3final]=-1
        
        df1[var4final] = df1.loc[df1[var1].shift(-1)>1, var4]
        df1[var4final] = df1[var4final].shift()
        df1.loc[df1[item].isna(),var4final]=-1 
            

    df1['Age_Since_Establishment'] = ( pd.to_datetime('today') - df1['License Start Date'])/np.timedelta64(1, 'M')
    df1['Age_Since_Last_License_Renewal'] = ( pd.to_datetime('today')- df1['Last License renewal date'])/np.timedelta64(1, 'M')
    df1['Days_to_License_Expiry'] = (df1[ 'License Expiry Date'] -   pd.to_datetime('today'))/np.timedelta64(1, 'M')
    df1['Days_Since_Last_Inspection'] = ( pd.to_datetime('today') - df1['Prev_Inspection_Date'])/np.timedelta64(1, 'M')


    df1_new = df1[df1['count'] == df1['countall']]


    df1['Area of the retail outlet'] = df1['Area of the retail outlet'].fillna(30)

    df1['Status of Work'].value_counts()

    #THINK ABOUT - 'Issued fine amount','SADAD NO', 'Fine payment status', 'SADAD PAYMENT DATE',
    #   'License Start Date', 'License Expiry Date','Last License renewal date', 
    
    #DROP THE UNNECESSARY COLUMNS
    df1_trim = df1[[ 'License ID (MOMRAH)','Degree of Compliance',
        'Latitude', 'Longitude', 'Commercial Reg. Number', '700 - number',
        'Facility type', 'List of activities', 'Business activity',
        'Business activity weight', 'License Start Date', 'License Expiry Date',
        'Operating hours', 'Tenancy (Own/Rented)','Area of the retail outlet',
        'Commercial Building ID', 'AMANA', 'MUNICIPALITY', 'SUB_MUNICIPALITY',
        'DISTRICT_ID', 'DISTRICT_NAME', 'Last License renewal date',"STATUS_ID",
                'Number of clauses','Establishment Name','License ID (MOMRAH) INT',
        #'Status of Work',
            #'TYPE OF VISIT', 
                #'Business Activity Number', 
                'Business Activity Weight',
                'Prev_Max_Degree of Compliance', 'Prev_Min_Degree of Compliance',
    'Violated','countall', 'count',
                'Prev_Number of clauses',
                'Prev_Mean_Degree of Compliance', 
                'Prev_Mean_Number of non-compliant clauses',
        'Prev_Degree of Compliance', 'Prev_Number of non-compliant clauses',
        'Prev_Number of non-compliant clauses and High risk',
        'Prev_Number of non-compliant clauses and medium risk',
        'Prev_Issued fine amount', #'Prev_INSPECTION NAME',
        #'Prev_Inspector_Action', 'Prev_NO LICENSE',
        #'Prev_Degree of Compliance_FLAG', 
                'Prev_Number of compliant clauses',
                #'Prev_Number of clauses',
    #        'Prev_INSPECTION NAME_InspectionCount', 'Prev_INSPECTION NAME_viorate',
    #        'Prev_INSPECTION NAME_compscore', 'Prev_INSPECTION NAME_failedclauses',
    #        'Prev_INSPECTION NAME_AvgViolationRate',
    #        'Prev_INSPECTION NAME_AvgComplianceScore',
    #        'Prev_INSPECTION NAME_AvgFailedClauses',
        'Prev_Prev_INSPECTION NAME_AvgViolationRate',
        'Prev_Prev_INSPECTION NAME_AvgComplianceScore',
        'Prev_Prev_INSPECTION NAME_AvgFailedClauses',
    #        'Prev_TYPE_OF_VISIT_InspectionCount', 'Prev_TYPE_OF_VISIT_viorate',
    #        'Prev_TYPE_OF_VISIT_compscore', 'Prev_TYPE_OF_VISIT_failedclauses',
    #        'Prev_TYPE_OF_VISIT_AvgViolationRate',
    #        'Prev_TYPE_OF_VISIT_AvgComplianceScore',
    #        'Prev_TYPE_OF_VISIT_AvgFailedClauses',
        'Prev_Prev_TYPE_OF_VISIT_AvgViolationRate',
        'Prev_Prev_TYPE_OF_VISIT_AvgComplianceScore',
        'Prev_Prev_TYPE_OF_VISIT_AvgFailedClauses',
    #        'Prev_NO LICENSE_InspectionCount', 'Prev_NO LICENSE_viorate',
    #        'Prev_NO LICENSE_compscore', 'Prev_NO LICENSE_failedclauses',
    #        'Prev_NO LICENSE_AvgViolationRate',
    #        'Prev_NO LICENSE_AvgComplianceScore',
    #          'Prev_NO LICENSE_AvgFailedClauses',
        'Prev_Prev_NO LICENSE_AvgViolationRate',
        'Prev_Prev_NO LICENSE_AvgComplianceScore',
        'Prev_Prev_NO LICENSE_AvgFailedClauses',
    #        'Facility type_InspectionCount', 'Facility type_viorate',
    #        'Facility type_compscore', 'Facility type_failedclauses',
    #        'Facility type_AvgViolationRate', 'Facility type_AvgComplianceScore',
    #        'Facility type_AvgFailedClauses', 
        'Prev_Facility type_AvgViolationRate',
        'Prev_Facility type_AvgComplianceScore',
        'Prev_Facility type_AvgFailedClauses',
    #        'Business Activity Number_InspectionCount',
    #        'Business Activity Number_viorate',
    #        'Business Activity Number_compscore',
    #        'Business Activity Number_failedclauses',
    #        'Business Activity Number_AvgViolationRate',
    #        'Business Activity Number_AvgComplianceScore',
    #        'Business Activity Number_AvgFailedClauses',
        'Prev_Business Activity Number_AvgViolationRate',
        'Prev_Business Activity Number_AvgComplianceScore',
        'Prev_Business Activity Number_AvgFailedClauses',
    #        'Prev_Inspector_Action_InspectionCount',
    #        'Prev_Inspector_Action_viorate', 'Prev_Inspector_Action_compscore',
                'Prev_AMANA_AvgViolationRate',
        'Prev_AMANA_AvgComplianceScore',
        'Prev_AMANA_AvgFailedClauses',
    #        'Prev_Inspector_Action_failedclauses',
    #        'Prev_Inspector_Action_AvgViolationRate',
    #        'Prev_Inspector_Action_AvgComplianceScore',
    #        'Prev_Inspector_Action_AvgFailedClauses',
        'Prev_Prev_Inspector_Action_AvgViolationRate',
        'Prev_Prev_Inspector_Action_AvgComplianceScore',
        'Prev_Prev_Inspector_Action_AvgFailedClauses',
    #        'Prev_Status of Work_InspectionCount', 'Prev_Status of Work_viorate',
    #        'Prev_Status of Work_compscore', 'Prev_Status of Work_failedclauses',
    #        'Prev_Status of Work_AvgViolationRate',
    #        'Prev_Status of Work_AvgComplianceScore',
    #        'Prev_Status of Work_AvgFailedClauses',
        'Prev_Prev_Status of Work_AvgViolationRate',
        'Prev_Prev_Status of Work_AvgComplianceScore',
        'Prev_Prev_Status of Work_AvgFailedClauses',
    #        'Tenancy (Own/Rented)_InspectionCount', 'Tenancy (Own/Rented)_viorate',
    #        'Tenancy (Own/Rented)_compscore', 'Tenancy (Own/Rented)_failedclauses',
    #        'Tenancy (Own/Rented)_AvgViolationRate',
    #        'Tenancy (Own/Rented)_AvgComplianceScore',
    #        'Tenancy (Own/Rented)_AvgFailedClauses',
        'Prev_Tenancy (Own/Rented)_AvgViolationRate',
        'Prev_Tenancy (Own/Rented)_AvgComplianceScore',
        'Prev_Tenancy (Own/Rented)_AvgFailedClauses',
    #        'Prev_Degree of Compliancemean', 'Prev_Degree of Compliancemin',
    #        'Prev_Degree of Compliancemax',
    #        'Prev_Number of non-compliant clausesmean',
    #        'Prev_Number of non-compliant clausesmin',
    #        'Prev_Number of non-compliant clausesmax', 
                'Age_Since_Establishment',
        'Age_Since_Last_License_Renewal', 'Days_to_License_Expiry',
        'Days_Since_Last_Inspection']]


    #df1_trim['Area of the retail outlet'] =df1_trim['Area of the retail outlet']#.str.replace(',','')
    df1_trim['Number of clauses'] =df1_trim['Number of clauses'].astype('string').str.replace(',','')
    #df1_trim.loc[condition, column_label] = new_value
    #df1_trim['Area of the retail outlet'] =df1_trim['Area of the retail outlet'].astype(float)

    #Take only the latest inspection entry
    print("DF1_TRIM_before_taking one entry per outlet", df1_trim.shape)
    print(df1_trim['License ID (MOMRAH)'].nunique())
    df1_trim = df1_trim[df1_trim['count'] == df1_trim['countall']]
    # ### Using XG BOOST

    # model_pkl_filename = 'xgboost_classifier_1.pkl'
    # #decision_tree_pkl_filename = 'decision_tree_classifier_1.pkl'
    # #decision_tree_pkl_filename = 'healthcare_decision_tree_classifier_2.pkl'
    # # Loading the saved decision tree model pickle
    # model_pkl = open(model_pkl_filename, 'rb')
    # model = pickle.load(model_pkl)
    model = model



    dt = model

    df1_trim.columns
    print("DF1_TRIM_ALL",df1_trim.shape )
    df1_trim = df1_trim[df1_trim['Days_to_License_Expiry'] > -12]

    license_Number_df = df1_trim[['License ID (MOMRAH)','Degree of Compliance',
        'Latitude', 'Longitude', 'Commercial Reg. Number', '700 - number',
        'Facility type', 'List of activities', 'Business activity',
        'Business activity weight', 'License Start Date', 'License Expiry Date',
            'Operating hours', 'Tenancy (Own/Rented)','Area of the retail outlet',
        'Commercial Building ID', 'AMANA', 'MUNICIPALITY', 'SUB_MUNICIPALITY',
        'DISTRICT_ID', 'DISTRICT_NAME', 'Last License renewal date','Establishment Name',
                    "STATUS_ID",'License ID (MOMRAH) INT','Number of clauses','Business Activity Weight']]

    df1_trim = df1_trim.drop(['License ID (MOMRAH)','Degree of Compliance',
        'Latitude', 'Longitude', 'Commercial Reg. Number', '700 - number',
        'Facility type', 'List of activities', 'Business activity',
        'Business activity weight', 'License Start Date', 'License Expiry Date',
            'Operating hours', 'Tenancy (Own/Rented)','Area of the retail outlet',
        'Commercial Building ID', 'AMANA', 'MUNICIPALITY', 'SUB_MUNICIPALITY',
        'DISTRICT_ID', 'DISTRICT_NAME', 'Last License renewal date','Establishment Name',
                "STATUS_ID",'License ID (MOMRAH) INT', 'Number of clauses','Business Activity Weight'
                            ], axis=1)


    # pt_transformer_pkl_filename = 'pt_transformer.pkl'
    # #decision_tree_pkl_filename = 'decision_tree_classifier_1.pkl'
    # #decision_tree_pkl_filename = 'healthcare_decision_tree_classifier_2.pkl'
    # # Loading the saved decision tree model pickle
    # pt_transformer = open(pt_transformer_pkl_filename, 'rb')
    # pt = pickle.load(pt_transformer)
    pt = transformer

    numeric_columns = df1_trim.columns
    df1_trim[numeric_columns] = pt.transform(df1_trim[numeric_columns])

    output_table_name = 'RBD_HEALTH_FEATURES'# + str(pd.to_datetime('today').strftime("%d/%m/%Y"))


    column_names = df1_trim.columns

    df1_trim = pd.DataFrame(df1_trim)
    #X_test = pd.DataFrame(X_test)

    colnames = df1_trim.columns

    df1_trim = df1_trim.replace({pd.NA: np.nan})

    df1_trim = df1_trim.apply(pd.to_numeric)

    df1_trim = pd.DataFrame(df1_trim)

    df1_trim.columns = column_names

    y_pred = dt.predict(df1_trim)
    y_pred = pd.DataFrame(y_pred)

    y_pred.columns= ['Compliace_Score_Predicted']

    Final_output_df = pd.concat([license_Number_df.reset_index(drop=True),df1_trim.reset_index(drop=True), y_pred], axis=1)

    Final_output_df['License ID (MOMRAH)'].nunique()


    # ### AGGREGATION STARTS HERE ### COURTESY MARC


    


    # In[112]:


    # mapbox_access_token='pk.eyJ1IjoibWFyY2dyb25kaWVyIiwiYSI6ImNrY2JrY25kMjI2MHQyenRpNnI3NnA3eGIifQ.bjLtRjMuR7xcLscw2B9Yrg'
    # pd.set_option('display.max_columns',None)
    # #Marc's personal mapbox access token
    # px.set_mapbox_access_token(mapbox_access_token)
    # #raw_data_path = 'Z:/MOMRAH_WORKING/7. CV expansion/1. Retail Inspection/0. Raw_Data/'
    # #rbd_model_path = 'Z:/MOMRAH_WORKING/7. CV expansion/1. Retail Inspection/2. RBD/'
    # #outputs_path='Z:/MOMRAH_WORKING/7. CV expansion/1. Retail Inspection/3. Outputs/'

    # pd.options.mode.chained_assignment = None
    # # pio.kaleido.scope.default_format = "svg"


    # In[113]:


    model_output=y_pred
    #licenses_df_raw = Retail_Licenses_medina_df.copy()
    #pd.read_csv('Datav2/Retail License Data_Madena.csv')
    #population_grids= gpd.read_file('Population_AllGrids/standardized_grids_population_municipality_20220220.shp')
    #Amana = gpd.read_file(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'Amana', 'Amana.shp'))
    Amana = amana
    population_grids = population_grids

    population_grids['GridNumber'] = population_grids['GRIDUNIQUECODE'].astype(str) 

    #print(population_grids.dtype())
    #CHANGED HERE
    #population_grids = gpd.GeoDataFrame(population_grids, geometry=gpd.GeoSeries.from_wkt(population_grids.geometry.astype('str')), crs="EPSG:4326")

    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service']  ))
    #cx_Oracle.connect(config.Username, config.Password, cx_Oracle.makedsn('10.80.122.102',config.Port,'ORCLCDB'  ))
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM ' + config.DB['input_schema'] + '.CASES crm WHERE crm.CATEGORY =\'النظافة العامة\' AND crm.MAINCLASSIFICATION = \'النظافة\' AND crm.PXCREATEDATETIME > to_date(\'2021-09-01\', \'YYYY-MM-DD\')')
    crm_rows = cursor.fetchall()
    col_names = [row[0] for row in cursor.description]
    CRM_Cleanliness_df = pd.DataFrame(crm_rows)
    CRM_Cleanliness_df.columns = col_names
    SewerLines = gpd.read_file(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'SEWERAGE', 'SewerLines.shp'))
    SewerManholes = gpd.read_file(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'SEWERAGE', 'SewerManholes.shp'))
    geometry_per_grid = population_grids
    from shapely import wkt
    geometry_per_grid = gpd.GeoDataFrame(geometry_per_grid, geometry='geometry', crs="EPSG:4326")
    SewerManholes = SewerManholes.to_crs(epsg=4326)
    SewerManholes = gpd.GeoDataFrame(SewerManholes, geometry='geometry', crs="EPSG:4326")
    logging.info("""CHECK:SJOIN function:Perform_Predictions_Risk_Calculations 
        df1:geometry_per_grid Shape: {}
    	and df2:SewerManholes Shape: {}  how="inner",predicate="intersects"
        """.format(geometry_per_grid.shape, SewerManholes.shape))
    SewerManholes_grids = geometry_per_grid.sjoin( SewerManholes,how="inner",predicate="intersects")
    logging.info("""CHECK:SJOIN_OUTPUT df:SewerManholes_grids {}
        """.format(SewerManholes_grids.shape))
    SewerManholes_grids.columns
    SewerManholes_perGrid_df = SewerManholes_grids.groupby(['GridNumber'])['GridNumber'].count()
    SewerManholes_perGrid_df = pd.DataFrame(SewerManholes_perGrid_df)
    SewerManholes_perGrid_df.columns = ['SewerManholesCount']
    SewerLines = SewerLines.to_crs(epsg=4326)
    logging.info("""CHECK:SJOIN function:Perform_Predictions_Risk_Calculations 
        df1:SewerLines Shape: {}
    	and df2:geometry_per_grid Shape: {}  how="inner",predicate="intersects"
        """.format(SewerLines.shape, geometry_per_grid.shape))
    SewerLines_grids = SewerLines.sjoin(geometry_per_grid,how="inner",predicate="intersects")
    logging.info("""CHECK:SJOIN_OUTPUT df:SewerLines_grids {}""".format(SewerLines_grids.shape))
    SewerLines_perGrid_df = SewerLines_grids.groupby(['GridNumber'])['GridNumber'].count()


    SewerLines_perGrid_df = pd.DataFrame(SewerLines_perGrid_df)

    SewerLines_perGrid_df.columns = ['SewerLinesCount']


    #priority_areas=pd.read_csv('Datav2/madina_priority_areas.csv')
    #priority_areas=gdata.getPriorityAreasData()
    priority_areas = FetchFromDB.getPriorityAreasData()
    # priority_areas = priority_areas[priority_areas['geometry'].notna()]
    # priority_areas['geometry'] = priority_areas['geometry'].astype(str)
    # priority_areas['geometry'] = priority_areas['geometry'].apply(wkt.loads)
    # priority_areas = gpd.GeoDataFrame(priority_areas, geometry='geometry',crs='epsg:4326')
    # #priority_areas=priority_areas.drop(columns=['Unnamed: 0'])
    priority_areas=priority_areas.set_index('Name')


    # In[ ]:


    features_columns=['Business activity', 'Business Activity Weight', 'Facility type',
                    'inspection number', 'previously issued fines amount',
                    'cumulative_paid_fines', 'previously issued fines count',
                    'days_since_last_inspection', 'days_since_establishment',
                    'last_inspection_compliance','last_3_inspections_average_compliance',
                    'paid_fines_percentage_amount', 'new_business', 'last_inspection_high_risk_violations',
                    'last_inspection_fine_issued','last_3_inspections_percentage_of_compliance',
                    'last_inspection_clauses_non_compliance_percentage']

    facility_types_mapping={0: 'إصدار التراخيص بتأسيس أو تشغيل الصراف الألي',
    1: 'الأنشطة الأخرى',
    2: 'الأنشطة التعليمية ',
    3: 'الأنشطة الطبية',
    4: 'الاستراحات',
    5: 'الفنادق و الشقق الفندقية و المنتجعات و ما في حكمها',
    6: 'المحلات التجارية',
    7: 'المستودعات',
    8: 'المطابخ و المطاعم و ما في حكمها',
    9: 'الورش المهنية',
    10: 'قصور الأفراح',
    11: 'محطات الوقود ( داخل النطاق العمراني )',
    12: 'محلات التشليح',
    13: 'محلات تغيير الزيوت و الشحوم و غسيل السيارات',
    14: 'مدن الملاهي و الترفيه'}

    facility_types_english_mapping={
        0:'Issuing licenses to establish or operate ATMs',
        1:'other activities',
        2:'educational activities',
        3:'medical activities',
        4:'restrooms',
        5:'hotels, hotel apartments, resorts and the like',
        6:'shops',
        7:'Warehouses',
        8:'Kitchens, restaurants and the like',
        9:'Professional workshops',
        10:'Palaces of weddings',
        11:'Gas stations (within the urban area)',
        12:'Application shops',
        13:'Oil and grease changing shops and car washes',
        14:'Amusement parks and entertainment'}


    # In[ ]:


    from sklearn.preprocessing import MinMaxScaler, PowerTransformer, QuantileTransformer, StandardScaler,power_transform , RobustScaler
    # import streamlit as st
    scaler = MinMaxScaler()
    powerScaler=PowerTransformer()
    quantileScaler=QuantileTransformer()
    def scale(X, defined_scaler): #CHANGED function to handle exceptions
        try:
            X_ = np.atleast_2d(X)
            return pd.DataFrame(defined_scaler.fit_transform(X_), X.index)
        except ValueError:
            return pd.DataFrame((X_), X.index)


    # In[ ]:


    #licenses_df['LICENSE NUMBER'] = licenses_df['License ID (MOMRAH)']


    # In[ ]:


    #licenses_df.columns


    # In[ ]:


    Final_output_df_all = Final_output_df


    # In[ ]:


    Final_output_df_all.shape


    # #### FILTER OUT HEALTH INSPECTIONS

    # In[ ]:


    Final_output_df['Facility type'] = Final_output_df['Facility type'].astype(str)


    # In[ ]:


    Final_output_df['Area of the retail outlet'] = Final_output_df['Area of the retail outlet'].fillna(30)


    # In[ ]:


    Final_output_df['Area of the retail outlet'] = Final_output_df['Area of the retail outlet'].astype('int')


    # In[ ]:


    Final_output_df['Business activity'] = Final_output_df['Business activity'].astype('str')


    # In[ ]:


    pd.DataFrame(Final_output_df.groupby('Business activity')['Area of the retail outlet']).reset_index()


    # In[ ]:





    # In[ ]:



    #xgb_model = xgb.XGBClassifier()
    #xgb_model.load_model(rbd_model_path + 'model.bin')
    predictions=model.predict_proba(df1_trim)
    print("DFI_TRIM SIZE",df1_trim.shape)
    predictions_df=pd.DataFrame(predictions, columns=['probability of no violation','probability of violation'])

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:Final_output_df Shape: {}
    	and df2:predictions_df Shape: {}  left_index=True, right_index=True
        """.format(Final_output_df.shape, predictions_df.shape))
    predictions_df=Final_output_df.merge(predictions_df, left_index=True, right_index=True)
    logging.info("""CHECK:MERGE_OUTPUT df:predictions_df {}
        """.format(predictions_df.shape))

    #T: fill null areas with group mean. Maybe even in feature engineering
    #predictions_df=predictions_df.merge(licenses_df[['License ID (MOMRAH)','Area of the retail outlet']], on='License ID (MOMRAH)', how='left')
    predictions_df[['Scaled area of the retail outlet']] = predictions_df.groupby('Business activity')[['Area of the retail outlet']].apply(scale, (scaler))+1 #CHANGED (NEEDS TO CHANGE TO GROUPING BY AMANA TOO)
    predictions_df['Scaled area of the retail outlet']=predictions_df[['Scaled area of the retail outlet']].fillna(1)

    logging.info("""CHECK:SJOIN function:Perform_Predictions_Risk_Calculations 
        df1:population_grids Shape: {}
    	and df2:priority_areas Shape: {}   on=['GridNumber'], how='right'
        """.format(population_grids.shape, priority_areas.shape))
    n_priority_areas_per_grid=gpd.sjoin(population_grids, priority_areas, how='inner', predicate='intersects').groupby('GridNumber')['geometry'].count().reset_index().rename(columns={'geometry':'n_priority_intersection'})
    logging.info("""CHECK:SJOIN_OUTPUT df:predictions_df {}
        """.format(n_priority_areas_per_grid.shape))

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:n_priority_areas_per_grid Shape: {}
    	and df2:population_grids Shape: {}   on=['GridNumber'], how='right'
        """.format(n_priority_areas_per_grid.shape, population_grids.shape))
    print(n_priority_areas_per_grid.columns)
    print(population_grids.columns)
    population_grids['MUNICIPALITY'] = population_grids['MUNICIPALI']
    n_priority_areas_per_grid=n_priority_areas_per_grid.merge(population_grids[['GridNumber', 'AMANACODE', 'AMANA','MUNICIPALITY','MUNICIPA_1', 'DN']], on=['GridNumber'], how='right') #CHANGED MERGING ,'MUNICIPA_1 : MUNICIPALITYCODE'
    logging.info("""CHECK:MERGE_OUTPUT df:predictions_df {}
        """.format(n_priority_areas_per_grid.shape))
    
    n_priority_areas_per_grid['n_priority_intersection']=n_priority_areas_per_grid['n_priority_intersection'].fillna(0)
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:n_priority_areas_per_grid Shape: {}
    	and df2:population_grids Shape: {}  
        """.format(n_priority_areas_per_grid.shape, population_grids.shape))
    n_priority_areas_per_grid=n_priority_areas_per_grid.merge(population_grids[['GridNumber','DN']])
    logging.info("""CHECK:MERGE_OUTPUT df:predictions_df {}
        """.format(n_priority_areas_per_grid.shape))
    

    CRM_Cleanliness_df = gpd.GeoDataFrame(
        CRM_Cleanliness_df, geometry=gpd.points_from_xy(CRM_Cleanliness_df.LONGITUDE, CRM_Cleanliness_df.LATITUDE), crs="EPSG:4326")

    CRM_Cleanliness_df = CRM_Cleanliness_df[CRM_Cleanliness_df.geometry.x > 0]

    logging.info("""CHECK:SJOIN function:Perform_Predictions_Risk_Calculations 
        df1:CRM_Cleanliness_df Shape: {}
    	and df2:geometry_per_grid Shape: {}  how="inner",predicate="intersects"
        """.format(CRM_Cleanliness_df.shape, geometry_per_grid.shape))
    CRM_Cleanliness_withGrid_df = CRM_Cleanliness_df.sjoin(geometry_per_grid, how='inner'
                                                        , predicate='intersects'
                                                        )
    logging.info("""CHECK:SJOIN_OUTPUT df:CRM_Cleanliness_withGrid_df {}
        """.format(CRM_Cleanliness_withGrid_df.shape))
        

    CRM_Cleanliness_withGrid_df=CRM_Cleanliness_withGrid_df.drop(columns=['index_right'])


    CRM_Cleanliness_perGrid_df = CRM_Cleanliness_withGrid_df.groupby(['GridNumber'])['GridNumber'].count()


    CRM_Cleanliness_perGrid_df = pd.DataFrame(CRM_Cleanliness_perGrid_df)

    CRM_Cleanliness_perGrid_df.columns = ['CRM_CleanlinessCases_Count'] 


    CRM_Cleanliness_perGrid_df = CRM_Cleanliness_perGrid_df.reset_index()


    SewerLines_perGrid_df = SewerLines_perGrid_df.reset_index()

    SewerManholes_perGrid_df = SewerManholes_perGrid_df.reset_index()





    geometry_per_grid = geometry_per_grid.dropna()

    geometry_per_grid = geometry_per_grid.drop_duplicates()




    Health_Risk_Factors_perGrid_df = pd.concat(
        (iDF.set_index('GridNumber') for iDF in [geometry_per_grid,CRM_Cleanliness_perGrid_df, SewerLines_perGrid_df, SewerManholes_perGrid_df]),
        axis=1, join='outer'
    ).reset_index()



    # Introduce Health Factors
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:n_priority_areas_per_grid Shape: {}
    	and df2:population_grids Shape: {}  on=['GridNumber'], how='left'
        """.format(n_priority_areas_per_grid.shape, Health_Risk_Factors_perGrid_df.shape))
    n_priority_areas_per_grid = pd.merge(left=n_priority_areas_per_grid,right=Health_Risk_Factors_perGrid_df,on=['GridNumber'], how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:n_priority_areas_per_grid {}
        """.format(n_priority_areas_per_grid.shape))
    
    n_priority_areas_per_grid[['CRM_CleanlinessCases_Count', 'SewerLinesCount', 'SewerManholesCount']] =scaler.fit_transform(n_priority_areas_per_grid[['CRM_CleanlinessCases_Count', 'SewerLinesCount', 'SewerManholesCount']])
    # ADDED
    n_priority_areas_per_grid['CRM_CleanlinessCases_Count'] = n_priority_areas_per_grid['CRM_CleanlinessCases_Count'].fillna(0)
    n_priority_areas_per_grid[ 'SewerLinesCount'] = n_priority_areas_per_grid[ 'SewerLinesCount'].fillna(0)
    n_priority_areas_per_grid['SewerManholesCount'] = n_priority_areas_per_grid['SewerManholesCount'].fillna(0)

    n_priority_areas_per_grid['Health_risk'] = n_priority_areas_per_grid.CRM_CleanlinessCases_Count + n_priority_areas_per_grid.SewerLinesCount + n_priority_areas_per_grid.SewerManholesCount

    visibility_weight = 1/3
    impact_weight = 1/3
    Health_impact_weight = 1/3


    n_priority_areas_per_grid[['impact_risk']]=n_priority_areas_per_grid.groupby(['AMANA_x','MUNICIPALITY_x'])[['n_priority_intersection']].apply(scale, scaler)+1 #CHANGED TO GROUPING BY AMANA
    # n_priority_areas_per_grid[['visibility_risk']]=((n_priority_areas_per_grid.groupby(['AMANA','MUNICIPALI'], dropna=False)[['DN']].apply(scale, scaler)+1)*1.00001*np.random.rand(n_priority_areas_per_grid['DN'].shape[0],1)) #CHANGED TO GROUPING BY MUNICIPALITY
    n_priority_areas_per_grid[['visibility_risk']]=n_priority_areas_per_grid.groupby(['AMANA_x','MUNICIPALITY_x'])[['DN_x']].apply(scale, scaler)+1 #CHANGED TO GROUPING BY MUNICIPALITY
    n_priority_areas_per_grid['location_risk']=n_priority_areas_per_grid['visibility_risk']*visibility_weight+n_priority_areas_per_grid['impact_risk']*impact_weight +n_priority_areas_per_grid['Health_risk']*Health_impact_weight


    predictions_df['Business Activity Weight']=predictions_df['Business Activity Weight'].fillna(2)
    predictions_df.loc[predictions_df['probability of violation']>0.75, 'probability of violation (discrete)']='high (75% - 100%)'
    predictions_df.loc[predictions_df['probability of violation']<=0.75, 'probability of violation (discrete)']='medium (50% - 75%)'
    predictions_df.loc[predictions_df['probability of violation']<=0.5, 'probability of violation (discrete)']='low (0% - 50%)'
    # predictions_df.loc[predictions_df['probability of violation']<=0.1, 'probability of violation (discrete)']='low (0% - 10%)'
    predictions_df.groupby('probability of violation (discrete)')['License ID (MOMRAH)'].count()

    color_dict_points = {'low (0% - 50%)': '#8AC474',
                'medium (50% - 75%)': '#FEBF04 ',
                'high (75% - 100%)': '#FF0000'}


    # In[ ]:


    #ADDED THIS CELL
    predictions_df['Longitude']=pd.to_numeric(predictions_df['Longitude'], errors='coerce')
    predictions_df['Latitude']=pd.to_numeric(predictions_df['Latitude'], errors='coerce')

    predictions_df.loc[predictions_df['Longitude']>90,'Longitude']=np.nan
    predictions_df.loc[predictions_df['Latitude']>180,'Latitude']=np.nan


    predictions_df = predictions_df.dropna(subset=['Longitude','Latitude'])

    predictions_df = gpd.GeoDataFrame(
        predictions_df, geometry=gpd.points_from_xy(predictions_df.Latitude, predictions_df.Longitude), crs="EPSG:4326")

    logging.info("""CHECK:SJOIN function:Perform_Predictions_Risk_Calculations 
        df1:predictions_df Shape: {}
    	and df2:population_grids Shape: {}  how="inner",predicate="intersects"
        """.format(predictions_df.shape, population_grids.shape))
    grid_level_probabilities=gpd.sjoin( predictions_df,population_grids, op='within', how='inner').drop_duplicates(['GridNumber','License ID (MOMRAH)'])
    logging.info("""CHECK:SJOIN_OUTPUT df:grid_level_probabilities {}
        """.format(grid_level_probabilities.shape))

    grid_level_probabilities['License risk']=grid_level_probabilities['Business Activity Weight']*grid_level_probabilities['probability of violation']*grid_level_probabilities['Scaled area of the retail outlet']

    grid_level_probabilities['probability of violation (discrete)']=grid_level_probabilities['probability of violation (discrete)'].fillna(0)


    # grid_level_probabilities = grid_level_probabilities[
    #     grid_level_probabilities['Days_to_License_Expiry'] > -12]


    #grid_level_probabilities_subset['AMANACODE'].nunique()
    print("grid_level_probabbilities")
    print(grid_level_probabilities.shape)
    print(grid_level_probabilities.columns)

    grid_level_probabilities_subset = grid_level_probabilities[['License ID (MOMRAH)', 'Degree of Compliance', 'Latitude', 'Longitude',
    #        'Commercial Reg. Number', '700 - number', 'Facility type',
    #        'List of activities', 'Business activity', 'Business activity weight',
    #        'License Start Date', 'License Expiry Date', 'Operating hours',
    #        'Tenancy (Own/Rented)', 'Area of the retail outlet',
             'Commercial Building ID', 'AMANA_left', #'MUNICIPALITY',
    #        'SUB_MUNICIPALITY', 'DISTRICT_ID', 'DISTRICT_NAME',
    #        'Last License renewal date', 'Establishment Name',
    #        'License ID (MOMRAH) INT', 'Number of clauses',
    #        'Business Activity Weight', 'Prev_Max_Degree of Compliance',
    #        'Prev_Min_Degree of Compliance', 'Violated', 'countall', 'count',
    #        'Age_Since_Establishment',
    #        'Age_Since_Last_License_Renewal', 'Days_to_License_Expiry',
    #        'Days_Since_Last_Inspection', 'Compliace_Score_Predicted',
    #        'probability of no violation', 'probability of violation',
    #        'Scaled area of the retail outlet',
    #        'probability of violation (discrete)', 'geometry', 'index_right',
    #        'CHECK', 'AMANA_right', , 'GRID_ID', 'GridName',
             'MUNICIPALI', 'MUNICIPA_1', 'REGION', 'REGIONCODE', #'SHAPE_Leng',
    #        'SHAPE_Area', 
            'AMANACODE','GridNumber', 'DN', 'License risk']]


    grid_level_license_risks=grid_level_probabilities.groupby(['GridNumber','probability of violation (discrete)'])['License ID (MOMRAH)'].nunique().reset_index()

    grid_level_license_risks=pd.pivot_table(grid_level_license_risks, index='GridNumber', values='License ID (MOMRAH)', columns='probability of violation (discrete)').reset_index().fillna(0)


    grid_level_license_risks['high-risk license %']=100*grid_level_license_risks['high (75% - 100%)']/(grid_level_license_risks['high (75% - 100%)'] +grid_level_license_risks['medium (50% - 75%)']+ grid_level_license_risks['low (0% - 50%)'])
    grid_level_license_risks['medium-risk license %']=100*grid_level_license_risks['medium (50% - 75%)']/(grid_level_license_risks['high (75% - 100%)'] +grid_level_license_risks['medium (50% - 75%)']+ grid_level_license_risks['low (0% - 50%)'])
    grid_level_license_risks['low-risk license %']=100*grid_level_license_risks['low (0% - 50%)']/(grid_level_license_risks['high (75% - 100%)'] +grid_level_license_risks['medium (50% - 75%)']+ grid_level_license_risks['low (0% - 50%)'])

    grid_level_license_risks=grid_level_license_risks.rename(columns={'high (75% - 100%)':'number of high (75% - 100%) licenses',
                                                                    'medium (50% - 75%)':'number of medium (50% - 75%) licenses',
                                                                    'low (0% - 50%)':'number of low (0% - 50%) licenses'})
    #grid_level_new_businesses=grid_level_probabilities.groupby('GridNumber')['new_business'].sum().reset_index().rename(columns={'new_business':'# of new businesses'})

    #grid_level_license_risks=grid_level_license_risks.merge(grid_level_new_businesses, how='left', on='GridNumber')
    grid_level_license_risks=grid_level_license_risks.fillna(0)


    #pois_data=pd.read_csv('pois_licenses_comparison.csv')
    pois_data = pois_licenses_comparison

    licenses_per_grid=grid_level_probabilities.groupby('GridNumber')['License ID (MOMRAH)'].nunique().reset_index().rename(columns={'License ID (MOMRAH)':'Number of Licenses'})
    
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:licenses_per_grid Shape: {}
    	and df2:pois_data Shape: {}  on=['GridNumber'], how='left'
        """.format(licenses_per_grid.shape, pois_data.shape))
    licenses_per_grid=licenses_per_grid.merge(pois_data, on='GridNumber', how='left') #ADDED
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_grid {}
        """.format(licenses_per_grid.shape))

    population_per_grid=population_grids[['GridNumber','DN']].rename(columns={'DN':'Population'}) #CHANGED

    licenses_per_grid['potential unlicensed']=licenses_per_grid['pois_licenses_difference'].fillna(0) #ADDED
    licenses_per_grid.loc[licenses_per_grid['potential unlicensed']<0, 'potential unlicensed']=-0.1*licenses_per_grid[licenses_per_grid['potential unlicensed']<0]['potential unlicensed']/licenses_per_grid[licenses_per_grid['potential unlicensed']<0]['potential unlicensed'].min() #ADDED
    licenses_per_grid.loc[licenses_per_grid['potential unlicensed']>0, 'potential unlicensed']=0.1*licenses_per_grid[licenses_per_grid['potential unlicensed']>0]['potential unlicensed']/licenses_per_grid[licenses_per_grid['potential unlicensed']>0]['potential unlicensed'].max() #ADDED


    #Following was changed from "left" to "inner"
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:licenses_per_grid Shape: {}
    	and df2:population_per_grid Shape: {}  on=['GridNumber'], how='left'
        """.format(licenses_per_grid.shape, population_per_grid.shape))
    licenses_per_grid=licenses_per_grid.merge(population_per_grid, how='inner',on='GridNumber') #CHANGED
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_grid {}
        """.format(licenses_per_grid.shape))
    
    licenses_per_grid['Population percentile']=100*licenses_per_grid['Population'].rank(pct=True)


    licensed_risk_per_grid=grid_level_probabilities.groupby(['GridNumber','AMANACODE'])['License risk'].sum().reset_index()
    
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:licensed_risk_per_grid Shape: {}
    	and df2:licenses_per_grid Shape: {}  on=['GridNumber'], how='left'
        """.format(licensed_risk_per_grid.shape, licenses_per_grid.shape))
    risk_from_businesses=licensed_risk_per_grid.merge(licenses_per_grid, on='GridNumber', how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_grid {}
        """.format(risk_from_businesses.shape))

    risk_from_businesses['GridNumber'].isin(geometry_per_grid['GridNumber'])

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:risk_from_businesses Shape: {}
    	and df2:geometry_per_grid Shape: {}  on='GridNumber'
        """.format(risk_from_businesses.shape, geometry_per_grid.shape))
    risk_from_businesses=risk_from_businesses.merge(geometry_per_grid, on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:risk_from_businesses {}
        """.format(risk_from_businesses.shape))
    #Change following row to filter out empty grids
    # risk_from_businesses=risk_from_businesses[risk_from_businesses['Number of Licenses']>0]
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:risk_from_businesses Shape: {}
    	and df2:n_priority_areas_per_grid Shape: {}  on='GridNumber'
        """.format(risk_from_businesses.shape, n_priority_areas_per_grid.shape))
    total_risk_df=risk_from_businesses.merge(n_priority_areas_per_grid, on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:total_risk_df {}
        """.format(total_risk_df.shape))

    #TOO figure out why some risks are 0 whereas unlicensed risk cannot be 0. is it an error due to the multiplication being too small?
    total_risk_df['Risk from businesses']=total_risk_df['License risk']+total_risk_df['potential unlicensed']
    total_risk_df['Total risk']=total_risk_df['Risk from businesses']*total_risk_df['location_risk']

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:licensed_risk_per_grid Shape: {}
    	and df2:licenses_per_grid Shape: {}  on='GridNumber'
        """.format(licensed_risk_per_grid.shape, licenses_per_grid.shape))
    df_final = pd.merge(left = licensed_risk_per_grid, right= licenses_per_grid, on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:df_final {}
        """.format(df_final.shape))

    df_final.to_csv(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'qgis_grids_licensed.csv'))

    scaler=MinMaxScaler()
    scaler1=QuantileTransformer()
    scaler2=PowerTransformer(method="box-cox")
    # print(total_risk_df[total_risk_df['Number of Licenses']>=1])
    temp_risk_df=total_risk_df[total_risk_df['Number of Licenses']>=1]
    temp_risk_df[['Total risk']]=temp_risk_df.groupby('MUNICIPALITY')[['Total risk']].apply(scale, scaler2)+4 #ADDED
    temp_risk_df[['Total risk']]=temp_risk_df.groupby(['AMANA','MUNICIPALITY'])[['Total risk']].apply(scale, scaler)*100 #ADDED
    temp_risk_df[['Total risk']]=np.power((temp_risk_df[['Total risk']]),3) #ADDED
    temp_risk_df[['Total risk']]=temp_risk_df.groupby(['AMANA','MUNICIPALITY'])[['Total risk']].apply(scale, scaler)*100 #ADDED
    total_risk_df.loc[temp_risk_df.index, :]=temp_risk_df #ADDED


    total_risk_df = gpd.GeoDataFrame(total_risk_df, geometry='geometry_y',crs='epsg:4326')
    total_risk_df.set_index('GridNumber', inplace=True)

    total_risk_df.loc[total_risk_df['Total risk']>70, 'Total risk (discrete)']='very high (70%-100%)'
    total_risk_df.loc[total_risk_df['Total risk']<=70, 'Total risk (discrete)']='high (40%-70%)'
    total_risk_df.loc[total_risk_df['Total risk']<=40, 'Total risk (discrete)']='medium (10%-40%)'
    total_risk_df.loc[total_risk_df['Total risk']<=10, 'Total risk (discrete)']='low (0%-10%)'


    color_dict_grids = {'low (0%-10%)': '#8AC474',
                'medium (10%-40%)': '#F1F905',
                'high (40%-70%)':'#FEBF04',
                'very high (70%-100%)': '#FF0000'}


    english_facility_types=pd.DataFrame.from_dict(facility_types_english_mapping, columns=['Facility type (English)'], orient='index')
    english_facility_types=english_facility_types.reset_index().rename(columns={'index':'Facility type'})


    # In[ ]:

    print("Predictions")
    print(predictions_df.shape)
    print(predictions_df.columns)

    # In[ ]:


    points_for_qgis=predictions_df[['License ID (MOMRAH)','STATUS_ID','Latitude', 'Longitude',
        'Business activity', 'Business Activity Weight', 'probability of no violation', 'probability of violation',
        'Area of the retail outlet','Facility type','Scaled area of the retail outlet', 'countall','probability of violation (discrete)',
        'geometry','AMANA', 'MUNICIPALITY', 'SUB_MUNICIPALITY',
       'DISTRICT_ID', 'DISTRICT_NAME', 'Last License renewal date',
       'Establishment Name']]


    points_for_qgis['Business risk']=points_for_qgis['probability of violation']*points_for_qgis['Scaled area of the retail outlet']*points_for_qgis['Business Activity Weight']

    #points_for_qgis=points_for_qgis.merge(english_facility_types, on='Facility type', how='left') #CHANGED TO LEFT JOIN

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:risk_from_businesses Shape: {}
    	and df2:points_for_qgis Shape: {}  how='right', on='License ID (MOMRAH)'
        """.format(grid_level_probabilities.shape, points_for_qgis.shape))
    points_for_qgis=grid_level_probabilities[~grid_level_probabilities['License ID (MOMRAH)'].isna()][['GridNumber','License ID (MOMRAH)']].merge(points_for_qgis, how='right', on='License ID (MOMRAH)')
    logging.info("""CHECK:MERGE_OUTPUT df:points_for_qgis {}
        """.format(points_for_qgis.shape))

    #points_for_qgis.loc[points_for_qgis['new_business']==1, 'Never inspected']=True
    #points_for_qgis['Never inspected']=points_for_qgis['Never inspected'].fillna(False)
    points_for_qgis['Number of past inspections']=points_for_qgis['countall']-1
    points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry='geometry',crs='epsg:4326')
    points_for_qgis=points_for_qgis.drop_duplicates('License ID (MOMRAH)')
    print(len(points_for_qgis))

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:points_for_qgis Shape: {}
    	and df2:population_grids Shape: {}  on='GridNumber'
        """.format(points_for_qgis.shape, population_grids.shape))
    points_for_qgis=points_for_qgis.merge(population_grids[['GRID_ID','GridNumber']], on='GridNumber', how='inner') #ADDED Rejoining with preassigned grid ids
    logging.info("""CHECK:MERGE_OUTPUT df:points_for_qgis {}
        """.format(points_for_qgis.shape))

    print(len(points_for_qgis)) #THIS SHOULD BE THE SAME LENGTH AS THE LENGTH BEFORE MERGE. Probably an issue with unassigned grids
    points_for_qgis.to_csv(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'points_for_qgis.csv'))

    points_for_qgis = points_for_qgis.drop_duplicates()

    # ADDED TO GET AMANA< MUNICIPALITYCODE< SUNMUNICIPALITY code etc in LICENCE level data
    #points_for_qgis = geometry_per_grid.sjoin( points_for_qgis,how="inner",predicate="intersects")
    
    licenses_per_facility=points_for_qgis.groupby(['GridNumber', 'Facility type'])['License ID (MOMRAH)'].count().reset_index().rename(columns={'License ID (MOMRAH)':'Number of Licenses'})
    unique_facilities=licenses_per_facility['Facility type'].unique()
    n_licenses_per_facility=points_for_qgis.groupby(['GridNumber'])['License ID (MOMRAH)'].count().reset_index().rename(columns={'License ID (MOMRAH)':'Total Number of Licenses'})
    licenses_per_facility=pd.pivot_table(licenses_per_facility, index='GridNumber', values='Number of Licenses', columns='Facility type').reset_index()
    
    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:licenses_per_facility Shape: {}
    	and df2:n_licenses_per_facility Shape: {} how='left', on='GridNumber'
        """.format(licenses_per_facility.shape, n_licenses_per_facility.shape))
    licenses_per_facility=licenses_per_facility.merge(n_licenses_per_facility, how='left', on='GridNumber')
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_facility {}
        """.format(licenses_per_facility.shape))

    for col in unique_facilities:
        licenses_per_facility.loc[licenses_per_facility[col]>=0.5*licenses_per_facility['Total Number of Licenses'], 'Area facility type']=col

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:total_risk_df Shape: {}
    	and df2:grid_level_license_risks Shape: {}  on='GridNumber', how='left'
        """.format(total_risk_df.shape, grid_level_license_risks.shape))
    tooltip_df=total_risk_df.merge(grid_level_license_risks, on='GridNumber', how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_facility {}
        """.format(tooltip_df.shape))

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:tooltip_df Shape: {}
    	and df2:licenses_per_facility Shape: {}  how='left', on='GridNumber'
        """.format(tooltip_df.shape, licenses_per_facility.shape))
    tooltip_df=tooltip_df.merge(licenses_per_facility[['GridNumber','Area facility type']], how='left', on='GridNumber')
    #tooltip_df = gpd.GeoDataFrame(tooltip_df, geometry=tooltip_df['GEOMETRY_x'].astype('str'),crs='epsg:4326')
    logging.info("""CHECK:MERGE_OUTPUT df:licenses_per_facility {}
        """.format(tooltip_df.shape))

    tooltip_df=tooltip_df.round(2)

    logging.info("""CHECK:FILTER_OUTPUT df:lines_priority_areas {}
        """.format(priority_areas.shape))
    lines_priority_areas=priority_areas[priority_areas['geometry'].apply(lambda x : x.type!='Polygon' )]
    logging.info("""CHECK:FILTER_OUTPUT df:lines_priority_areas {} removing non polygon areas
        """.format(lines_priority_areas.shape))

    lines_priority_areas=lines_priority_areas.to_crs(epsg=4326)
    priority_areas=priority_areas.to_crs(epsg=4326)

    n_licenses_per_probability=points_for_qgis.groupby('probability of violation (discrete)')['License ID (MOMRAH)'].nunique().reset_index().rename(columns={'License ID (MOMRAH)':'number of licenses'})
    n_licenses_per_probability['Percentage']=np.round(100*n_licenses_per_probability['number of licenses']/n_licenses_per_probability['number of licenses'].sum(),2)
    n_licenses_per_probability.to_csv(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'license_probability_distributions.csv'))

    logging.info("""CHECK:MERGE function:Perform_Predictions_Risk_Calculations 
        df1:population_grids Shape: {}
    	and df2:population_grids Shape: {}  on='GridNumber', how='outer'
        """.format(population_grids.shape, population_grids.shape))
    tooltip_df=tooltip_df.merge(population_grids[['GRID_ID','GridNumber']], on='GridNumber', how='outer') #ADDED Rejoining with preassigned grid ids
    logging.info("""CHECK:MERGE_OUTPUT df:tooltip_df {}
        """.format(tooltip_df.shape))

    print(len(tooltip_df))

    tooltip_df.to_csv(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'qgis_grids.csv'))

    return points_for_qgis , tooltip_df
