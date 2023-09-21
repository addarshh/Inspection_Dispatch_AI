
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import os
import config
import cx_Oracle
import sqlalchemy as sqla
import Database as DB
import logging

def Create_Health_Model(Retail_Licenses_medina_df,Inspections_df, Health_df):
    
    print("CHECK1")

    Health_df = Health_df
    #TB_ISIC_Activities_df = pd.read_csv("Datav2/TB_ISIC_ACTIVITIES.csv")
    #Facility_Businessnumber_map_df = pd.read_csv("Datav2/facily-businessnumberMAP.csv")

    Health_df['Business Activity Description'] = Health_df['D_ACTIVITIES_NAME']
    logging.info("""CHECK:MERGE function:Create_Health_Model df1:Inspections_df Shape: {}
    	and df2:Health_df Shape: {}  on='License ID (MOMRAH)', how='left' 
        """.format(Inspections_df.shape, Health_df.shape))  
    print(Inspections_df.columns)
    print(Health_df.columns)
    df = pd.merge(left=Inspections_df, right = 
                    Health_df, on='Business Activity Description', how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:df {}""".format(df.shape))

    df = df.drop(df[df["Degree of Compliance"].isna()].index)
    logging.info("""CHECK:DROP_OUTPUT degree of compliance = na df:df {}
        """.format(df.shape))
    df = df.drop(df[df["LICENSE NUMBER"].isna()].index)
    logging.info("""CHECK:DROP_OUTPUT Licence number is na df:df {}
        """.format(df.shape))
    df = df[df['Inspection Date'].notna()]
    df['Inspection Date'] = pd.to_datetime(df['Inspection Date'])
    df['LICENSE NUMBER'] = df['LICENSE NUMBER'].astype('int64')

    df = df.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])
    df = df.reset_index(drop=True)
    df['count'] = df.groupby('LICENSE NUMBER').cumcount() + 1


    df['Degree of Compliance'] = abs(df['Degree of Compliance'].astype('float'))


    df['Degree of Compliance_FLAG'] = df['Degree of Compliance'].apply(lambda x: 1 if x ==100 else 0 )

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


    # In[130]:


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


    df[['LICENSE NUMBER', 'Inspection Date', 'count','Prev_Inspection_Date','Prev_TYPE_OF_VISIT']][df['LICENSE NUMBER'] == 604133 ]


    df['countall'] = df.groupby('LICENSE NUMBER')['count'].transform('count')

    df['Inspection Date_Max'] = df.groupby(['LICENSE NUMBER'])['Inspection Date'].transform('max')


    df['Degree of Compliance_Max'] = df.groupby(['LICENSE NUMBER'])['Degree of Compliance'].transform('max')

    df['Degree of Compliance_Min'] = df.groupby(['LICENSE NUMBER'])['Degree of Compliance'].transform('min')

    Retail_Licenses_medina_df['License ID (MOMRAH) INT'] = Retail_Licenses_medina_df['License ID (MOMRAH)']#.str.replace(',','')

    Retail_Licenses_medina_df['LICENSE NUMBER'] = Retail_Licenses_medina_df['License ID (MOMRAH) INT'].astype('int64')


    logging.info("""CHECK:MERGE function:Create_Health_Model df1:df Shape: {}
    	and df2:Retail_Licenses_medina_df Shape: {}  on='License ID (MOMRAH)', how='left' 
        """.format(df.shape, Retail_Licenses_medina_df.shape))
    df1 = pd.merge(left=df, right = Retail_Licenses_medina_df, on='LICENSE NUMBER',how='left')
    logging.info("""CHECK:MERGE_OUTPUT df:df1 {}
        """.format(df1.shape))

    df1['SADAD PAYMENT DATE'] = pd.to_datetime(df1['SADAD PAYMENT DATE'])
    df1['License Start Date'] = pd.to_datetime(df1['License Start Date'])
    df1['License Expiry Date'] = pd.to_datetime(df1['License Expiry Date'])
    df1['Last License renewal date'] = pd.to_datetime(df1['Last License renewal date'])
    df1['Inspection Date'] = pd.to_datetime(df1['Inspection Date'])
    df1['Inspection Date_Max'] = pd.to_datetime(df1['Inspection Date_Max'])

    df1['Violated'] = (df1['Degree of Compliance'] < 100) | (df1['Number of non-compliant clauses'] > 0)
    df1.loc[df1['Status of Work'].isin(['Resolved-NoViolations','Resolved-Rejected','Resolved-Cancelled']),'Violated']= False

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


    # In[143]:


    df1 = df1.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=[True, True])


    # In[144]:


    # df['Prev_Degree of Compliance_Mean'] = df.groupby(['LICENSE NUMBER'])['Prev_Degree of Compliance'].transform('mean')
    # df['Prev_Degree of Compliance_Min'] = df.groupby(['LICENSE NUMBER'])['Prev_Degree of Compliance'].transform('min')
    # df['Prev_Degree of Compliance_Max'] = df.groupby(['LICENSE NUMBER'])['Prev_Degree of Compliance'].transform('max')

    # df['Prev_Number of non-compliant clauses_Mean'] = df.groupby(['LICENSE NUMBER'])['Prev_Number of non-compliant clauses'].transform('mean')
    # df['Prev_Number of non-compliant clauses_Min'] = df.groupby(['LICENSE NUMBER'])['Prev_Number of non-compliant clauses'].transform('min')
    # df['Prev_Number of non-compliant clauses_Max'] = df.groupby(['LICENSE NUMBER'])['Prev_Number of non-compliant clauses'].transform('max')


    # In[145]:


    df1['Age_Since_Establishment'] = (df1['Inspection Date'] - df1['License Start Date'])/np.timedelta64(1, 'M')
    df1['Age_Since_Last_License_Renewal'] = (df1[ 'Inspection Date'] - df1['Last License renewal date'])/np.timedelta64(1, 'M')
    df1['Days_to_License_Expiry'] = (df1[ 'License Expiry Date'] -  df1['Inspection Date'])/np.timedelta64(1, 'M')
    df1['Days_Since_Last_Inspection'] = (df1['Inspection Date'] - df1['Prev_Inspection_Date'])/np.timedelta64(1, 'M')


    # ### TRMMING STARTS HERE

    # In[146]:


    #df1.columns[0:50]


    # In[147]:


    #df1.columns[50:100]


    # In[148]:


    #df1.columns[100:]


    # In[149]:


    #del df1_trim


    # In[150]:


    df1['Area of the retail outlet'] = df1['Area of the retail outlet'].fillna(30)


    # In[151]:


    #THINK ABOUT - 'Issued fine amount','SADAD NO', 'Fine payment status', 'SADAD PAYMENT DATE',
    #   'License Start Date', 'License Expiry Date','Last License renewal date', 
    
    #DROP THE UNNECESSARY COLUMNS    
    
    df1_trim = df1[['Prev_Max_Degree of Compliance', 'Prev_Min_Degree of Compliance',
    'Violated','Area of the retail outlet', 'countall', 'count',
                'Prev_Number of clauses',
                'Prev_Mean_Degree of Compliance', 
                'Prev_Mean_Number of non-compliant clauses',
        'Prev_Degree of Compliance', 'Prev_Number of non-compliant clauses',
        'Prev_Number of non-compliant clauses and High risk',
        'Prev_Number of non-compliant clauses and medium risk',
        'Prev_Issued fine amount', #'Prev_INSPECTION NAME',
                #'Area of the retail outlet', #'AMANA',
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


    # In[152]:


    df1_trim.shape


    # In[153]:


    #Take only the latest inspection entry
    df1_trim = df1_trim[df1_trim['count'] == df1_trim['countall']]


    # In[154]:


    df1_trim.shape


    # In[155]:


    #df1_trim['Area of the retail outlet'] =df1_trim['Area of the retail outlet'].str.replace(',','')
    df1_trim['Prev_Number of clauses'] =df1_trim['Prev_Number of clauses'].astype('string').str.replace(',','')
    #df1_trim.loc[condition, column_label] = new_value
    #df1_trim['Area of the retail outlet'] =df1_trim['Area of the retail outlet'].astype(float)


    # In[156]:


    df1_trim.replace([np.inf, -np.inf], np.nan, inplace=True)


    # In[157]:


    df1_trim['Prev_Number of clauses'] =df1_trim['Prev_Number of clauses'].astype('float')


    # In[158]:


    df1_trim['Area of the retail outlet'] =df1_trim['Area of the retail outlet'].astype('float')


    # ## Step 4: Splitting the Data into Training and Testing Sets
    # 
    # As you know, the first basic step for regression is performing a train-test split.

    # In[159]:


    from sklearn.model_selection import train_test_split

    # We specify this so that the train and test data set always have the same rows, respectively
    np.random.seed(30)
    df_train, df_test = train_test_split(df1_trim, train_size = 0.7, test_size = 0.3, random_state = 100)


    # In[160]:


    from sklearn.metrics import r2_score


    # In[161]:


    y_train = df_train.pop("Violated")
    X_train = df_train


    # In[162]:


    y_test = df_test.pop("Violated")
    X_test = df_test


    # In[180]:


    # POWER TRANSFORM - Train Data
    import sklearn.preprocessing
    from sklearn.preprocessing import PowerTransformer
    pt = PowerTransformer()
    numeric_columns = X_train.columns
    X_train[numeric_columns] = pt.fit_transform(X_train[numeric_columns])


    X_test[numeric_columns] = pt.transform(X_test[numeric_columns])


    # In[181]:


    import pickle
    # Dump the trained decision tree classifier with Pickle
    pt_transformer_pkl_filename = 'pt_transformer.pkl'
    # Open the file to save as pkl file
    pt_transformer_pkl = open(pt_transformer_pkl_filename, 'wb')
    pickle.dump(pt, pt_transformer_pkl)
    # Close the pickle instances
    pt_transformer_pkl.close()


    # In[164]:


    X_test.shape, y_test.shape, X_train.shape, y_train.shape


    # In[165]:


    X_train = pd.DataFrame(X_train)
    X_test = pd.DataFrame(X_test)
    y_train = pd.DataFrame(y_train)
    y_test = pd.DataFrame(y_test)


    # In[166]:


    scale_pos_weight = y_train[y_train.Violated != True].shape[0]/y_train[y_train.Violated == True].shape[0]


    # In[167]:


    import xgboost as xgb
    from sklearn import metrics

    xgclf = xgb.XGBClassifier(scale_pos_weight = scale_pos_weight)

    xgclf.fit(X_train, y_train)

    def myscores(smat):
        tn, fp, fn, tp = smat.ravel()

        print(smat)
        return tp/(tp+fp), tp/(tp+fn)


    #print('AUC on train data by XGBoost =', metrics.roc_auc_score(y_true=y_train,y_score=xgclf.predict_proba(X_train)[:, 1]))


    #print('AUC on test data by XGBoost =', metrics.roc_auc_score(y_true=y_test,y_score=xgclf.predict_proba(X_test)[:, 1]))


    print('Precesion and Recall-Train', myscores(metrics.confusion_matrix( y_train,xgclf.predict(X_train) )))
    print('Precesion and Recall - Test', myscores(metrics.confusion_matrix( y_test,xgclf.predict(X_test) )))


    # In[172]:


    import pickle
    # Dump the trained decision tree classifier with Pickle
    xgboost_pkl_filename = 'xgboost_classifier_1.pkl'
    # Open the file to save as pkl file
    xgboost_model_pkl = open(xgboost_pkl_filename, 'wb')
    pickle.dump(xgclf, xgboost_model_pkl)
    # Close the pickle instances
    xgboost_model_pkl.close()

    column_names = X_train.columns
    pd.DataFrame(column_names).to_pickle('xgboost_column_names.pkl')

    return pt , xgclf




    




