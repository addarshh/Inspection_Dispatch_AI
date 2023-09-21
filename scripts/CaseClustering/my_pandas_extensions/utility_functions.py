import os
import sys
from logging import exception
from shutil import ExecError
import pandas as pd
from tenacity import retry
# from tqdm import tqdm
import smtplib
from my_pandas_extensions.database import collect_caseclust_model_inputs
import logging
from time import gmtime, strftime
import logging
import numpy as np


def unique_cluster_labes(df):
    
    #df = data
 try:
    
    df['Unique_Cluster_Label'] = None
    
    for i in range(0, df.shape[0]):
     for j in range(0, len(df['VP_Label'].unique())):
        if df['VP_Label'][i] == df['VP_Label'].unique()[j]:
         df['Unique_Cluster_Label'][i] = str(j+1)+'_'+str(df['Cluster_Label'][i])
        
    logging.info('Unique clusters assigned successfully to all VP categories')
 except Exception as err:
     logging.exception('Error occured while assiging unique clusters to VP categories')

 return df


def unique_cluster_SC_labels(df):
    
    #df = data
    
    df['Unique_Cluster_Label'] = None
    
          
    for i in range(0, df.shape[0]):
     for j in range(0, len(df['VPId'].unique())):
        if df.at[i,'VPId'] == df['VPId'].unique()[j]:
         df.at[i,'Unique_Cluster_Label'] = str(j+1) + '_' + str(df.at[i,'Cluster_Label'])
         
    return df

def unique_cluster_SC_labels_1(df):
    try:
        df['Unique_Cluster_Label'] = None
        df2 = collect_caseclust_model_inputs(sqlQuery_="SELECT max(to_number(REGEXP_SUBSTR(CLusterID,'[^_]+'))) clusterid FROM {}.PR_MOMRA_FW_CS_DATA_AICRMCASES".format(os.getenv('DB_CRM_SCM_PEGADATA')))
        df2 = df2.reset_index()
        #get the unique num of values from the database
        uniques_num = df2.at[0,'SPLCLASSIFICATIONID']
        if uniques_num == None:
            uniques_num = 0

        uniques = df['VPId'].unique()
    
        for i in range(0, df.shape[0]):
            j = np.where(uniques == df.at[i,'VPId'])[0][0]
                #start the counter with j + uniques_num
            df.at[i,'Unique_Cluster_Label'] =  str(j + uniques_num + 1) + '_' + str(df.at[i,'Cluster_Label'])

        logging.info('Unique clusters assigned successfully to all classification ids')

    except Exception as err:
        logging.exception('Error occured while assiging unique clusters to classification ids')

    return df


def sendEmail():
    
    
    try:      


        server = smtplib.SMTP_SSL('mail.momra.gov.sa', 465)
        server.login('ghouse@momrah.gov.sa', 'xxxx')

        server.sendmail('ghouse@momrah.gov.sa','g.mohammed@intelmatix.ai',
                        'Seize the means of production!')
    
    except Exception as err:
        print("Email send error",err)
    finally:
        print("Email sent successfully!")
pass

    
def myFunc(e):
  return len(e)



