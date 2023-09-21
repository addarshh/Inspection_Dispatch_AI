import pandas as pd
import cx_Oracle
import sqlalchemy as sql
from sqlalchemy.types import NVARCHAR
import logging
from cleantext import removeNonArabicSymbols
from time import strftime

def collect_data(sqlQuery_, connectString_ ="aiengine/AyC8KUJi@//ruhmpp-exa-scan:1521/medium_crmprod.momra.net"):

    # Connect to database
    try:
        conn = cx_Oracle.connect(connectString_)
    except Exception as err:
       logging.info('Exeption occured while trying to connect ')
    else:
        try:
            curr =conn.cursor()
           
            curr.execute(sqlQuery_)
            row = curr.fetchall()

        except Exception as err:
           logging.info('Exception occured while trying to fetch records ')
        else:
            #print('Completed')
            None
        finally:
            curr.close()
            #print('Cursor closed')
    
    finally:
        conn.close
        #print('Connection closed')
    
    df = pd.DataFrame(row)
    
    if df.shape[0] > 0:
        df.columns = ['CreationDate', 'dLatitude','dLongitude','MunicipalityId','SubMunicipalityId','Sub_SubMunicipalityId', 'DistrictName', 'StreetName', 'VPId','Sub_VPId', 'Spl_VPId', 'strCaseDescription']
    return df

# def collect_TM_model_inputs(sqlQuery_ = None, connectString_ ="PEGA_APP/pega@//10.80.160.151:1521/pegastg"):
    
#     #Checks
    
#     # print(connectString_)
    
    
#     # Connect to database
#     try:
#         conn = cx_Oracle.connect(connectString_)
#     except Exception as err:
#         logging.exception('Exeption occured while trying to connect ')
#     else:
#         try:
#             curr =conn.cursor()
           
#             curr.execute(sqlQuery_)
#             row = curr.fetchall()


#         except Exception as err:
#             logging.exception('Exception occured while trying to fetch records ')
#         else:
#             logging.info('Input config fetched successfully')
#             None
#         finally:
#             curr.close()
#             logging.info('Cursor closed')
    
#     finally:
#         conn.close
#         logging.info('Connection closed')
    
#     df = pd.DataFrame(row)
#     if df.shape[0] > 0 and len(df.columns) > 1:
        
#         df.columns = ['GUID','MUNICIPALITYID', 'SPECIALCLASSIFICATION','SPECIALCLASSIFICATIONID','RANGE','REPITITTIONPERIODDAYS','REPITITTIONPERIODHOURS','NOOFTIMES','SPLCLASSCATEGORY','STARTTIME']
    
#     # 
#     elif df.shape[0] > 0 and len(df.columns) == 1:
        
#         df.columns = ['SPLCLASSIFICATIONID']
        
#     return df

def collect_stopwords(sqlQuery_= None, connectstring_="<DB Use>/<Password>@//<IP or Hostname>:1521/<DB Service name>"):
    
    return None


def export_output(keywords, MunicipalityId, VP_Label, Past_Months, Number_Of_Topics, Number_Of_Cases, SpclId):
    # Arranging result
    res = pd.DataFrame(keywords).transpose()
    # Remove index column
    res = res.reset_index().iloc[: , 1:]
    probs = pd.DataFrame()
    renameProbs = 0
    for i in range(0, res.shape[1]):
        prob = []
        col = res[i]
        
        #Separate the probability of each word in a topic into different columns
        for j in range(0, col.shape[0]):
            x = float(col[j].split('*')[0])
            #x = ''.join([n for n in col[j] if n.isdigit() or n == '.'])
            #x = "{:.2f}".format(float(x)*100)
            prob.append(x)
            #Remove the probability from the original topic column
            res[i][j] = removeNonArabicSymbols(res[i][j])
            
        dfprob = pd.Series(prob)
        dfprob.rename(renameProbs, inplace=True)
        #increment column index (name)
        renameProbs+=1
        probs = pd.concat([probs, dfprob], axis=1)
        
    #Prepare number of columns
    for i in range(res.shape[1], 10):
        res[i] = " "
        probs[i] = " "
    
    #Rename Columns
    for i in range(0, 10):                           
        res = res.rename(columns={i: f"Topic{i+1}"})
        probs = probs.rename(columns={i: f"Probability{i+1}"})

    #Merge the topics and probabilities dataframes
    df = pd.DataFrame()
    for (word_col, prob_col) in zip(res.columns, probs.columns):
        df = pd.concat([df, res[word_col], probs[prob_col]], axis=1)
    #logging.info(df)
    
    #Include other important columns
    df["MunicipalityId"]=str(MunicipalityId)
    df["VP_Label"]=VP_Label
    df["Past_Months"]= Past_Months
    df["Number_Of_Topics"]=Number_Of_Topics
    df["Number_of_Cases"] = Number_Of_Cases

    df["Load_date"] = strftime("%d-%b-%Y %I:%M:%S %p")
    
    df['Load_date'] = pd.to_datetime(df['Load_date'], errors='coerce')
    
    df["SPL_Classification_id"] = str(SpclId)

    # df.to_excel(f"TM_output_Madinah.xlsx",encoding='utf-8-sig', index = False, mode='a', header=False)
    # df.columns = map(str.lower, df.columns)
    # write_engines_output_to_database(df,table_name ="topic_model_hist_tbl", if_exists ='append')

    # df.to_pickle(r'C:\MoMRA\1_RawData\TopicModelSnapshot.pkl')
    
    return df
    
# The database utility for writing case clustering model out into MOMRA production database.
# for (Oracle 19C version) connection string (MEDIUM_AIDBPRO).
def write_engines_output_to_database(
    data,
    table_name      = "topic_model_hist_tbl",
    if_exists       = None,
    uid_pwd         = "AIDB_USER:aidb", # DB User and Password (with ":" )
    hostname        = "ruhmpp-exa-scan.momra.net", # DB server IP address/Hostname
    service_name_   = "medium_aidbpro.momra.net", # Database service name
    schema_         = "AIDB_USER", # Database schema name
    SCID_lst_       = None,
    INCID_lst_      = None  
):
    
    
    oracle_connection_string = f'oracle+cx_oracle://{uid_pwd}' + '@' + cx_Oracle.makedsn(hostname, '1521', service_name=service_name_)
    # oracle_connection_string = "AIDB_USER/f8s5x8@//ruhmsv-ora19c-scan.net:1521/AIDB.momra.net" 
    try:
        engine = sql.create_engine(oracle_connection_string)
        

        conn = engine.connect()
    
        
        if schema_ == "PEGADATA":
            engine.execute(f"DELETE FROM PEGADATA.pr_momra_fw_cs_data_aicrmcases where SPLCLASSIFICATIONID in ({SCID_lst_})")
            engine.execute("DELETE FROM PEGADATA.pr_momra_fw_cs_data_aicrmcases where INCIDENTNUMBER in (select distinct vpid from  AIDB_USER.temp_scid_tbl@AIDBPROD)")

            
    except Exception as e:
        
        logging.exception(f'Exeption occured while trying to connect\n\n {e} ')
        
    else:
        
        try:
            
                  
            df = data

            # Make Table
            if schema_ == "PEGADATA":
                
                    # logging.info(df.head())
                    # logging.info( df.applymap(type).nunique())
		      
                      
                    dtyp = {c:sql.types.VARCHAR(df[c].str.len().max()) 
                            for c in df.columns[df.dtypes == 'object'].tolist()}
                    logging.info("In PEGADATA")
                                       
                    df.to_sql(
                    con       = conn,
                    schema    = schema_,
                    dtype     = dtyp,
                    name      = table_name,
                    if_exists = if_exists,
                    index     = False,
                    chunksize = 10**4
                    )
                    logging.info(f"Model Output completed/exported successfully {schema_}")
                    
                    # engine.execute("delete from  AIDB_USER.temp_scid_tbl@AIDBPROD")
                    
                
            else:
                    
                    try:
                        logging.info("In AIDB_USER>>>>>")
			  
                        dtyp = {c:sql.types.VARCHAR(df[c].str.len().max()) 
                                for c in df.columns[df.dtypes == 'object'].tolist()}
                        
                
                        df.to_sql(
                        con       = conn,
                        schema    = schema_,
			            dtype     = dtyp,                       
                        name      = table_name.lower(),
                        if_exists = if_exists,
                        index     = False,
                        chunksize = 10**4   
                        )
                        logging.info(f"Added into historical table>>>>")
                        
            
                        logging.info(f"Model Output completed/exported successfully {schema_}")
                        
                    except Exception as e:
                        
                        logging.exception(f"Error while exporting the model output into historical table \n\n {e}")
                       
                    
                        
        
                
                
        except Exception as e:
            
            
            logging.exception(f"Error while exporting the model output \n\n {e}")
        
        
            

        
    

        
    finally:
           
   
        # Close connection
        conn.close()
        #logging.info('Connection closed')

pass