import pandas as pd
import config
from classes.engines import Helper as Help
Helper=Help.Helper()
df=pd.read_csv("test.csv")
Helper.insert_df_Batchwise( df, config.OUTPUT_TABLE_NAME, 50000)