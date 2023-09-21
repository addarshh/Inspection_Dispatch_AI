from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import statistics
import config

def feature_score(df,col):
	#defining bin labels
	bins_label1=[1,2,3,4]
	bins_label2=[2,3,4]
	bins_label3=[2,4]
	bins_label4=[4]

	df[col] = df[col].astype(float)

	col_bins1 = [-1, np.percentile(df[col], 25), np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
	col_bins2 = [-1, np.percentile(df[col], 50), np.percentile(df[col], 75), df[col].max()]
	col_bins3 = [-1, np.percentile(df[col], 75), df[col].max()]
	col_bins4 = [-1, df[col].max()]

	if np.array_equal(df[col].unique(), df[col].unique().astype(bool)) is False:
		bins_count = pd.qcut(df[col],q=4,duplicates='drop')
		bins_count_value = len(bins_count.unique())
		col_bins = col_bins1 if bins_count_value == 4 else col_bins2 if bins_count_value == 3 else col_bins3 if bins_count_value == 2 else col_bins4
		col_labels = bins_label1 if bins_count_value == 4 else bins_label2 if bins_count_value == 3 else bins_label3 if bins_count_value == 2 else bins_label4
		df[col+'_bins'] = pd.cut(df[col], bins= col_bins, labels= col_labels, duplicates='drop').astype(int)

		df[col+'_score']=(df[col+ '_bins'])*df[col]/statistics.mean(df[col])
		#print(df[col+'_score'])
	else:
		df.loc[df[col] == 1, col+'_score'] = 1
		df.loc[df[col] == 0, col+'_score'] = 0
		df[col+'_score']=(df[col+'_score'])/statistics.mean(df[col+'_score'])
