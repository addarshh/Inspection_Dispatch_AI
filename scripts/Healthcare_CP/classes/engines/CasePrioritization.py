#Caze prioritization engine
import pandas as pd
import os.path as path
import numpy as np
import math
from statistics import mean
from sklearn.preprocessing import MinMaxScaler
import config

from .Engine import Engine
import classes.Inspection as insp
from .CazePrioritizationsDictionary import dictionary as dict

class CasePrioritization (Engine):
        def __init__(self):
            Engine.__init__(self)
            # self.vp = vp
            # self.df = None
            self.hardcodedMetrics = ['Priority_Value', 'satisfaction_level']
            # self.feature_creation_path = config.FEATURE_CREATION_OUTPUT_FOLDER
            # self.model_score_path = config.MODEL_SCORING_OUTPUT_FOLDER + '/cp'
            # self.needToBeNormalilzed = ['DN', 'Days_lapsed', 'Width of Road']
        

        def _getFeatureIndex(self, column):
            return column / mean(column)

        # def _normalize(self, column):
        #     return column.map(lambda i: np.log(i) if i > 0 else 0)

        def _scoreHardcodedMetrics(self, columnName, df, values = []):
            if len(values) > 0:

                sorted = df[columnName].unique()
                print(columnName)
                print(sorted)
                sorted.sort()
                for i in sorted:
                     df.loc[df[columnName] == i, 'temp'] = values[i]
            else:
                df['temp'] = df[columnName]
            
            df[columnName + '_score'] = self._getFeatureIndex(df['temp'])
            
        
        def getScoreForOneColumn(self, column, df ):
            bins = {
                4 : { 'labels' : [1,2,3,4], 'values' : []},
                3 : { 'labels' : [2,3,4], 'values' : []},
                2 : { 'labels' : [3,4], 'values' : []},
                1 : { 'labels' : [4], 'values' : []},
            }

            # if column in self.needToBeNormalilzed:
            #     df[column] = self._normalize(df[column])
            if column in self.hardcodedMetrics:
                self._scoreHardcodedMetrics(column, df, dict[column]['values'])
            else:
                bins[4]['values'] = [-1, np.percentile(df[column], 25), np.percentile(df[column], 50), np.percentile(df[column], 75), df[column].max()]
                bins[3]['values'] = [-1, np.percentile(df[column], 50), np.percentile(df[column], 75), df[column].max()]
                bins[2]['values'] = [-1, np.percentile(df[column], 75), df[column].max()]
                bins[1]['values'] = [-1, df[column].max()]

                if np.array_equal(df[column].unique(), df[column].unique().astype(bool)) is False:
                    bins_count = pd.qcut(df[column], q=4, duplicates='drop')
                    bins_count_value = len(bins_count.unique())
                    
                    bin_id = 1 if bins_count_value == 4 else 3 if bins_count_value == 3 else 2 if bins_count_value == 2 else 4
                    df[column + ' bins'] = pd.cut(df[column], bins = bins[bins_count_value]['values'], labels = bins[bins_count_value]['labels'], duplicates='drop').astype('int')
                    df[column + '_score'] = (df[column] * df[column + ' bins']) / mean(df[column])
                    df.drop(columns=[column + ' bins'], inplace = True)
                else:
                    df.loc[df[column] == 1, 'temp'] = 1
                    df.loc[df[column] == 0, 'temp'] = 0

                    #indexing the feature
                    df[column + '_score'] = self._getFeatureIndex(df['temp'])

                    # for label in df['temp'].unique():
                    #     df.loc[df['temp'] == label, dict[column]['title']] = dict[column]['rate'][label]
                df[column + '_score'].fillna(0,inplace=True)

        def getScores(self):

            # if self.df is None:
            #     self.readInput(self)
            columns = self.df.columns.values.tolist()[4:]
            
            for column in columns:
                # print(column)
                # print(type(column))
                self.getScoreForOneColumn(column, self.df)
            self.df.drop(columns=['temp'], inplace = True)
        
        def scoreModel(self):
            scaler = MinMaxScaler()
            columnsNames = list(filter(lambda columnName: '_score' in columnName, self.df.columns.values.tolist()))
            self.df[columnsNames] = scaler.fit_transform(self.df[columnsNames])

            impactColumnsNames = ['avg_compliance_percent_score', 'satisfaction_level_score', 'Priority_Value_score']
            visibilityColumnsNames = filter(lambda columnName: columnName not in impactColumnsNames, columnsNames)

            #Final scoring of each case for risk
            self.df['Final_score_Visibility'] = self.df[visibilityColumnsNames].sum(axis = 1)
            self.df['Final_score_Impact'] = self.df[impactColumnsNames].sum(axis = 1)  

            self.df[['Final_score_Visibility','Final_score_Impact']]= scaler.fit_transform(self.df[['Final_score_Visibility','Final_score_Impact']])

            # BA team input to update the weights dynamically
            self.df['Total_score'] = self.df['Final_score_Impact']*.32 + self.df['Final_score_Visibility']*.68
            self.df['Total_score'] = ((self.df['Total_score'] - min(self.df['Total_score'])) / (max(self.df['Total_score']) - min(self.df['Total_score']))) *100
            
            self.df.drop_duplicates(inplace=True)
            self.df.sort_values('Total_score', ascending=False, inplace = True )
            
            columnsNames += ['Total_score', 'Final_score_Impact', 'Final_score_Visibility' ]
            self.df[columnsNames] = self.df[columnsNames].astype('float').round(2)

            self.df.loc[self.df['Total_score'] > 70, 'Total_score_classes'] = 'Very high (70%-100%)'
            self.df.loc[self.df['Total_score'] <= 70, 'Total_score_classes'] = 'High (40%-70%)'
            self.df.loc[self.df['Total_score'] <= 40, 'Total_score_classes'] = 'Medium (10%-40%)'
            self.df.loc[self.df['Total_score'] <= 10, 'Total_score_classes'] = 'Low (0%-10%)'
            self._outputToFile('cp')
            # self.df.to_excel(path.join(path.dirname(__file__), self.model_score_path, ('file' if self.vp is None else self.vp.vp_label) + ".xlsx"), index = False) 
  
        def process(self):
            self._readInput('cp')
            columns = self.df.columns

            check = [i for i in columns if "_score" in i]

            if not len(check):
                self.getScores()
            self.scoreModel()
            self._outputToFile('cp')

        