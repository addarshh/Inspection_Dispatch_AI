import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import classes.Database as DB
import pandas as pd
from helpers import getDateString
import traceback
from classes.engines.RiskBasedDispatch import RiskBasedDispatch
from classes.engines.CasePrioritization import CasePrioritization
pd.options.mode.chained_assignment = None
import classes.engines.Helper as Help
import geopandas as gpd
import warnings
import logging
from classes.types.RBD.GroupHousing import GroupHousing as GroupHousingRBD
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
warnings.filterwarnings("ignore")
Helper=Help.Helper()
Helper.Engine_Start_Metadata_Update()

if __name__ == "__main__":
    if len(config.MODELS_TO_RUN) and len(config.INSPECTION_CATEGORIES):
        engines = {}
        data = DB.Database(config.AMANA_CODE)
        recordId = data.insertModelStatus('DATA_FETCHING_KATE', getDateString(), 'In progress')
        if 'RBD' in config.MODELS_TO_RUN:
            engines['RBD'] = RiskBasedDispatch()
            if 'Excavation' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getExcavationData()
            elif 'Construction' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getConstructionData()
            elif 'GroupHousing' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getInspectionsData()
                data.getLicensesGroupHousingData()
            else :
                data.getPoiDataGdf()
                data.getLicensesData()
                data.getLicensesKeysData()
                data.getInspectionsData()
                data.getAmanaPopulationOverlay()
                data.getPriorityAreasData()
                data.updateModelStatus(recordId, None, 'rbd data fetched')
                if 'Excavation' in config.INSPECTION_CATEGORIES:
                    data.getExcavationData()
                if 'Construction' in config.INSPECTION_CATEGORIES:
                    data.getConstructionData()
                if 'GroupHousing' in config.INSPECTION_CATEGORIES:
                    data.getLicensesGroupHousingData()
        if 'CP' in config.MODELS_TO_RUN:
            data.getCommonData()
            data.getCrmCases()
            data.getClassConfig()
            data.getAmanaDataGdf()
            data.getPopulationDataGdf()
            data.getAmanaPopulationOverlay2()
            data.getPriorityAreasData()
            engines['CP'] = CasePrioritization()
            data.updateModelStatus(recordId, None, 'CP data fetched')
        data.updateModelStatus(recordId, getDateString(), 'Finished')
        for engine in config.MODELS_TO_RUN:
            if engine == 'CP':
                for inspection in config.INSPECTION_CATEGORIES:
                    recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, getDateString(), 'In progress')

                    try :
                        inspectionName = globals()[inspection + engine]
                        inspectionInstance = inspectionName(data)
                        
                        data.updateModelStatus(recordId, getDateString(), 'building features')
                        inspectionInstance.getFeatures()
                        engines[engine].setInspectionType(inspectionInstance)
                        data.updateModelStatus(recordId, getDateString(), 'running model')
                        engines[engine].process()
                        data.updateModelStatus(recordId, getDateString(), 'Finished')
                        del inspectionInstance
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        data.updateModelStatus(recordId, getDateString(), 'Error occured')
            if engine == 'RBD':
                for inspection in config.INSPECTION_CATEGORIES:
                    recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, getDateString(), 'In progress')
                    try:
                        inspectionName = globals()[inspection + engine]
                        inspectionInstance = inspectionName(data)
                        
                        data.updateModelStatus(recordId, None, 'building feature DF')
                        trainDataset : pd.DataFrame = inspectionInstance.buildFeatureDf()

                        trainTestModelFunc = getattr(engines[engine], inspectionInstance.methods['trainTestModel'])
                        trainTestModelFunc(trainDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)
                        trainDataset = None
 
                        
                        data.updateModelStatus(recordId, None, 'building artifical DF')
                        artificialDataset : pd.DataFrame = inspectionInstance.buildArtificialDf()

                        processModelFunc = getattr(engines[engine], inspectionInstance.methods['processModel'])
                        predictedResult = processModelFunc(artificialDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)

                        data.updateModelStatus(recordId, None, 'running model')
                        getModelResultsFunc = getattr(engines[engine], inspectionInstance.methods['getModelResults'])
                        tooltip_df, points_for_qgis = getModelResultsFunc(data, predictedResult, inspection,  inspectionInstance.data.licensesDf)

                        data.updateModelStatus(recordId, None, 'saving to the database')
 
                        if tooltip_df is not None:
                            # data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_GRIDS')
                            # data.updateModelStatus(recordId, None, 'saving to the database grids')
                            # data.saveRBDOutput(tooltip_df, table_name = f"inspections_rbd_{inspection}_output_grids".lower())
                            print(tooltip_df.shape)
                            output_table_name = config.GRIDS_OUTPUT_TABLE_NAME
                            Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( tooltip_df_subset, output_table_name, 10000)

                        if points_for_qgis is not None:
                            # data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_LICENSES')
                            if 'FACILITY_TYPE_ENGLISH' in points_for_qgis.columns:
                                points_for_qgis['FACILITY_TYPE_ENGLISH'] = points_for_qgis['FACILITY_TYPE_ENGLISH'].astype(str)
                            # data.updateModelStatus(recordId, None, 'saving to the database licenses')
                            # data.saveRBDOutput(points_for_qgis.iloc[:, :], table_name = f"inspections_rbd_{inspection}_output_licenses".lower())
                            print(points_for_qgis.shape)
                            output_table_name = config.LICENSES_OUTPUT_TABLE

                            #ADDED
                            populationgrid_df = DB.getPopulationData()
                            print(populationgrid_df.shape)
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)
                            #points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry='GEOMETRY', crs="EPSG:4326")
                            # points_for_qgis['long'] = pd.to_numeric(points_for_qgis['long'], errors='coerce')
                            # points_for_qgis['lat'] = pd.to_numeric(points_for_qgis['lat'], errors='coerce')
                            
                            # # REVERSING LAT AND LONG
                            #points_for_qgis.rename(columns = {'START_POINT_Y':'START_POINT_X', 'START_POINT_X':'START_POINT_Y'}, inplace = True)
                            
                            points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry=gpd.points_from_xy(points_for_qgis.LATITUDE, points_for_qgis.LONGITUDE), crs="EPSG:4326")
    
                            points_for_qgis = gpd.sjoin(populationgrid_df, points_for_qgis,how="right",predicate="intersects")
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)

                            Zone_df = DB.get_grid_zone()
                            logging.info("""Fetched zone_grids shape: {}
                            """.format(Zone_df.shape))
                            print(Zone_df.shape)
                            print(Zone_df.head())
                            print(points_for_qgis.head())
                            points_for_qgis_withzone=points_for_qgis.merge(Zone_df, how="left", left_on="GridNumber", right_on="GRIDUNIQUECODE")
                            logging.info("""OUTPUT tooltip_df_subset_withzone shape: {}
                            """.format(points_for_qgis_withzone.shape))
                            print(points_for_qgis_withzone.shape)
                            print("HERE")
                            print(points_for_qgis_withzone.columns)
                            points_for_qgis_withzone.to_csv('tooltip_df_subset_withzone.csv')
                            points_for_qgis_withzone = points_for_qgis_withzone.loc[:,points_for_qgis_withzone.columns != 'GEOMETRY']

                            Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( points_for_qgis_withzone, output_table_name, 10000)


                        data.updateModelStatus(recordId, getDateString(), 'Finished')

                        del inspectionInstance
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        data.updateModelStatus(recordId, getDateString(), 'Error occured')
                    Helper.Engine_End_Metadata_Update()
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)