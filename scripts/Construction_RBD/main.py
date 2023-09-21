import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import classes.FileDatabase as fDB
import classes.Database as DB
# from classes.types.RBD.Retail import Retail as RetailRBD
# from classes.types.RBD.Health import Health as HealthRBD
# from classes.types.RBD.Excavation import Excavation as ExcavationRBD
from classes.types.RBD.Construction import Construction as ConstructionRBD
# from classes.types.CP.Retail import Retail as RetailCP
# from classes.types.CP.Health import Health as HealthCP
import pandas as pd
from helpers import getDateString
import traceback
import warnings
warnings.filterwarnings('ignore')
from classes.engines.RiskBasedDispatch import RiskBasedDispatch
from classes.engines.CasePrioritization import CasePrioritization
import classes.engines.Helper as Help
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase
import logging
import geopandas as gpd
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
Helper=Help.Helper()

if __name__ == "__main__":

    if len(config.MODELS_TO_RUN) and len(config.INSPECTION_CATEGORIES):
        logging.basicConfig(filename="Construction_RBD.txt", level=logging.INFO)
        logging.basicConfig(filename="Construction_RBD.txt", level=logging.ERROR)
        engines = {}
        data = fDB.fileDatabase(config.AMANA_CODE)
        Helper.Engine_Start_Metadata_Update()

        recordId = data.insertModelStatus('DATA_FETCHING', getDateString(), 'In progress')
        
        data.updateModelStatus(recordId, None, 'common data fetched')
   
        if 'RBD' in config.MODELS_TO_RUN:
            engines['RBD'] = RiskBasedDispatch()
            if 'Excavation' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getExcavationData()
            elif 'Construction' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getConstructionData()
            else :
                data.getPoiDataGdf()
                data.getCommonData()
                data.getAmanaPopulationOverlay()
                gdata.getPriorityAreasData()
                data.updateModelStatus(recordId, None, 'rbd data fetched')
                
                if 'Excavation' in config.INSPECTION_CATEGORIES:
                    data.getExcavationData()

                if 'Construction' in config.INSPECTION_CATEGORIES:
                    data.getConstructionData()

        
        if 'CP' in config.MODELS_TO_RUN:
            data.getCommonData()
            data.getCrmCases()
            data.getClassConfig()
            data.getAmanaDataGdf()
            data.getPopulationDataGdf()
            data.getAmanaPopulationOverlay2()
            gdata.getPriorityAreasData()
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
                    #data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_GRIDS')
                    #data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_LICENSES')
                    print('delete from table')
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
                        print(artificialDataset.shape)
                        logging.info("Artificialdf shape is :"+str(artificialDataset.shape))
                        if artificialDataset is not None:
                            output_table_name = config.FEATURES_OUTPUT_TABLE_NAME
                            #Helper.backup(output_table_name)
                            #Helper.insert_df_Batchwise( artificialDataset, output_table_name, 50000)
                        
                        processModelFunc = getattr(engines[engine], inspectionInstance.methods['processModel'])
                        predictedResult = processModelFunc(artificialDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)

                        data.updateModelStatus(recordId, None, 'running model')
                        getModelResultsFunc = getattr(engines[engine], inspectionInstance.methods['getModelResults'])
                        tooltip_df, points_for_qgis = getModelResultsFunc(data, predictedResult, inspection,  inspectionInstance.data.licensesDf)

                        data.updateModelStatus(recordId, None, 'saving to the database')
                        #print(tooltip_df.shape)
                        #print(points_for_qgis.shape)
                        if tooltip_df is not None:
                            data.updateModelStatus(recordId, None, 'saving to the database grids')
                            #data.saveRBDOutput(tooltip_df, table_name = f"inspections_rbd_{inspection}_output_grids".lower())
                            
                            output_table_name = config.GRIDS_OUTPUT_TABLE_NAME
                            #Helper.backup(output_table_name)
                            #tooltip_df_subset = tooltip_df[tooltip_df['NUMBER_OF_LICENSES'] > 0]
                            print(tooltip_df.columns)
                            print(tooltip_df.shape)
                            logging.info("tooltipdf shape is :"+str(tooltip_df.shape))
                            Helper.insert_df_Batchwise( tooltip_df, output_table_name, 10000)

                        if points_for_qgis is not None:
                            logging.info("points_for_qgis shape is : "+str(points_for_qgis.shape))
                            if 'FACILITY_TYPE_ENGLISH' in points_for_qgis.columns:
                                points_for_qgis['FACILITY_TYPE_ENGLISH'] = points_for_qgis['FACILITY_TYPE_ENGLISH'].astype(str)
                            data.updateModelStatus(recordId, None, 'saving to the database licenses')
                            #data.saveRBDOutput(points_for_qgis.iloc[:, :], table_name = f"inspections_rbd_{inspection}_output_licenses".lower())
                            output_table_name = config.LICENSES_OUTPUT_TABLE

                            populationgrid_df = GDB.getPopulationData()
                            print(populationgrid_df.shape)
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)
                            #points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry='geometry', crs="EPSG:4326")
                            points_for_qgis['long'] = pd.to_numeric(points_for_qgis['long'], errors='coerce')
                            points_for_qgis['lat'] = pd.to_numeric(points_for_qgis['lat'], errors='coerce')
                            
                            # # REVERSING LAT AND LONG
                            points_for_qgis.rename(columns = {'lat':'long', 'long':'lat'}, inplace = True)
                            
                            ## Rounding off LAT and LONG
                            #points_for_qgis.loc[points_for_qgis['long'] > 100 && points_for_qgis['long'] > 0]



                            points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry=gpd.points_from_xy(points_for_qgis.long, points_for_qgis.lat), crs="EPSG:4326")
    
                            points_for_qgis = gpd.sjoin(populationgrid_df, points_for_qgis,how="right",predicate="intersects")
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)

                            Zone_df = GDB.get_grid_zone()
                            logging.info("""Fetched zone_grids shape: {}
                            """.format(Zone_df.shape))
                            print(Zone_df.shape)
                            points_for_qgis_withzone=points_for_qgis.merge(Zone_df, how="left", left_on="GridNumber", right_on="GRIDUNIQUECODE")
                            logging.info("""OUTPUT tooltip_df_subset_withzone shape: {}
                            """.format(points_for_qgis_withzone.shape))

                            print(points_for_qgis_withzone.columns)
                            
                            points_for_qgis_subset = points_for_qgis_withzone[['Amana', 'Municipality', 
                            # 'Sub-Municipality', 
                            'ISSUE_DATE','EXPIRATION_DATE', 'lat', 'long', 'consultant name',
                            'GridNumber', 'AMANACODE', 'GRID_ID', 'GridName',
                            'MUNICIPALI', 'MUNICIPA_1', 'REGION', 'REGIONCODE', 'DN',
                            # 'Consultant license id', 'BUILDING TYPE ID', 'Building main use id',
                            'License ID', 
                            # 'Building sub use id', 'Parcel area', 'Contractor',
                            # 'Contractor license ID', 'owner_id', 'BUILDING TYPE_eng',
                            # 'Building main use_eng', 'Building Sub use_eng', 'Amana Name English',
                            # 'Building_Tag', 
                            'Log_Parcel_Area', 
                            # 'LicenseDuration', 'Building height',
                            'probability of no violation', 'probability of violation',
                            # 'VOLUME_SCALED', 
                            'LICENSE_RISK',  'probability of violation (discrete)',
                            # 'LICENSE_RISK_points_size',
                            'GRIDUNIQUECODE', 'GUID', 'INSPECTIONTYPE',

                            'Category']]
                            points_for_qgis_subset = points_for_qgis_subset[points_for_qgis_subset['Category'] == 'Active']
                            print(points_for_qgis_subset.GUID.isna().sum())
                            print(points_for_qgis_subset.GRIDUNIQUECODE.isna().sum())
                            print(points_for_qgis_subset.shape)
                            logging.info("points_for_qgis_subset shape is :"+str(points_for_qgis_subset.shape))
                            #top100perAmana = points_for_qgis_subset.groupby('Amana').head(100).reset_index(drop=True)
                            Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( points_for_qgis_subset, output_table_name, 50000)

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