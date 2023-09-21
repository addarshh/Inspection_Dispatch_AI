import os
import sys
import config
from datetime import datetime
if config.DB['DEBUG']:
    start_time_final = datetime.now()
import sqlalchemy as sql
import pandas as pd
import geopandas as gpd
import SqlQueries
import cx_Oracle
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 500)

connectionString = "oracle+cx_oracle://" + config.DB["gisuser"] + ":" + config.DB["gispassword"] + "@" + cx_Oracle.makedsn(config.DB["gishost"], config.DB["gisport"], service_name = config.DB["gisservice"])
engine = sql.create_engine(connectionString)
connection = engine.connect()

def _executeQuery(sqlQuery : str) -> pd.DataFrame:
    result = connection.execute(sqlQuery)
    columnsNames = result.keys()
    result = result.fetchall()
    return pd.DataFrame(result, columns = columnsNames)

gisdata = {
    "VDPRIORITYAREAS": SqlQueries.VDPRIORITYAREAS,
    "MUNICIPALITY": SqlQueries.MUNICIPALITY,
    "SUBMUNICIPALITY": SqlQueries.SUBMUNICIPALITY,
    "tnStreetLightingP": SqlQueries.tnStreetLightingP,
    "BBAMANABOUNDARYS": SqlQueries.BBAMANABOUNDARYS,
    "usManholeP": SqlQueries.usManholeP,
    "buBuildingFootPrintS": SqlQueries.buBuildingFootPrintS,
    "tnRoadCenterLineL": SqlQueries.tnRoadCenterLineL,
    "tnPavementsS": SqlQueries.tnPavementsS,
    "lcBaladyBuildinglicenseP": SqlQueries.lcBaladyBuildinglicenseP,
    "LCBALADYDIGGINGLICENSEL": SqlQueries.LCBALADYDIGGINGLICENSEL,
    "LUPARKS": SqlQueries.LUPARKS,
    "tnBridgesL": SqlQueries.tnBridgesL,
    "tnParkingAreaS": SqlQueries.tnParkingAreaS,
    "tnTrafficLightP": SqlQueries.tnTrafficLightP,
    "usWastewaterPipeL": SqlQueries.usWastewaterPipeL,
    "LMPUBLICFACILTITIESP": SqlQueries.LMPUBLICFACILTITIESP,
    "USSOLIDWASTEDUMPP":SqlQueries.USSOLIDWASTEDUMPP,
    "INBALADYINVESTMENTLOCATIONP":SqlQueries.INBALADYINVESTMENTLOCATIONP,
    "INBALADYINVESTMENTASSETSS":SqlQueries.INBALADYINVESTMENTASSETSS,
    "AACITYBOUNDARYS": SqlQueries.AACITYBOUNDARYS,
    "GGGRIDINSPECTIONZONEST": SqlQueries.GGGRIDINSPECTIONZONEST,
    # "GGINSPECTIONGRIDS": SqlQueries.GGINSPECTIONGRIDS # this script launching inside Municipal Assets RBD once engine launch
}

for key in gisdata:
    try:
        targetFilename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "GISdata", key+".csv"))
        gisLayer = _executeQuery(gisdata[key])
        if key != 'GGGRIDINSPECTIONZONEST':
            gisLayer.geometry = gisLayer.geometry.astype("str")
            gisLayer = gpd.GeoDataFrame(gisLayer , geometry=gpd.GeoSeries.from_wkt(gisLayer.geometry, crs = "epsg:4326"))
        if config.DB['DEBUG']:
            print('Script '+ key + ' has been finished fine at ' + str(datetime.now()))
        gisLayer.to_csv(targetFilename, index=False)
    except:
        print('When executing the script ' + key + ' an error has occured')

if config.DB['DEBUG']:
    print("TOTAL TIME: " + str(datetime.now() - start_time_final))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)