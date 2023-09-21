import os
import cx_Oracle
import sys
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

class GisDatabase:

    def __init__(self):
        self.sql = {}
        self.db_string = "oracle+cx_oracle://{}:{}@".format(
            os.getenv('DB_GIS_USER'),
            os.getenv('DB_GIS_PSWD')) + cx_Oracle.makedsn(
            os.getenv('DB_GIS_HOST'),
            os.getenv('DB_GIS_PORT'),
            service_name=os.getenv('DB_GIS_BASE'))
        self.engine = create_engine(self.db_string)
        self.db = self.engine.connect()
        self.defineSQL()
        for key in self.sql:
            try:
                if os.getenv('APP_DEBUG'):
                    print('Script ' + key + ' has been started at ' + str(datetime.now()))
                target_filename = os.path.join('/var', 'www', 'html', 'scripts', 'GISdata', key + ".csv")
                gis_layer = self.executeQuery(self.sql[key])
                gis_layer.to_csv(target_filename, index=False)
                if os.getenv('APP_DEBUG'):
                    print('Script ' + key + ' has been finished fine at ' + str(datetime.now()))
            except:
                if os.getenv('APP_DEBUG'):
                    print('When executing the script ' + key + ' an error has occured')

    def defineSQL(self):
        self.sql = {
            'GGINSPECTIONGRIDS': f"SELECT g.GRIDNUMBER GRIDUNIQUECODE, g.AMANABALADIARNAME AMANA, \
                g.AMANABALADI AMANACODE,                        g.GRID_ID, \
                g.GRIDNAME,                                     g.MUNICIPALITYBALADIARNAME MUNICIPALITY, \
                g.MUNICIPALITYBALADI MUNICIPALITYCODE,          g.REGIONBALADIARNAME REGION, \
                g.REGIONBALADI REGIONCODE,                      g.DN, \
                SDE.ST_AREA(g.SHAPE) SHAPE_AREA,                SDE.ST_LENGTH(g.SHAPE) SHAPE_LEN, \
                SDE.ST_ASTEXT(g.SHAPE) geometry \
                FROM GISOWNER.GGINSPECTIONGRIDS g  \
                WHERE SHAPE is not NULL"
        }

    def executeQuery(self, sql: str) -> pd.DataFrame:
        result = self.db.execute(sql)
        columns_names = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns=columns_names)
