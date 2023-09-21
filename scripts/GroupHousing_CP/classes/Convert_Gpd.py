# Defining a function that helps us in converting a dataframe to geopandas dataframe
## Convert Dataframe into Geopandas Dataframe
# Arguements: data - dataset that needs to be converted in geopandas dataframe, x = Longitude column, y = Latitude column.
#X,Y are optional arguements, which needs to be when Latitude and Longitude columns are present within data

import geopandas
from shapely import wkt
import logging
from geopandas.tools import sjoin
def convert_gpd(data, x=True, y=True):
    logging.info("Started gpd conversion")
    if 'geometry' in data.columns:
        try:
            gpd = geopandas.GeoDataFrame(data, geometry='geometry', crs='epsg:4326')
        except:
            data['geometry'] = data['geometry'].astype(str)
            data['geometry'] = data['geometry'].apply(wkt.loads)
            gpd = geopandas.GeoDataFrame(data, geometry='geometry', crs='epsg:4326')
    else:
        gpd = geopandas.GeoDataFrame(data, geometry=geopandas.points_from_xy(x, y), crs='epsg:4326')
    # gpd.to_crs('epsg:32637',inplace = True)
    logging.info("gpd conversion completed")
    return gpd