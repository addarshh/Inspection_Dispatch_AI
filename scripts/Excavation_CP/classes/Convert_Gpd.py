import geopandas
from geopandas.tools import sjoin
from shapely import wkt
def convert_gpd(data, x=True, y=True):
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
    return gpd