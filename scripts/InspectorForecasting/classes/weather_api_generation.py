#get_ipython().system('pip3 install meteostat')
#get_ipython().system('pip3 install geopandas')
from meteostat import Stations
import geopandas as gpd
import pandas as pd
from shapely import wkt
from datetime import datetime
import matplotlib.pyplot as plt
from meteostat import Stations, Daily

data = pd.read_csv("amana_areas.csv")

len(data['SUBMUNIC_3'].unique())

data['geometry'] = data.GEOMETRY.apply(wkt.loads)


# Geopandas GeoDataFrame
gdf = gpd.GeoDataFrame(data, geometry='geometry')
gdf
gdf.drop("Unnamed: 0",inplace=True,axis=1)
gdf['Center_point'] = gdf['geometry'].centroid
#Extract lat and lon from the centerpoint
gdf["long"] = gdf.Center_point.map(lambda p: p.x)
gdf["lat"] = gdf.Center_point.map(lambda p: p.y)
gdf['MUNICIPALI'] = gdf['MUNICIPALI'].apply(lambda x: "00"+str(x))
len(gdf['SUBMUNIC_3'].unique())
gdf.loc[0,["lat","long"]]

final_df = pd.DataFrame()
for idx in range(0, len(gdf)):
  data = pd.DataFrame()
  stations = Stations()
  stations = stations.nearby(lat = gdf.loc[idx,"lat"], lon = gdf.loc[idx,"long"])
  station = stations.fetch(750)


  # Set time period
  start = datetime(2022, 1, 1)
  end = datetime(2022, 12, 31)
  # print(list(station.index)[0])
  # Get daily data
  data = Daily(list(station.index)[0], start, end)
  data = data.fetch()
  data = data.reset_index()

  data['amana_code'] = gdf.loc[idx, "SUBMUNIC_3"]
  if idx==0:
    final_df = data
  if data.empty:
    for st in list(station.index):
      data = pd.DataFrame()
      data = Daily(st, start, end)
      data = data.fetch()
      data = data.reset_index()
      data['amana_code'] = gdf.loc[idx, "SUBMUNIC_3"]
      if data.empty:
        continue
      else:
        final_df = final_df.append(data)
        break
    if data.empty:
      print("I have entered here")
      data = pd.DataFrame()
      data = Daily(40435, start, end)
      data = data.fetch()
      data = data.reset_index()
      data['amana_code'] = gdf.loc[idx, "SUBMUNIC_3"]
      final_df = final_df.append(data)

  else:
    final_df = final_df.append(data)
len(final_df['amana_code'].unique())
final_df['time']
final_df.reset_index(drop=True).to_csv("weather_KSA_2022.csv")

