import pandas as pd
import numpy as np
import geopandas as gpd
import json
import shapely



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/bronx/'


df=pd.read_csv(path+'bronxcitibike.csv')
df['dailytrips'].describe(percentiles=np.arange(0.2,1,0.2))
df['cat']=np.where(df['dailytrips']>25,'>25',
          np.where(df['dailytrips']>20,'21~25',
          np.where(df['dailytrips']>15,'16~20',
          np.where(df['dailytrips']>10,'11~15',
          '<=10'))))
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['lon'],df['lat'])],crs='epsg:4326')
df.to_file(path+'bronxcitibike.geojson',driver='GeoJSON',index=True)
