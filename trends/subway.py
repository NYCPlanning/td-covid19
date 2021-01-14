import pandas as pd
import geopandas as gpd
import shapely
import numpy as np



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/trends/'



df=pd.read_csv(path+'STATION.csv',dtype=float,converters={'Borough':str,'Complex_Name':str,'Daytime_Routes':str,'flagentry':float})
df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['Complex_Longitude'],df['Complex_Latitude'])],crs='epsg:4326')
df['Weekday_2019'].describe(percentiles=np.arange(0.2,1,0.2))
df['Cat']=np.where(df['Weekday_2019']<=3000,'243~3000',
                    np.where(df['Weekday_2019']<=6000,'3001~6000',
                    np.where(df['Weekday_2019']<=9000,'6001~9000',
                    np.where(df['Weekday_2019']<=12000,'9001~12000',
                             '12001~205159'))))
df.to_file(path+'station.geojson',driver='GeoJSON')


df=gpd.read_file(path+'station.geojson')