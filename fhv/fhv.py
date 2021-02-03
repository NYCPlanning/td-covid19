import pandas as pd
import numpy as np
import geopandas as gpd



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/'


df=gpd.read_file(path+'fhv/PercentChange_NTA_April.geojson')
df.crs='epsg:4326'
df=df[(df['ntacode']!='BX98')&(df['ntacode']!='QN98')].reset_index(drop=True)
df['perc'].describe(percentiles=np.arange(0.2,1,0.2))
df['DiffPctCat']=np.where(df['perc']>-0.65,'>-65%',
          np.where(df['perc']>-0.7,'-69%~-65%',
          np.where(df['perc']>-0.75,'-74%~-70%',
           np.where(df['perc']>-0.8,'-79%~-75%',
         '<=-80%'))))
df['Average2019'].describe(percentiles=np.arange(0.2,1,0.2))
df['Cat2019']=np.where(df['Average2019']<=1000,'0~1000',
          np.where(df['Average2019']<=2000,'1001~2000',
          np.where(df['Average2019']<=3000,'2001~3000',
          np.where(df['Average2019']<=4000,'3001~4000',
         '4001~38435'))))
df['Average2020'].describe(percentiles=np.arange(0.2,1,0.2))
df['Cat2020']=np.where(df['Average2020']<=1000,'0~1000',
          np.where(df['Average2020']<=2000,'1001~2000',
          np.where(df['Average2020']<=3000,'2001~3000',
          np.where(df['Average2020']<=4000,'3001~4000',
         '4001~38435'))))
df.to_file(path+'fhv/fhv.geojson',driver='GeoJSON')







