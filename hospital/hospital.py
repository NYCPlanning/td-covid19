import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport
import usaddress



pd.set_option('display.max_columns', None)

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/HOSPITAL/'

g=Geosupport()

df=pd.read_excel(path+'CT_with_hospital_total population and age groups.xlsx',sheetname='Hospitals with Total Population')
df['geosupport']=''
df['lat']=np.nan
df['long']=np.nan

for i in df.index:
    try:
        k=usaddress.parse(df.loc[i,'ADDRESS'])
        k=g['1B'](house_number=k[0][0], street_name=' '.join([x[0] for x in k[1:]]), borough_code=df.loc[i,'CITY'].split(',')[-1])
        df.loc[i,'geosupport']=k['House Number - Display Format']+' '+k['First Street Name Normalized']+', '+k['First Borough Name']
        df.loc[i,'lat']=k['Latitude']
        df.loc[i,'long']=k['Longitude']
    except:
        print(str(i))

df.to_csv(path+'hospital.csv',index=False)
