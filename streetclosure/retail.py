import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
from geosupport import Geosupport

pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'


ct=gpd.read_file(path+'input/census/nycctclipped.shp')
ct.crs={'init':'epsg:4326'}
ct=ct.to_crs({'init':'epsg:6539'})
ct['area']=[x.area for x in ct['geometry']]
pop=pd.read_csv(path+'input/census/tractpop2018.csv',dtype=str,converters={'pop':float})
ctpop=pd.merge(ct,pop,how='inner',on='tractid')
ctpop['poparea']=ctpop['pop']/ctpop['area']*5280*5280
ctpop=ctpop.to_crs({'init':'epsg:4326'})
ctpop.to_file(path+'retail/ctpop.shp')





g=Geosupport()
grocery=gpd.read_file(path+'retail/grocery/GroceryStores_NYC_Final_180802.shp')
grocery=grocery.to_crs({'init':'epsg:4326'})
grocery['lat']=np.nan
grocery['long']=np.nan
for i in grocery.index:
    try:
        housenumber=str(grocery.loc[i,'StNum'])
        streetname=str(grocery.loc[i,'StName'])
        zipcode=str(grocery.loc[i,'Zip'])
        addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
        if addr['Latitude']=='':
            grocery.loc[i,'lat']=pd.to_numeric(grocery.loc[i,'geometry'].y)
            grocery.loc[i,'long']=pd.to_numeric(grocery.loc[i,'geometry'].x)
        else:
            grocery.loc[i,'lat']=pd.to_numeric(addr['Latitude'])
            grocery.loc[i,'long']=pd.to_numeric(addr['Longitude'])
    except:
        print(str(i))
        grocery.loc[i,'lat']=pd.to_numeric(grocery.loc[i,'geometry'].y)
        grocery.loc[i,'long']=pd.to_numeric(grocery.loc[i,'geometry'].x)
grocery=grocery.drop('geometry',axis=1)
grocery=gpd.GeoDataFrame(grocery,geometry=[shapely.geometry.Point(x,y) for x,y in zip(grocery['long'],grocery['lat'])],crs={'init' :'epsg:4326'})
grocery.to_file(path+'retail/grocery.shp')


