import geopandas as gpd
import pandas as pd
import requests
import shapely
from shapely import wkt



pd.set_option('display.max_columns', None)
#path='/home/mayijun/POPS/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/POPS/'
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
#path='C:/Users/Y_Ma2/Desktop/amazon/'
#doserver='http://142.93.21.138:8801/'
doserver='http://localhost:8801/'
doserver='http://159.65.64.166:8801/'



pops=pd.read_csv(path+'pops/COVID REQUESTS 2020.csv')
popsorg=pd.read_csv(path+'pops/nycpops_20191220csv/nycpops_20191220.csv')
pops=pd.merge(pops,popsorg,how='left',left_on='POPS Number',right_on='POPS_Number')
popspt=gpd.GeoDataFrame(pops,geometry=[shapely.geometry.Point(x, y) for x, y in zip(pops['Longitude'],pops['Latitude'])],crs={'init':'epsg:4326'})
popspt['CITIBIKE']=popspt['CITIBIKE'].fillna('N')
popspt['TESTING']=popspt['TESTING'].fillna('N')
popspt['FOOD']=popspt['FOOD'].fillna('N')
popspt['Combo?']=popspt['Combo?'].fillna('N')
popspt['High Potential']=popspt['High Potential'].fillna('N')
popspt.to_file(path+'output/popspt.shp')

popsiso=pops.copy()
popsiso['isogeom']=''
for i in pops.index:
    url=doserver+'otp/routers/default/isochrone?batch=true&mode=WALK'
    url+='&fromPlace='+str(popsiso.loc[i,'Latitude'])+','+str(popsiso.loc[i,'Longitude'])
    url+='&cutoffSec=600'
    headers={'Accept':'application/json'}  
    req=requests.get(url=url,headers=headers)
    js=req.json()
    iso=gpd.GeoDataFrame.from_features(js,crs={'init': 'epsg:4326'})
    popsiso.loc[i,'isogeom']=str(iso.loc[0,'geometry'])
popsiso=gpd.GeoDataFrame(popsiso,geometry=popsiso['isogeom'].map(wkt.loads),crs={'init':'epsg:4326'})
popsiso.to_file(path+'output/popsiso.shp')









gravity=pd.read_csv(path+'travelshed/workbkgravity3.csv',dtype=float,converters={'Unnamed: 0':str})
gravity['tractid']=[str(x)[4:15] for x in gravity['Unnamed: 0']]
nycct=gpd.read_file(path+'travelshed/nycctclipped.shp')
nycct.crs={'init':'epsg:4326'}
gravity=pd.merge(nycct,gravity,how='left',on='tractid')
gravity=gravity[['tractid','GRAVITYRAC','geometry']].reset_index(drop=True)
gravity.to_file(path+'output/travelshed.shp')

gravity=pd.read_csv(path+'travelshed/workctgravitypop.csv',dtype=float,converters={'Unnamed: 0':str})
gravity['tractid']=[str(x)[4:15] for x in gravity['Unnamed: 0']]
nycct=gpd.read_file(path+'travelshed/nycctclipped.shp')
nycct.crs={'init':'epsg:4326'}
gravity=pd.merge(nycct,gravity,how='left',on='tractid')
gravity=gravity[['tractid','GRAVITYPOP','geometry']].reset_index(drop=True)
gravity.to_file(path+'output/travelshedpop.shp')
