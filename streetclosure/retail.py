import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
import re
import usaddress
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







# Restaurant
dohmh=pd.read_csv(path+'retail/restaurant/DOHMH_New_York_City_Restaurant_Inspection_Results.csv',dtype=str)
dohmh=dohmh[['CAMIS','DBA','BORO','BUILDING','STREET','ZIPCODE','PHONE','CUISINE DESCRIPTION','Latitude','Longitude','BBL','NTA']].reset_index(drop=True)
dohmh=dohmh.drop_duplicates(keep='first').reset_index(drop=True)
dohmh['Latitude']=pd.to_numeric(dohmh['Latitude'])
dohmh['Longitude']=pd.to_numeric(dohmh['Longitude'])
dohmh['BBL']=pd.to_numeric(dohmh['BBL'])
nta=pd.read_csv(path+'retail/restaurant/nta.csv',dtype=str)
dohmh=pd.merge(dohmh,nta,how='left',on='NTA')
mappluto=gpd.read_file(path+'retail/restaurant/mappluto20v3.shp')
mappluto.crs={'init':'epsg:4326'}
dohmh=pd.merge(dohmh,mappluto,how='left',on='BBL')
dohmh=gpd.GeoDataFrame(dohmh,geometry=[shapely.geometry.Point(x,y) for x,y in zip(dohmh['Longitude'],dohmh['Latitude'])],crs={'init' :'epsg:4326'})
dohmh.to_file(path+'retail/restaurant/dohmh.shp')
dohmh=dohmh.drop('geometry',axis=1)
dohmh.to_csv(path+'retail/restaurant/dohmh.csv',index=False)





# Open Restaurant
g=Geosupport()
openrest=pd.read_csv(path+'retail/restaurant/NYC_Open_Restaurants_Application_v2_6_0.csv',dtype=str)
openrest['id']=openrest['ObjectID'].copy()
openrest['name']=openrest['Restaurant Name'].copy()
openrest['address']=openrest['bizAddress'].copy()
openrest['boro']=openrest['Borough'].copy()
openrest['zipcode']=openrest['ZIP Code'].copy()
openrest['interested']=openrest['I am a ground-floor restaurant operator interested in adding outdoor seating to:'].copy()
openrest['qlsidewalk']=openrest['qualify_sidewalk'].copy()
openrest['qlroadway']=openrest['qualify_roadway'].copy()
openrest['alcohol']=openrest['Does your establishment serve or plan to serve alcohol in the outdoor seating area(s)?'].copy()
openrest['landmark']=openrest['Are you in a Landmark District or in an Individual Landmark building?'].copy()
openrest['subtime']=openrest['submissionTime'].copy()
openrest=openrest[['id','name','address','boro','zipcode','interested','qlsidewalk','qlroadway','alcohol','landmark','subtime','x','y']].reset_index(drop=True)
openrest['lat']=np.nan
openrest['long']=np.nan
openrest['bbl']=np.nan
for i in openrest.index:
    try:
        housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(openrest.loc[i,'address']) if re.search('AddressNumber',x[1])])
        streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(openrest.loc[i,'address']) if re.search('StreetName',x[1])])
        zipcode=openrest.loc[i,'zipcode']
        addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
        openrest.loc[i,'lat']=pd.to_numeric(addr['Latitude'])
        openrest.loc[i,'long']=pd.to_numeric(addr['Longitude'])
        openrest.loc[i,'bbl']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
    except:
        print(str(i))
for i in openrest.index:
    if pd.isna(openrest.loc[i,'lat'])|pd.isna(openrest.loc[i,'long'])|pd.isna(openrest.loc[i,'bbl']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(openrest.loc[i,'bizAddress']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(openrest.loc[i,'bizAddress']) if re.search('StreetName',x[1])])
            boroughcode=np.where(openrest.loc[i,'boro']=='Manhattan',1,np.where(openrest.loc[i,'boro']=='Bronx',2,
                        np.where(openrest.loc[i,'boro']=='Brooklyn',3,np.where(openrest.loc[i,'boro']=='Queens',4,
                        np.where(openrest.loc[i,'boro']=='Staten Island',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            openrest.loc[i,'lat']=pd.to_numeric(addr['Latitude'])
            openrest.loc[i,'long']=pd.to_numeric(addr['Longitude'])
            openrest.loc[i,'bbl']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
        except:
            print(str(i))
openrest['lat']=np.where(pd.isna(openrest['lat']),pd.to_numeric(openrest['y']),openrest['lat'])
openrest['long']=np.where(pd.isna(openrest['long']),pd.to_numeric(openrest['x']),openrest['long'])
mappluto=gpd.read_file(path+'retail/restaurant/mappluto20v3.shp')
mappluto.crs={'init':'epsg:4326'}
mappluto['bbl']=mappluto['BBL'].copy()
mappluto['lotfront']=mappluto['LotFront'].copy()
mappluto['bldgfront']=mappluto['BldgFront'].copy()
mappluto=mappluto[['bbl','lotfront','bldgfront']].reset_index(drop=True)
openrest=pd.merge(openrest,mappluto,how='left',on='bbl')
openrest=gpd.GeoDataFrame(openrest,geometry=[shapely.geometry.Point(x,y) for x,y in zip(openrest['long'],openrest['lat'])],crs={'init' :'epsg:4326'})
nta=gpd.read_file(path+'retail/ntaclippedadj.shp')
nta.crs={'init':'epsg:4326'}
nta['ntacode']=nta['NTACode'].copy()
nta['ntaname']=nta['NTAName'].copy()
nta=nta[['ntacode','ntaname','geometry']].reset_index(drop=True)
openrest=gpd.sjoin(openrest,nta,how='left',op='intersects')
openrest=openrest.drop(['index_right'],axis=1)
openrest.to_file(path+'retail/restaurant/openrest1.shp')

