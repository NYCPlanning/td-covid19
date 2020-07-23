import pandas as pd
import geopandas as gpd
import numpy as np

pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/STREET CLOSURE/'



lion=gpd.read_file(path+'sidewalk/input/lion/lion.shp')
# lion=gpd.read_file(path+'school/test.shp')
lion.crs={'init':'epsg:4326'}
lion=lion.to_crs({'init':'epsg:6539'})
lion['length']=[x.length for x in lion['geometry']]
lionsp=lion[['SegmentID','PhysicalID','RB_Layer','FeatureTyp','SegmentTyp','NonPed','RW_TYPE','TrafDir','TRUCK_ROUT',
             'Number_Tra','Number_Par','BIKE_TRAFD','BikeLane','StreetWidt','StreetWi_1','LBlockFace','RBlockFace',
             'length','geometry']].reset_index(drop=True)
lionsp['segmentid']=pd.to_numeric(lionsp['SegmentID'])
lionsp=lionsp[pd.notna(lionsp['segmentid'])].reset_index(drop=True)
lionsp['physicalid']=pd.to_numeric(lionsp['PhysicalID'])
lionsp=lionsp[pd.notna(lionsp['physicalid'])].reset_index(drop=True)
lionsp['rblayer']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['RB_Layer']]
lionsp=lionsp[np.isin(lionsp['rblayer'],['B','R'])].reset_index(drop=True)
lionsp['featuretype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['FeatureTyp']]
lionsp=lionsp[np.isin(lionsp['featuretype'],['0','6','A','W','C'])].reset_index(drop=True)
lionsp['segmenttype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['SegmentTyp']]
lionsp=lionsp[np.isin(lionsp['segmenttype'],['B','R','U','S'])].reset_index(drop=True)
lionsp['nonped']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['NonPed']]
lionsp=lionsp[np.isin(lionsp['nonped'],['','D'])].reset_index(drop=True)
lionsp['rwtype']=pd.to_numeric(lionsp['RW_TYPE'])
lionsp=lionsp[np.isin(lionsp['rwtype'],[1,5,6,7,8,10])].reset_index(drop=True)
lionsp['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['TrafDir']]
lionsp=lionsp[np.isin(lionsp['trafficdir'],['T','W','A','P'])].reset_index(drop=True)
lionsp['trucktype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['TRUCK_ROUT']]
lionsp=lionsp[np.isin(lionsp['trucktype'],[''])].reset_index(drop=True)
lionsp['travellane']=pd.to_numeric(lionsp['Number_Tra'])
lionsp['travellane']=lionsp['travellane'].fillna(0)
lionsp['parkinglane']=pd.to_numeric(lionsp['Number_Par'])
lionsp['parkinglane']=lionsp['parkinglane'].fillna(0)
lionsp['bikedir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['BIKE_TRAFD']]
lionsp['bikedir']=lionsp['bikedir'].fillna('')
lionsp['bikelane']=pd.to_numeric(lionsp['BikeLane'])
lionsp['bikelane']=lionsp['bikelane'].fillna(0)
lionsp['stwidth1']=pd.to_numeric(lionsp['StreetWidt'])
lionsp['stwidth2']=pd.to_numeric(lionsp['StreetWi_1'])
lionsp['stwidth']=np.where(pd.notna(lionsp['stwidth1']),lionsp['stwidth1'],lionsp['stwidth2'])
lionsp=lionsp[pd.notna(lionsp['stwidth'])].reset_index(drop=True)
lionsp['lbkfaceid']=pd.to_numeric(lionsp['LBlockFace'])
lionsp['rbkfaceid']=pd.to_numeric(lionsp['RBlockFace'])
lionsp=lionsp[['segmentid','physicalid','rblayer','featuretype','segmenttype','nonped','rwtype','trafficdir','trucktype',
               'travellane','parkinglane','bikedir','bikelane','stwidth','lbkfaceid','rbkfaceid','length','geometry']].reset_index(drop=True)
lionsp=lionsp.drop_duplicates(['segmentid'],keep='first').reset_index(drop=True)
lionspseg=lionsp[['segmentid','lbkfaceid','rbkfaceid','stwidth','geometry']].reset_index(drop=True)
bfsdwkwdimp=gpd.read_file(path+'sidewalk/output/sdwkwdimp.shp')
bfsdwkwdimp.crs={'init':'epsg:4326'}
bfsdwkwdimp['swlength']=bfsdwkwdimp['orgswmedia']*bfsdwkwdimp['length']
bfsdwkwdimp=bfsdwkwdimp.groupby('bkfaceid',as_index=False).agg({'swlength':'sum','length':'sum'})
bfsdwkwdimp['orgsw']=bfsdwkwdimp['swlength']/bfsdwkwdimp['length']
bfsdwkwdimp=bfsdwkwdimp[['bkfaceid','orgsw']].reset_index(drop=True)
lionspseg=pd.merge(lionspseg,bfsdwkwdimp,how='left',left_on='lbkfaceid',right_on='bkfaceid')
lionspseg=pd.merge(lionspseg,bfsdwkwdimp,how='left',left_on='rbkfaceid',right_on='bkfaceid')
lionspseg['orgsw_x']=lionspseg['orgsw_x'].fillna(0)
lionspseg['orgsw_y']=lionspseg['orgsw_y'].fillna(0)
lionspseg['rowwidth']=lionspseg['stwidth']+lionspseg['orgsw_x']+lionspseg['orgsw_y']
lionspseg=lionspseg[['segmentid','rowwidth']].reset_index(drop=True)
lionsp=pd.merge(lionsp,lionspseg,on='segmentid',how='left')
lionsp=lionsp.dissolve(by='physicalid',as_index=False).reset_index(drop=True)
lionsp=lionsp[['physicalid','rblayer','featuretype','segmenttype','nonped','rwtype','trafficdir','trucktype',
               'travellane','parkinglane','bikedir','bikelane','stwidth','rowwidth','geometry']].reset_index(drop=True)


bus=gpd.read_file(path+'school/bus.shp')
bus.crs={'init':'epsg:4326'}
bus=bus.to_crs({'init':'epsg:6539'})
bus['geometry']=bus.buffer(50)


df=gpd.overlay(lionsp,bus,how='intersection')
df['buflen']=[x.length for x in df['geometry']]
df=df.loc[df['buflen']>100,['physicalid','buflen']].drop_duplicates('physicalid',keep='first').reset_index(drop=True)
df=pd.merge(lionsp,df,how='left',on='physicalid')
df=df[pd.isna(df['buflen'])].reset_index(drop=True)
df=df[['physicalid','rblayer','featuretype','segmenttype','nonped','rwtype','trafficdir','trucktype',
       'travellane','parkinglane','bikedir','bikelane','stwidth','rowwidth','geometry']].reset_index(drop=True)
df=df.to_crs({'init':'epsg:4326'})
df.to_file(path+'school/nobusnotrk.shp')







sc=gpd.read_file(path+'school/school.shp')
sc.crs={'init':'epsg:4326'}
sc=sc.to_crs({'init':'epsg:6539'})
sc['geometry']=sc.buffer(500)
sc=sc.to_crs({'init':'epsg:4326'})
df=gpd.read_file(path+'school/nobusnotrk.shp')
df.crs={'init':'epsg:4326'}
df=gpd.sjoin(df,sc,how='inner',op='intersects')
df=df[df['rowwidth']<=75].reset_index(drop=True)
df=df[['physicalid','rowwidth','location_c','location_n','geometry']].reset_index(drop=True)
df.columns=['physicalid','rowwidth','schoolcode','schoolname','geometry']
df.to_file(path+'school/schoolstreet500.shp')




