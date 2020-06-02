import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport



pd.set_option('display.max_columns', None)
path1='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/'
path2='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/ONSTPARKING/'



lion=gpd.read_file(path2+'LION/LION.shp')
lion=lion.to_crs({'init':'epsg:4326'})
lion=lion[['SegmentID','NodeIDFrom','NodeIDTo','geometry']].drop_duplicates('SegmentID',keep='first').reset_index(drop=True)
lion['SegmentID']=pd.to_numeric(lion['SegmentID'])
lion['NodeIDFrom']=pd.to_numeric(lion['NodeIDFrom'])
lion['NodeIDTo']=pd.to_numeric(lion['NodeIDTo'])



g = Geosupport()

openstreet=pd.read_csv(path1+'openstreet/open street 0507.csv')
openstreet['boro']=np.where(openstreet['Borough']=='MN',1,np.where(openstreet['Borough']=='BX',2,np.where(openstreet['Borough']=='BK',3,np.where(openstreet['Borough']=='QN',4,np.where(openstreet['Borough']=='SI',5,0)))))
df=[]
for i in openstreet.index:
    borocode=str(openstreet.loc[i,'boro'])
    onstreet=str(openstreet.loc[i,'Open Street'])
    fromstreet=str(openstreet.loc[i,'From'])
    tostreet=str(openstreet.loc[i,'To'])
    try:
        strech=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet})
        tp=pd.DataFrame(strech['LIST OF INTERSECTIONS'])
        tp['fromnode']=pd.to_numeric(tp['Node Number'])
        tp['tonode']=np.roll(tp['fromnode'],-1)
        tp=tp[:-1].reset_index(drop=True)
        tp1=tp[['fromnode','tonode']].reset_index(drop=True)
        tp2=tp1.copy()
        tp2['fromnode']=tp1['tonode'].copy()
        tp2['tonode']=tp1['fromnode'].copy()
        tp=pd.concat([tp1,tp2],axis=0,ignore_index=True)
        segment=pd.concat([openstreet.loc[[i]]]*len(tp),axis=0,ignore_index=True)
        segment['fromnode']=tp['fromnode'].copy()
        segment['tonode']=tp['tonode'].copy()
        segment=pd.merge(segment,lion,how='left',left_on=['fromnode','tonode'],right_on=['NodeIDFrom','NodeIDTo'])
        segment=segment[pd.notna(segment['geometry'])].reset_index(drop=True)
        df+=[segment]
    except:
        print(openstreet.loc[[i]])
df=pd.concat(df,axis=0,ignore_index=True)
df.to_csv(path1+'openstreet/openstreet.csv',index=False)
df=gpd.GeoDataFrame(df,geometry=df['geometry'],crs={'init':'epsg:4326'})
df=df.dissolve(by=['Borough','Open Street','From','To','Type','Park/Area/Partner']).reset_index(drop=False)
df=df[['Borough','Open Street','From','To','Type','Park/Area/Partner','geometry']].reset_index(drop=True)
df.to_file(path1+'openstreet/openstreet.shp')











# Street Typology
opst=pd.read_csv(path1+'openstreet/open street 0601.csv')
opst=opst.drop_duplicates(keep='first').reset_index(drop=True)
opst=opst.drop(['Length In Miles','Shape Length'],axis=1)
opst=opst[opst['Type']!='Protected Bike Lane'].reset_index(drop=True)
sdwkwdimp=gpd.read_file(path1+'sidewalk/output/sdwkwdimp.shp')
sdwkwdimp.crs={'init':'epsg:4326'}
lion=gpd.read_file(path1+'sidewalk/input/lion/lion.shp')
lion.crs={'init':'epsg:4326'}
lionsp=lion[['SegmentID','TrafDir','Number_Tra','Number_Par','BIKE_TRAFD','BikeLane','StreetWidt','StreetWi_1','LBlockFace','RBlockFace','geometry']].reset_index(drop=True)
lionsp['segmentid']=pd.to_numeric(lionsp['SegmentID'])
lionsp=lionsp[pd.notna(lionsp['segmentid'])].reset_index(drop=True)
lionsp['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['TrafDir']]
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
lionsp['lbkfaceid']=pd.to_numeric(lionsp['LBlockFace'])
lionsp['rbkfaceid']=pd.to_numeric(lionsp['RBlockFace'])
lionsp['geometry']=[str(x) for x in lionsp['geometry']]
lionsp=lionsp[['segmentid','trafficdir','travellane','parkinglane','bikedir','bikelane','stwidth','lbkfaceid','rbkfaceid','geometry']].reset_index(drop=True)
lionsp=lionsp.drop_duplicates(keep='first').reset_index(drop=True)
lionsp=lionsp.drop_duplicates(['segmentid'],keep='first').reset_index(drop=True)
opst=pd.merge(opst,lionsp,how='left',left_on='SegmentID',right_on='segmentid')
opst['geometry']=np.where(pd.isna(opst['geometry']),opst['the_geom'],opst['geometry'])
opst=gpd.GeoDataFrame(opst,geometry=opst['geometry'].map(wkt.loads),crs={'init':'epsg:4326'})
opst=opst.to_crs({'init':'epsg:6539'})
opst['length']=[x.length for x in opst['geometry']]
opst=opst.to_crs({'init':'epsg:4326'})
bfsdwkwdimp=sdwkwdimp.copy()
bfsdwkwdimp['swlength']=bfsdwkwdimp['impswmedia']*bfsdwkwdimp['length']
bfsdwkwdimp=bfsdwkwdimp.groupby('bkfaceid',as_index=False).agg({'swlength':'sum','length':'sum'})
bfsdwkwdimp['impsw']=bfsdwkwdimp['swlength']/bfsdwkwdimp['length']
bfsdwkwdimp=bfsdwkwdimp[['bkfaceid','impsw']].reset_index(drop=True)
opst=pd.merge(opst,bfsdwkwdimp,how='left',left_on='lbkfaceid',right_on='bkfaceid')
opst=pd.merge(opst,bfsdwkwdimp,how='left',left_on='rbkfaceid',right_on='bkfaceid')
opst['limpsw']=opst['impsw_x'].fillna(0)
opst['rimpsw']=opst['impsw_y'].fillna(0)
opst['exsqft']=(opst['limpsw']+opst['rimpsw'])*opst['length']
opst['exppl']=opst['exsqft']/3.14/3/3
opst['addsqft']=(opst['stwidth']-opst['parkinglane']*6.5-np.where(opst['bikelane']==1,10,np.where(opst['bikelane']==2,5,0)))*opst['length']
opst['addppl']=opst['addsqft']/3.14/3/3
opst['opstsqft']=opst['exsqft']+opst['addsqft']
opst['opstppl']=opst['exppl']+opst['addppl']
opst=opst.drop(['the_geom','bkfaceid_x','impsw_x','bkfaceid_y','impsw_y'],axis=1)
opst.to_file(path1+'openstreet/opst.shp')












