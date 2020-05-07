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
