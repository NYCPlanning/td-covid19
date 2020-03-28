import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport



pd.set_option('display.max_columns', None)
path1='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/STREET CLOSURE/'
path2='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/ONSTPARKING/'


snd=pd.read_csv(path2+'SND/SND.csv',dtype=float,converters={'streetname':str})

g = Geosupport()

weekendwalk=pd.read_csv(path1+'weekendwalk/2020weekendwalkslocations.csv')
weekendwalk['boro']=np.where(weekendwalk['Borough']=='Manhattan',1,np.where(weekendwalk['Borough']=='The Bronx',2,np.where(weekendwalk['Borough']=='Brooklyn',3,np.where(weekendwalk['Borough']=='Queens',4,np.where(weekendwalk['Borough']=='Staten Island',5,0)))))
weekendwalk['fromnode']=np.nan
weekendwalk['tonode']=np.nan
df=pd.DataFrame()
for i in weekendwalk.index:
    borocode=str(weekendwalk.loc[i,'boro'])
    onstreet=str(weekendwalk.loc[i,'On Street'])
    fromstreet=str(weekendwalk.loc[i,'From Street'])
    tostreet=str(weekendwalk.loc[i,'To Street'])
    try:
        strech=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet})
        tp=pd.DataFrame(strech['LIST OF INTERSECTIONS'])
        tp=tp[[x!=[] for x in tp['List of Cross Streets at this Intersection']]].reset_index(drop=True)
        weekendwalk.loc[i,'fromnode']=pd.to_numeric(tp.loc[0,'Node Number'])
        weekendwalk.loc[i,'tonode']=pd.to_numeric(tp.loc[len(tp)-1,'Node Number'])
        for j in range(0,len(tp)-1):
            fromstreet=list(snd.loc[snd['streetcode']==pd.to_numeric(tp.loc[j,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
            tostreet=list(snd.loc[snd['streetcode']==pd.to_numeric(tp.loc[j+1,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
            segment=pd.concat([weekendwalk.loc[[i]]]*len(fromstreet)*len(tostreet),ignore_index=True)
            segment['segfromstreet']=[x for x in fromstreet for y in tostreet]
            segment['segtostreet']=[y for x in fromstreet for y in tostreet]
            segment['segfromnode']=pd.to_numeric(tp.loc[j,'Node Number'])
            segment['segtonode']=pd.to_numeric(tp.loc[j+1,'Node Number'])
            segment['segmentid']=np.nan
            for k in segment.index:
                try:
                    segment.loc[k,'segmentid']=pd.to_numeric(g['3']({'borough_code':borocode,'on':onstreet,'from':segment.loc[k,'segfromstreet'],'to':segment.loc[k,'segtostreet']})['Segment Identifier'])
                except:
                    print(segment.loc[[k]])
            segment=segment[pd.notna(segment['segmentid'])].reset_index(drop=True)
            segment=segment.drop(['segfromstreet','segtostreet'],axis=1).drop_duplicates(keep='first').reset_index(drop=True)
            df=pd.concat([df,segment],axis=0)
    except:
        print(weekendwalk.loc[[i]])
df.to_csv(path1+'weekendwalk/df.csv',index=False)


df=pd.read_csv(path1+'weekendwalk/df.csv')
lion=gpd.read_file(path2+'LION/LION.shp')
lion=lion.to_crs({'init':'epsg:4326'})
lion=lion[['SegmentID','geometry']].drop_duplicates('SegmentID',keep='first').reset_index(drop=True)
lion['SegmentID']=pd.to_numeric(lion['SegmentID'])
df=pd.merge(lion,df,how='right',left_on='SegmentID',right_on='segmentid')
df=df.dissolve(by=['On Street','From Street','To Street','Borough']).reset_index(drop=False)
df=df[['On Street','From Street','To Street','Borough','geometry']].reset_index(drop=True)
df.to_file(path1+'weekendwalk/weekendwalk2.shp')

