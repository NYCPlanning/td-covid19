import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/STREET CLOSURE/'



start=datetime.datetime.now()

permit=pd.read_csv(path+'mome/MOME Shooting Permits2018_2019.csv',dtype=str)
permit['start']=[datetime.datetime.strptime(x,'%m/%d/%Y %H:%M') for x in permit['Start Date']]
permit['end']=[datetime.datetime.strptime(x,'%m/%d/%Y %H:%M') for x in permit['End Date']]
permit['duration']=permit['end']-permit['start']
permit['eid']=pd.to_numeric(permit['ID'])
permit['location']=permit['Cross streets and intersections'].copy()
permit['boro']=np.where(permit['Boro']=='Manhattan',1,np.where(permit['Boro']=='Bronx',2,
                np.where(permit['Boro']=='Brooklyn',3,np.where(permit['Boro']=='Queens',4,
                np.where(permit['Boro']=='Staten Island',5,0)))))
permit=permit[['eid','location','boro']].reset_index(drop=True)


permitclean=[]
for i in permit.index:
    tp=permit.loc[[i]].reset_index(drop=True)
    tp=pd.concat([tp]*len(tp.loc[0,'location'].split(', ')),axis=0,ignore_index=True)
    tp['locsplit']=tp.loc[0,'location'].split(', ')
    tp['onstreet']=''
    tp['fromstreet']=''
    tp['tostreet']=''
    for j in tp.index:
        if tp.loc[j,'locsplit'].find(' between ')!=-1:
            try:
                tp.loc[j,'onstreet']=tp.loc[j,'locsplit'].split(' between ')[0].strip().upper()
                tp.loc[j,'fromstreet']=tp.loc[j,'locsplit'].split(' between ')[1].split(' and ')[0].strip().upper()
                tp.loc[j,'tostreet']=tp.loc[j,'locsplit'].split(' between ')[1].split(' and ')[1].strip().upper()
            except:
                print(tp.loc[j,'locsplit'])
    permitclean+=[tp]
permitclean=pd.concat(permitclean,axis=0,ignore_index=True)
permitclean=permitclean[permitclean['onstreet']!=''].reset_index(drop=True)
permitclean.to_csv(path+'mome/permitclean.csv',index=False)


g=Geosupport()
permitclean=pd.read_csv(path+'mome/permitclean.csv',dtype=str,converters={'eid':float})
snd=pd.read_csv(path+'mome/SND.csv',dtype=float,converters={'streetname':str})
permitgeocode=[]
for i in permitclean.index:
    borocode=str(permitclean.loc[i,'boro'])
    onstreet=str(permitclean.loc[i,'onstreet'])
    fromstreet=str(permitclean.loc[i,'fromstreet'])
    tostreet=str(permitclean.loc[i,'tostreet'])
    try:
        stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'E'})
        stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
        stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
        stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
        stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
        segment=pd.concat([permitclean.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
        segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
        permitgeocode+=[segment]
    except:
        try:
            stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'S'})
            stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
            stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
            stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
            stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
            segment=pd.concat([permitclean.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
            segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
            permitgeocode+=[segment]
        except:
            try:
                stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'W'})
                stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
                stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
                stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
                stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
                segment=pd.concat([permitclean.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
                segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
                permitgeocode+=[segment]
            except:
                try:
                    stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'N'})
                    stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
                    stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
                    stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
                    stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
                    segment=pd.concat([permitclean.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
                    segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
                    permitgeocode+=[segment]
                except:
                    print(str(i))
permitgeocode=pd.concat(permitgeocode,axis=0,ignore_index=True)
permitgeocode=permitgeocode.drop_duplicates(keep='first').reset_index(drop=True)
permitgeocode.to_csv(path+'mome/permitgeocode.csv',index=False)


permitgeocode=pd.read_csv(path+'mome/permitgeocode.csv',dtype=str,converters={'eid':float,'boro':float,
                                                                               'segfromnode':float,'segtonode':float})
permitgeocoderev=permitgeocode.copy()
permitgeocoderev['temp']=permitgeocoderev['segfromnode'].copy()
permitgeocoderev['segfromnode']=permitgeocoderev['segtonode'].copy()
permitgeocoderev['segtonode']=permitgeocoderev['temp'].copy()
permitgeocoderev=permitgeocoderev.drop('temp',axis=1).reset_index(drop=True)
permitgeocode=pd.concat([permitgeocode,permitgeocoderev],axis=0,ignore_index=True)
lion=gpd.read_file(path+'sidewalk/input/lion/lion.shp')
lion.crs={'init':'epsg:4326'}
lion['segmentid']=pd.to_numeric(lion['SegmentID'],errors='coerce')
lion['segfromnode']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
lion['segtonode']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
lion=lion[['segmentid','segfromnode','segtonode','geometry']].reset_index(drop=True)
lion=lion.drop_duplicates(['segmentid','segfromnode','segtonode'],keep='first').reset_index(drop=True)
momegeocode=pd.merge(lion,permitgeocode,how='inner',on=['segfromnode','segtonode'])
momegeocode=momegeocode.sort_values('segmentid').reset_index(drop=True)
momegeocode=momegeocode.drop_duplicates(['eid','locsplit','segfromnode','segtonode'],keep='first').reset_index(drop=True)
momegeocode=momegeocode[['segmentid','eid']].drop_duplicates(keep='first').reset_index(drop=True)
momegeocode['eidconcat']=[str(int(x)) for x in momegeocode['eid']]
momegeocode=momegeocode.groupby('segmentid',as_index=False).agg({'eid':'count','eidconcat':lambda x:'/'.join(x)}).reset_index(drop=True)
momegeocode.columns=['segmentid','count','eid']
lion=lion.drop_duplicates(['segmentid'],keep='first').reset_index(drop=True)
momegeocode=pd.merge(lion,momegeocode,how='inner',on='segmentid')
momegeocode=momegeocode[['segmentid','count','eid','geometry']].reset_index(drop=True)
momegeocode.to_file(path+'mome/momegeocode.shp')

print(datetime.datetime.now()-start)





























# Backup


# permit=pd.read_csv(path+'mome/MOME Shooting Permits2018_2019.csv',dtype=str)
# permit['eid']=pd.to_numeric(permit['ID'])
# permit['location']=permit['Cross streets and intersections'].copy()
# permit=permit[['eid','location']].reset_index(drop=True)


# shp2018=pd.read_csv(path+'mome/MOME Shape 2018.csv',dtype=str)
# eid2018=shp2018[['EventId']].reset_index(drop=True)
# eid2018['eid']=pd.to_numeric(eid2018['EventId'])
# eid2018=eid2018.loc[pd.notna(eid2018['eid']),['eid']].reset_index(drop=True)
# geom2018=shp2018[['Spatial Data']].reset_index(drop=True)
# geom2018['flag1']=[len(x)==32759 for x in geom2018['Spatial Data']]
# geom2018['geom']=geom2018['Spatial Data'].copy()
# geom2018['flag2']=True
# for i in geom2018.index:
#     if geom2018.loc[i,'flag1']==True:
#         geom2018.loc[i,'geom']=geom2018.loc[i,'geom']+geom2018.loc[i+1,'geom']
#         geom2018.loc[i+1,'flag2']=False
# geom2018=geom2018.loc[geom2018['flag2']==True,['geom']].reset_index(drop=True)
# shp2018=pd.concat([eid2018,geom2018],axis=1,ignore_index=False)


# shp2019=pd.read_csv(path+'mome/MOME Shape 2019.csv',dtype=str)
# eid2019=shp2019[['EventId']].reset_index(drop=True)
# eid2019['eid']=pd.to_numeric(eid2019['EventId'])
# eid2019=eid2019.loc[pd.notna(eid2019['eid']),['eid']].reset_index(drop=True)
# geom2019=shp2019[['Spatial Data']].reset_index(drop=True)
# geom2019['flag1']=[len(x)==32759 for x in geom2019['Spatial Data']]
# geom2019['geom']=geom2019['Spatial Data'].copy()
# geom2019['flag2']=True
# for i in geom2019.index:
#     if geom2019.loc[i,'flag1']==True:
#         geom2019.loc[i,'geom']=geom2019.loc[i,'geom']+geom2019.loc[i+1,'geom']
#         geom2019.loc[i+1,'flag2']=False
# geom2019=geom2019.loc[geom2019['flag2']==True,['geom']].reset_index(drop=True)
# shp2019=pd.concat([eid2019,geom2019],axis=1,ignore_index=False)


# shp=pd.concat([shp2018,shp2019],axis=0,ignore_index=True)
# mome=pd.merge(permit,shp,how='inner',on='eid')
# l=[]
# for i in mome.index:
#     try:
#         wkt.loads(mome.loc[i,'geom'])
#     except:
#         l+=[i]
# mome=mome.loc[[x for x in mome.index if x not in l]].reset_index(drop=True)
# mome=gpd.GeoDataFrame(mome,geometry=mome['geom'].map(wkt.loads),crs={'init':'epsg:6539'})
# mome=mome.drop('geom',axis=1).reset_index(drop=True)
# mome=mome.explode().reset_index(drop=True)
# mome=mome.explode().reset_index(drop=True)
# set([type(x) for x in mome['geometry']])
# mome=mome[[type(x) in [shapely.geometry.linestring.LineString] for x in mome['geometry']]].reset_index(drop=True)
# mome.to_file(path+'mome/mome.shp')


# momesplit=gpd.read_file(path+'mome/mome.shp')
# momesplit.crs={'init':'epsg:6539'}
# momesplit['geom']=''
# for i in momesplit.index:
#     momesplit.loc[i,'geom']=shapely.ops.split(momesplit.loc[i,'geometry'],
#                                               shapely.geometry.MultiPoint(momesplit.loc[i,'geometry'].coords)).wkt
# momesplit=gpd.GeoDataFrame(momesplit,geometry=momesplit['geom'].map(wkt.loads),crs={'init':'epsg:6539'})
# momesplit=momesplit.explode().reset_index(drop=True)
# momesplit=momesplit.drop('geom',axis=1).reset_index(drop=True)
# momesplit.to_file(path+'mome/momesplit.shp')













# start=datetime.datetime.now()
# g=Geosupport()
# permitclean=pd.read_csv(path+'mome/permitclean.csv',dtype=str,converters={'eid':float})
# snd=pd.read_csv(path+'mome/SND.csv',dtype=float,converters={'streetname':str})
# permitclean['fromnode']=np.nan
# permitclean['tonode']=np.nan
# permitgeocode=[]
# for i in permitclean.index:
#     borocode=str(permitclean.loc[i,'boro'])
#     onstreet=str(permitclean.loc[i,'onstreet'])
#     fromstreet=str(permitclean.loc[i,'fromstreet'])
#     tostreet=str(permitclean.loc[i,'tostreet'])
#     try:
#         strech=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet})
#         strech=pd.DataFrame(strech['LIST OF INTERSECTIONS'])
#         strech=strech[[x!=[] for x in strech['List of Cross Streets at this Intersection']]].reset_index(drop=True)
#         permitclean.loc[i,'fromnode']=pd.to_numeric(strech.loc[0,'Node Number'])
#         permitclean.loc[i,'tonode']=pd.to_numeric(strech.loc[len(strech)-1,'Node Number'])
#         for j in range(0,len(strech)-1):
#             fromstlist=list(snd.loc[snd['streetcode']==pd.to_numeric(strech.loc[j,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
#             tostlist=list(snd.loc[snd['streetcode']==pd.to_numeric(strech.loc[j+1,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
#             segment=pd.concat([permitclean.loc[[i]]]*len(fromstlist)*len(tostlist),axis=0,ignore_index=True)
#             segment['segfromstreet']=[x for x in fromstlist for y in tostlist]
#             segment['segtostreet']=[y for x in fromstlist for y in tostlist]
#             segment['segfromnode']=pd.to_numeric(strech.loc[j,'Node Number'])
#             segment['segtonode']=pd.to_numeric(strech.loc[j+1,'Node Number'])
#             segment['segmentid']=np.nan
#             for k in segment.index:
#                 try:
#                     segment.loc[k,'segmentid']=pd.to_numeric(g['3']({'borough_code':borocode,'on':onstreet,'from':segment.loc[k,'segfromstreet'],'to':segment.loc[k,'segtostreet']})['Segment Identifier'])
#                 except:
#                     print(str(i)+'|'+str(j)+'|'+str(k))
#             segment=segment[pd.notna(segment['segmentid'])].reset_index(drop=True)
#             segment=segment.drop(['segfromstreet','segtostreet'],axis=1).drop_duplicates(keep='first').reset_index(drop=True)
#             permitgeocode+=[segment]
#     except:
#         print(str(i))
# permitgeocode=pd.concat(permitgeocode,axis=0,ignore_index=True)
# permitgeocode=permitgeocode.drop_duplicates(keep='first').reset_index(drop=True)
# permitgeocode.to_csv(path+'mome/permitgeocode.csv',index=False)
# print(datetime.datetime.now()-start)

