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
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/'
# path='/home/mayijun/'




# Adjust BUSSTOP to closest blockface
def adjbusstop(bs):
    global pvmtedgemdn
    global busstopbuffer
    bsbk=pvmtedgemdn[np.isin(pvmtedgemdn['bkfaceid'],busstopbuffer.loc[busstopbuffer['stopid']==bs['stopid'],'bkfaceid'])].reset_index(drop=True)
    if len(bsbk)>0:
        bs['bkfaceid']=bsbk.loc[np.argmin([bs['geometry'].distance(x) for x in bsbk['geometry']]),'bkfaceid']
        bs['snapdist']=min([bs['geometry'].distance(x) for x in bsbk['geometry']])
        bs['adjgeom']=shapely.ops.nearest_points(bs['geometry'],bsbk.loc[np.argmin([bs['geometry'].distance(x) for x in bsbk['geometry']]),'geometry'])[1].wkt
    else:
        print(str(bs['stopid'])+' no bkfaceid joined')
    return bs

# Clean BUSSTOP (2 mins)
start=datetime.datetime.now()
busstop=gpd.read_file(path+'SIDEWALK CAFE/ROADWAY CAFE/BUSSTOP.shp')
busstop.crs=4326
busstop=busstop.to_crs(6539)
busstop['stopid']=pd.to_numeric(busstop['stopid'])
busstopadj=busstop[['stopid','geometry']].drop_duplicates(['stopid'],keep='first').reset_index(drop=True)
busstopadj['orggeom']=[x.wkt for x in busstopadj['geometry']]
busstopadj['bkfaceid']=np.nan
busstopadj['snapdist']=np.nan
busstopadj['adjgeom']=''
pvmtedgemdn=gpd.read_file(path+'SIDEWALK CAFE/ROADWAY CAFE/PVMTEDGEMDN.shp')
pvmtedgemdn.crs=4326
pvmtedgemdn=pvmtedgemdn.to_crs(6539)
busstopbuffer=gpd.GeoDataFrame(busstopadj[['stopid']],geometry=busstopadj['geometry'].buffer(100),crs=6539).reset_index(drop=True)
busstopbuffer=gpd.sjoin(busstopbuffer,pvmtedgemdn,how='left',op='intersects')
busstopbuffer=busstopbuffer[['stopid','bkfaceid']].dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
busstopadj=busstopadj.apply(adjbusstop,axis=1)
busstopadj=busstopadj.drop(['geometry'],axis=1).dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
busstopadj=gpd.GeoDataFrame(busstopadj,geometry=busstopadj['adjgeom'].map(wkt.loads),crs=6539)
busstopadj=busstopadj.to_crs(4326)
busstopadj.to_file(path+'SIDEWALK CAFE/ROADWAY CAFE/BUSSTOPADJ.shp')
print(datetime.datetime.now()-start)







# Add HYDRANT and BUSSTOP to PAVEMENTEDGEMDN
# hb=pvhdtbs[pvhdtbs['bkfaceid']==1222605049]
def hydrantbusstop(hb):
    global pvmtedgemdn
    global hdtbs
    try:
        hbbk=list(hb['bkfaceid'])[0]
        hb=hb.reset_index(drop=True)
        hbgm=list(pvmtedgemdn.loc[pvmtedgemdn['bkfaceid']==hbbk,'geometry'])[0]
        hb=pd.concat([hb,hb,hb],axis=0,ignore_index=True)
        hb.loc[0,'dist1']=0
        hb.loc[0,'dist2']=20/hbgm.length
        hb.loc[0,'restrict']=1
        hb.loc[1,'dist1']=20/hbgm.length
        hb.loc[1,'dist2']=(hbgm.length-20)/hbgm.length 
        hb.loc[2,'dist1']=(hbgm.length-20)/hbgm.length
        hb.loc[2,'dist2']=1
        hb.loc[2,'restrict']=1
        hbdt=hdtbs[hdtbs['bkfaceid']==hbbk].reset_index(drop=True)
        hbdt=[hbgm.project(x,normalized=True) for x in hbdt['geometry']]
        for i in hbdt:
            hbdtmin=hb[(hb['dist1']<=i-15/hbgm.length)&(hb['dist2']>=i-15/hbgm.length)]
            hbdtmax=hb[(hb['dist1']<=i+15/hbgm.length)&(hb['dist2']>=i+15/hbgm.length)]
            if (len(hbdtmin)==0)&(len(hbdtmax)==0):
                hb['restrict']=1
            elif len(hbdtmin)==0:
                hbdtminmax=pd.concat([hb[:hbdtmax.index[0]+1],hbdtmax],axis=0,ignore_index=True)
                hbdtminmax.loc[len(hbdtminmax)-2,'dist2']=i+15/hbgm.length
                hbdtminmax.loc[len(hbdtminmax)-1,'dist1']=i+15/hbgm.length
                hbdtminmax.loc[:len(hbdtminmax)-2,'restrict']=1
                hb=hb[hbdtmax.index[0]+1:].reset_index(drop=True)
                hb=pd.concat([hb,hbdtminmax],axis=0,ignore_index=True)
                hb=hb.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
            elif len(hbdtmax)==0:
                hbdtminmax=pd.concat([hbdtmin,hb[hbdtmin.index[0]:]],axis=0,ignore_index=True)
                hbdtminmax.loc[0,'dist2']=i-15/hbgm.length
                hbdtminmax.loc[1,'dist1']=i-15/hbgm.length
                hbdtminmax.loc[1:,'restrict']=1
                hb=hb[:hbdtmin.index[0]].reset_index(drop=True)
                hb=pd.concat([hb,hbdtminmax],axis=0,ignore_index=True)
                hb=hb.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
            elif hbdtmax.index-hbdtmin.index>=0:
                hbdtminmax=pd.concat([hbdtmin,hb[hbdtmin.index[0]:hbdtmax.index[0]+1],hbdtmax],axis=0).reset_index(drop=True)
                hbdtminmax.loc[0,'dist2']=i-15/hbgm.length
                hbdtminmax.loc[1,'dist1']=i-15/hbgm.length
                hbdtminmax.loc[len(hbdtminmax)-2,'dist2']=i+15/hbgm.length
                hbdtminmax.loc[len(hbdtminmax)-1,'dist1']=i+15/hbgm.length
                hbdtminmax.loc[1:len(hbdtminmax)-2,'restrict']=1
                hb=pd.concat([hb[:hbdtmin.index[0]],hb[hbdtmax.index[0]+1:]],axis=0,ignore_index=True)
                hb=pd.concat([hb,hbdtminmax],axis=0,ignore_index=True)
                hb=hb.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
        return hb
    except:
        print(str(hbbk)+' ERROR')

# Split geometry
# sg=pvhdtbs[pvhdtbs['bkfaceid']==1222605049]    
def splitgeom(sg):
    global pvmtedgemdn
    try:
        sgbk=list(sg['bkfaceid'])[0]
        sg=sg.reset_index(drop=True)
        sggm=list(pvmtedgemdn.loc[pvmtedgemdn['bkfaceid']==sgbk,'geometry'])[0]
        splitpos=[x for x in list(sg['dist1'])+[list(sg['dist2'])[-1]]]
        splitter=shapely.geometry.MultiPoint([sggm.interpolate(x,normalized=True) for x in splitpos])
        shapesplit=shapely.ops.split(sggm,splitter.buffer(1e-8))
        shapesplit=[shapesplit[x].wkt for x in range(1,len(shapesplit),2)]
        sg['geom']=shapesplit
        return sg
    except:
        print(str(sgbk)+' ERROR')

# Add HYDRANT and BUSSTOP to PAVEMENTEDGEMDN (70 mins)
start=datetime.datetime.now()
pvmtedgemdn=gpd.read_file(path+'SIDEWALK CAFE/ROADWAY CAFE/PVMTEDGEMDN.shp')
pvmtedgemdn.crs=4326
pvmtedgemdn=pvmtedgemdn.to_crs(6539)
pvhdtbs=pvmtedgemdn.copy()
pvhdtbs['dist1']=np.nan
pvhdtbs['dist2']=np.nan
pvhdtbs['restrict']=0
hydrantadj=gpd.read_file(path+'SIDEWALK CAFE/ROADWAY CAFE/HYDRANTADJ.shp')
hydrantadj.crs=4326
hydrantadj=hydrantadj.to_crs(6539)
busstopadj=gpd.read_file(path+'SIDEWALK CAFE/ROADWAY CAFE/BUSSTOPADJ.shp')
busstopadj.crs=4326
busstopadj=busstopadj.to_crs(6539)
hdtbs=pd.concat([hydrantadj,busstopadj],axis=0,ignore_index=True)
pvhdtbs=pvhdtbs.groupby('bkfaceid',as_index=False).apply(hydrantbusstop).reset_index(drop=True)
pvmtedgemdn=pvmtedgemdn.to_crs(4326)
pvhdtbsgm=pvhdtbs.groupby('bkfaceid',as_index=False).apply(splitgeom).reset_index(drop=True)
pvhdtbsgm=pvhdtbsgm[['bkfaceid','dist1','dist2','medians','restrict','geom']].reset_index(drop=True)
pvhdtbsgm=gpd.GeoDataFrame(pvhdtbsgm,geometry=pvhdtbsgm['geom'].map(wkt.loads),crs=4326)
pvhdtbsgm=pvhdtbsgm.drop('geom',axis=1).reset_index(drop=True)
pvhdtbsgm.to_file(path+'SIDEWALK CAFE/ROADWAY CAFE/ROADWAYCAFE.shp')
print(datetime.datetime.now()-start)






