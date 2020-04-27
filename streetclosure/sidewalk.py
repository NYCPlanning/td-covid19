import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
from shapely import wkt
import datetime



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'
#path='/home/mayijun/sidewalk/'




# Data Source
# Planimetrics: https://github.com/CityOfNewYork/nyc-planimetrics/blob/master/Capture_Rules.md#transport-structure
# CityBench: https://data.cityofnewyork.us/Transportation/City-Bench-Locations/8d5p-rji6
# WalkNYC: https://data.cityofnewyork.us/Transportation/WalkNYC-Sign-Locations/q49j-2bun
# Meter: https://data.cityofnewyork.us/Transportation/Parking-Meters-GPS-Coordinates-and-Status/5jsj-cq4s
# Bus Shelter: https://data.cityofnewyork.us/Transportation/Bus-Stop-Shelters/qafz-7myz
# LinkNYC: https://data.cityofnewyork.us/Social-Services/LinkNYC-Locations-Shapefile/7b32-6xny
# Pay Phone: https://data.cityofnewyork.us/Social-Services/Public-Pay-Telephone-Locations-Map/sq67-3hcy
# News Stand: https://data.cityofnewyork.us/Transportation/News-Stands/kfum-nzw3
# Tree: https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/pi5s-9p35
# Hydrant: https://data.cityofnewyork.us/Environment/Hydrants-of-the-City-of-New-York/6pui-xhxz
# Litter Basket: https://data.cityofnewyork.us/dataset/DSNY-Litter-Basket-Inventory/uhim-nea2
# Recycle: https://data.cityofnewyork.us/Environment/Public-Recycling-Bins/sxx4-xhzg



# Simplify Pavement Edge
pvmtedge=gpd.read_file(path+'input/planimetrics/pvmtedge.shp')
pvmtedge.crs={'init':'epsg:4326'}
pvmtedge['bkfaceid']=pd.to_numeric(pvmtedge['BLOCKFACEI'])
pvmtsp=pvmtedge.loc[pd.notna(pvmtedge['bkfaceid'])&(pvmtedge['FEATURE_CO']==2260),['bkfaceid','geometry']].reset_index(drop=True)
pvmtsp=pvmtsp.drop_duplicates('bkfaceid',keep='first').reset_index(drop=True)
pvmtsp.to_file(path+'output/pvmtsp.shp')

# WalkNYC
walknyc=gpd.read_file(path+'input/impediments/walknyc.shp')
walknyc.crs={'init':'epsg:4326'}
walknyc=walknyc.to_crs({'init':'epsg:6539'})
walknyc=walknyc[[x in ['Installed','Sign Held'] for x in walknyc['status']]].reset_index(drop=True)
walknyc=walknyc[[x not in ['Wall Mount','Fingerpost'] for x in walknyc['status']]].reset_index(drop=True)
walknyc['id']=range(0,len(walknyc))
walknycbuffer=walknyc.copy()
walknycbuffer['geometry']=walknycbuffer.buffer(50)
pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
pvmtsp.crs={'init':'epsg:4326'}
pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
walknycbuffer=gpd.sjoin(walknycbuffer,pvmtsp,how='inner',op='intersects')

walknycadj=pd.DataFrame()
for i in walknyc['id']:
    walknyctp=pd.concat([walknyc.loc[walknyc['id']==i]]*2,ignore_index=True)
    walknycpv=pvmtsp[np.isin(pvmtsp['bkfaceid'],walknycbuffer.loc[walknycbuffer['id']==i,'bkfaceid'])].reset_index(drop=True)
    if len(walknycpv)>0:
        try:
            walknycpv=walknycpv.loc[[np.argmin([walknyctp.loc[0,'geometry'].distance(x) for x in walknycpv['geometry']])]].reset_index(drop=True)
            walknyctp['bkfaceid']=walknycpv.loc[0,'bkfaceid']
            walknyctp['snapdist']=walknyctp.loc[0,'geometry'].distance(walknycpv.loc[0,'geometry'])
            adjgeom=shapely.ops.nearest_points(walknyctp.loc[0,'geometry'],walknycpv.loc[0,'geometry'])[1]
            intplt=walknycpv.loc[0,'geometry'].project(adjgeom)
            splitter=shapely.geometry.MultiPoint([walknycpv.loc[0,'geometry'].interpolate(x) for x in [intplt-0.5,intplt+0.5]])
            splitseg=shapely.ops.split(walknycpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
            walknyctp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(5)]).convex_hull.wkt
            walknyctp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-5)]).convex_hull.wkt
            walknycadj=pd.concat([walknycadj,walknyctp],ignore_index=True)
            insidewalk
            area~4
        except:
            print(str(i)+'error!')
    else:
        print(str(i)+' no bkfaceid joined!')

walknycadj=walknycadj[walknycadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
walknycadj=gpd.GeoDataFrame(walknycadj,geometry=walknycadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})

walknycadj=walknycadj[[not x.is_empty for x in walknycadj['adjgeom']]].reset_index(drop=True)
walknycadj=gpd.GeoDataFrame(walknycadj,geometry=walknycadj['adjgeom'],crs={'init':'epsg:6539'})

walknycadj.to_file(path+'output/walknycadj.shp')




    
    
    

k=gpd.GeoDataFrame()
k['id']=[0,1]
k['geometry']=shapely.ops.nearest_points(pv.loc[0,'geometry'],tree.loc[0,'geometry'])[0]
itplt=pv.loc[0,'geometry'].project(k.loc[0,'geometry'])
splitter=shapely.geometry.MultiPoint([pv.loc[0,'geometry'].interpolate(x) for x in [itplt-2.5,itplt+2.5]])
k['geometry']=shapely.ops.split(pv.loc[0,'geometry'],splitter.buffer(0.000001))[2]
k['geometry']=[shapely.geometry.MultiLineString([k.loc[0,'geometry'],k.loc[0,'geometry'].parallel_offset(5)]).convex_hull,
               shapely.geometry.MultiLineString([k.loc[0,'geometry'],k.loc[0,'geometry'].parallel_offset(-5)]).convex_hull]
k.to_file(path+'k.shp')



























# Find sidewalk width
start=datetime.datetime.now()
pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
pvmtsp.crs={'init':'epsg:4326'}
pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
sidewalk=gpd.read_file(path+'input/sidewalk.shp')
sidewalk.crs={'init':'epsg:4326'}
sidewalk=sidewalk.to_crs({'init':'epsg:6539'})
sdwkpvmt=gpd.sjoin(sidewalk,pvmtsp,how='left',op='intersects')
sdwkpvmt=sdwkpvmt.loc[pd.notna(sdwkpvmt['bkfaceid']),['FID','bkfaceid','geometry']].reset_index(drop=True)
sw=pd.DataFrame()
swtm=pd.DataFrame()
for i in pvmtsp.index:
    try:
        tp=pvmtsp.loc[[i]].reset_index(drop=True)
        tp=pd.concat([tp]*14,axis=0,ignore_index=True)
        tp['side']=['L']*7+['R']*7
        tp['sdwkwidth']=np.nan
        sd=sdwkpvmt[sdwkpvmt['bkfaceid']==tp.loc[0,'bkfaceid']].reset_index(drop=True)
        splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(x,normalized=True) for x in [0.2,0.3,0.4,0.5,0.6,0.7,0.8]])
        tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(1e-8))
        tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(50,'left').boundary[1]])
        tp.loc[1,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(50,'left').boundary[1]])
        tp.loc[2,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(50,'left').boundary[1]])
        tp.loc[3,'geometry']=shapely.geometry.LineString([splitter[3],tpsplit[6].parallel_offset(50,'left').boundary[1]])
        tp.loc[4,'geometry']=shapely.geometry.LineString([splitter[4],tpsplit[8].parallel_offset(50,'left').boundary[1]])
        tp.loc[5,'geometry']=shapely.geometry.LineString([splitter[5],tpsplit[10].parallel_offset(50,'left').boundary[1]])        
        tp.loc[6,'geometry']=shapely.geometry.LineString([splitter[6],tpsplit[12].parallel_offset(50,'left').boundary[1]])        
        tp.loc[7,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(50,'right').boundary[0]])
        tp.loc[8,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(50,'right').boundary[0]])
        tp.loc[9,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(50,'right').boundary[0]])
        tp.loc[10,'geometry']=shapely.geometry.LineString([splitter[3],tpsplit[6].parallel_offset(50,'right').boundary[0]])
        tp.loc[11,'geometry']=shapely.geometry.LineString([splitter[4],tpsplit[8].parallel_offset(50,'right').boundary[0]])
        tp.loc[12,'geometry']=shapely.geometry.LineString([splitter[5],tpsplit[10].parallel_offset(50,'right').boundary[0]])        
        tp.loc[13,'geometry']=shapely.geometry.LineString([splitter[6],tpsplit[12].parallel_offset(50,'right').boundary[0]])             
        for j in tp.index:
            sdwkwidth=[x for x in [tp.loc[j,'geometry'].intersection(x) for x in sd.geometry] if x.length!=0]
            if len(sdwkwidth)==1:
                tp.loc[j,'geometry']=sdwkwidth[0]
                tp.loc[j,'sdwkwidth']=sdwkwidth[0].length
            elif len(sdwkwidth)==0:
                tp.loc[j,'sdwkwidth']=0
            elif len(sdwkwidth)>1:
                print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
        tp['geometry']=np.where(tp['sdwkwidth']<0.1,'',tp['geometry'])
        tp['sdwkwidth']=np.where(tp['sdwkwidth']<0.1,np.nan,tp['sdwkwidth'])
        tptm=tp.loc[pd.notna(tp['sdwkwidth']),['bkfaceid','sdwkwidth','geometry']].reset_index(drop=True)
        swtm=pd.concat([swtm,tptm],axis=0,ignore_index=True)
        tp=tp.groupby(['bkfaceid','side'],as_index=False).agg({'sdwkwidth':['min','max','median']}).reset_index(drop=True)
        tp.columns=['bkfaceid','side','swmin','swmax','swmedian']
        tp=tp[pd.notna(tp['swmedian'])].reset_index(drop=True)
        if len(tp)==1:
            tp=tp[['bkfaceid','swmin','swmax','swmedian']].reset_index(drop=True)
            sw=pd.concat([sw,tp],axis=0,ignore_index=True)
        else:
            print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
    except:
        print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
swtm=swtm.to_crs({'init':'epsg:4326'})
swtm.to_file(path+'output/swtm.shp')
sw=pd.merge(pvmtsp,sw,how='inner',on='bkfaceid')
sw['length']=[x.length for x in sw['geometry']]
sw=sw[['bkfaceid','swmin','swmax','swmedian','length','geometry']].reset_index(drop=True)
sw=sw.to_crs({'init':'epsg:4326'})
sw.to_file(path+'output/sw.shp')
print(datetime.datetime.now()-start)
#480 mins






# Tract
start=datetime.datetime.now()
nycct=gpd.read_file(path+'input/nycct.shp')
nycct.crs={'init':'epsg:4326'}
nycctclipped=gpd.read_file(path+'input/nycctclipped.shp')
nycctclipped.crs={'init':'epsg:4326'}
sw=gpd.read_file(path+'output/sw.shp')
sw.crs={'init':'epsg:4326'}
sidewalk=gpd.read_file(path+'input/sidewalk.shp')
sidewalk.crs={'init':'epsg:4326'}
tracttonta=pd.read_csv(path+'input/tracttonta.csv',dtype=str)
swct=pd.merge(nycctclipped,tracttonta,how='inner',left_on='tractid',right_on='tract')
swct=swct.loc[[str(x) not in ['BX99','BK99','MN99','QN99','SI99','QN98'] for x in swct['nta']],['tractid','geometry']].reset_index(drop=True)
swct=swct.to_crs({'init':'epsg:6539'})
swct['area']=[x.area for x in swct['geometry']]
swct=swct[['tractid','area']].reset_index(drop=True)
swctmedian=gpd.sjoin(sw,nycct,how='inner',op='intersects')
swctmedian['swlength']=swctmedian['swmedian']*swctmedian['length']
swctmedian=swctmedian.groupby('tractid',as_index=False).agg({'swlength':'sum','length':'sum'}).reset_index(drop=True)
swctmedian['swmdn']=swctmedian['swlength']/swctmedian['length']
swctmedian['swmdnrk']=10-pd.qcut(swctmedian['swmdn'],10,labels=False)
swctmedian=swctmedian[['tractid','swmdn','swmdnrk']].reset_index(drop=True)
swct=pd.merge(swct,swctmedian,how='inner',on='tractid')
swctarea=gpd.overlay(sidewalk,nycct,how='intersection')
swctarea.crs={'init':'epsg:4326'}
swctarea=swctarea.to_crs({'init':'epsg:6539'})
swctarea['swarea']=[x.area for x in swctarea['geometry']]
swctarea=swctarea.groupby('tractid',as_index=False).agg({'swarea':'sum'}).reset_index(drop=True)
swct=pd.merge(swct,swctarea,how='inner',on='tractid')
swct['swareaarea']=swct['swarea']/swct['area']
swct['areaareark']=10-pd.qcut(swct['swareaarea'],10,labels=False)
ctpop=pd.read_csv(path+'input/tractpop2018.csv',dtype=str,converters={'pop':float})
swct=pd.merge(swct,ctpop,how='inner',on='tractid')
swct['poparea']=swct['pop']/swct['area']
swct['poprk']=pd.qcut(swct['poparea'],10,labels=False)+1
swct['swareapop']=swct['swarea']/swct['pop']
swct['areapoprk']=10-pd.qcut(swct['swareapop'],10,labels=False)
swct=pd.merge(nycctclipped,swct,how='inner',on='tractid')
swct.to_file(path+'output/swct.shp')
print(datetime.datetime.now()-start)

















# Backup for future use

## Generate SegmentID Order
##sg=sgod[sgod['physicalid']==175067].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==3].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==5152].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==183].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==11676].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==187692].reset_index(drop=True)
##sg=sgod[sgod['physicalid']==17700].reset_index(drop=True)
#def segmentorder(sg):
#    sg=sg.reset_index(drop=True)
#    sgph=sg.loc[0,'physicalid']
#    nd=pd.concat([sg['nodeidfrom'],sg['nodeidto']],axis=0,ignore_index=True)
#    nd=list(nd.value_counts()[nd.value_counts()==1].index)
#    sgtp=sg[np.isin(sg['nodeidfrom'],nd)].reset_index(drop=True)
#    sgtp.columns=['physicalid1','segmentid1','nodeidfrom1','nodeidto1']
#    if len(sgtp)==1:
#        sgrs=sg[sg['segmentid']!=sgtp.loc[0,'segmentid1']].reset_index(drop=True)
#        for i in range(0,len(sgrs)):
#            sgtp=pd.merge(sgtp,sgrs,how='inner',left_on='nodeidto'+str(i+1),right_on='nodeidfrom')
#            sgtp=sgtp.rename(columns={'physicalid':'physicalid'+str(i+2),'segmentid':'segmentid'+str(i+2),'nodeidfrom':'nodeidfrom'+str(i+2),'nodeidto':'nodeidto'+str(i+2)})
#        if len(sgtp)==1:
#            sgtp=pd.wide_to_long(sgtp,stubnames=['segmentid','nodeidfrom','nodeidto'],i='physicalid1',j='segorder').reset_index(drop=False)
#            sgtp=sgtp[['physicalid1','segmentid','segorder']].reset_index(drop=True)
#            sgtp=sgtp.rename(columns={'physicalid1':'physicalid'})
#            return sgtp
#        else:
#            print(str(sgph)+' merge error!')
#    else:
#        print(str(sgph)+' nodeidfrom error!')
#
## Simplify LION
#start=datetime.datetime.now()
#lion=gpd.read_file(path+'input/lion.shp')
#lion.crs={'init':'epsg:4326'}
#lionsp=lion[['PhysicalID','SegmentID','NodeIDFrom','NodeIDTo','RB_Layer','FeatureTyp','SegmentTyp','NonPed','TrafDir','RW_TYPE','StreetWidt','geometry']].reset_index(drop=True)
#lionsp['physicalid']=pd.to_numeric(lionsp['PhysicalID'])
#lionsp=lionsp[pd.notna(lionsp['physicalid'])].reset_index(drop=True)
#lionsp['segmentid']=pd.to_numeric(lionsp['SegmentID'])
#lionsp=lionsp[pd.notna(lionsp['segmentid'])].reset_index(drop=True)
#lionsp['nodeidfrom']=pd.to_numeric(lionsp['NodeIDFrom'])
#lionsp=lionsp[pd.notna(lionsp['nodeidfrom'])].reset_index(drop=True)
#lionsp['nodeidto']=pd.to_numeric(lionsp['NodeIDTo'])
#lionsp=lionsp[pd.notna(lionsp['nodeidfrom'])].reset_index(drop=True)
#lionsp['rblayer']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['RB_Layer']]
#lionsp=lionsp[np.isin(lionsp['rblayer'],['B','R'])].reset_index(drop=True)
#lionsp['featuretype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['FeatureTyp']]
#lionsp=lionsp[np.isin(lionsp['featuretype'],['0','6','A','C'])].reset_index(drop=True)
#lionsp['segmenttype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['SegmentTyp']]
#lionsp=lionsp[np.isin(lionsp['segmenttype'],['B','R','U','S'])].reset_index(drop=True)
#lionsp['nonped']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['NonPed']]
#lionsp=lionsp[np.isin(lionsp['nonped'],['','D'])].reset_index(drop=True)
#lionsp['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['TrafDir']]
#lionsp=lionsp[np.isin(lionsp['trafficdir'],['T','W','A'])].reset_index(drop=True)
#lionsp['rwtype']=pd.to_numeric(lionsp['RW_TYPE'])
#lionsp=lionsp[np.isin(lionsp['rwtype'],[1])].reset_index(drop=True)
#lionsp['stwidth']=pd.to_numeric(lionsp['StreetWidt'])
#lionsp=lionsp[['physicalid','segmentid','nodeidfrom','nodeidto','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth','geometry']].reset_index(drop=True)
#lionsp=lionsp.drop_duplicates(['physicalid','segmentid','nodeidfrom','nodeidto','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth'],keep='first').reset_index(drop=True)
#sgod=lionsp[['physicalid','segmentid','nodeidfrom','nodeidto']].reset_index(drop=True)
#sgod=sgod.groupby('physicalid',as_index=False).apply(segmentorder).reset_index(drop=True)
#sgod['segorder']=pd.to_numeric(sgod['segorder'])
#lionsp=pd.merge(lionsp,sgod,how='inner',on=['physicalid','segmentid'])
#lionsp=lionsp.sort_values(['physicalid','segorder']).reset_index(drop=True)
#lionsp['geom1']=[str(x).replace('LINESTRING (','').replace(')','') for x in lionsp['geometry']]
#lionsp['geom2']=[', '.join(str(x).replace('LINESTRING (','').replace(')','').split(', ')[1:]) for x in lionsp['geometry']]
#lionsp['geom']=np.where(lionsp['segorder']==1,lionsp['geom1'],lionsp['geom2'])
#lionsp=lionsp.groupby('physicalid',as_index=False).agg({'stwidth':'mean','geom':lambda x:', '.join(x)}).reset_index(drop=True)
#lionsp['geom']=['LINESTRING ('+str(x)+')' for x in lionsp['geom']]
#lionsp=gpd.GeoDataFrame(lionsp,geometry=lionsp['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#lionsp=lionsp.drop('geom',axis=1)
#lionsp.to_file(path+'output/lionsp.shp')
#print(datetime.datetime.now()-start)
#
## Find sidewalk width
#start=datetime.datetime.now()
#sidewalk=gpd.read_file(path+'input/sidewalk.shp')
#sidewalk.crs={'init':'epsg:4326'}
#sidewalk=sidewalk.to_crs({'init':'epsg:6539'})
#lionsp=gpd.read_file(path+'output/lionsp.shp')
#lionsp.crs={'init':'epsg:4326'}
#lionsp=lionsp.to_crs({'init':'epsg:6539'})
#lionsidewalk=lionsp.copy()
#lionsidewalk['geometry']=lionsidewalk['geometry'].buffer(200)
#lionsidewalk=gpd.sjoin(sidewalk,lionsidewalk,how='inner',op='intersects')
#df=pd.DataFrame()
#for i in lionsp.index:
#    try:
#        tp=lionsp.loc[[i]].reset_index(drop=True)
#        tp=pd.concat([tp]*6,axis=0,ignore_index=True)
#        tp['side']=['L']*3+['R']*3
#        tp['sidewalk']=np.nan
#        sd=lionsidewalk[lionsidewalk['physicalid']==tp.loc[0,'physicalid']].reset_index(drop=True)
#        splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(x,normalized=True) for x in [0.25,0.5,0.75]])
#        tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(1e-8))
#        tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
#        tp.loc[1,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
#        tp.loc[2,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
#        tp.loc[3,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
#        tp.loc[4,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
#        tp.loc[5,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
#        for j in tp.index:
#            sdwidth=[x for x in [tp.loc[j,'geometry'].intersection(x).length for x in sd.geometry] if x!=0]
#            if len(sdwidth)==1:
#                tp.loc[j,'sidewalk']=sdwidth[0]
#            else:
#                print(str(lionsp.loc[i,'physicalid'])+' error!')
#        tp=tp.groupby(['physicalid','side'],as_index=False).agg({'sidewalk':['min','max']}).reset_index(drop=True)
#        tp.columns=['physicalid','side','min','max']
#        tp=tp.pivot(index='physicalid',columns='side').reset_index(drop=False)
#        tp.columns=['physicalid','lmin','rmin','lmax','rmax']
#        tp=tp[['physicalid','lmin','lmax','rmin','rmax']].reset_index(drop=True)
#        df=pd.concat([df,tp],axis=0,ignore_index=True)
#    except:
#        print(str(lionsp.loc[i,'physicalid'])+' error!')
#df=pd.merge(lionsp,df,how='inner',on='physicalid')
#df=df.to_crs({'init':'epsg:4326'})
#df.to_file(path+'output/df.shp')
#print(datetime.datetime.now()-start)
## 120 mins
#
## Validation
#df=gpd.read_file(path+'output/df.shp')
#df.crs={'init':'epsg:4326'}
#df['ldiff']=df['lmax']-df['lmin']
#df['rdiff']=df['rmax']-df['rmin']
#df['lmaxdiff']=df['stwidth']/2+50-df['lmax']
#df['rmaxdiff']=df['stwidth']/2+50-df['rmax']
#df.to_file(path+'output/dfdiff.shp')