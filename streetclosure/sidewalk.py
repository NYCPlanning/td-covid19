import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
from shapely import wkt
import datetime
import multiprocessing as mp



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'
path='/home/mayijun/sidewalk/'



# Data Source
# Planimetrics: https://github.com/CityOfNewYork/nyc-planimetrics/blob/master/Capture_Rules.md
# CityBench: https://data.cityofnewyork.us/Transportation/City-Bench-Locations/8d5p-rji6
# WalkNYC: https://data.cityofnewyork.us/Transportation/WalkNYC-Sign-Locations/q49j-2bun
# Meter: https://data.cityofnewyork.us/Transportation/Parking-Meters-GPS-Coordinates-and-Status/5jsj-cq4s
# Bus Shelter: https://data.cityofnewyork.us/Transportation/Bus-Stop-Shelters/qafz-7myz
# LinkNYC: https://data.cityofnewyork.us/Social-Services/LinkNYC-Locations-Shapefile/7b32-6xny
# Pay Phone: https://data.cityofnewyork.us/Social-Services/Public-Pay-Telephone-Locations-Map/sq67-3hcy
# News Stand: https://data.cityofnewyork.us/Transportation/News-Stands/kfum-nzw3
# Hydrant: https://data.cityofnewyork.us/Environment/Hydrants-of-the-City-of-New-York/6pui-xhxz
# Litter Bin: https://data.cityofnewyork.us/dataset/DSNY-Litter-Basket-Inventory/uhim-nea2
# Recycle: https://data.cityofnewyork.us/Environment/Public-Recycling-Bins/sxx4-xhzg
# Tree: https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/pi5s-9p35







## Combine Sidewalk and Plaza
#start=datetime.datetime.now()
#sidewalk=gpd.read_file(path+'input/planimetrics/sidewalk.shp')
#sidewalk.crs={'init':'epsg:4326'}
#sidewalk=sidewalk[['geometry']].reset_index(drop=True)
#sidewalk['sid']=['s'+str(x) for x in range(0,len(sidewalk))]
#sdwk1=sidewalk.copy()
#sdwk2=sidewalk.copy()
#sdwkdis=gpd.sjoin(sdwk1,sdwk2,how='inner',op='intersects')
#sdwkdis=sdwkdis.groupby('sid_left',as_index=False).agg({'sid_right':'count'}).reset_index(drop=True)
#sdwkdis.columns=['sid','count']
#sdwkdis=pd.merge(sidewalk,sdwkdis,how='inner',on='sid')
#sdwkuni=sdwkdis.loc[sdwkdis['count']==1,['sid','geometry']].reset_index(drop=True)
#sdwkdis=sdwkdis.loc[sdwkdis['count']>1,['sid','geometry']].reset_index(drop=True)
#sdwkdis['id']=0
#sdwkdis=sdwkdis.dissolve(by='id').reset_index(drop=False)
#sdwkdis=sdwkdis.explode().reset_index(drop=True)[['geometry']]
##sdwkdis=gpd.GeoDataFrame(geometry=sdwkdis.explode().reset_index(drop=True),crs={'init':'epsg:4326'})
#sdwkdis=pd.concat([sdwkuni,sdwkdis],ignore_index=True)
#sdwkdis['sid']=['s'+str(x) for x in range(0,len(sdwkdis))]
#plaza=gpd.read_file(path+'input/planimetrics/plaza.shp')
#plaza.crs={'init':'epsg:4326'}
#plaza=plaza[['geometry']].reset_index(drop=True)
#plaza['pid']=['p'+str(x) for x in range(0,len(plaza))]
#sdwkonly=gpd.sjoin(sdwkdis,plaza,how='left',op='intersects')
#sdwkonly=sdwkonly.loc[pd.isna(sdwkonly['pid']),['sid','geometry']].reset_index(drop=True)
#plazaonly=gpd.sjoin(plaza,sdwkdis,how='left',op='intersects')
#plazaonly=plazaonly.loc[pd.isna(plazaonly['sid']),['pid','geometry']].reset_index(drop=True)
#sdwkplaza=gpd.sjoin(sdwkdis,plaza,how='inner',op='intersects')
#sdwkplazasdwk=pd.merge(sdwkdis,sdwkplaza[['sid']].drop_duplicates(keep='first'),how='inner',on='sid')
#sdwkplazaplaza=pd.merge(plaza,sdwkplaza[['pid']].drop_duplicates(keep='first'),how='inner',on='pid')
#sdwkplaza=pd.concat([sdwkplazasdwk,sdwkplazaplaza],ignore_index=True)
#sdwkplaza['id']=0
#sdwkplaza=sdwkplaza.dissolve(by='id').reset_index(drop=False)
#sdwkplaza=sdwkplaza.explode().reset_index(drop=True)[['geometry']]
##sdwkplaza=gpd.GeoDataFrame(geometry=sdwkplaza.explode().reset_index(drop=True),crs={'init':'epsg:4326'})
#sdwkplaza=pd.concat([sdwkplaza,sdwkonly[['geometry']],plazaonly[['geometry']]],ignore_index=True)
#sdwkplaza['spid']=range(0,len(sdwkplaza))
#sdwkplaza.to_file(path+'output/sdwkplaza.shp')
#print(datetime.datetime.now()-start)
## 20 mins



## Simplify Pavement Edge
#start=datetime.datetime.now()
#pvmtedge=gpd.read_file(path+'input/planimetrics/pvmtedge.shp')
#pvmtedge.crs={'init':'epsg:4326'}
#pvmtedge['bkfaceid']=pd.to_numeric(pvmtedge['BLOCKFACEI'])
#pvmtedge=pvmtedge.loc[pd.notna(pvmtedge['bkfaceid'])&(pvmtedge['FEATURE_CO']==2260),['bkfaceid','geometry']].reset_index(drop=True)
#pvmtedge=pvmtedge.drop_duplicates('bkfaceid',keep='first').reset_index(drop=True)
#pvmtedge=pvmtedge[[type(x)==shapely.geometry.linestring.LineString for x in pvmtedge['geometry']]].reset_index(drop=True)
#pvmtedge['geometry']=[shapely.geometry.LineString(list(zip(x.xy[0],x.xy[1]))) for x in pvmtedge['geometry']]
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza['geometry']=[shapely.geometry.LineString(list(x.exterior.coords)) for x in sdwkplaza['geometry']]
#pvmtspsdwk=gpd.sjoin(pvmtedge,sdwkplaza,how='inner',op='intersects')
#pvmtspsdwk=pvmtspsdwk[['bkfaceid','spid']].reset_index(drop=True)
#def pvmtsimplify(ps):
#    global pvmtedge
#    global sdwkplaza
#    global pvmtspsdwk
#    ps=ps.reset_index(drop=True)
#    tp=sdwkplaza[np.isin(sdwkplaza['spid'],pvmtspsdwk.loc[pvmtspsdwk['bkfaceid']==ps.loc[0,'bkfaceid'],'spid'])].reset_index(drop=True)
#    tp['geometry']=[ps.loc[0,'geometry'].intersection(x) for x in tp['geometry']]
#    tp=tp[[type(x)==shapely.geometry.multilinestring.MultiLineString for x in tp['geometry']]].reset_index(drop=True)
#    if len(tp)>0:
#        df=pd.concat([ps]*len(tp),ignore_index=True)
#        df['spid']=tp['spid']
#        df['geometry']=[shapely.ops.linemerge(x) for x in tp['geometry']]
#        dfsingle=df[[type(x)==shapely.geometry.linestring.LineString for x in df['geometry']]].reset_index(drop=True)
#        dfmulti=df[[type(x)==shapely.geometry.multilinestring.MultiLineString for x in df['geometry']]].reset_index(drop=True)
#        df=[]
#        df+=[dfsingle]
#        for j in dfmulti.index:
#            tpmulti=pd.concat([dfmulti.loc[[j]]]*len(dfmulti.loc[j,'geometry']),ignore_index=True)
#            tpmulti['geometry']=[x for x in tpmulti.loc[0,'geometry']]
#            df+=[tpmulti]
#        df=pd.concat(df,ignore_index=True)
#        return df
#    else:
#        print(str(ps.loc[0,'bkfaceid'])+' error!')
#
#def pvmtsimplifycompile(pscp):
#    pvmtsptp=pscp.groupby('bkfaceid',as_index=False).apply(pvmtsimplify)
#    return pvmtsptp
#
#def parallelize(data,func):
#    data_split=np.array_split(data,mp.cpu_count()-1)
#    pool=mp.Pool(mp.cpu_count()-1)
#    dt=pool.map(func,data_split)
#    dt=pd.concat(dt,axis=0,ignore_index=True)
#    pool.close()
#    pool.join()
#    return dt
#
#if __name__=='__main__':
#    pvmtsp=parallelize(pvmtedge,pvmtsimplifycompile)
#    pvmtsp['pvid']=range(0,len(pvmtsp))
#    pvmtsp=pvmtsp[['pvid','bkfaceid','spid','geometry']].reset_index(drop=True)
#    pvmtsp.to_file(path+'output/pvmtsp.shp')
#    print(datetime.datetime.now()-start)
#    # 20 mins



## Utility Strip
#start=datetime.datetime.now()
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#
#def utilitystrip(us):
#    global pvmtsp
#    global sdwkplaza
#    us=us.reset_index(drop=True)
#    try:
#        sd=sdwkplaza[sdwkplaza['spid']==us.loc[0,'spid']].reset_index(drop=True)
#        rightgeom=us.loc[0,'geometry'].parallel_offset(2,'right')
#        if type(rightgeom)==shapely.geometry.linestring.LineString:
#            rightgeom=rightgeom.intersection(sd.loc[0,'geometry']).length
#        elif type(rightgeom)==shapely.geometry.multilinestring.MultiLineString:
#            rightgeom=[x.intersection(sd.loc[0,'geometry']) for x in rightgeom]
#            rightgeom=max([x.length for x in rightgeom])
#        else:
#            print(str(us.loc[0,'pvid'])+' rightgeom error!')
#        leftgeom=us.loc[0,'geometry'].parallel_offset(2,'left')
#        if type(leftgeom)==shapely.geometry.linestring.LineString:
#            leftgeom=leftgeom.intersection(sd.loc[0,'geometry']).length
#        elif type(leftgeom)==shapely.geometry.multilinestring.MultiLineString:
#            leftgeom=[x.intersection(sd.loc[0,'geometry']) for x in leftgeom]
#            leftgeom=max([x.length for x in leftgeom])
#        else:
#            print(str(us.loc[0,'pvid'])+' leftgeom error!')
#        if rightgeom>leftgeom:
#            offgeom=us.loc[0,'geometry'].parallel_offset(2,'right')
#            if type(offgeom)==shapely.geometry.linestring.LineString:
#                geom=list(us.loc[0,'geometry'].coords)
#                geom+=list(offgeom.coords)
#                geom+=list(us.loc[0,'geometry'].boundary[0].coords)
#            elif type(offgeom)==shapely.geometry.multilinestring.MultiLineString:
#                geom=list(us.loc[0,'geometry'].coords)
#                geom+=list(offgeom[np.argmax([x.length for x in offgeom])].coords)
#                geom+=list(us.loc[0,'geometry'].boundary[0].coords)
#            else:
#                print(str(us.loc[0,'pvid'])+' offgeom type error!') 
#        elif rightgeom<leftgeom:
#            offgeom=us.loc[0,'geometry'].parallel_offset(2,'left')
#            if type(offgeom)==shapely.geometry.linestring.LineString:
#                geom=list(us.loc[0,'geometry'].coords)
#                geom+=list(offgeom.coords)[::-1]
#                geom+=list(us.loc[0,'geometry'].boundary[0].coords)
#            elif type(offgeom)==shapely.geometry.multilinestring.MultiLineString:
#                geom=list(us.loc[0,'geometry'].coords)
#                geom+=list(offgeom[np.argmax([x.length for x in offgeom])].coords)[::-1]
#                geom+=list(us.loc[0,'geometry'].boundary[0].coords)            
#            else:
#                print(str(us.loc[0,'pvid'])+' offgeom type error!') 
#        else:
#            print(str(us.loc[0,'pvid'])+' rightgeom=leftgeom error!')
#        geom=shapely.geometry.Polygon(geom)
#        us.loc[0,'geometry']=geom
#        return us
#    except:
#        print(str(us.loc[0,'pvid'])+' error!')
#
#def utilitystripcompile(uscp):
#    utistriptp=uscp.groupby('pvid',as_index=False).apply(utilitystrip)
#    return utistriptp
#
#def parallelize(data,func):
#    data_split=np.array_split(data,mp.cpu_count()-1)
#    pool=mp.Pool(mp.cpu_count()-1)
#    dt=pool.map(func,data_split)
#    dt=pd.concat(dt,axis=0,ignore_index=True)
#    pool.close()
#    pool.join()
#    return dt
#
#if __name__=='__main__':
#    utistrip=parallelize(pvmtsp,utilitystripcompile)
#    utistrip=utistrip[[type(x)==shapely.geometry.polygon.Polygon for x in utistrip['geometry']]].reset_index(drop=True)
#    utistrip=utistrip.to_crs({'init':'epsg:4326'})
#    utistrip['usid']=range(0,len(utistrip))
#    utistrip.to_file(path+'output/utistrip.shp')
#    print(datetime.datetime.now()-start)
#    # 25 mins



## CityBench
#start=datetime.datetime.now()
#citybench=gpd.read_file(path+'input/impediments/citybench.shp')
#citybench.crs={'init':'epsg:4326'}
#citybench=citybench.to_crs({'init':'epsg:6539'})
#citybench['cbid']=range(0,len(citybench))
#citybenchbuffer=citybench.copy()
#citybenchbuffer['geometry']=citybenchbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#citybenchbuffer=gpd.sjoin(citybenchbuffer,pvmtsp,how='inner',op='intersects')
#citybenchadj=[]
#for i in citybench['cbid']:
#    citybenchtp=pd.concat([citybench.loc[citybench['cbid']==i]]*2,ignore_index=True)
#    citybenchpv=pvmtsp[np.isin(pvmtsp['pvid'],citybenchbuffer.loc[citybenchbuffer['cbid']==i,'pvid'])].reset_index(drop=True)
#    if len(citybenchpv)>0:
#        try:
#            citybenchpv=citybenchpv.loc[[np.argmin([citybenchtp.loc[0,'geometry'].distance(x) for x in citybenchpv['geometry']])]].reset_index(drop=True)
#            citybenchtp['pvid']=citybenchpv.loc[0,'pvid']
#            citybenchtp['snapdist']=citybenchtp.loc[0,'geometry'].distance(citybenchpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(citybenchtp.loc[0,'geometry'],citybenchpv.loc[0,'geometry'])[1]
#            intplt=citybenchpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([citybenchpv.loc[0,'geometry'].interpolate(x) for x in [intplt-4,intplt+4]])
#            splitseg=shapely.ops.split(citybenchpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            citybenchtp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(3)]).convex_hull.wkt
#            citybenchtp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-3)]).convex_hull.wkt
#            citybenchadj+=[citybenchtp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#citybenchadj=pd.concat(citybenchadj,ignore_index=True)
#citybenchadj=citybenchadj[citybenchadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#citybenchadj=citybenchadj.drop('geometry',axis=1)
#citybenchadj=gpd.GeoDataFrame(citybenchadj,geometry=citybenchadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#citybenchadj['area']=[x.area for x in citybenchadj['geometry']]
#citybenchadj=citybenchadj[(citybenchadj['area']>=14)&(citybenchadj['area']<=18)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#citybenchadj=gpd.sjoin(citybenchadj,sdwkplaza,how='inner',op='within')
#citybenchadj=citybenchadj.drop(['index_right'],axis=1)
#citybenchadj=citybenchadj.to_crs({'init':'epsg:4326'})
#citybenchadj.to_file(path+'output/citybenchadj.shp')
#print(datetime.datetime.now()-start)
## 3 mins



## WalkNYC
#start=datetime.datetime.now()
#walknyc=gpd.read_file(path+'input/impediments/walknyc.shp')
#walknyc.crs={'init':'epsg:4326'}
#walknyc=walknyc.to_crs({'init':'epsg:6539'})
#walknyc=walknyc[[x in ['Installed','Sign Held'] for x in walknyc['status']]].reset_index(drop=True)
#walknyc=walknyc[[x not in ['Wall Mount','Fingerpost'] for x in walknyc['status']]].reset_index(drop=True)
#walknyc['wnid']=range(0,len(walknyc))
#walknycbuffer=walknyc.copy()
#walknycbuffer['geometry']=walknycbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#walknycbuffer=gpd.sjoin(walknycbuffer,pvmtsp,how='inner',op='intersects')
#walknycadj=[]
#for i in walknyc['wnid']:
#    walknyctp=pd.concat([walknyc.loc[walknyc['wnid']==i]]*2,ignore_index=True)
#    walknycpv=pvmtsp[np.isin(pvmtsp['pvid'],walknycbuffer.loc[walknycbuffer['wnid']==i,'pvid'])].reset_index(drop=True)
#    if len(walknycpv)>0:
#        try:
#            walknycpv=walknycpv.loc[[np.argmin([walknyctp.loc[0,'geometry'].distance(x) for x in walknycpv['geometry']])]].reset_index(drop=True)
#            walknyctp['pvid']=walknycpv.loc[0,'pvid']
#            walknyctp['snapdist']=walknyctp.loc[0,'geometry'].distance(walknycpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(walknyctp.loc[0,'geometry'],walknycpv.loc[0,'geometry'])[1]
#            intplt=walknycpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([walknycpv.loc[0,'geometry'].interpolate(x) for x in [intplt-0.5,intplt+0.5]])
#            splitseg=shapely.ops.split(walknycpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            walknyctp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(5)]).convex_hull.wkt
#            walknyctp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-5)]).convex_hull.wkt
#            walknycadj+=[walknyctp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#walknycadj=pd.concat(walknycadj,ignore_index=True)
#walknycadj=walknycadj[walknycadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#walknycadj=walknycadj.drop('geometry',axis=1)
#walknycadj=gpd.GeoDataFrame(walknycadj,geometry=walknycadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#walknycadj['area']=[x.area for x in walknycadj['geometry']]
#walknycadj=walknycadj[(walknycadj['area']>=3)&(walknycadj['area']<=5)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#walknycadj=gpd.sjoin(walknycadj,sdwkplaza,how='inner',op='within')
#walknycadj=walknycadj.drop(['index_right'],axis=1)
#walknycadj=walknycadj.to_crs({'init':'epsg:4326'})
#walknycadj.to_file(path+'output/walknycadj.shp')
#print(datetime.datetime.now()-start)
## 3 mins



## Meter
#start=datetime.datetime.now()
#meter=gpd.read_file(path+'input/impediments/meter.shp')
#meter.crs={'init':'epsg:4326'}
#meter=meter.to_crs({'init':'epsg:6539'})
#meter=meter[[x in ['Active'] for x in meter['status']]].reset_index(drop=True)
#meter['mtid']=range(0,len(meter))
#meterbuffer=meter.copy()
#meterbuffer['geometry']=meterbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#meterbuffer=gpd.sjoin(meterbuffer,pvmtsp,how='inner',op='intersects')
#meteradj=[]
#for i in meter['mtid']:
#    metertp=pd.concat([meter.loc[meter['mtid']==i]]*2,ignore_index=True)
#    meterpv=pvmtsp[np.isin(pvmtsp['pvid'],meterbuffer.loc[meterbuffer['mtid']==i,'pvid'])].reset_index(drop=True)
#    if len(meterpv)>0:
#        try:
#            meterpv=meterpv.loc[[np.argmin([metertp.loc[0,'geometry'].distance(x) for x in meterpv['geometry']])]].reset_index(drop=True)
#            metertp['pvid']=meterpv.loc[0,'pvid']
#            metertp['snapdist']=metertp.loc[0,'geometry'].distance(meterpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(metertp.loc[0,'geometry'],meterpv.loc[0,'geometry'])[1]
#            intplt=meterpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([meterpv.loc[0,'geometry'].interpolate(x) for x in [intplt-0.5,intplt+0.5]])
#            splitseg=shapely.ops.split(meterpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            metertp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(2)]).convex_hull.wkt
#            metertp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-2)]).convex_hull.wkt
#            meteradj+=[metertp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#meteradj=pd.concat(meteradj,ignore_index=True)
#meteradj=meteradj[meteradj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#meteradj=meteradj.drop('geometry',axis=1)
#meteradj=gpd.GeoDataFrame(meteradj,geometry=meteradj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#meteradj['area']=[x.area for x in meteradj['geometry']]
#meteradj=meteradj[(meteradj['area']>=0.8)&(meteradj['area']<=1.2)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#meteradj=gpd.sjoin(meteradj,sdwkplaza,how='inner',op='within')
#meteradj=meteradj.drop(['index_right'],axis=1)
#meteradj=meteradj.to_crs({'init':'epsg:4326'})
#meteradj.to_file(path+'output/meteradj.shp')
#print(datetime.datetime.now()-start)
## 10 mins



## Bus Shelter
#start=datetime.datetime.now()
#busshelter=gpd.read_file(path+'input/impediments/busshelter.shp')
#busshelter.crs={'init':'epsg:4326'}
#busshelter=busshelter.to_crs({'init':'epsg:6539'})
#busshelter['bsid']=range(0,len(busshelter))
#busshelterbuffer=busshelter.copy()
#busshelterbuffer['geometry']=busshelterbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#busshelterbuffer=gpd.sjoin(busshelterbuffer,pvmtsp,how='inner',op='intersects')
#busshelteradj=[]
#for i in busshelter['bsid']:
#    bussheltertp=pd.concat([busshelter.loc[busshelter['bsid']==i]]*2,ignore_index=True)
#    busshelterpv=pvmtsp[np.isin(pvmtsp['pvid'],busshelterbuffer.loc[busshelterbuffer['bsid']==i,'pvid'])].reset_index(drop=True)
#    if len(busshelterpv)>0:
#        try:
#            busshelterpv=busshelterpv.loc[[np.argmin([bussheltertp.loc[0,'geometry'].distance(x) for x in busshelterpv['geometry']])]].reset_index(drop=True)
#            bussheltertp['pvid']=busshelterpv.loc[0,'pvid']
#            bussheltertp['snapdist']=bussheltertp.loc[0,'geometry'].distance(busshelterpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(bussheltertp.loc[0,'geometry'],busshelterpv.loc[0,'geometry'])[1]
#            intplt=busshelterpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([busshelterpv.loc[0,'geometry'].interpolate(x) for x in [intplt-7,intplt+7]])
#            splitseg=shapely.ops.split(busshelterpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            bussheltertp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1.5),splitseg.parallel_offset(6.5)]).convex_hull.wkt
#            bussheltertp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1.5),splitseg.parallel_offset(-6.5)]).convex_hull.wkt
#            busshelteradj+=[bussheltertp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#busshelteradj=pd.concat(busshelteradj,ignore_index=True)
#busshelteradj=busshelteradj[busshelteradj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#busshelteradj=busshelteradj.drop('geometry',axis=1)
#busshelteradj=gpd.GeoDataFrame(busshelteradj,geometry=busshelteradj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#busshelteradj['area']=[x.area for x in busshelteradj['geometry']]
#busshelteradj=busshelteradj[(busshelteradj['area']>=60)&(busshelteradj['area']<=80)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#busshelteradj=gpd.sjoin(busshelteradj,sdwkplaza,how='inner',op='within')
#busshelteradj=busshelteradj.drop(['index_right'],axis=1)
#busshelteradj=busshelteradj.to_crs({'init':'epsg:4326'})
#busshelteradj.to_file(path+'output/busshelteradj.shp')
#print(datetime.datetime.now()-start)
## 5 mins



## LinkNYC
#start=datetime.datetime.now()
#linknyc=gpd.read_file(path+'input/impediments/linknyc.shp')
#linknyc.crs={'init':'epsg:4326'}
#linknyc=linknyc.to_crs({'init':'epsg:6539'})
#linknyc['lnid']=range(0,len(linknyc))
#linknycbuffer=linknyc.copy()
#linknycbuffer['geometry']=linknycbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#linknycbuffer=gpd.sjoin(linknycbuffer,pvmtsp,how='inner',op='intersects')
#linknycadj=[]
#for i in linknyc['lnid']:
#    linknyctp=pd.concat([linknyc.loc[linknyc['lnid']==i]]*2,ignore_index=True)
#    linknycpv=pvmtsp[np.isin(pvmtsp['pvid'],linknycbuffer.loc[linknycbuffer['lnid']==i,'pvid'])].reset_index(drop=True)
#    if len(linknycpv)>0:
#        try:
#            linknycpv=linknycpv.loc[[np.argmin([linknyctp.loc[0,'geometry'].distance(x) for x in linknycpv['geometry']])]].reset_index(drop=True)
#            linknyctp['pvid']=linknycpv.loc[0,'pvid']
#            linknyctp['snapdist']=linknyctp.loc[0,'geometry'].distance(linknycpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(linknyctp.loc[0,'geometry'],linknycpv.loc[0,'geometry'])[1]
#            intplt=linknycpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([linknycpv.loc[0,'geometry'].interpolate(x) for x in [intplt-0.5,intplt+0.5]])
#            splitseg=shapely.ops.split(linknycpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            linknyctp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(5)]).convex_hull.wkt
#            linknyctp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-5)]).convex_hull.wkt
#            linknycadj+=[linknyctp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#linknycadj=pd.concat(linknycadj,ignore_index=True)
#linknycadj=linknycadj[linknycadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#linknycadj=linknycadj.drop('geometry',axis=1)
#linknycadj=gpd.GeoDataFrame(linknycadj,geometry=linknycadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#linknycadj['area']=[x.area for x in linknycadj['geometry']]
#linknycadj=linknycadj[(linknycadj['area']>=3)&(linknycadj['area']<=5)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#linknycadj=gpd.sjoin(linknycadj,sdwkplaza,how='inner',op='within')
#linknycadj=linknycadj.drop(['index_right'],axis=1)
#linknycadj=linknycadj.to_crs({'init':'epsg:4326'})
#linknycadj.to_file(path+'output/linknycadj.shp')
#print(datetime.datetime.now()-start)
## 4 mins



## Pay Phone
#start=datetime.datetime.now()
#payphone=gpd.read_file(path+'input/impediments/payphone.shp')
#payphone.crs={'init':'epsg:4326'}
#payphone=payphone.to_crs({'init':'epsg:6539'})
#payphone=payphone[[x in ['Live','Repair Pending'] for x in payphone['status']]].reset_index(drop=True)
#payphone['ppid']=range(0,len(payphone))
#payphonebuffer=payphone.copy()
#payphonebuffer['geometry']=payphonebuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#payphonebuffer=gpd.sjoin(payphonebuffer,pvmtsp,how='inner',op='intersects')
#payphoneadj=[]
#for i in payphone['ppid']:
#    payphonetp=pd.concat([payphone.loc[payphone['ppid']==i]]*2,ignore_index=True)
#    payphonepv=pvmtsp[np.isin(pvmtsp['pvid'],payphonebuffer.loc[payphonebuffer['ppid']==i,'pvid'])].reset_index(drop=True)
#    if len(payphonepv)>0:
#        try:
#            payphonepv=payphonepv.loc[[np.argmin([payphonetp.loc[0,'geometry'].distance(x) for x in payphonepv['geometry']])]].reset_index(drop=True)
#            payphonetp['pvid']=payphonepv.loc[0,'pvid']
#            payphonetp['snapdist']=payphonetp.loc[0,'geometry'].distance(payphonepv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(payphonetp.loc[0,'geometry'],payphonepv.loc[0,'geometry'])[1]
#            intplt=payphonepv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([payphonepv.loc[0,'geometry'].interpolate(x) for x in [intplt-2,intplt+2]])
#            splitseg=shapely.ops.split(payphonepv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            payphonetp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(5)]).convex_hull.wkt
#            payphonetp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-5)]).convex_hull.wkt
#            payphoneadj+=[payphonetp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#payphoneadj=pd.concat(payphoneadj,ignore_index=True)
#payphoneadj=payphoneadj[payphoneadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#payphoneadj=payphoneadj.drop('geometry',axis=1)
#payphoneadj=gpd.GeoDataFrame(payphoneadj,geometry=payphoneadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#payphoneadj['area']=[x.area for x in payphoneadj['geometry']]
#payphoneadj=payphoneadj[(payphoneadj['area']>=14)&(payphoneadj['area']<=18)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#payphoneadj=gpd.sjoin(payphoneadj,sdwkplaza,how='inner',op='within')
#payphoneadj=payphoneadj.drop(['index_right'],axis=1)
#payphoneadj=payphoneadj.to_crs({'init':'epsg:4326'})
#payphoneadj.to_file(path+'output/payphoneadj.shp')
#print(datetime.datetime.now()-start)
## 6 mins



## News Stand
#start=datetime.datetime.now()
#newsstand=gpd.read_file(path+'input/impediments/newsstand.shp')
#newsstand.crs={'init':'epsg:4326'}
#newsstand=newsstand.to_crs({'init':'epsg:6539'})
#newsstand['nsid']=range(0,len(newsstand))
#newsstandbuffer=newsstand.copy()
#newsstandbuffer['geometry']=newsstandbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#newsstandbuffer=gpd.sjoin(newsstandbuffer,pvmtsp,how='inner',op='intersects')
#newsstandadj=[]
#for i in newsstand['nsid']:
#    newsstandtp=pd.concat([newsstand.loc[newsstand['nsid']==i]]*2,ignore_index=True)
#    newsstandpv=pvmtsp[np.isin(pvmtsp['pvid'],newsstandbuffer.loc[newsstandbuffer['nsid']==i,'pvid'])].reset_index(drop=True)
#    if len(newsstandpv)>0:
#        try:
#            newsstandpv=newsstandpv.loc[[np.argmin([newsstandtp.loc[0,'geometry'].distance(x) for x in newsstandpv['geometry']])]].reset_index(drop=True)
#            newsstandtp['pvid']=newsstandpv.loc[0,'pvid']
#            newsstandtp['snapdist']=newsstandtp.loc[0,'geometry'].distance(newsstandpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(newsstandtp.loc[0,'geometry'],newsstandpv.loc[0,'geometry'])[1]
#            intplt=newsstandpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([newsstandpv.loc[0,'geometry'].interpolate(x) for x in [intplt-5,intplt+5]])
#            splitseg=shapely.ops.split(newsstandpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            newsstandtp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(6)]).convex_hull.wkt
#            newsstandtp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-6)]).convex_hull.wkt
#            newsstandadj+=[newsstandtp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#newsstandadj=pd.concat(newsstandadj,ignore_index=True)
#newsstandadj=newsstandadj[newsstandadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#newsstandadj=newsstandadj.drop('geometry',axis=1)
#newsstandadj=gpd.GeoDataFrame(newsstandadj,geometry=newsstandadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#newsstandadj['area']=[x.area for x in newsstandadj['geometry']]
#newsstandadj=newsstandadj[(newsstandadj['area']>=40)&(newsstandadj['area']<=60)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#newsstandadj=gpd.sjoin(newsstandadj,sdwkplaza,how='inner',op='within')
#newsstandadj=newsstandadj.drop(['index_right'],axis=1)
#newsstandadj=newsstandadj.to_crs({'init':'epsg:4326'})
#newsstandadj.to_file(path+'output/newsstandadj.shp')
#print(datetime.datetime.now()-start)
## 3 min



## Hydrant
#start=datetime.datetime.now()
#hydrant=gpd.read_file(path+'input/impediments/hydrant.shp')
#hydrant.crs={'init':'epsg:4326'}
#hydrant=hydrant.to_crs({'init':'epsg:6539'})
#hydrant['hdid']=range(0,len(hydrant))
#hydrantbuffer=hydrant.copy()
#hydrantbuffer['geometry']=hydrantbuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#hydrantbuffer=gpd.sjoin(hydrantbuffer,pvmtsp,how='inner',op='intersects')
#hydrantadj=[]
#for i in hydrant['hdid']:
#    hydranttp=pd.concat([hydrant.loc[hydrant['hdid']==i]]*2,ignore_index=True)
#    hydrantpv=pvmtsp[np.isin(pvmtsp['pvid'],hydrantbuffer.loc[hydrantbuffer['hdid']==i,'pvid'])].reset_index(drop=True)
#    if len(hydrantpv)>0:
#        try:
#            hydrantpv=hydrantpv.loc[[np.argmin([hydranttp.loc[0,'geometry'].distance(x) for x in hydrantpv['geometry']])]].reset_index(drop=True)
#            hydranttp['pvid']=hydrantpv.loc[0,'pvid']
#            hydranttp['snapdist']=hydranttp.loc[0,'geometry'].distance(hydrantpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(hydranttp.loc[0,'geometry'],hydrantpv.loc[0,'geometry'])[1]
#            intplt=hydrantpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([hydrantpv.loc[0,'geometry'].interpolate(x) for x in [intplt-0.75,intplt+0.75]])
#            splitseg=shapely.ops.split(hydrantpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            hydranttp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(2.5)]).convex_hull.wkt
#            hydranttp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-2.5)]).convex_hull.wkt
#            hydrantadj+=[hydranttp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#hydrantadj=pd.concat(hydrantadj,ignore_index=True)
#hydrantadj=hydrantadj[hydrantadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#hydrantadj=hydrantadj.drop('geometry',axis=1)
#hydrantadj=gpd.GeoDataFrame(hydrantadj,geometry=hydrantadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#hydrantadj['area']=[x.area for x in hydrantadj['geometry']]
#hydrantadj=hydrantadj[(hydrantadj['area']>=2)&(hydrantadj['area']<=2.5)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#hydrantadj=gpd.sjoin(hydrantadj,sdwkplaza,how='inner',op='within')
#hydrantadj=hydrantadj.drop(['index_right'],axis=1)
#hydrantadj=hydrantadj.to_crs({'init':'epsg:4326'})
#hydrantadj.to_file(path+'output/hydrantadj.shp')
#print(datetime.datetime.now()-start)
## 80 mins



## Litter Bin
#start=datetime.datetime.now()
#litterbin=gpd.read_file(path+'input/impediments/litterbin.shp')
#litterbin.crs={'init':'epsg:4326'}
#litterbin=litterbin.to_crs({'init':'epsg:6539'})
#litterbin['lbid']=range(0,len(litterbin))
#litterbinbuffer=litterbin.copy()
#litterbinbuffer['geometry']=litterbinbuffer.buffer(100)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#litterbinbuffer=gpd.sjoin(litterbinbuffer,pvmtsp,how='inner',op='intersects')
#litterbinadj=[]
#for i in litterbin['lbid']:
#    litterbintp=pd.concat([litterbin.loc[litterbin['lbid']==i]]*2,ignore_index=True)
#    litterbinpv=pvmtsp[np.isin(pvmtsp['pvid'],litterbinbuffer.loc[litterbinbuffer['lbid']==i,'pvid'])].reset_index(drop=True)
#    if len(litterbinpv)>0:
#        try:
#            litterbinpv=litterbinpv.loc[[np.argmin([litterbintp.loc[0,'geometry'].distance(x) for x in litterbinpv['geometry']])]].reset_index(drop=True)
#            litterbintp['pvid']=litterbinpv.loc[0,'pvid']
#            litterbintp['snapdist']=litterbintp.loc[0,'geometry'].distance(litterbinpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(litterbintp.loc[0,'geometry'],litterbinpv.loc[0,'geometry'])[1]
#            intplt=litterbinpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([litterbinpv.loc[0,'geometry'].interpolate(x) for x in [intplt-1,intplt+1]])
#            splitseg=shapely.ops.split(litterbinpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            litterbintp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(3)]).convex_hull.wkt
#            litterbintp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-3)]).convex_hull.wkt
#            litterbinadj+=[litterbintp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#litterbinadj=pd.concat(litterbinadj,ignore_index=True)
#litterbinadj=litterbinadj[litterbinadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#litterbinadj=litterbinadj.drop('geometry',axis=1)
#litterbinadj=gpd.GeoDataFrame(litterbinadj,geometry=litterbinadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#litterbinadj['area']=[x.area for x in litterbinadj['geometry']]
#litterbinadj=litterbinadj[(litterbinadj['area']>=3)&(litterbinadj['area']<=5)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#litterbinadj=gpd.sjoin(litterbinadj,sdwkplaza,how='inner',op='within')
#litterbinadj=litterbinadj.drop(['index_right'],axis=1)
#litterbinadj=litterbinadj.to_crs({'init':'epsg:4326'})
#litterbinadj.to_file(path+'output/litterbinadj.shp')
#print(datetime.datetime.now()-start)
## 20 mins



## Recycle Bin
#start=datetime.datetime.now()
#recyclebin=gpd.read_file(path+'input/impediments/recyclebin.shp')
#recyclebin.crs={'init':'epsg:4326'}
#recyclebin=recyclebin.to_crs({'init':'epsg:6539'})
#recyclebin['rbid']=range(0,len(recyclebin))
#recyclebinbuffer=recyclebin.copy()
#recyclebinbuffer['geometry']=recyclebinbuffer.buffer(100)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#recyclebinbuffer=gpd.sjoin(recyclebinbuffer,pvmtsp,how='inner',op='intersects')
#recyclebinadj=[]
#for i in recyclebin['rbid']:
#    recyclebintp=pd.concat([recyclebin.loc[recyclebin['rbid']==i]]*2,ignore_index=True)
#    recyclebinpv=pvmtsp[np.isin(pvmtsp['pvid'],recyclebinbuffer.loc[recyclebinbuffer['rbid']==i,'pvid'])].reset_index(drop=True)
#    if len(recyclebinpv)>0:
#        try:
#            recyclebinpv=recyclebinpv.loc[[np.argmin([recyclebintp.loc[0,'geometry'].distance(x) for x in recyclebinpv['geometry']])]].reset_index(drop=True)
#            recyclebintp['pvid']=recyclebinpv.loc[0,'pvid']
#            recyclebintp['snapdist']=recyclebintp.loc[0,'geometry'].distance(recyclebinpv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(recyclebintp.loc[0,'geometry'],recyclebinpv.loc[0,'geometry'])[1]
#            intplt=recyclebinpv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([recyclebinpv.loc[0,'geometry'].interpolate(x) for x in [intplt-1,intplt+1]])
#            splitseg=shapely.ops.split(recyclebinpv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            recyclebintp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(3)]).convex_hull.wkt
#            recyclebintp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-3)]).convex_hull.wkt
#            recyclebinadj+=[recyclebintp]
#        except:
#            print(str(i)+' error!')
#    else:
#        print(str(i)+' no pvid joined!')
#recyclebinadj=pd.concat(recyclebinadj,ignore_index=True)
#recyclebinadj=recyclebinadj[recyclebinadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#recyclebinadj=recyclebinadj.drop('geometry',axis=1)
#recyclebinadj=gpd.GeoDataFrame(recyclebinadj,geometry=recyclebinadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#recyclebinadj['area']=[x.area for x in recyclebinadj['geometry']]
#recyclebinadj=recyclebinadj[(recyclebinadj['area']>=3)&(recyclebinadj['area']<=5)].reset_index(drop=True)
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#recyclebinadj=gpd.sjoin(recyclebinadj,sdwkplaza,how='inner',op='within')
#recyclebinadj=recyclebinadj.drop(['index_right'],axis=1)
#recyclebinadj=recyclebinadj.to_crs({'init':'epsg:4326'})
#recyclebinadj.to_file(path+'output/recyclebinadj.shp')
#print(datetime.datetime.now()-start)
## 3 mins



## Tree
## On Curb
#start=datetime.datetime.now()
#curbtree=gpd.read_file(path+'input/impediments/tree.shp')
#curbtree.crs={'init':'epsg:4326'}
#curbtree=curbtree.to_crs({'init':'epsg:6539'})
#curbtree=curbtree[[x in ['OnCurb'] for x in curbtree['curb_loc']]].reset_index(drop=True)
#curbtree['ctid']=range(0,len(curbtree))
#curbtree=curbtree
#curbtreebuffer=curbtree.copy()
#curbtreebuffer['geometry']=curbtreebuffer.buffer(50)
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#curbtreebuffer=gpd.sjoin(curbtreebuffer,pvmtsp,how='inner',op='intersects')
#
#def curbtreeadjust(ct):
#    global curbtree
#    global pvmtsp
#    global curbtreebuffer
#    ct=ct.reset_index(drop=True)
#    curbtreetp=pd.concat([ct]*2,ignore_index=True)
#    curbtreepv=pvmtsp[np.isin(pvmtsp['pvid'],curbtreebuffer.loc[curbtreebuffer['ctid']==ct.loc[0,'ctid'],'pvid'])].reset_index(drop=True)
#    if len(curbtreepv)>0:
#        try:
#            curbtreepv=curbtreepv.loc[[np.argmin([curbtreetp.loc[0,'geometry'].distance(x) for x in curbtreepv['geometry']])]].reset_index(drop=True)
#            curbtreetp['pvid']=curbtreepv.loc[0,'pvid']
#            curbtreetp['snapdist']=curbtreetp.loc[0,'geometry'].distance(curbtreepv.loc[0,'geometry'])
#            adjgeom=shapely.ops.nearest_points(curbtreetp.loc[0,'geometry'],curbtreepv.loc[0,'geometry'])[1]
#            intplt=curbtreepv.loc[0,'geometry'].project(adjgeom)
#            splitter=shapely.geometry.MultiPoint([curbtreepv.loc[0,'geometry'].interpolate(x) for x in [intplt-2.5,intplt+2.5]])
#            splitseg=shapely.ops.split(curbtreepv.loc[0,'geometry'],splitter.buffer(0.01))[2]
#            curbtreetp.loc[0,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(1),splitseg.parallel_offset(6)]).convex_hull.wkt
#            curbtreetp.loc[1,'adjgeom']=shapely.geometry.MultiLineString([splitseg.parallel_offset(-1),splitseg.parallel_offset(-6)]).convex_hull.wkt
#            return curbtreetp
#        except:
#            print(str(ct.loc[0,'ctid'])+' error!')
#    else:
#        print(str(ct.loc[0,'ctid'])+' no pvid joined!')
#
#def curbtreeadjustcompile(ctcp):
#    curbtreeadjtp=ctcp.groupby('ctid',as_index=False).apply(curbtreeadjust)
#    return curbtreeadjtp
#
#def parallelize(data,func):
#    data_split=np.array_split(data,mp.cpu_count()-1)
#    pool=mp.Pool(mp.cpu_count()-1)
#    dt=pool.map(func,data_split)
#    dt=pd.concat(dt,axis=0,ignore_index=True)
#    pool.close()
#    pool.join()
#    return dt
#
#if __name__=='__main__':
#    curbtreeadj=parallelize(curbtree,curbtreeadjustcompile)
#    curbtreeadj=curbtreeadj[curbtreeadj['adjgeom']!='GEOMETRYCOLLECTION EMPTY'].reset_index(drop=True)
#    curbtreeadj=curbtreeadj.drop('geometry',axis=1)
#    curbtreeadj=gpd.GeoDataFrame(curbtreeadj,geometry=curbtreeadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#    curbtreeadj['area']=[x.area for x in curbtreeadj['geometry']]
#    curbtreeadj=curbtreeadj[(curbtreeadj['area']>=20)&(curbtreeadj['area']<=30)].reset_index(drop=True)
#    sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#    sdwkplaza.crs={'init':'epsg:4326'}
#    sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#    curbtreeadj=gpd.sjoin(curbtreeadj,sdwkplaza,how='inner',op='within')
#    curbtreeadj=curbtreeadj.drop(['index_right'],axis=1)
#    curbtreeadj=curbtreeadj.to_crs({'init':'epsg:4326'})
#    curbtreeadj.to_file(path+'output/curbtreeadj.shp')
#    print(datetime.datetime.now()-start)
#    # 60 mins



# Off Set Curb



# Grass Strip



## Cobmine all Impediments
#start=datetime.datetime.now()
#utistrip=gpd.read_file(path+'output/utistrip.shp')
#utistrip.crs={'init':'epsg:4326'}
#utistrip['impid']=['us'+str(x) for x in utistrip['usid']]
#utistrip['imptype']='Utility Strip'
#utistrip=utistrip[['impid','imptype','geometry']].reset_index(drop=True)
#citybenchadj=gpd.read_file(path+'output/citybenchadj.shp')
#citybenchadj.crs={'init':'epsg:4326'}
#citybenchadj['impid']=['cb'+str(x) for x in citybenchadj['cbid']]
#citybenchadj['imptype']='City Bench'
#citybenchadj=citybenchadj[['impid','imptype','geometry']].reset_index(drop=True)
#walknycadj=gpd.read_file(path+'output/walknycadj.shp')
#walknycadj.crs={'init':'epsg:4326'}
#walknycadj['impid']=['wn'+str(x) for x in walknycadj['wnid']]
#walknycadj['imptype']='WalkNYC'
#walknycadj=walknycadj[['impid','imptype','geometry']].reset_index(drop=True)
#meteradj=gpd.read_file(path+'output/meteradj.shp')
#meteradj.crs={'init':'epsg:4326'}
#meteradj['impid']=['mt'+str(x) for x in meteradj['mtid']]
#meteradj['imptype']='Meter'
#meteradj=meteradj[['impid','imptype','geometry']].reset_index(drop=True)
#busshelteradj=gpd.read_file(path+'output/busshelteradj.shp')
#busshelteradj.crs={'init':'epsg:4326'}
#busshelteradj['impid']=['bs'+str(x) for x in busshelteradj['bsid']]
#busshelteradj['imptype']='Bus Shelter'
#busshelteradj=busshelteradj[['impid','imptype','geometry']].reset_index(drop=True)
#linknycadj=gpd.read_file(path+'output/linknycadj.shp')
#linknycadj['impid']=['ln'+str(x) for x in linknycadj['lnid']]
#linknycadj['imptype']='LinkNYC'
#linknycadj=linknycadj[['impid','imptype','geometry']].reset_index(drop=True)
#payphoneadj=gpd.read_file(path+'output/payphoneadj.shp')
#payphoneadj.crs={'init':'epsg:4326'}
#payphoneadj['impid']=['pp'+str(x) for x in payphoneadj['ppid']]
#payphoneadj['imptype']='Pay Phone'
#payphoneadj=payphoneadj[['impid','imptype','geometry']].reset_index(drop=True)
#newsstandadj=gpd.read_file(path+'output/newsstandadj.shp')
#newsstandadj.crs={'init':'epsg:4326'}
#newsstandadj['impid']=['ns'+str(x) for x in newsstandadj['nsid']]
#newsstandadj['imptype']='News Stand'
#newsstandadj=newsstandadj[['impid','imptype','geometry']].reset_index(drop=True)
#hydrantadj=gpd.read_file(path+'output/hydrantadj.shp')
#hydrantadj.crs={'init':'epsg:4326'}
#hydrantadj['impid']=['hd'+str(x) for x in hydrantadj['hdid']]
#hydrantadj['imptype']='Hydrant'
#hydrantadj=hydrantadj[['impid','imptype','geometry']].reset_index(drop=True)
#litterbinadj=gpd.read_file(path+'output/litterbinadj.shp')
#litterbinadj.crs={'init':'epsg:4326'}
#litterbinadj['impid']=['lb'+str(x) for x in litterbinadj['lbid']]
#litterbinadj['imptype']='Litter Bin'
#litterbinadj=litterbinadj[['impid','imptype','geometry']].reset_index(drop=True)
#recyclebinadj=gpd.read_file(path+'output/recyclebinadj.shp')
#recyclebinadj.crs={'init':'epsg:4326'}
#recyclebinadj['impid']=['rb'+str(x) for x in recyclebinadj['rbid']]
#recyclebinadj['imptype']='Recycle Bin'
#recyclebinadj=recyclebinadj[['impid','imptype','geometry']].reset_index(drop=True)
#curbtreeadj=gpd.read_file(path+'output/curbtreeadj.shp')
#curbtreeadj.crs={'init':'epsg:4326'}
#curbtreeadj['impid']=['ct'+str(x) for x in curbtreeadj['ctid']]
#curbtreeadj['imptype']='Curb Tree'
#curbtreeadj=curbtreeadj[['impid','imptype','geometry']].reset_index(drop=True)
#railroadstruct=gpd.read_file(path+'input/planimetrics/railroadstruct.shp')
#railroadstruct.crs={'init':'epsg:4326'}
#railroadstruct=railroadstruct[railroadstruct['FEATURE_CO']==2485].reset_index(drop=True)
#railroadstruct['impid']=['rs'+str(x) for x in range(0,len(railroadstruct))]
#railroadstruct['imptype']='Railroad Structure'
#railroadstruct=railroadstruct[['impid','imptype','geometry']].reset_index(drop=True)
#impediment=pd.concat([utistrip,citybenchadj,walknycadj,meteradj,busshelteradj,linknycadj,payphoneadj,newsstandadj,hydrantadj,
#                      litterbinadj,recyclebinadj,curbtreeadj,railroadstruct],axis=0,ignore_index=True)
#impediment.to_file(path+'output/impediment.shp')
#print(datetime.datetime.now()-start)
## 20 mins



## Sidewalk and Plaza Excluding Impediments
#start=datetime.datetime.now()
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#impediment=gpd.read_file(path+'output/impediment.shp')
#impediment.crs={'init':'epsg:4326'}
#sdwkplazaimp=gpd.overlay(sdwkplaza,impediment,how='difference')
#sdwkplazaimp.to_file(path+'output/sdwkplazaimp.shp')
#print(datetime.datetime.now()-start)
## 250 mins







# Test
start=datetime.datetime.now()
sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')[0:10].reset_index(drop=True)
sdwkplaza.crs={'init':'epsg:4326'}
sdwkplaza['id']=0
sdwkplazadis=sdwkplaza.dissolve(by='id')
sdwkplazadis.to_file(path+'output/sdwkplazadis.shp')
print(datetime.datetime.now()-start)

start=datetime.datetime.now()
impediment=gpd.read_file(path+'output/impediment.shp')[0:10].reset_index(drop=True)
impediment.crs={'init':'epsg:4326'}
impediment['id']=0
impedimentdis=impediment.dissolve(by='id')
impedimentdis.to_file(path+'output/impedimentdis.shp')
print(datetime.datetime.now()-start)




#start=datetime.datetime.now()
#sdwktest['id']=0
#sdwktestdis=sdwktest.dissolve(by='id').reset_index(drop=True)
#litterbinadj['id']=0
#litterbinadjdis=litterbinadj.dissolve(by='id').reset_index(drop=True)
#k=pd.DataFrame()
#k['geom']=''
#k.loc[0,'geom']=sdwktestdis.loc[0,'geometry'].difference(litterbinadjdis.loc[0,'geometry']).wkt
#k=gpd.GeoDataFrame(geometry=k['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#k.to_file(path+'k.shp')
#print(datetime.datetime.now()-start)
## 0.5 sec
#
#










## Find original sidewalk width
#start=datetime.datetime.now()
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#pvmtsp['length']=[x.length for x in pvmtsp['geometry']]
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#
#def sidewalkwidth(sw):
#    global pvmtsp
#    global sdwkplaza
#    sw=sw.reset_index(drop=True)
#    tpseg=[0 if sw.loc[0,'length']<=40 else int((sw.loc[0,'length']-40)/5)+1][0]
#    if tpseg!=0:
#        try:
#            tp=pd.concat([sw]*tpseg*2,axis=0,ignore_index=True)
#            tp['side']=['L']*tpseg+['R']*tpseg
#            tp['orgsw']=np.nan
#            tp['count']=np.nan
#            sd=sdwkplaza[sdwkplaza['spid']==tp.loc[0,'spid']].reset_index(drop=True)  
#            splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(20+x*5,normalized=False) for x in range(0,tpseg)])
#            tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(0.01))
#            if len(tpsplit[2].parallel_offset(50,'left').boundary)==2:
#                tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[2].parallel_offset(50,'left').boundary[0]])
#            else:
#                tp.loc[0,'geometry']=''
#            if len(tpsplit[2].parallel_offset(50,'right').boundary)==2:
#                tp.loc[tpseg,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[2].parallel_offset(50,'right').boundary[1]])
#            else:
#                tp.loc[tpseg,'geometry']=''
#            for i in range(1,tpseg):
#                if len(tpsplit[i*2].parallel_offset(50,'left').boundary)==2:
#                    tp.loc[i,'geometry']=shapely.geometry.LineString([splitter[i],tpsplit[i*2].parallel_offset(50,'left').boundary[1]])
#                else:
#                    tp.loc[i,'geometry']=''
#                if len(tpsplit[i*2].parallel_offset(50,'right').boundary)==2:
#                    tp.loc[i+tpseg,'geometry']=shapely.geometry.LineString([splitter[i],tpsplit[i*2].parallel_offset(50,'right').boundary[0]])
#                else:
#                    tp.loc[i+tpseg,'geometry']=''  
#            tp=tp[tp['geometry']!=''].reset_index(drop=True) 
#            for j in tp.index:
#                sdwkint=[tp.loc[j,'geometry'].intersection(sd.loc[0,'geometry'])]
#                if sdwkint[0].length<=0.01:
#                    tp.loc[j,'geometry']=''
#                    tp.loc[j,'orgsw']=0
#                    tp.loc[j,'count']=0
#                elif type(sdwkint[0])==shapely.geometry.linestring.LineString:
#                    tp.loc[j,'geometry']=sdwkint[0]
#                    tp.loc[j,'orgsw']=sdwkint[0].length
#                    tp.loc[j,'count']=1
#                elif type(sdwkint[0])==shapely.geometry.multilinestring.MultiLineString:
#                    tp.loc[j,'geometry']=sdwkint[0][0]
#                    tp.loc[j,'orgsw']=sdwkint[0][0].length
#                    tp.loc[j,'count']=len(sdwkint[0])
#            tp['geometry']=np.where(tp['orgsw']<=0.01,'',tp['geometry'])
#            tp['orgsw']=np.where(tp['orgsw']<=0.01,np.nan,tp['orgsw'])
#            tp=tp.loc[pd.notna(tp['orgsw']),['pvid','bkfaceid','spid','side','orgsw','count','geometry']].reset_index(drop=True)
#            if len(tp.side.unique())==1:
#                return tp
#            else:
#                print(str(sw.loc[0,'pvid'])+' tickmarks on both sides!')
#        except:
#            print(str(sw.loc[0,'pvid'])+' error!')
#    else:
#        print(str(sw.loc[0,'pvid'])+' shorter than 40 feet!')
#
#def sidewalkwidthcompile(swcp):
#    sdwktmtp=swcp.groupby('pvid',as_index=False).apply(sidewalkwidth)
#    return sdwktmtp
#
#def parallelize(data,func):
#    data_split=np.array_split(data,mp.cpu_count()-1)
#    pool=mp.Pool(mp.cpu_count()-1)
#    dt=pool.map(func,data_split)
#    dt=pd.concat(dt,axis=0,ignore_index=True)
#    pool.close()
#    pool.join()
#    return dt
#
#if __name__=='__main__':
#    sdwktm=parallelize(pvmtsp,sidewalkwidthcompile)
#    sdwktm=sdwktm.to_crs({'init':'epsg:4326'})
#    sdwktm.to_file(path+'output/sdwktm.shp')
#    sdwkwd=sdwktm.groupby(['pvid','bkfaceid','spid','side'],as_index=False).agg({'orgsw':['min','max','median']}).reset_index(drop=True)
#    sdwkwd.columns=['pvid','bkfaceid','spid','side','orgswmin','orgswmax','orgswmedian']
#    sdwkwd=pd.merge(pvmtsp,sdwkwd,how='inner',on=['pvid','bkfaceid','spid'])
#    sdwkwd['length']=[x.length for x in sdwkwd['geometry']]
#    sdwkwd=sdwkwd[['pvid','bkfaceid','spid','side','orgswmin','orgswmax','orgswmedian','length','geometry']].reset_index(drop=True)
#    sdwkwd=sdwkwd.to_crs({'init':'epsg:4326'})
#    sdwkwd.to_file(path+'output/sdwkwd.shp')
#    print(datetime.datetime.now()-start)
#    # 2400 mins






















## Find original sidewalk width
#start=datetime.datetime.now()
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#pvmtsp['length']=[x.length for x in pvmtsp['geometry']]
#sdwkplaza=gpd.read_file(path+'output/sdwkplaza.shp')
#sdwkplaza.crs={'init':'epsg:4326'}
#sdwkplaza=sdwkplaza.to_crs({'init':'epsg:6539'})
#sw=[]
#swtm=[]
#for i in pvmtsp.index:
#    tp=pvmtsp.loc[[i]].reset_index(drop=True)
#    tpseg=[0 if tp.loc[0,'length']<=40 else int((tp.loc[0,'length']-40)/5)+1][0]
#    if tpseg!=0:
#        try:
#            tp=pd.concat([tp]*tpseg*2,axis=0,ignore_index=True)
#            tp['side']=['L']*tpseg+['R']*tpseg
#            tp['orgsw']=np.nan
#            tp['count']=np.nan
#            sd=sdwkplaza[sdwkplaza['spid']==tp.loc[0,'spid']].reset_index(drop=True)  
#            splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(20+x*5,normalized=False) for x in range(0,tpseg)])
#            tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(0.01))
#            if len(tpsplit[2].parallel_offset(50,'left').boundary)==2:
#                tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[2].parallel_offset(50,'left').boundary[0]])
#            else:
#                tp.loc[0,'geometry']=''
#            if len(tpsplit[2].parallel_offset(50,'right').boundary)==2:
#                tp.loc[tpseg,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[2].parallel_offset(50,'right').boundary[1]])
#            else:
#                tp.loc[tpseg,'geometry']=''
#            for j in range(1,tpseg):
#                if len(tpsplit[j*2].parallel_offset(50,'left').boundary)==2:
#                    tp.loc[j,'geometry']=shapely.geometry.LineString([splitter[j],tpsplit[j*2].parallel_offset(50,'left').boundary[1]])
#                else:
#                    tp.loc[j,'geometry']=''
#                if len(tpsplit[j*2].parallel_offset(50,'right').boundary)==2:
#                    tp.loc[j+tpseg,'geometry']=shapely.geometry.LineString([splitter[j],tpsplit[j*2].parallel_offset(50,'right').boundary[0]])
#                else:
#                    tp.loc[j+tpseg,'geometry']=''  
#            tp=tp[tp['geometry']!=''].reset_index(drop=True) 
#            for k in tp.index:
#                sdwkint=[tp.loc[k,'geometry'].intersection(sd.loc[0,'geometry'])]
#                if sdwkint[0].length<=0.01:
#                    tp.loc[k,'geometry']=''
#                    tp.loc[k,'orgsw']=0
#                    tp.loc[k,'count']=0
#                elif type(sdwkint[0])==shapely.geometry.linestring.LineString:
#                    tp.loc[k,'geometry']=sdwkint[0]
#                    tp.loc[k,'orgsw']=sdwkint[0].length
#                    tp.loc[k,'count']=1
#                elif type(sdwkint[0])==shapely.geometry.multilinestring.MultiLineString:
#                    tp.loc[k,'geometry']=sdwkint[0][0]
#                    tp.loc[k,'orgsw']=sdwkint[0][0].length
#                    tp.loc[k,'count']=len(sdwkint[0])
#            tp['geometry']=np.where(tp['orgsw']<=0.01,'',tp['geometry'])
#            tp['orgsw']=np.where(tp['orgsw']<=0.01,np.nan,tp['orgsw'])
#            tp=tp.loc[pd.notna(tp['orgsw']),['pvid','bkfaceid','spid','side','orgsw','count','geometry']].reset_index(drop=True)
#            if len(tp.side.unique())==1:
#                swtm+=[tp]
#                tp=tp.groupby(['pvid','bkfaceid','spid','side'],as_index=False).agg({'orgsw':['min','max','median']}).reset_index(drop=True)
#                tp.columns=['pvid','bkfaceid','spid','side','orgswmin','orgswmax','orgswmedian']
#                sw+=[tp]
#            else:
#                print(str(pvmtsp.loc[i,'pvid'])+' tickmarks on both sides!')
#        except:
#            print(str(pvmtsp.loc[i,'pvid'])+' error!')
#    else:
#        print(str(pvmtsp.loc[i,'pvid'])+' shorter than 40 feet!')
#swtm=pd.concat(swtm,ignore_index=True)
#swtm=swtm.to_crs({'init':'epsg:4326'})
#swtm.to_file(path+'output/swtm.shp')
#sw=pd.concat(sw,ignore_index=True)
#sw=pd.merge(pvmtsp,sw,how='inner',on=['pvid','bkfaceid','spid'])
#sw['length']=[x.length for x in sw['geometry']]
#sw=sw[['pvid','bkfaceid','spid','side','orgswmin','orgswmax','orgswmedian','length','geometry']].reset_index(drop=True)
#sw=sw.to_crs({'init':'epsg:4326'})
#sw.to_file(path+'output/sw.shp')
#print(datetime.datetime.now()-start)
##2400 mins














# Backup for future use

## Find sidewalk width
#start=datetime.datetime.now()
#pvmtsp=gpd.read_file(path+'output/pvmtsp.shp')
#pvmtsp.crs={'init':'epsg:4326'}
#pvmtsp=pvmtsp.to_crs({'init':'epsg:6539'})
#sidewalk=gpd.read_file(path+'input/sidewalk.shp')
#sidewalk.crs={'init':'epsg:4326'}
#sidewalk=sidewalk.to_crs({'init':'epsg:6539'})
#sdwkpvmt=gpd.sjoin(sidewalk,pvmtsp,how='left',op='intersects')
#sdwkpvmt=sdwkpvmt.loc[pd.notna(sdwkpvmt['bkfaceid']),['FID','bkfaceid','geometry']].reset_index(drop=True)
#sw=pd.DataFrame()
#swtm=pd.DataFrame()
#for i in pvmtsp.index:
#    try:
#        tp=pvmtsp.loc[[i]].reset_index(drop=True)
#        tp=pd.concat([tp]*14,axis=0,ignore_index=True)
#        tp['side']=['L']*7+['R']*7
#        tp['sdwkwidth']=np.nan
#        sd=sdwkpvmt[sdwkpvmt['bkfaceid']==tp.loc[0,'bkfaceid']].reset_index(drop=True)
#        splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(x,normalized=True) for x in [0.2,0.3,0.4,0.5,0.6,0.7,0.8]])
#        tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(1e-8))
#        tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(50,'left').boundary[1]])
#        tp.loc[1,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(50,'left').boundary[1]])
#        tp.loc[2,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(50,'left').boundary[1]])
#        tp.loc[3,'geometry']=shapely.geometry.LineString([splitter[3],tpsplit[6].parallel_offset(50,'left').boundary[1]])
#        tp.loc[4,'geometry']=shapely.geometry.LineString([splitter[4],tpsplit[8].parallel_offset(50,'left').boundary[1]])
#        tp.loc[5,'geometry']=shapely.geometry.LineString([splitter[5],tpsplit[10].parallel_offset(50,'left').boundary[1]])        
#        tp.loc[6,'geometry']=shapely.geometry.LineString([splitter[6],tpsplit[12].parallel_offset(50,'left').boundary[1]])        
#        tp.loc[7,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(50,'right').boundary[0]])
#        tp.loc[8,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(50,'right').boundary[0]])
#        tp.loc[9,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(50,'right').boundary[0]])
#        tp.loc[10,'geometry']=shapely.geometry.LineString([splitter[3],tpsplit[6].parallel_offset(50,'right').boundary[0]])
#        tp.loc[11,'geometry']=shapely.geometry.LineString([splitter[4],tpsplit[8].parallel_offset(50,'right').boundary[0]])
#        tp.loc[12,'geometry']=shapely.geometry.LineString([splitter[5],tpsplit[10].parallel_offset(50,'right').boundary[0]])        
#        tp.loc[13,'geometry']=shapely.geometry.LineString([splitter[6],tpsplit[12].parallel_offset(50,'right').boundary[0]])             
#        for j in tp.index:
#            sdwkwidth=[x for x in [tp.loc[j,'geometry'].intersection(x) for x in sd.geometry] if x.length!=0]
#            if len(sdwkwidth)==1:
#                tp.loc[j,'geometry']=sdwkwidth[0]
#                tp.loc[j,'sdwkwidth']=sdwkwidth[0].length
#            elif len(sdwkwidth)==0:
#                tp.loc[j,'sdwkwidth']=0
#            elif len(sdwkwidth)>1:
#                print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
#        tp['geometry']=np.where(tp['sdwkwidth']<0.1,'',tp['geometry'])
#        tp['sdwkwidth']=np.where(tp['sdwkwidth']<0.1,np.nan,tp['sdwkwidth'])
#        tptm=tp.loc[pd.notna(tp['sdwkwidth']),['bkfaceid','sdwkwidth','geometry']].reset_index(drop=True)
#        swtm=pd.concat([swtm,tptm],axis=0,ignore_index=True)
#        tp=tp.groupby(['bkfaceid','side'],as_index=False).agg({'sdwkwidth':['min','max','median']}).reset_index(drop=True)
#        tp.columns=['bkfaceid','side','swmin','swmax','swmedian']
#        tp=tp[pd.notna(tp['swmedian'])].reset_index(drop=True)
#        if len(tp)==1:
#            tp=tp[['bkfaceid','swmin','swmax','swmedian']].reset_index(drop=True)
#            sw=pd.concat([sw,tp],axis=0,ignore_index=True)
#        else:
#            print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
#    except:
#        print(str(pvmtsp.loc[i,'bkfaceid'])+' error!')
#swtm=swtm.to_crs({'init':'epsg:4326'})
#swtm.to_file(path+'output/swtm.shp')
#sw=pd.merge(pvmtsp,sw,how='inner',on='bkfaceid')
#sw['length']=[x.length for x in sw['geometry']]
#sw=sw[['bkfaceid','swmin','swmax','swmedian','length','geometry']].reset_index(drop=True)
#sw=sw.to_crs({'init':'epsg:4326'})
#sw.to_file(path+'output/sw.shp')
#print(datetime.datetime.now()-start)
#480 mins

## Tract
#start=datetime.datetime.now()
#nycct=gpd.read_file(path+'input/nycct.shp')
#nycct.crs={'init':'epsg:4326'}
#nycctclipped=gpd.read_file(path+'input/nycctclipped.shp')
#nycctclipped.crs={'init':'epsg:4326'}
#sw=gpd.read_file(path+'output/sw.shp')
#sw.crs={'init':'epsg:4326'}
#sidewalk=gpd.read_file(path+'input/sidewalk.shp')
#sidewalk.crs={'init':'epsg:4326'}
#tracttonta=pd.read_csv(path+'input/tracttonta.csv',dtype=str)
#swct=pd.merge(nycctclipped,tracttonta,how='inner',left_on='tractid',right_on='tract')
#swct=swct.loc[[str(x) not in ['BX99','BK99','MN99','QN99','SI99','QN98'] for x in swct['nta']],['tractid','geometry']].reset_index(drop=True)
#swct=swct.to_crs({'init':'epsg:6539'})
#swct['area']=[x.area for x in swct['geometry']]
#swct=swct[['tractid','area']].reset_index(drop=True)
#swctmedian=gpd.sjoin(sw,nycct,how='inner',op='intersects')
#swctmedian['swlength']=swctmedian['swmedian']*swctmedian['length']
#swctmedian=swctmedian.groupby('tractid',as_index=False).agg({'swlength':'sum','length':'sum'}).reset_index(drop=True)
#swctmedian['swmdn']=swctmedian['swlength']/swctmedian['length']
#swctmedian['swmdnrk']=10-pd.qcut(swctmedian['swmdn'],10,labels=False)
#swctmedian=swctmedian[['tractid','swmdn','swmdnrk']].reset_index(drop=True)
#swct=pd.merge(swct,swctmedian,how='inner',on='tractid')
#swctarea=gpd.overlay(sidewalk,nycct,how='intersection')
#swctarea.crs={'init':'epsg:4326'}
#swctarea=swctarea.to_crs({'init':'epsg:6539'})
#swctarea['swarea']=[x.area for x in swctarea['geometry']]
#swctarea=swctarea.groupby('tractid',as_index=False).agg({'swarea':'sum'}).reset_index(drop=True)
#swct=pd.merge(swct,swctarea,how='inner',on='tractid')
#swct['swareaarea']=swct['swarea']/swct['area']
#swct['areaareark']=10-pd.qcut(swct['swareaarea'],10,labels=False)
#ctpop=pd.read_csv(path+'input/tractpop2018.csv',dtype=str,converters={'pop':float})
#swct=pd.merge(swct,ctpop,how='inner',on='tractid')
#swct['poparea']=swct['pop']/swct['area']
#swct['poprk']=pd.qcut(swct['poparea'],10,labels=False)+1
#swct['swareapop']=swct['swarea']/swct['pop']
#swct['areapoprk']=10-pd.qcut(swct['swareapop'],10,labels=False)
#swct=pd.merge(nycctclipped,swct,how='inner',on='tractid')
#swct.to_file(path+'output/swct.shp')
#print(datetime.datetime.now()-start)



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