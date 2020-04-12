import pandas as pd
import geopandas as gpd
import numpy as np
import shapely
import datetime



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'
path='/home/mayijun/sidewalk/'


## SImplify LION
#lion=gpd.read_file(path+'lion.shp')
#lion.crs={'init':'epsg:4326'}
#lionsp=lion[['PhysicalID','SeqNum','SegmentID','RB_Layer','FeatureTyp','SegmentTyp','NonPed','TrafDir','RW_TYPE','StreetWidt','geometry']].reset_index(drop=True)
#lionsp['physicalid']=pd.to_numeric(lionsp['PhysicalID'])
#lionsp=lionsp[pd.notna(lionsp['physicalid'])].reset_index(drop=True)
#lionsp['seqnum']=pd.to_numeric(lionsp['SeqNum'])
#lionsp=lionsp[pd.notna(lionsp['seqnum'])].reset_index(drop=True)
#lionsp['segmentid']=pd.to_numeric(lionsp['SegmentID'])
#lionsp=lionsp[pd.notna(lionsp['segmentid'])].reset_index(drop=True)
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
#lionsp=lionsp[['physicalid','seqnum','segmentid','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth','geometry']].reset_index(drop=True)
#lionsp=lionsp.drop_duplicates(['segmentid','seqnum','physicalid','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth'],keep='first').reset_index(drop=True)
#lionsp=lionsp.sort_values(['physicalid','seqnum']).reset_index(drop=True)
#lionsp=lionsp[['physicalid','stwidth','geometry']].dissolve(by='physicalid',as_index=False)
#lionsp.to_file(path+'lionsp.shp')



# 

sidewalk=gpd.read_file(path+'sidewalk.shp')
sidewalk.crs={'init':'epsg:4326'}
sidewalk=sidewalk.to_crs({'init':'epsg:6539'})

lionsp=gpd.read_file(path+'lionsp.shp')
lionsp.crs={'init':'epsg:4326'}
lionsp=lionsp.to_crs({'init':'epsg:6539'})
lionsidewalk=lionsp.copy()
lionsidewalk['geometry']=lionsidewalk['geometry'].buffer(200)
lionsidewalk=gpd.sjoin(sidewalk,lionsidewalk,how='inner',op='intersects')

start=datetime.datetime.now()
df=pd.DataFrame()
for i in lionsp.index:
    try:
        tp=lionsp.loc[[i]].reset_index(drop=True)
        tp=pd.concat([tp]*6,axis=0,ignore_index=True)
        tp['side']=['L']*3+['R']*3
        tp['sidewalk']=np.nan
        sd=lionsidewalk[lionsidewalk['physicalid']==tp.loc[0,'physicalid']].reset_index(drop=True)
        splitter=shapely.geometry.MultiPoint([tp.loc[0,'geometry'].interpolate(x,normalized=True) for x in [0.25,0.5,0.75]])
        tpsplit=shapely.ops.split(tp.loc[0,'geometry'],splitter.buffer(1e-8))
        tp.loc[0,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
        tp.loc[1,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
        tp.loc[2,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(tp.loc[0,'stwidth']/2+50,'left').boundary[1]])
        tp.loc[3,'geometry']=shapely.geometry.LineString([splitter[0],tpsplit[0].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
        tp.loc[4,'geometry']=shapely.geometry.LineString([splitter[1],tpsplit[2].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
        tp.loc[5,'geometry']=shapely.geometry.LineString([splitter[2],tpsplit[4].parallel_offset(tp.loc[0,'stwidth']/2+50,'right').boundary[0]])
        for j in tp.index:
            sdwidth=[x for x in [tp.loc[j,'geometry'].intersection(x).length for x in sd.geometry] if x!=0]
            if len(sdwidth)==1:
                tp.loc[j,'sidewalk']=sdwidth[0]
            else:
                print(str(lionsp.loc[i,'physicalid'])+' error!')
        tp=tp.groupby(['physicalid','side'],as_index=False).agg({'sidewalk':['min','max']}).reset_index(drop=True)
        tp.columns=['physicalid','side','min','max']
        tp=tp.pivot(index='physicalid',columns='side').reset_index(drop=False)
        tp.columns=['physicalid','lmin','rmin','lmax','rmax']
        tp=tp[['physicalid','lmin','lmax','rmin','rmax']].reset_index(drop=True)
        df=pd.concat([df,tp],axis=0,ignore_index=True)
    except:
        print(str(lionsp.loc[i,'physicalid'])+' error!')
df=pd.merge(lionsp,df,how='inner',on='physicalid')
df=df.to_crs({'init':'epsg:4326'})
df.to_file(path+'df.shp')
print(datetime.datetime.now()-start)

# 120 mins







