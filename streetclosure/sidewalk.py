import pandas as pd
import geopandas as gpd
import numpy as np
import shapely



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'



lion=gpd.read_file(path+'lion.shp')
lion.crs={'init':'epsg:4326'}

sidewalk=gpd.read_file(path+'sidewalk.shp')
sidewalk.crs={'init':'epsg:4326'}
sidewalk=sidewalk.to_crs({'init':'epsg:6539'})


lionsp=lion[['PhysicalID','SeqNum','SegmentID','RB_Layer','FeatureTyp','SegmentTyp','NonPed','TrafDir','RW_TYPE','StreetWidt','geometry']].reset_index(drop=True)
lionsp['physicalid']=pd.to_numeric(lionsp['PhysicalID'])
lionsp=lionsp[pd.notna(lionsp['physicalid'])].reset_index(drop=True)
lionsp['seqnum']=pd.to_numeric(lionsp['SeqNum'])
lionsp=lionsp[pd.notna(lionsp['seqnum'])].reset_index(drop=True)
lionsp['segmentid']=pd.to_numeric(lionsp['SegmentID'])
lionsp=lionsp[pd.notna(lionsp['segmentid'])].reset_index(drop=True)
lionsp['rblayer']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['RB_Layer']]
lionsp=lionsp[np.isin(lionsp['rblayer'],['B','R'])].reset_index(drop=True)
lionsp['featuretype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['FeatureTyp']]
lionsp=lionsp[np.isin(lionsp['featuretype'],['0','6','A','C'])].reset_index(drop=True)
lionsp['segmenttype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['SegmentTyp']]
lionsp=lionsp[np.isin(lionsp['segmenttype'],['B','R','U','S'])].reset_index(drop=True)
lionsp['nonped']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['NonPed']]
lionsp=lionsp[np.isin(lionsp['nonped'],['','D'])].reset_index(drop=True)
lionsp['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionsp['TrafDir']]
lionsp=lionsp[np.isin(lionsp['trafficdir'],['T','W','A'])].reset_index(drop=True)
lionsp['rwtype']=pd.to_numeric(lionsp['RW_TYPE'])
lionsp=lionsp[np.isin(lionsp['rwtype'],[1])].reset_index(drop=True)
lionsp['stwidth']=pd.to_numeric(lionsp['StreetWidt'])
lionsp=lionsp[['physicalid','seqnum','segmentid','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth','geometry']].reset_index(drop=True)
lionsp=lionsp.drop_duplicates(['segmentid','seqnum','physicalid','rblayer','featuretype','segmenttype','nonped','trafficdir','rwtype','stwidth'],keep='first').reset_index(drop=True)
lionsp=lionsp.sort_values(['physicalid','seqnum']).reset_index(drop=True)
lionsp=lionsp[['physicalid','stwidth','geometry']].dissolve(by='physicalid',as_index=False)
lionsp.to_file(path+'lionsp.shp')



# 
lionsp=gpd.read_file(path+'lionsp.shp')
lionsp.crs={'init':'epsg:4326'}
lionsp=lionsp.to_crs({'init':'epsg:6539'})

for i in lionsp.index:
    k=lionsp.loc[[i]]
    l=shapely.ops.split(k.loc[0,'geometry'],lion.loc[0,'geometry'].centroid)[0]


l=lionsp.loc[[i]]
l.to_file(path+'l.shp')
#p=l.rotate(90,'center',False)
#p=gpd.GeoDataFrame(geometry=[l.loc[0,'geometry'],p.geometry[0]],crs={'init':'epsg:6539'})
splitter=shapely.geometry.MultiPoint([lionsp.loc[i,'geometry'].interpolate(x,normalized=True) for x in [0.25,0.5,0.75]])
k=shapely.ops.split(lionsp.loc[i,'geometry'],splitter.buffer(1e-8))
k1=shapely.geometry.LineString([splitter[0],k[0].parallel_offset(lionsp.loc[i,'stwidth']/2+50,'left',3).boundary[1]])
k2=shapely.geometry.LineString([splitter[1],k[2].parallel_offset(lionsp.loc[i,'stwidth']/2+50,'left',3).boundary[1]])
k3=shapely.geometry.LineString([splitter[2],k[4].parallel_offset(lionsp.loc[i,'stwidth']/2+50,'left',3).boundary[1]])
m=gpd.GeoDataFrame(geometry=[k1,k2,k3],crs={'init':'epsg:6539'})
m.to_file(path+'m.shp')

s=gpd.sjoin(sidewalk,m,how='inner',op='intersects')
s=s.drop_duplicates('FID').reset_index(drop=True)


for i in m.index:
    o=[m.loc[i,'geometry'].intersection(x).length for x in s.geometry]
    o=[x for x in o if x!=0]



o=gpd.sjoin(sidewalk,m,how='left',op='intersects')

    o=gpd.sjoin(sidewalk,m.loc[[i]],how='inner',op='intersects').reset_index(drop=True)
    o=[m.loc[i,'geometry'].intersection(x).length for x in sidewalk.geometry]
o=m.loc[0,'geometry']


o=[o.intersection(x).length for x in sidewalk.geometry]
o=o.intersection(sidewalk.geometry)


lion=gpd.read_file(path+'liontest.shp')
sidewalk=gpd.read_file(path+'sidewalktest.shp')


k=lion.loc[0,'geometry']
l=shapely.ops.split(lion.loc[0,'geometry'],lion.loc[0,'geometry'].centroid)[0]
m=shapely.geometry.LineString([lion.loc[0,'geometry'].centroid,l.parallel_offset(30,'left',3).boundary[1]])
n=gpd.GeoDataFrame(geometry=[m])
n['id']='lionpep'


o=n.loc[0,'geometry']
o=o.intersection(sidewalk.geometry[1])
o=gpd.GeoDataFrame(geometry=[o])
o.to_file(path+'test.shp')


for i in lion.index:
    lionsplitter=lion.loc[i,'geometry'].interpolate(0.5,normalized=True)
