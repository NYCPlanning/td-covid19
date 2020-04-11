import pandas as pd
import geopandas as gpd
import numpy as np
import shapely

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/STREET CLOSURE/sidewalk/'



## Filter LION blockface and calculate blockface parkability (1 min)
#start=datetime.datetime.now()
lion=gpd.read_file(path+'LION.shp')

lionbfpk=lion[['SegmentID','PhysicalID','FeatureTyp','SegmentTyp','NonPed','RB_Layer','RW_TYPE','TrafDir']].drop_duplicates().reset_index(drop=True)
lionbfpk['segmentid']=pd.to_numeric(lionbfpk['SegmentID'])

lionbfpk['rwtype']=pd.to_numeric(lionbfpk['RW_TYPE'])

lionbfpk['featuretype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['FeatureTyp']]
lionbfpk=lionbfpk[np.isin(lionbfpk['featuretype'],['0','6','A','C'])].reset_index(drop=True)
lionbfpk['segmenttype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['SegmentTyp']]
lionbfpk=lionbfpk[np.isin(lionbfpk['segmenttype'],['B','R','U'])].reset_index(drop=True)


lionbfpk['rwtype']=pd.to_numeric(lionbfpk['RW_TYPE'])
lionbfpk=lionbfpk[np.isin(lionbfpk['rwtype'],[1])].reset_index(drop=True)
lionbfpk['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['TrafDir']]
lionbfpk=lionbfpk[np.isin(lionbfpk['trafficdir'],['T','W','A'])].reset_index(drop=True)
lionbfpk['parkinglane']=pd.to_numeric(lionbfpk['Number_Par']).fillna(0)






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
