itimport geopandas as gpd
import shapely

path='C:/Users/Yijun Ma/Desktop/'

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
