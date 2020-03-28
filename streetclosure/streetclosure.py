import geopandas as gpd
import pandas as pd
import numpy as np
import shapely


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/STREET CLOSURE/'



quadstatectpt=gpd.read_file(path+'population/quadstatectpt.shp')
quadstatectpt.crs={'init':'epsg:4326'}
nycctpt=quadstatectpt[[str(x)[0:5] in ['36005','36047','36061','36081','36085'] for x in quadstatectpt['tractid']]]
nycctpt.to_file(path+'population/nycctpt.shp')

quadstatectclipped=gpd.read_file(path+'population/quadstatectclipped.shp')
quadstatectclipped.crs={'init':'epsg:4326'}
nycctclipped=quadstatectclipped[[str(x)[0:5] in ['36005','36047','36061','36081','36085'] for x in quadstatectclipped['tractid']]]
nycctclipped.to_file(path+'population/nycctclipped.shp')



openspace=gpd.read_file(path+'openspace/openspace.shp')
openspace.crs={'init':'epsg:4326'}
openspacebuffer=openspace.to_crs({'init':'epsg:6539'})
openspacebuffer=openspacebuffer.buffer(2640)
openspacebuffer=openspacebuffer.to_crs({'init':'epsg:4326'})
openspace['geometry']=openspacebuffer
openspace.to_file(path+'openspace/openspacebuffer.shp')



openspacebuffer=gpd.read_file(path+'openspace/openspacebuffer.shp')
openspacebuffer.crs={'init':'epsg:4326'}
pop=pd.read_csv(path+'population/tractpop2018.csv',dtype=str,converters={'pop':float})


bxpt=gpd.read_file(path+'population/nycctpt.shp')
bxpt.crs={'init':'epsg:4326'}
bxpt=bxpt[[str(x)[0:5] in ['36005'] for x in bxpt['tractid']]]
bxpt=gpd.sjoin(bxpt,openspacebuffer,how='left',op='intersects')
bxpt=bxpt.loc[pd.isna(bxpt['index_right']),['tractid']].reset_index(drop=True)
bxct=gpd.read_file(path+'population/nycctclipped.shp')
bxct.crs={'init':'epsg:4326'}
bxct=pd.merge(bxct,bxpt,how='inner',on='tractid')
bxct=pd.merge(bxct,pop,how='inner',on='tractid')
bxct['popquint']=pd.qcut(bxct['pop'],4,labels=False)+1
bxct.to_file(path+'bxct.shp')



bkpt=gpd.read_file(path+'population/nycctpt.shp')
bkpt.crs={'init':'epsg:4326'}
bkpt=bkpt[[str(x)[0:5] in ['36047'] for x in bkpt['tractid']]]
bkpt=gpd.sjoin(bkpt,openspacebuffer,how='left',op='intersects')
bkpt=bkpt.loc[pd.isna(bkpt['index_right']),['tractid']].reset_index(drop=True)
bkct=gpd.read_file(path+'population/nycctclipped.shp')
bkct.crs={'init':'epsg:4326'}
bkct=pd.merge(bkct,bkpt,how='inner',on='tractid')
bkct=pd.merge(bkct,pop,how='inner',on='tractid')
bkct['popquint']=pd.qcut(bkct['pop'],4,labels=False)+1
bkct.to_file(path+'bkct.shp')


mnpt=gpd.read_file(path+'population/nycctpt.shp')
mnpt.crs={'init':'epsg:4326'}
mnpt=mnpt[[str(x)[0:5] in ['36061'] for x in mnpt['tractid']]]
mnpt=gpd.sjoin(mnpt,openspacebuffer,how='left',op='intersects')
mnpt=mnpt.loc[pd.isna(mnpt['index_right']),['tractid']].reset_index(drop=True)
mnct=gpd.read_file(path+'population/nycctclipped.shp')
mnct.crs={'init':'epsg:4326'}
mnct=pd.merge(mnct,mnpt,how='inner',on='tractid')
mnct=pd.merge(mnct,pop,how='inner',on='tractid')
mnct['popquint']=pd.qcut(mnct['pop'],4,labels=False)+1
mnct.to_file(path+'mnct.shp')



qnpt=gpd.read_file(path+'population/nycctpt.shp')
qnpt.crs={'init':'epsg:4326'}
qnpt=qnpt[[str(x)[0:5] in ['36081'] for x in qnpt['tractid']]]
qnpt=gpd.sjoin(qnpt,openspacebuffer,how='left',op='intersects')
qnpt=qnpt.loc[pd.isna(qnpt['index_right']),['tractid']].reset_index(drop=True)
qnct=gpd.read_file(path+'population/nycctclipped.shp')
qnct.crs={'init':'epsg:4326'}
qnct=pd.merge(qnct,qnpt,how='inner',on='tractid')
qnct=pd.merge(qnct,pop,how='inner',on='tractid')
qnct['popquint']=pd.qcut(qnct['pop'],4,labels=False)+1
qnct.to_file(path+'qnct.shp')

sipt=gpd.read_file(path+'population/nycctpt.shp')
sipt.crs={'init':'epsg:4326'}
sipt=sipt[[str(x)[0:5] in ['36085'] for x in sipt['tractid']]]
sipt=gpd.sjoin(sipt,openspacebuffer,how='left',op='intersects')
sipt=sipt.loc[pd.isna(sipt['index_right']),['tractid']].reset_index(drop=True)
sict=gpd.read_file(path+'population/nycctclipped.shp')
sict.crs={'init':'epsg:4326'}
sict=pd.merge(sict,sipt,how='inner',on='tractid')
sict=pd.merge(sict,pop,how='inner',on='tractid')
sict['popquint']=pd.qcut(sict['pop'],4,labels=False)+1
sict.to_file(path+'sict.shp')

community=gpd.read_file(path+'capital/communityclipped.shp')
community.crs={'init':'epsg:4326'}
osr=pd.read_csv(path+'capital/cb_osr_2018_31_12_dpr20191210_1.csv')
osr=pd.merge(community,osr,how='left',left_on='BoroCD',right_on='cb')
osr=osr[pd.notna(osr['osr'])].reset_index(drop=True)
osr.to_file(path+'capital/osr.shp')


