import pandas as pd
import numpy as np
import geopandas as gpd
import shapely
import datetime



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/HUB/DEMAND/'


puma=gpd.read_file(path+'puma/tl_2019_36_puma10.shp')
puma=puma.to_crs({'init':'epsg:4326'})
puma['boro']=[str(x)[0:3] for x in puma['PUMACE10']]
puma['boro']=np.where(puma['boro']=='037','Bronx',
             np.where(puma['boro']=='040','Brooklyn',
             np.where(puma['boro']=='038','Manhattan',
             np.where(puma['boro']=='041','Queens',
             np.where(puma['boro']=='039','Staten Island','Other')))))
puma=puma[np.isin(puma['boro'],['Bronx','Brooklyn','Manhattan','Queens','Staten Island'])].reset_index(drop=True)
puma=puma[['boro','PUMACE10','geometry']].reset_index(drop=True)




ps=pd.read_csv(path+'mta survey/Person.csv',dtype=str,encoding ='latin1')
ps=ps[pd.notna(ps['per_weight_wd'])].reset_index(drop=True)
ps['per_weight_wd']=pd.to_numeric(ps['per_weight_wd'])
ps['homect']=[str(x)[0:11] for x in ps['home_bg']]
ps=ps.groupby('homect',as_index=False).agg({'personid':'count'}).reset_index(drop=True)
ct=gpd.read_file(path+'quadstatectclipped.shp')
ps=pd.merge(ct,ps,how='inner',left_on='tractid',right_on='homect')
ps.to_file(path+'ps.shp')







df=pd.read_csv(path+'mta survey/Unlinked.csv',dtype=str,encoding ='latin1')


k=df[['legid','tour_purpose','departure_time_mam','arrival_time_mam','orig_countyfp','orig_puma10','dest_countyfp',
      'dest_puma10','transit_system','route_id','board_stop_id','board_stop_name','board_stop_lat','board_stop_lon',
      'alight_stop_id','alight_stop_name','alight_stop_lat','alight_stop_lon','per_weight_wd_trips_rsadj',
      'trip_eval']].reset_index(drop=True)
k=k[pd.notna(k['per_weight_wd_trips_rsadj'])].reset_index(drop=True)
k['per_weight_wd_trips_rsadj']=pd.to_numeric(k['per_weight_wd_trips_rsadj'])
k=k[np.isin(k['transit_system'],['New York City Transit Subway'])].reset_index(drop=True)
k=k[(pd.notna(k['departure_time_mam']))|(pd.notna(k['arrival_time_mam']))].reset_index(drop=True)
k['departure_time_mam']=pd.to_numeric(k['departure_time_mam'])
k['arrival_time_mam']=pd.to_numeric(k['arrival_time_mam'])
k['hour']=np.where(pd.isna(k['departure_time_mam']),k['arrival_time_mam']/60,
          np.where(pd.isna(k['arrival_time_mam']),k['departure_time_mam']/60,
          (k['arrival_time_mam']+k['departure_time_mam'])/2/60))
k['hour']=[int(x) for x in k['hour']]
k=k[np.isin(k['hour'],[6,7,8,9])].reset_index(drop=True)
k['board_stop_lat']=pd.to_numeric(k['board_stop_lat'])
k['board_stop_lon']=pd.to_numeric(k['board_stop_lon'])
k['alight_stop_lat']=pd.to_numeric(k['alight_stop_lat'])
k['alight_stop_lon']=pd.to_numeric(k['alight_stop_lon'])
k=k[(pd.notna(k['board_stop_lat']))&(pd.notna(k['board_stop_lon']))&
    (pd.notna(k['alight_stop_lat']))&(pd.notna(k['alight_stop_lon']))].reset_index(drop=True)
k=gpd.GeoDataFrame(k,geometry=[shapely.geometry.Point(x,y) for x,y in zip(k['board_stop_lon'],k['board_stop_lat'])],crs={'init' :'epsg:4326'})
k=gpd.sjoin(k,puma,how='left',op='intersects')
k['origboro']=k['boro'].copy()
k['origpuma']=k['PUMACE10'].copy()
k=k.drop(['index_right','boro','PUMACE10'],axis=1).reset_index(drop=True)
k=gpd.GeoDataFrame(k,geometry=[shapely.geometry.Point(x,y) for x,y in zip(k['alight_stop_lon'],k['alight_stop_lat'])],crs={'init' :'epsg:4326'})
k=gpd.sjoin(k,puma,how='left',op='intersects')
k['destboro']=k['boro'].copy()
k['destpuma']=k['PUMACE10'].copy()
k=k.drop(['index_right','boro','PUMACE10'],axis=1).reset_index(drop=True)


l=k[np.isin(k['origboro'],['Brooklyn'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan','Queens','Bronx'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Bronx'])].reset_index(drop=True)
l=l[np.isin(l['destboro'],['Queens'])].reset_index(drop=True)
l=l[np.isin(l['tour_purpose'],['Work'])].reset_index(drop=True)
l=l.groupby(['route_id'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)



l=k[np.isin(k['origboro'],['Queens'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan','Brooklyn'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan'])].reset_index(drop=True)
l=l[np.isin(l['destboro'],['Brooklyn'])].reset_index(drop=True)
l=l[np.isin(l['tour_purpose'],['Work'])].reset_index(drop=True)
l=l.groupby(['route_id'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)



l=k[(np.isin(k['origboro'],['Bronx']))|(np.isin(k['origpuma'],['03801','03802','03803','03804','03805','03806']))].reset_index(drop=True)
l=l[(np.isin(l['destboro'],['Queens','Brooklyn']))|(np.isin(l['destpuma'],['03807','03808','03809','03810']))].reset_index(drop=True)
l=l.groupby(['route_id','hour'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)



l=k[np.isin(k['origboro'],['Bronx'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan','Brooklyn','Queens'])].reset_index(drop=True)
# l=l[np.isin(l['destboro'],['Manhattan'])].reset_index(drop=True)
l=l[np.isin(l['destboro'],['Brooklyn'])].reset_index(drop=True)
l=l[np.isin(l['tour_purpose'],['Work'])].reset_index(drop=True)
l=l.groupby(['route_id'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)



