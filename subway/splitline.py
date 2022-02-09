import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

st=pd.read_csv('C:/Users/Y_Ma2/Desktop/Stations.csv',dtype=str)

df=pd.read_excel('C:/Users/Y_Ma2/Desktop/Unlinked.xlsx',sheet_name='NYCTravel_Unlinked_HHVars',dtype=str)
df['per_weight_wd_trips_rsadj']=pd.to_numeric(df['per_weight_wd_trips_rsadj'])

tp=df.groupby(['board_stop_id','board_stop_name','transit_system','route_id'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)
tp=tp[tp['transit_system']=='New York City Transit Subway'].reset_index(drop=True)
tp['GTFSID']=[str(x)[0:3] for x in tp['board_stop_id']]
tp['GTFSID']=np.where(tp['GTFSID']=='140','142',tp['GTFSID'])
tp['Route']=[str(x)[0] for x in tp['route_id']]
tp=pd.merge(st,tp,how='outer',left_on='GTFS Stop ID',right_on='GTFSID')
tp=tp.groupby(['Complex ID','Route'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)
tpsum=tp.groupby(['Complex ID'],as_index=False).agg({'per_weight_wd_trips_rsadj':'sum'}).reset_index(drop=True)
tp=pd.merge(tp,tpsum,how='outer',on='Complex ID')
tp['ComplexID']=pd.to_numeric(tp['Complex ID'])
tp['Percent']=tp['per_weight_wd_trips_rsadj_x']/tp['per_weight_wd_trips_rsadj_y']
tp=tp[['ComplexID','Route','Percent']].reset_index(drop=True)
tp.to_csv('C:/Users/Y_Ma2/Desktop/SplitLine.csv',index=False)


