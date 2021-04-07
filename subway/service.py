import pandas as pd
import numpy as np
import datetime
import pytz
import plotly.io as pio
import plotly.graph_objects as go


pio.renderers.default="browser"
pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/'


# GTFS Schedule
calendar=pd.read_csv(path+'google_transit/calendar.txt')
trips=pd.read_csv(path+'google_transit/trips.txt')
stoptimes=pd.read_csv(path+'google_transit/stop_times.txt')
stoptimes['hour']=[str(x)[0:2] for x in stoptimes['arrival_time']]
tripfirsttime=stoptimes.drop_duplicates('trip_id',keep='first').reset_index(drop=True)
triplasttime=stoptimes.drop_duplicates('trip_id',keep='last').reset_index(drop=True)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=tripfirsttime[np.isin(tripfirsttime['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=triplasttime[np.isin(triplasttime['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)

df=pd.DataFrame()
for i in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
    serviceid=list(calendar.loc[calendar[i]==1,'service_id'])
    tripid=list(trips.loc[np.isin(trips['service_id'],serviceid),'trip_id'])
    tp=stoptimes[np.isin(stoptimes['trip_id'],tripid)].reset_index(drop=True)
    tp=tp.groupby('hour',as_index=False).agg({'trip_id':'count'}).reset_index(drop=True)
    tp['day']=str(i)
    df=pd.concat([df,tp],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)







# GTFS Realtime
k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='first').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='last').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='first').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.drop_duplicates('tripid',keep='last').reset_index(drop=True)
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200213.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200409.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200416.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200423.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200430.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200507.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200514.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200521.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200528.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200604.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200608.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200611.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200618.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200625.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200709.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200813.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200910.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20201008.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20201112.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20201203.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'SERVICE/gtfsrt/tp_20210311.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'SERVICE/k.csv',index=False)

k=pd.read_csv(path+'SERVICE/gtfsrt/tp_20210325.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k.groupby('hour',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'SERVICE/k.csv',index=False)









k=pd.read_csv(path+'gtfsrt/tp_20200213.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200312.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200326.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200409.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200416.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200423.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200430.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200507.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200514.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200521.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200528.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200604.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200608.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200611.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200618.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)

k=pd.read_csv(path+'gtfsrt/tp_20200625.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
k=k[k['starttime']!=0].reset_index(drop=True)
k=k[k['endtime']!=0].reset_index(drop=True)
k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
k['hour']=[x.strftime('%H') for x in k['time']]
k=k[np.isin(k['hour'],['08','17'])].reset_index(drop=True)
k=k.groupby('routeid',as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
k.to_csv(path+'k.csv',index=False)










t=['10/10/2019','03/12/2020','04/09/2020','05/07/2020','06/11/2020','07/09/2020','08/13/2020','09/10/2020','10/08/2020']
# Demand and Supply
tp=pd.DataFrame()
tp['Date']=t
tp['Ridership']=np.nan
tp['Trips']=np.nan
for i in t:
    # Demand
    dfunitentry=pd.read_csv(path+'TURNSTILE/OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
    amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
            '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
    cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],i)].reset_index(drop=True)
    cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
    cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
    rc=pd.read_csv(path+'TURNSTILE/RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float,'Hub':float})
    cplxampre=pd.merge(cplxampre,rc,how='inner',left_on='unit',right_on='Remote')
    cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
    cplxampre.columns=['CplxID','Time','E'+i[6:10]+i[0:2]+i[3:5]]
    cplxampre['E'+i[6:10]+i[0:2]+i[3:5]]=cplxampre['E'+i[6:10]+i[0:2]+i[3:5]]/4
    # Supply
    k=pd.read_csv(path+'SERVICE/gtfsrt/tp_'+i[6:10]+i[0:2]+i[3:5]+'.csv',dtype=float,converters={'routeid':str,'tripdate':str,'tripid':str,'startstopid':str,'endstopid':str})
    k=k[k['starttime']!=0].reset_index(drop=True)
    k=k[k['endtime']!=0].reset_index(drop=True)
    k['time']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in k['starttime']]
    k['hour']=[x.strftime('%H') for x in k['time']]
    k['stopid']=[x[:-1] for x in k['startstopid']]
    k=k[['stopid','hour','tripid']].reset_index(drop=True)
    st=pd.read_csv(path+'SERVICE/Stations.csv',dtype=str)
    st['CplxID']=pd.to_numeric(st['Complex ID'])
    st['stopid']=st['GTFS Stop ID'].copy()
    st=st[['stopid','CplxID']].reset_index(drop=True)
    k=pd.merge(k,st,on='stopid',how='inner')
    k=pd.merge(k,cplxampre,on='CplxID',how='inner')
    k['ct']=0
    sorted(k.Time.unique())
    k.loc[(k['Time']=='05:00:00-09:00:00')&(np.isin(k['hour'],['05','06','07','08'])),'ct']=1
    k.loc[(k['Time']=='06:00:00-10:00:00')&(np.isin(k['hour'],['06','07','08','09'])),'ct']=1
    k.loc[(k['Time']=='06:30:00-10:30:00')&(np.isin(k['hour'],['06','07','08','09'])),'ct']=1
    k.loc[(k['Time']=='08:00:00-12:00:00')&(np.isin(k['hour'],['08','09','10','11'])),'ct']=1
    k.loc[(k['Time']=='08:22:00-12:22:00')&(np.isin(k['hour'],['08','09','10','11'])),'ct']=1
    k.loc[(k['Time']=='08:30:00-12:30:00')&(np.isin(k['hour'],['08','09','10','11'])),'ct']=1
    k.loc[(k['Time']=='05:00:00-09:00:00|07:00:00-11:00:00')&(np.isin(k['hour'],['05','06','07','08','09','10'])),'ct']=1
    k.loc[(k['Time']=='05:00:00-09:00:00|08:00:00-12:00:00')&(np.isin(k['hour'],['05','06','07','08','09','10','11'])),'ct']=1
    k.loc[(k['Time']=='06:00:00-10:00:00|08:00:00-12:00:00')&(np.isin(k['hour'],['06','07','08','09','10','11'])),'ct']=1
    k=k[k['ct']==1].reset_index(drop=True)
    k=k.groupby(['CplxID','Time'],as_index=False).agg({'tripid':'count'}).reset_index(drop=True)
    k['hours']=np.nan
    k.loc[k['Time']=='05:00:00-09:00:00','hours']=4
    k.loc[k['Time']=='06:00:00-10:00:00','hours']=4
    k.loc[k['Time']=='06:30:00-10:30:00','hours']=4
    k.loc[k['Time']=='08:00:00-12:00:00','hours']=4
    k.loc[k['Time']=='08:22:00-12:22:00','hours']=4
    k.loc[k['Time']=='08:30:00-12:30:00','hours']=4
    k.loc[k['Time']=='05:00:00-09:00:00|07:00:00-11:00:00','hours']=6
    k.loc[k['Time']=='05:00:00-09:00:00|08:00:00-12:00:00','hours']=7
    k.loc[k['Time']=='06:00:00-10:00:00|08:00:00-12:00:00','hours']=6
    k['T'+i[6:10]+i[0:2]+i[3:5]]=k['tripid']/k['hours']
    k=k[['CplxID','T'+i[6:10]+i[0:2]+i[3:5]]].reset_index(drop=True)
    k=pd.merge(k,cplxampre,on='CplxID',how='inner')
    k=k[['CplxID','Time','E'+i[6:10]+i[0:2]+i[3:5],'T'+i[6:10]+i[0:2]+i[3:5]]].reset_index(drop=True)
    # Summary
    tp.loc[tp['Date']==i,'Ridership']=sum(k['E'+i[6:10]+i[0:2]+i[3:5]])
    tp.loc[tp['Date']==i,'Trips']=sum(k['T'+i[6:10]+i[0:2]+i[3:5]])
tp.to_csv(path+'SERVICE/ds.csv',index=False)












df=pd.read_csv(path+'SERVICE/service.csv')
dfcolors={'3/12/2020':'#fde725',
          '4/9/2020':'#5dc962',
          '5/14/2020':'#20908d',
          '6/11/2020':'#3a528b',
          '3/25/2021':'#440154'}
fig=go.Figure()
fig=fig.add_trace(go.Scattergl(name='',
                               x=df['time'],
                               y=df['3/12/2020'],
                               opacity=0,
                               showlegend=False,
                               hovertext='<b>'+df['time']+'</b>',
                               hoverinfo='text'))
for i in df.columns[1:]:
    fig=fig.add_trace(go.Scattergl(name=i+'   ',
                                   mode='lines',
                                   x=df['time'],
                                   y=df[i],
                                    line={'color':dfcolors[i],
                                          'width':3},
                                   hovertext=[i+': '+'{0:,}'.format(x) for x in df[i]],
                                   hoverinfo='text'))
fig.update_layout(
    template='plotly_white',
    title={'text':'<b>MTA Subway System-wide Weekday Service Change by Hour</b>',
           'font_size':24,
           'x':0.5,
           'xanchor':'center'},
    legend={'orientation':'h',
            'title_text':'',
            'font_size':16,
            'x':0.5,
            'xanchor':'center',
            'y':1,
            'yanchor':'bottom'},
    xaxis={'tickangle':-90,
           'tickfont_size':14,
           'fixedrange':True,
           'showgrid':False},
    yaxis={'title':{'text':'<b>Stops per Hour</b>',
                    'font_size':16},
           'tickfont_size':14,
           'rangemode':'nonnegative',
           'fixedrange':True,
           'showgrid':False},
    hoverlabel={'font_size':14},
    font={'family':'Arial',
          'color':'black'},
    dragmode=False,
    hovermode='x unified',
    )
fig.write_html('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/service.html',
               include_plotlyjs='cdn',
               config={'displaylogo':False,'modeBarButtonsToRemove':['select2d','lasso2d']})








