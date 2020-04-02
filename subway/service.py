import pandas as pd
import numpy as np



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/SERVICE/'



calendar=pd.read_csv(path+'google_transit/calendar.txt')
trips=pd.read_csv(path+'google_transit/trips.txt')
stoptimes=pd.read_csv(path+'google_transit/stop_times.txt')
tripfirsttime=stoptimes.drop_duplicates('trip_id',keep='first').reset_index(drop=True)
tripfirsttime['hour']=[str(x)[0:2] for x in tripfirsttime['arrival_time']]
triplasttime=stoptimes.drop_duplicates('trip_id',keep='last').reset_index(drop=True)
triplasttime['hour']=[str(x)[0:2] for x in triplasttime['arrival_time']]



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

