import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz
import geopandas as gpd
import shapely



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/COMPARE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float,'Hub':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)



# # Download data
# dl=datetime.datetime(2019,1,5)
# for i in range(0,60):
#     dl=dl-datetime.timedelta(days=7)
#     url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt'
#     req=urllib.request.urlopen(url)
#     file = open(path+'data/'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt', "wb")
#     shutil.copyfileobj(req,file)
#     file.close()



# Clean Entries based on Unit-C/A-SCP
# ucsentry=tpunit[tpunit['id']=='R104|N207|00-00-00'].reset_index(drop=True)
def unitcascpentry(ucsentry):
    global rtunit
    ucsentry=ucsentry.reset_index(drop=True)
    ucsid=ucsentry.loc[0,'id']
    ucsentry=pd.merge(rtunit,ucsentry,how='left',on=['unit','firstdate','firsttime'])
    ucsentry['nextdate']=np.roll(ucsentry['firstdate'],-1)
    ucsentry['nexttime']=np.roll(ucsentry['firsttime'],-1)
    ucsentry['nextdesc']=np.roll(ucsentry['firstdesc'],-1)
    ucsentry['nextentries']=np.roll(ucsentry['firstentries'],-1)
    ucsentry['time']=ucsentry['firsttime']+'-'+ucsentry['nexttime']
    ucsentry['entries']=abs(ucsentry['nextentries']-ucsentry['firstentries'])
    ucsentry=ucsentry[:-1].reset_index(drop=True)
    ucsentry=ucsentry[['id','unit','firstdate','time','entries']].reset_index(drop=True)
    ucsentry['id']=ucsid
    ucsentry['flagtime']=np.where(pd.isna(ucsentry['entries']),1,0)
    ucsentry['flagentry']=np.where((ucsentry['entries']<0)|(ucsentry['entries']>5000),1,0)
    ucsentry['entries']=ucsentry['entries'].fillna(0)
    return ucsentry



start=datetime.datetime.now()

# Compile data
tp=pd.DataFrame()
for i in sorted(os.listdir(path+'data')):
    tp=pd.concat([tp,pd.read_csv(path+'data/'+str(i),dtype=str)],ignore_index=True)
tp['id']=tp['UNIT']+'|'+tp['C/A']+'|'+tp['SCP']
tp['unit']=tp['UNIT'].copy()
tp['firstdate']=tp['DATE'].copy()
tp['firsttime']=tp['TIME'].copy()
tp['firstdesc']=tp['DESC'].copy()
tp['firstentries']=pd.to_numeric(tp['ENTRIES'])
tp['firstexits']=pd.to_numeric(tp['EXITS                                                               '])
tp=tp[['id','unit','firstdate','firsttime','firstdesc','firstentries','firstexits']].reset_index(drop=True)

# Entries
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    try:
        tpunit=tp[tp['unit']==i].reset_index(drop=True)
        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tp['firstdate'].unique())*6-1):
            rttp+=4*3600
            rtlist+=[rttp]
        rtlist=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in rtlist]
        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
        rtunit['unit']=rtunit['Remote'].copy()
        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpentry).reset_index(drop=True)
        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
        print(str(i)+': success')
    except:
        print(str(i)+': fail')
df=df[['id','unit','firstdate','time','entries','flagtime','flagentry']].reset_index(drop=True)
dfflagtime=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
dfflagentry=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagentry':'sum'}).reset_index(drop=True)
dfunitentry=df[(df['flagtime']==0)&(df['flagentry']==0)].reset_index(drop=True)
dfunitentry=dfunitentry.groupby(['unit','firstdate','time'],as_index=False).agg({'entries':'sum','id':'count'}).reset_index(drop=True)
dfunitentry.columns=['unit','firstdate','time','entries','gooducs']
dfunitentry=pd.merge(dfunitentry,dfflagtime,how='left',on=['unit','firstdate','time'])
dfunitentry=pd.merge(dfunitentry,dfflagentry,how='left',on=['unit','firstdate','time'])
dfunitentry.to_csv(path+'dfunitentry.csv',index=False)

dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
dfdateentry=dfunitentry.groupby(['unit','firstdate'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
dfdateentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
dfdateentry['year']=[x.year for x in dfdateentry['firstdate']]
dfdateentry['weekday']=[x.dayofweek for x in dfdateentry['firstdate']]
dfdateentry=dfdateentry[(dfdateentry['year']==2018)&(np.isin(dfdateentry['weekday'],[0,1,2,3,4]))].reset_index(drop=True)
dfdateentry=dfdateentry.groupby(['unit'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
dfdateentry=pd.merge(dfdateentry,rc,how='inner',left_on='unit',right_on='Remote')
dfdateentry=dfdateentry.groupby(['CplxID'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
dfdateentry.to_csv(path+'dfdateentry.csv',index=False)

print(datetime.datetime.now()-start)
# 40 mins

