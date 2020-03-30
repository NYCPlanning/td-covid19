import pandas as pd
import numpy as np
import datetime
import pytz


pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/TURNSTILE/'


rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)



#ucs=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
#ucs=tpunit[tpunit['id']=='R158|N335|01-00-00'].reset_index(drop=True)
#ucs=tpunit[tpunit['id']=='R208|R529|00-00-01'].reset_index(drop=True)
#ucs=tpunit[tpunit['id']=='R023|N507|00-06-01'].reset_index(drop=True)
def unitcascp(ucs):
    global rtunit
    ucs=ucs.sort_values(['firstdate','firsttime']).reset_index(drop=True)
    ucs=pd.merge(rtunit,ucs,how='left',on=['unit','firstdate','firsttime'])
    ucs['nextdate']=np.roll(ucs['firstdate'],-1)
    ucs['nexttime']=np.roll(ucs['firsttime'],-1)
    ucs['nextdesc']=np.roll(ucs['firstdesc'],-1)
    ucs['nextentries']=np.roll(ucs['firstentries'],-1)
    ucs['time']=ucs['firsttime']+'-'+ucs['nexttime']
    ucs['entries']=ucs['nextentries']-ucs['firstentries']
    ucs=ucs[:-1].reset_index(drop=True)
    ucs=ucs[['id','unit','firstdate','time','entries']].reset_index(drop=True)
    ucs['id']=ucs.loc[pd.notna(ucs['id']),'id'].unique()[0]
    ucs['flagtime']=np.where(pd.isna(ucs['entries']),1,0)
    ucs['flagentry']=np.where((ucs['entries']<0)|(ucs['entries']>5000),1,0)
    ucs['entries']=ucs['entries'].fillna(0)
    return ucs



tp=pd.DataFrame()
#for i in ['200307','200314','200321','200328']:
#    tp=pd.concat([tp,pd.read_csv(path+'turnstile_'+str(i)+'.txt',dtype=str)],ignore_index=True)
for i in ['190302','190309','190316','190323','190330']:
    tp=pd.concat([tp,pd.read_csv(path+'turnstile_'+str(i)+'.txt',dtype=str)],ignore_index=True)
tp['id']=tp['UNIT']+'|'+tp['C/A']+'|'+tp['SCP']
tp['unit']=tp['UNIT'].copy()
tp['firstdate']=tp['DATE'].copy()
tp['firsttime']=tp['TIME'].copy()
tp['firstdesc']=tp['DESC'].copy()
tp['firstentries']=pd.to_numeric(tp['ENTRIES'])
tp=tp[['id','unit','firstdate','firsttime','firstdesc','firstentries']].reset_index(drop=True)
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    try:
        tpunit=tp[tp['unit']==i].reset_index(drop=True)
        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
        rttp=datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S').astimezone(pytz.timezone('America/New_York')).timestamp()
        rtlist=[rttp]
        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
            rttp+=4*3600
            rtlist+=[rttp]
        rtlist=[datetime.datetime.fromtimestamp(x).astimezone(pytz.timezone('America/New_York')) for x in rtlist]
        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
        rtunit['unit']=rtunit['Remote'].copy()
        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascp).reset_index(drop=True)
        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
    except:
        print(str(i))
df=df[['id','unit','firstdate','time','entries','flagtime','flagentry']].reset_index(drop=True)
df.to_csv(path+'dfid.csv',index=False)
df=df.groupby(['unit','firstdate','time'],as_index=False).agg({'id':'count','entries':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
df.to_csv(path+'dfunit.csv',index=False)



