import pandas as pd
import numpy as np



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/TURNSTILE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)

#ucs=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
#ucs=tpunit[tpunit['id']=='R158|N335|01-00-00'].reset_index(drop=True)
def unitcascp(ucs):
    global rtunit
    ucs=ucs.sort_values(['firstdate','firsttime']).reset_index(drop=True)
    ucs['nextdate']=np.roll(ucs['firstdate'],-1)
    ucs['nexttime']=np.roll(ucs['firsttime'],-1)
    ucs['nextdesc']=np.roll(ucs['firstdesc'],-1)
    ucs['nextentries']=np.roll(ucs['firstentries'],-1)
    ucs['nextexits']=np.roll(ucs['firstexits'],-1)
    ucs['time']=ucs['firsttime']+'-'+ucs['nexttime']
    ucs['entries']=ucs['nextentries']-ucs['firstentries']
    ucs['exits']=ucs['nextexits']-ucs['firstexits']
    ucs=ucs[:-1].reset_index(drop=True)
    ucs=ucs[['id','unit','firstdate','time','entries','exits']].reset_index(drop=True)
    ucs=pd.merge(ucs,rtunit,how='outer',left_on='time',right_on='Time').sort_values(['firstdate','time']).reset_index(drop=True)
    ucs=ucs[pd.notna(ucs['Remote'])&pd.notna(ucs['id'])].reset_index(drop=True)
    ucsflag=ucs.groupby('firstdate',as_index=False).agg({'id':'count'}).reset_index(drop=True)
    ucsflag['flag']=np.where(ucsflag['id']==6,0,1)
    ucsflag=ucsflag[['firstdate','flag']].reset_index(drop=True)
    ucs=pd.merge(ucs,ucsflag,how='left',on='firstdate')
    ucs=ucs[['id','unit','firstdate','time','entries','exits','flag']].reset_index(drop=True)
    # Another flag on negative and suspicious numbers (>5000)
    return ucs


tp=pd.read_csv(path+'turnstile_200321.txt',dtype=str)
#tp=pd.read_csv(path+'turnstile_200314.txt',dtype=str)
tp['id']=tp['UNIT']+'|'+tp['C/A']+'|'+tp['SCP']
tp['unit']=tp['UNIT'].copy()
tp['firstdate']=tp['DATE'].copy()
tp['firsttime']=tp['TIME'].copy()
tp['firstdesc']=tp['DESC'].copy()
tp['firstentries']=pd.to_numeric(tp['ENTRIES'])
tp['firstexits']=pd.to_numeric(tp['EXITS                                                               '])
tp=tp[['id','unit','firstdate','firsttime','firstdesc','firstentries','firstexits']].reset_index(drop=True)
df=pd.DataFrame()
for i in list(rc['Remote'].unique()):
    rtunit=rt[rt['Remote']==i].reset_index(drop=True)
    tpunit=tp[tp['unit']==i].reset_index(drop=True)
    tpucs=tpunit.groupby('id',as_index=False).apply(unitcascp).reset_index(drop=True)
#    tpucs=tpucs.groupby(['unit','firstdate','time'],as_index=False).agg({'entries':'sum','exits':'sum','flag':'sum'}).reset_index(drop=True)
    df=pd.concat([df,tpucs],axis=0,ignore_index=True)
df.to_csv(path+'df.csv',index=False)









