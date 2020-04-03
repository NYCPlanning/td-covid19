import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/TURNSTILE/'
path='/home/mayijun/TURNSTILE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float})
rt=pd.read_csv(path+'RemoteTime.csv',dtype=str)



# Download data
dl=datetime.datetime(2020,4,4)
for i in range(0,300):
    dl=dl-datetime.timedelta(days=7)
    url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt'
    req=urllib.request.urlopen(url)
    file = open(path+'DATA/'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt', "wb")
    shutil.copyfileobj(req,file)
    file.close()


#
## Clean Entries based on Unit-C/A-SCP
##ucsentry=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
##ucsentry=tpunit[tpunit['id']=='R018|N324|00-06-03'].reset_index(drop=True)
##ucsentry=tpunit[tpunit['id']=='R158|N335|01-00-00'].reset_index(drop=True)
##ucsentry=tpunit[tpunit['id']=='R208|R529|00-00-01'].reset_index(drop=True)
##ucsentry=tpunit[tpunit['id']=='R023|N507|00-06-01'].reset_index(drop=True)
##ucsentry=tpunit[tpunit['id']=='R044|R210|00-00-01'].reset_index(drop=True)
#def unitcascpentry(ucsentry):
#    global rtunit
#    ucsentry=ucsentry.sort_values(['firstdate','firsttime']).reset_index(drop=True)
#    ucsid=ucsentry.loc[0,'id']
#    ucsentry=pd.merge(rtunit,ucsentry,how='left',on=['unit','firstdate','firsttime'])
#    ucsentry['nextdate']=np.roll(ucsentry['firstdate'],-1)
#    ucsentry['nexttime']=np.roll(ucsentry['firsttime'],-1)
#    ucsentry['nextdesc']=np.roll(ucsentry['firstdesc'],-1)
#    ucsentry['nextentries']=np.roll(ucsentry['firstentries'],-1)
#    ucsentry['time']=ucsentry['firsttime']+'-'+ucsentry['nexttime']
#    ucsentry['entries']=ucsentry['nextentries']-ucsentry['firstentries']
#    ucsentry=ucsentry[:-1].reset_index(drop=True)
#    ucsentry=ucsentry[['id','unit','firstdate','time','entries']].reset_index(drop=True)
#    ucsentry['id']=ucsid
#    ucsentry['flagtime']=np.where(pd.isna(ucsentry['entries']),1,0)
#    ucsentry['flagentry']=np.where((ucsentry['entries']<0)|(ucsentry['entries']>5000),1,0)
#    ucsentry['entries']=ucsentry['entries'].fillna(0)
#    return ucsentry
#
## Clean Exits based on Unit-C/A-SCP
#def unitcascpexit(ucsexit):
#    global rtunit
#    ucsexit=ucsexit.sort_values(['firstdate','firsttime']).reset_index(drop=True)
#    ucsid=ucsexit.loc[0,'id']
#    ucsexit=pd.merge(rtunit,ucsexit,how='left',on=['unit','firstdate','firsttime'])
#    ucsexit['nextdate']=np.roll(ucsexit['firstdate'],-1)
#    ucsexit['nexttime']=np.roll(ucsexit['firsttime'],-1)
#    ucsexit['nextdesc']=np.roll(ucsexit['firstdesc'],-1)
#    ucsexit['nextexits']=np.roll(ucsexit['firstexits'],-1)
#    ucsexit['time']=ucsexit['firsttime']+'-'+ucsexit['nexttime']
#    ucsexit['exits']=ucsexit['nextexits']-ucsexit['firstexits']
#    ucsexit=ucsexit[:-1].reset_index(drop=True)
#    ucsexit=ucsexit[['id','unit','firstdate','time','exits']].reset_index(drop=True)
#    ucsexit['id']=ucsid
#    ucsexit['flagtime']=np.where(pd.isna(ucsexit['exits']),1,0)
#    ucsexit['flagexit']=np.where((ucsexit['exits']<0)|(ucsexit['exits']>5000),1,0)
#    ucsexit['exits']=ucsexit['exits'].fillna(0)
#    return ucsexit
#
#
#
#start=datetime.datetime.now()
#
## Compile data
#tp=pd.DataFrame()
#for i in sorted(os.listdir(path+'DATA')):
#    tp=pd.concat([tp,pd.read_csv(path+'DATA/'+str(i),dtype=str)],ignore_index=True)
#tp['id']=tp['UNIT']+'|'+tp['C/A']+'|'+tp['SCP']
#tp['unit']=tp['UNIT'].copy()
#tp['firstdate']=tp['DATE'].copy()
#tp['firsttime']=tp['TIME'].copy()
#tp['firstdesc']=tp['DESC'].copy()
#tp['firstentries']=pd.to_numeric(tp['ENTRIES'])
#tp['firstexits']=pd.to_numeric(tp['EXITS                                                               '])
#tp=tp[['id','unit','firstdate','firsttime','firstdesc','firstentries','firstexits']].reset_index(drop=True)
#
## Entries
#df=pd.DataFrame()
#for i in list(rc['Remote'].unique()):
#    try:
#        tpunit=tp[tp['unit']==i].reset_index(drop=True)
#        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
#        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
#        rtlist=[rttp]
#        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
#            rttp+=4*3600
#            rtlist+=[rttp]
#        rtlist=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in rtlist]
#        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
#        rtunit['unit']=rtunit['Remote'].copy()
#        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
#        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
#        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
#        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpentry).reset_index(drop=True)
#        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
#        print(str(i)+': success')
#    except:
#        print(str(i)+': fail')
#df=df[['id','unit','firstdate','time','entries','flagtime','flagentry']].reset_index(drop=True)
#dfflagtime=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
#dfflagentry=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagentry':'sum'}).reset_index(drop=True)
#dfunitentry=df[(df['flagtime']==0)&(df['flagentry']==0)].reset_index(drop=True)
#dfunitentry=dfunitentry.groupby(['unit','firstdate','time'],as_index=False).agg({'entries':'sum','id':'count'}).reset_index(drop=True)
#dfunitentry.columns=['unit','firstdate','time','entries','gooducs']
#dfunitentry=pd.merge(dfunitentry,dfflagtime,how='left',on=['unit','firstdate','time'])
#dfunitentry=pd.merge(dfunitentry,dfflagentry,how='left',on=['unit','firstdate','time'])
#dfunitentry.to_csv(path+'dfunitentry.csv',index=False)
#dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#dfdateentry=dfunitentry.groupby('firstdate',as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
#dfdateentry['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
#dfdateentry=dfdateentry.sort_values('firstdate').reset_index(drop=True)
#dfdateentry['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateentry['firstdate']]
#dfdateentry.to_csv(path+'dfdateentry.csv',index=False)
#dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#dfwk=pd.DataFrame()
#dfwk['firstdate']=sorted([datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitentry['firstdate'].unique()])
#dfwk['firstdate']=[x.strftime('%m/%d/%Y') for x in dfwk['firstdate']]
#dfwk['weekid']=np.repeat(list(range(1,int(len(dfwk)/7)+2)),7)[0:len(dfwk)]
#dfwk['weekfirstdate']=np.repeat(list(dfwk.drop_duplicates('weekid',keep='first')['firstdate']),7)[0:len(dfwk)]
#dfunitwkentry=pd.merge(dfunitentry,dfwk,how='left',on='firstdate')
#dfunitwkentry=dfunitwkentry.groupby(['unit','weekid','weekfirstdate'],as_index=False).agg({'entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
#dfunitwkentry.to_csv(path+'dfunitwkentry.csv',index=False)
#
#
#
## Exits
#df=pd.DataFrame()
#for i in list(rc['Remote'].unique()):
#    try:
#        tpunit=tp[tp['unit']==i].reset_index(drop=True)
#        rtunit=rt[rt['Remote']==i].reset_index(drop=True)
#        rttp=pytz.timezone('America/New_York').localize(datetime.datetime.strptime(tpunit['firstdate'].unique()[0]+' '+rtunit.loc[0,'Time'],'%m/%d/%Y %H:%M:%S')).timestamp()
#        rtlist=[rttp]
#        for j in range(0,len(tpunit['firstdate'].unique())*6-1):
#            rttp+=4*3600
#            rtlist+=[rttp]
#        rtlist=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('America/New_York')) for x in rtlist]
#        rtunit=pd.concat([rtunit]*len(rtlist),ignore_index=True)
#        rtunit['unit']=rtunit['Remote'].copy()
#        rtunit['firstdate']=[x.strftime('%m/%d/%Y') for x in rtlist]
#        rtunit['firsttime']=[x.strftime('%H:%M:%S') for x in rtlist]
#        rtunit=rtunit[['unit','firstdate','firsttime']].reset_index(drop=True)
#        tpucs=tpunit.groupby('id',as_index=False).apply(unitcascpexit).reset_index(drop=True)
#        df=pd.concat([df,tpucs],axis=0,ignore_index=True)
#        print(str(i)+': success')
#    except:
#        print(str(i)+': fail')
#df=df[['id','unit','firstdate','time','exits','flagtime','flagexit']].reset_index(drop=True)
#dfflagtime=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagtime':'sum'}).reset_index(drop=True)
#dfflagexit=df.groupby(['unit','firstdate','time'],as_index=False).agg({'flagexit':'sum'}).reset_index(drop=True)
#dfunitexit=df[(df['flagtime']==0)&(df['flagexit']==0)].reset_index(drop=True)
#dfunitexit=dfunitexit.groupby(['unit','firstdate','time'],as_index=False).agg({'exits':'sum','id':'count'}).reset_index(drop=True)
#dfunitexit.columns=['unit','firstdate','time','exits','gooducs']
#dfunitexit=pd.merge(dfunitexit,dfflagtime,how='left',on=['unit','firstdate','time'])
#dfunitexit=pd.merge(dfunitexit,dfflagexit,how='left',on=['unit','firstdate','time'])
#dfunitexit.to_csv(path+'dfunitexit.csv',index=False)
#dfunitexit=pd.read_csv(path+'dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
#dfdateexit=dfunitexit.groupby('firstdate',as_index=False).agg({'exits':'sum'}).reset_index(drop=True)
#dfdateexit['firstdate']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
#dfdateexit=dfdateexit.sort_values('firstdate').reset_index(drop=True)
#dfdateexit['firstdate']=[datetime.datetime.strftime(x,'%m/%d/%Y') for x in dfdateexit['firstdate']]
#dfdateexit.to_csv(path+'dfdateexit.csv',index=False)
#dfunitexit=pd.read_csv(path+'dfunitexit.csv',dtype=str,converters={'exits':float,'gooducs':float,'flagtime':float,'flagexit':float})
#dfwk=pd.DataFrame()
#dfwk['firstdate']=sorted([datetime.datetime.strptime(x,'%m/%d/%Y') for x in dfunitexit['firstdate'].unique()])
#dfwk['firstdate']=[x.strftime('%m/%d/%Y') for x in dfwk['firstdate']]
#dfwk['weekid']=np.repeat(list(range(1,int(len(dfwk)/7)+2)),7)[0:len(dfwk)]
#dfwk['weekfirstdate']=np.repeat(list(dfwk.drop_duplicates('weekid',keep='first')['firstdate']),7)[0:len(dfwk)]
#dfunitwkexit=pd.merge(dfunitexit,dfwk,how='left',on='firstdate')
#dfunitwkexit=dfunitwkexit.groupby(['unit','weekid','weekfirstdate'],as_index=False).agg({'exits':'sum','gooducs':'sum','flagtime':'sum','flagexit':'sum'}).reset_index(drop=True)
#dfunitwkexit.to_csv(path+'dfunitwkexit.csv',index=False)
#
#print(datetime.datetime.now()-start)
## 80 mins
#
#
#
## Validation
#turnstile=pd.read_csv(path+'dfunitwkentry.csv',dtype=float,converters={'unit':str,'weekfirstdate':str})
#fare=pd.read_csv('C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/FARE/fare.csv',dtype=str,converters={'fare':float})
#fare['weekfirstdate']=[str(x)[0:10] for x in fare['week']]
#unitwkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
#unitwkvld=unitwkvld[['unit','weekid','week','weekfirstdate','fare','entries','gooducs','flagtime','flagentry']].sort_values(['unit','weekid']).reset_index(drop=True)
#unitwkvld['diff']=unitwkvld['entries']-unitwkvld['fare']
#unitwkvld['diffpct']=unitwkvld['diff']/unitwkvld['fare']
#unitwkvld.to_csv(path+'unitwkvld.csv',index=False)
#wkvld=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
#wkvld=wkvld.groupby(['weekid','week','weekfirstdate'],as_index=False).agg({'fare':'sum','entries':'sum','gooducs':'sum','flagtime':'sum','flagentry':'sum'}).reset_index(drop=True)
#wkvld=wkvld.sort_values('weekid').reset_index(drop=True)
#wkvld['diff']=wkvld['entries']-wkvld['fare']
#wkvld['diffpct']=wkvld['diff']/wkvld['fare']
#wkvld.to_csv(path+'wkvld.csv',index=False)
#
#
#
## Comparison
#turnstile=pd.read_csv(path+'dfunitwkentry.csv',dtype=float,converters={'unit':str,'weekfirstdate':str})
#fare=pd.read_csv('C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SUBWAY/FARE/fare.csv',dtype=str,converters={'fare':float})
#fare['weekfirstdate']=[str(x)[0:10] for x in fare['week']]
#cplxwk=pd.merge(fare,turnstile,how='left',on=['unit','weekfirstdate'])
#cplxwk=cplxwk[['unit','weekid','week','weekfirstdate','fare','entries']].sort_values(['unit','weekid']).reset_index(drop=True)
#cplxwk=pd.merge(cplxwk,rc,how='left',left_on='unit',right_on='Remote')
#cplxwkpre=cplxwk[np.isin(cplxwk['weekid'],range(106,114))].reset_index(drop=True)
#cplxwkpre=cplxwkpre.groupby(['CplxID'],as_index=False).agg({'fare':'mean','entries':'mean'}).reset_index(drop=True)
#cplxwkpre.columns=['CplxID','prefare','preentries']
#cplxwkpost=cplxwk[cplxwk['weekid']==116].reset_index(drop=True)
#cplxwkpost=cplxwkpost.groupby(['CplxID'],as_index=False).agg({'fare':'mean','entries':'mean'}).reset_index(drop=True)
#cplxwkpost.columns=['CplxID','postfare','postentries']
#cplxwkdiff=pd.merge(cplxwkpre,cplxwkpost,how='inner',on='CplxID')
#cplxwkdiff['farediff']=cplxwkdiff['postfare']-cplxwkdiff['prefare']
#cplxwkdiff['farediffpct']=cplxwkdiff['farediff']/cplxwkdiff['prefare']
#cplxwkdiff['entriesdiff']=cplxwkdiff['postentries']-cplxwkdiff['preentries']
#cplxwkdiff['entriesdiffpct']=cplxwkdiff['entriesdiff']/cplxwkdiff['preentries']
#cplxwkdiff=pd.merge(cplxwkdiff,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
#cplxwkdiff=cplxwkdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','prefare','postfare','farediff','farediffpct','preentries','postentries','entriesdiff','entriesdiffpct']].reset_index(drop=True)
#cplxwkdiff.to_csv(path+'cplxwkdiff.csv',index=False)
#
#dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
##predates=['01/06/2020','01/07/2020','01/08/2020','01/09/2020','01/10/2020','01/13/2020','01/14/2020','01/15/2020','01/16/2020','01/17/2020',
##          '01/21/2020','01/22/2020','01/23/2020','01/24/2020','01/27/2020','01/28/2020','01/29/2020','01/30/2020','01/31/2020',
##          '02/03/2020','02/04/2020','02/05/2020','02/06/2020','02/07/2020','02/10/2020','02/11/2020','02/12/2020','02/13/2020','02/14/2020',
##          '02/18/2020','02/19/2020','02/20/2020','02/21/2020','02/24/2020','02/25/2020','02/26/2020','02/27/2020','02/28/2020']
#predates=['03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019','03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019']
#postdates=['03/16/2020','03/17/2020','03/18/2020','03/19/2020','03/20/2020','03/23/2020','03/24/2020','03/25/2020','03/26/2020']
#amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
#cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
#cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
#cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
#cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(x),'entries':'sum'}).reset_index(drop=True)
#cplxampre.columns=['CplxID','PreTime','PreEntries']
#cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
#cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
#cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
#cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
#cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(x),'entries':'sum'}).reset_index(drop=True)
#cplxampost.columns=['CplxID','PostTime','PostEntries']
#cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
#cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
#cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
#cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
#cplxamdiff=pd.merge(cplxamdiff,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
#cplxamdiff=cplxamdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','PreEntries','PostEntries','Diff','DiffPct']].reset_index(drop=True)
#cplxamdiff.to_csv(path+'cplxamdiff.csv',index=False)
#
#
#dfunitentry=pd.read_csv(path+'dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
#predates=['03/18/2019','03/19/2019','03/20/2019','03/21/2019','03/22/2019','03/25/2019','03/26/2019','03/27/2019','03/28/2019','03/29/2019']
#postdates=['03/16/2020','03/17/2020','03/18/2020','03/19/2020','03/20/2020','03/23/2020','03/24/2020','03/25/2020','03/26/2020']
#period1=['01:00:00-05:00:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
#period2=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
#period3=['09:00:00-13:00:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
#period4=['13:00:00-17:00:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
#period5=['17:00:00-21:00:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00']
#period6=['21:00:00-01:00:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00']
#period1=pd.DataFrame(period1,columns=['timeperiod'])
#period1['periodid']=1
#period2=pd.DataFrame(period2,columns=['timeperiod'])
#period2['periodid']=2
#period3=pd.DataFrame(period3,columns=['timeperiod'])
#period3['periodid']=3
#period4=pd.DataFrame(period4,columns=['timeperiod'])
#period4['periodid']=4
#period5=pd.DataFrame(period5,columns=['timeperiod'])
#period5['periodid']=5
#period6=pd.DataFrame(period6,columns=['timeperiod'])
#period6['periodid']=6
#periodlist=pd.concat([period1,period2,period3,period4,period5,period6],ignore_index=True)
#pdpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
#pdpre=pd.merge(pdpre,periodlist,how='left',left_on='time',right_on='timeperiod')
#pdpre=pdpre.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#pdpre.columns=['Date','Period','PreEntries']
#pdpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
#pdpost=pd.merge(pdpost,periodlist,how='left',left_on='time',right_on='timeperiod')
#pdpost=pdpost.groupby(['firstdate','periodid'],as_index=False).agg({'entries':'sum'}).reset_index(drop=True)
#pdpost.columns=['Date','Period','PostEntries']
#pddiff=pd.concat([pdpre,pdpost],ignore_index=True)
#pddiff.to_csv(path+'pddiff.csv',index=False)
#
