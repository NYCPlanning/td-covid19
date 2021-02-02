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
path='C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/'


df=gpd.read_file(path+'fhv/PercentChange_NTA_April.geojson')
df.crs='epsg:4326'

df['perc'].describe(percentiles=np.arange(0.2,1,0.2))
df['DiffPctCat']=np.where(df['perc']>-0.65,'>-65%',
          np.where(df['perc']>-0.7,'-69%~-65%',
          np.where(df['perc']>-0.75,'-74%~-70%',
           np.where(df['perc']>-0.8,'-79%~-75%',
         '<=-80%'))))
df['Average2019'].describe(percentiles=np.arange(0.2,1,0.2))

df.to_file(path+'fhv/fhv.geojson',driver='GeoJSON')



















# PM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E201909','E202009']
cplxpmhed=cplxpmdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E202004','E202006']
cplxpmhed=pd.merge(cplxpmhed,cplxpmdiffnta,how='inner',on='NTACode')
cplxpmhed['Diff1']=cplxpmhed['E202009']-cplxpmhed['E201909']
cplxpmhed['DiffPct1']=cplxpmhed['Diff1']/cplxpmhed['E201909']
cplxpmhed['Diff2']=cplxpmhed['E202009']-cplxpmhed['E202004']
cplxpmhed['DiffPct2']=cplxpmhed['Diff2']/cplxpmhed['E202004']
cplxpmhed['Diff3']=cplxpmhed['E202009']-cplxpmhed['E202006']
cplxpmhed['DiffPct3']=cplxpmhed['Diff3']/cplxpmhed['E202006']
cplxpmhed=pd.merge(nta,cplxpmhed,how='inner',on='NTACode')
cplxpmhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat1']=np.where(cplxpmhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxpmhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxpmhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxpmhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxpmhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat2']=np.where(cplxpmhed['DiffPct2']<=2,'<=200%',
          np.where(cplxpmhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxpmhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxpmhed['DiffPct2']<=3.5,'301%~350%',
          '>350%'))))
cplxpmhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat3']=np.where(cplxpmhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxpmhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxpmhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxpmhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxpmhed['Pct']=cplxpmhed['E202009']/cplxpmhed['E201909']
cplxpmhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['PctCat']=np.where(cplxpmhed['Pct']<=0.25,'16%~25%',
                    np.where(cplxpmhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxpmhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxpmhed['Pct']<=0.4,'36%~40%',
                             '41%~55%'))))
cplxpmhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/ntapmhed.geojson',driver='GeoJSON')









# # Commuter Profile
# # AM Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# nadirdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# latestdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201904']
# cplxamnadir=dfunitentry[np.isin(dfunitentry['firstdate'],nadirdates)].reset_index(drop=True)
# cplxamnadir=cplxamnadir[np.isin(cplxamnadir['time'],amlist)].reset_index(drop=True)
# cplxamnadir=cplxamnadir.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamnadir=pd.merge(cplxamnadir,rc,how='left',left_on='unit',right_on='Remote')
# cplxamnadir=cplxamnadir.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamnadir.columns=['CplxID','NadirTime','E201909']
# cplxamlatest=dfunitentry[np.isin(dfunitentry['firstdate'],latestdates)].reset_index(drop=True)
# cplxamlatest=cplxamlatest[np.isin(cplxamlatest['time'],amlist)].reset_index(drop=True)
# cplxamlatest=cplxamlatest.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxamlatest=pd.merge(cplxamlatest,rc,how='left',left_on='unit',right_on='Remote')
# cplxamlatest=cplxamlatest.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxamlatest.columns=['CplxID','LatestTime','E202004']
# cplxamcp=pd.merge(cplxampre,cplxamnadir,how='inner',on='CplxID')
# cplxamcp=pd.merge(cplxamcp,cplxamlatest,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201904','E201909','E202004']].reset_index(drop=True)
# predates=['06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202006']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202009']
# cplxamcp=pd.merge(cplxamcp,cplxampre,how='inner',on='CplxID')
# cplxamcp=pd.merge(cplxamcp,cplxampost,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Time','E201904','E201909','E202004','E202006','E202009']].reset_index(drop=True)
# cplxamcp['Diff1']=cplxamcp['E202004']-cplxamcp['E201904']
# cplxamcp['DiffPct1']=cplxamcp['Diff1']/cplxamcp['E201904']
# cplxamcp['Diff2']=cplxamcp['E202006']-cplxamcp['E202004']
# cplxamcp['DiffPct2']=cplxamcp['Diff2']/cplxamcp['E202004']
# cplxamcp['Diff3']=cplxamcp['E202009']-cplxamcp['E202004']
# cplxamcp['DiffPct3']=cplxamcp['Diff3']/cplxamcp['E202004']
# cplxamcp['Diff4']=cplxamcp['E202009']-cplxamcp['E201909']
# cplxamcp['DiffPct4']=cplxamcp['Diff4']/cplxamcp['E201909']
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201904','E201909',
#                    'E202004','E202006','E202009','Diff1','DiffPct1','Diff2','DiffPct2','Diff3','DiffPct3',
#                    'Diff4','DiffPct4',]].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file(path+'OUTPUT/cplxamcp.shp')



# Commuter Profile
# AM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# predates=['12/03/2019','12/04/2019','12/05/2019','12/06/2019',
#           '12/09/2019','12/10/2019','12/11/2019','12/12/2019','12/13/2019',
#           '12/16/2019','12/17/2019','12/18/2019','12/19/2019','12/20/2019',
#           '12/23/2019','12/26/2019','12/27/2019',
#           '12/30/2019','12/31/2019','01/02/2020','01/03/2020']
# postdates=['12/01/2020','12/02/2020','12/03/2020','12/04/2020',
#            '12/07/2020','12/08/2020','12/09/2020','12/10/2020','12/11/2020',
#            '12/14/2020','12/15/2020','12/16/2020','12/17/2020','12/18/2020',
#            '12/21/2020','12/22/2020','12/23/2020',
#            '12/28/2020','12/29/2020','12/30/2020','12/31/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
                       np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
                       np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
                       np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
                       '<=-80%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.2,'8%~20%',
                   np.where(cplxamcp['Pct']<=0.4,'21%~40%',
                            '>40%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp.geojson',driver='GeoJSON')


# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/08/2020','09/09/2020','09/10/2020','09/11/2020']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202009']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E202009','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E202009']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202009']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>0.2,'>20%',
#                        np.where(cplxamcp['DiffPct']>0.15,'16%~20%',
#                        np.where(cplxamcp['DiffPct']>0.1,'11%~15%',
#                        np.where(cplxamcp['DiffPct']>0.05,'6%~10%',
#                        '<=5%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202009','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp2.geojson',driver='GeoJSON')


# PM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
postdates=['10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# predates=['12/03/2019','12/04/2019','12/05/2019','12/06/2019','12/09/2019','12/10/2019','12/11/2019','12/12/2019']
# postdates=['12/01/2020','12/02/2020','12/03/2020','12/04/2020','12/07/2020','12/08/2020','12/09/2020','12/10/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
                       np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
                       np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
                       np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
                       '<=-80%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.2,'8%~20%',
                   np.where(cplxamcp['Pct']<=0.4,'21%~40%',
                            '>40%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmcp.geojson',driver='GeoJSON')


# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/08/2020','09/09/2020','09/10/2020','09/11/2020']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E202009']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E202009','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E202009']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E202009']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>0.2,'>20%',
#                        np.where(cplxamcp['DiffPct']>0.15,'16%~20%',
#                        np.where(cplxamcp['DiffPct']>0.1,'11%~15%',
#                        np.where(cplxamcp['DiffPct']>0.05,'6%~10%',
#                        '<=5%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E202009','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmcp2.geojson',driver='GeoJSON')


# Telework capability
# tel=gpd.read_file(path+'OUTPUT/telework0.geojson')
# tel['ntacode']=tel['NTACode'].copy()
# tel['telework']=pd.to_numeric(tel['NYC Employed by Workplace and Residence_Industry_Race-Ethn_TELE Sheet1_Telework'])
# tel['cat']=np.where(tel['telework']<=0.3,'28%~30%',
#             np.where(tel['telework']<=0.35,'31%~35%',
#                         '36%~50%'))
# tel=tel[['ntacode','telework','cat','geometry']].reset_index(drop=True)
# tel.to_file(path+'OUTPUT/teleworkam.geojson',driver='GeoJSON')


# tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm0.geojson')
# tel=tel[[x not in ['MN99','BX98','BX99','BK99','QN98','QN99','SI99'] for x in tel['ntacode']]]
# tel['telework']=pd.to_numeric(tel['teleworkable_rate'])
# tel['telework'].describe(percentiles=np.arange(0.2,1,0.2))
# tel['cat']=np.where(tel['telework']<=0.2,'15%~20%',
#             np.where(tel['telework']<=0.3,'21%~30%',
#                         '31%~58%'))
# tel=tel[['ntacode','telework','cat','geometry']].reset_index(drop=True)
# tel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm.geojson',driver='GeoJSON')

# Telework Subway Commuters
telsub=pd.read_csv(path+'notel subway.csv')
telsub['telsubpct']=telsub['teleworkbySubway']/telsub['totalWorkerbySubway']
puma=gpd.read_file(path+'nypuma.shp')
puma=puma.to_crs('epsg:4326')
puma['puma']=pd.to_numeric(puma['PUMA'])
telsub=pd.merge(puma,telsub,left_on='puma',right_on='PUMA',how='inner')
telsub['telsubpct'].describe(percentiles=np.arange(0.2,1,0.2))
telsub['cat']=np.where(telsub['telsubpct']<=0.3,'22%~30%',
              np.where(telsub['telsubpct']<=0.35,'31%~35%',
                       '36%~54%'))
telsub=telsub[['puma','telsubpct','cat','geometry']].reset_index(drop=True)
telsub.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/telsubam.geojson',driver='GeoJSON')




# Scatter Plot
cplxamcp=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamcp.geojson')
cplxamcp.crs='epsg:4326'
tel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson')
tel.crs='epsg:4326'
cplxamcptel=gpd.sjoin(cplxamcp,tel,how='left',op='intersects')
cplxamcptelbf=cplxamcptel[pd.isna(cplxamcptel['ntacode'])]
cplxamcptelbf=cplxamcptelbf.drop(['index_right','ntacode','telework'],axis=1)
cplxamcptelbf=cplxamcptelbf.to_crs('epsg:6539')
cplxamcptelbf['geometry']=[x.buffer(100) for x in cplxamcptelbf['geometry']]
cplxamcptelbf=cplxamcptelbf.to_crs('epsg:4326')
cplxamcptelbf=gpd.sjoin(cplxamcptelbf,tel,how='left',op='intersects')
cplxamcptelbf=cplxamcptelbf[pd.notna(cplxamcptelbf['ntacode'])]
cplxamcptelbf=cplxamcptelbf[['CplxID','ntacode','telework']].drop_duplicates('CplxID').reset_index(drop=True)
cplxamcptel=pd.merge(cplxamcptel,cplxamcptelbf,how='left',on='CplxID')
cplxamcptel['telework']=np.where(pd.notna(cplxamcptel['telework_x']),cplxamcptel['telework_x'],cplxamcptel['telework_y'])
import plotly.io as pio
import plotly.express as px
pio.renderers.default = "browser"
fig=px.scatter(cplxamcptel,x='telework', y='DiffPct',trendline='ols',template='plotly_white',width=800,height=600)
fig.update_layout(
    xaxis_title="NTA Telework Capability",
    yaxis_title="Current subway ridership vs early September 2020 ridership",
    font=dict(
        size=15,
    )
)
fig.show()
fig.write_html('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/scatter.html',include_plotlyjs='cdn')






# Non-Telework
# Non-Telework capability by Residence Place
nontel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkam.geojson')
nontel['nontelework']=1-nontel['telework']
nontel['nontelework'].describe(percentiles=np.arange(0.2,1,0.2))
nontel['cat']=np.where(nontel['nontelework']>=0.7,'70%~85%',
              np.where(nontel['nontelework']>=0.65,'65%~69%',
                       '42%~64%'))
nontel=nontel[['ntacode','nontelework','cat','geometry']].reset_index(drop=True)
nontel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nonteleworkam.geojson',driver='GeoJSON')

# Non-Telework capability by Workplace
nontel=gpd.read_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/teleworkpm.geojson')
nontel['nontelework']=1-nontel['telework']
nontel['nontelework'].describe(percentiles=np.arange(0.2,1,0.2))
nontel['cat']=np.where(nontel['nontelework']>=0.7,'70%~85%',
              np.where(nontel['nontelework']>=0.65,'65%~69%',
                       '42%~64%'))
nontel=nontel[['ntacode','nontelework','cat','geometry']].reset_index(drop=True)
nontel.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nonteleworkpm.geojson',driver='GeoJSON')







# Non-Telework Subway Commuters
nontelsub=pd.read_csv(path+'notel subway.csv')
nontelsub['notelsubpct']=nontelsub['nonTeleSubway']/nontelsub['totalWorkerbySubway']
puma=gpd.read_file(path+'nypuma.shp')
puma=puma.to_crs('epsg:4326')
puma['puma']=pd.to_numeric(puma['PUMA'])
nontelsub=pd.merge(puma,nontelsub,left_on='puma',right_on='PUMA',how='inner')
nontelsub['notelsubpct'].describe(percentiles=np.arange(0.2,1,0.2))
nontelsub['cat']=np.where(nontelsub['notelsubpct']>=0.7,'70%~78%',
              np.where(nontelsub['notelsubpct']>=0.65,'65%~69%',
                       '46%~64%'))
nontelsub=nontelsub[['puma','notelsubpct','cat','geometry']].reset_index(drop=True)
nontelsub.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nontelsubam.geojson',driver='GeoJSON')





# # Weekday Off Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# dtoplist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
#           '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
# ntoplist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
#           '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00',
#           '21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
#           '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00',
#           '01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
#           '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']
# oplist=dtoplist+ntoplist
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# postdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],oplist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','OP201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],oplist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','OP201909']
# cplxop=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxop['Time']=cplxop['PreTime'].copy()
# cplxop=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxop,how='left',on='CplxID')
# cplxop=cplxop[['CplxID','CplxLat','CplxLong','OP201904','OP201909']].reset_index(drop=True)
# cplxop=gpd.GeoDataFrame(cplxop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxop['CplxLong'],cplxop['CplxLat'])],crs='epsg:4326')
# cplxop=cplxop.to_crs('epsg:6539')
# cplxop['geometry']=cplxop.buffer(2640)
# cplxop=cplxop.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxop=gpd.sjoin(nta,cplxop,how='left',op='intersects')
# cplxop=cplxop.groupby(['NTACode'],as_index=False).agg({'OP201904':'sum','OP201909':'sum'}).reset_index(drop=True)
# cplxop=cplxop[cplxop['OP201904']!=0].reset_index(drop=True)
# cplxop=cplxop[cplxop['OP201909']!=0].reset_index(drop=True)
# cplxop.columns=['NTACode','OP201904','OP201909']
# cplxopnta=cplxop.copy()
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],oplist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','OP202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],oplist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','OP202009']
# cplxop=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxop['Time']=cplxop['PreTime'].copy()
# cplxop=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxop,how='left',on='CplxID')
# cplxop=cplxop[['CplxID','CplxLat','CplxLong','OP202004','OP202009']].reset_index(drop=True)
# cplxop=gpd.GeoDataFrame(cplxop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxop['CplxLong'],cplxop['CplxLat'])],crs='epsg:4326')
# cplxop=cplxop.to_crs('epsg:6539')
# cplxop['geometry']=cplxop.buffer(2640)
# cplxop=cplxop.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxop=gpd.sjoin(nta,cplxop,how='left',op='intersects')
# cplxop=cplxop.groupby(['NTACode'],as_index=False).agg({'OP202004':'sum','OP202009':'sum'}).reset_index(drop=True)
# cplxop=cplxop[cplxop['OP202004']!=0].reset_index(drop=True)
# cplxop=cplxop[cplxop['OP202009']!=0].reset_index(drop=True)
# cplxop.columns=['NTACode','OP202004','OP202009']
# cplxopnta=pd.merge(cplxopnta,cplxop,how='inner',on='NTACode')
# cplxopnta['OPDiff1']=cplxopnta['OP202004']-cplxopnta['OP201904']
# cplxopnta['OPDiffPct1']=cplxopnta['OPDiff1']/cplxopnta['OP201904']
# cplxopnta['OPDiff2']=cplxopnta['OP202009']-cplxopnta['OP201909']
# cplxopnta['OPDiffPct2']=cplxopnta['OPDiff2']/cplxopnta['OP201909']
# cplxopnta['OPDiff3']=cplxopnta['OP202009']-cplxopnta['OP202004']
# cplxopnta['OPDiffPct3']=cplxopnta['OPDiff3']/cplxopnta['OP202004']
# cplxopnta=pd.merge(nta,cplxopnta,how='inner',on='NTACode')
# cplxopnta.to_file(path+'OUTPUT/cplxopnta.shp')

# cplxopnta=cplxopnta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxopnta['NTACode']]]
# (sum(cplxopnta['OP202009'])-sum(cplxopnta['OP201909']))/sum(cplxopnta['OP201909'])



# # Weekend
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['04/13/2019','04/14/2019']
# postdates=['08/31/2019','09/01/2019','09/07/2019','09/08/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','WK201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','WK201909']
# cplxwk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxwk['Time']=cplxwk['PreTime'].copy()
# cplxwk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxwk,how='left',on='CplxID')
# cplxwk=cplxwk[['CplxID','CplxLat','CplxLong','WK201904','WK201909']].reset_index(drop=True)
# cplxwk=gpd.GeoDataFrame(cplxwk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxwk['CplxLong'],cplxwk['CplxLat'])],crs='epsg:4326')
# cplxwk=cplxwk.to_crs('epsg:6539')
# cplxwk['geometry']=cplxwk.buffer(2640)
# cplxwk=cplxwk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxwk=gpd.sjoin(nta,cplxwk,how='left',op='intersects')
# cplxwk=cplxwk.groupby(['NTACode'],as_index=False).agg({'WK201904':'sum','WK201909':'sum'}).reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK201904']!=0].reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK201909']!=0].reset_index(drop=True)
# cplxwk.columns=['NTACode','WK201904','WK201909']
# cplxwknta=cplxwk.copy()
# predates=['04/18/2020','04/19/2020']
# postdates=['08/29/2020','08/30/2020','09/05/2020','09/06/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','WK202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','WK202009']
# cplxwk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxwk['Time']=cplxwk['PreTime'].copy()
# cplxwk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxwk,how='left',on='CplxID')
# cplxwk=cplxwk[['CplxID','CplxLat','CplxLong','WK202004','WK202009']].reset_index(drop=True)
# cplxwk=gpd.GeoDataFrame(cplxwk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxwk['CplxLong'],cplxwk['CplxLat'])],crs='epsg:4326')
# cplxwk=cplxwk.to_crs('epsg:6539')
# cplxwk['geometry']=cplxwk.buffer(2640)
# cplxwk=cplxwk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxwk=gpd.sjoin(nta,cplxwk,how='left',op='intersects')
# cplxwk=cplxwk.groupby(['NTACode'],as_index=False).agg({'WK202004':'sum','WK202009':'sum'}).reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK202004']!=0].reset_index(drop=True)
# cplxwk=cplxwk[cplxwk['WK202009']!=0].reset_index(drop=True)
# cplxwk.columns=['NTACode','WK202004','WK202009']
# cplxwknta=pd.merge(cplxwknta,cplxwk,how='inner',on='NTACode')
# cplxwknta['WKDiff1']=cplxwknta['WK202004']-cplxwknta['WK201904']
# cplxwknta['WKDiffPct1']=cplxwknta['WKDiff1']/cplxwknta['WK201904']
# cplxwknta['WKDiff2']=cplxwknta['WK202009']-cplxwknta['WK201909']
# cplxwknta['WKDiffPct2']=cplxwknta['WKDiff2']/cplxwknta['WK201909']
# cplxwknta['WKDiff3']=cplxwknta['WK202009']-cplxwknta['WK202004']
# cplxwknta['WKDiffPct3']=cplxwknta['WKDiff3']/cplxwknta['WK202004']
# cplxwknta=pd.merge(nta,cplxwknta,how='inner',on='NTACode')
# cplxwknta.to_file(path+'OUTPUT/cplxwknta.shp')

# cplxwknta=cplxwknta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxwknta['NTACode']]]
# (sum(cplxwknta['WK202009'])-sum(cplxwknta['WK201909']))/sum(cplxwknta['WK201909'])






# # Weekday Peak
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# pklist=amlist+pmlist
# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# postdates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pklist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PK201904']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pklist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PK201909']
# cplxpk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxpk['Time']=cplxpk['PreTime'].copy()
# cplxpk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpk,how='left',on='CplxID')
# cplxpk=cplxpk[['CplxID','CplxLat','CplxLong','PK201904','PK201909']].reset_index(drop=True)
# cplxpk=gpd.GeoDataFrame(cplxpk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpk['CplxLong'],cplxpk['CplxLat'])],crs='epsg:4326')
# cplxpk=cplxpk.to_crs('epsg:6539')
# cplxpk['geometry']=cplxpk.buffer(2640)
# cplxpk=cplxpk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxpk=gpd.sjoin(nta,cplxpk,how='left',op='intersects')
# cplxpk=cplxpk.groupby(['NTACode'],as_index=False).agg({'PK201904':'sum','PK201909':'sum'}).reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK201904']!=0].reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK201909']!=0].reset_index(drop=True)
# cplxpk.columns=['NTACode','PK201904','PK201909']
# cplxpknta=cplxpk.copy()
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pklist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PK202004']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pklist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PK202009']
# cplxpk=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxpk['Time']=cplxpk['PreTime'].copy()
# cplxpk=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpk,how='left',on='CplxID')
# cplxpk=cplxpk[['CplxID','CplxLat','CplxLong','PK202004','PK202009']].reset_index(drop=True)
# cplxpk=gpd.GeoDataFrame(cplxpk,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpk['CplxLong'],cplxpk['CplxLat'])],crs='epsg:4326')
# cplxpk=cplxpk.to_crs('epsg:6539')
# cplxpk['geometry']=cplxpk.buffer(2640)
# cplxpk=cplxpk.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxpk=gpd.sjoin(nta,cplxpk,how='left',op='intersects')
# cplxpk=cplxpk.groupby(['NTACode'],as_index=False).agg({'PK202004':'sum','PK202009':'sum'}).reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK202004']!=0].reset_index(drop=True)
# cplxpk=cplxpk[cplxpk['PK202009']!=0].reset_index(drop=True)
# cplxpk.columns=['NTACode','PK202004','PK202009']
# cplxpknta=pd.merge(cplxpknta,cplxpk,how='inner',on='NTACode')
# cplxpknta['PKDiff1']=cplxpknta['PK202004']-cplxpknta['PK201904']
# cplxpknta['PKDiffPct1']=cplxpknta['PKDiff1']/cplxpknta['PK201904']
# cplxpknta['PKDiff2']=cplxpknta['PK202009']-cplxpknta['PK201909']
# cplxpknta['PKDiffPct2']=cplxpknta['PKDiff2']/cplxpknta['PK201909']
# cplxpknta['PKDiff3']=cplxpknta['PK202009']-cplxpknta['PK202004']
# cplxpknta['PKDiffPct3']=cplxpknta['PKDiff3']/cplxpknta['PK202004']
# cplxpknta=pd.merge(nta,cplxpknta,how='inner',on='NTACode')
# cplxpknta.to_file(path+'OUTPUT/cplxpknta.shp')

# cplxpknta=cplxpknta[[x not in ['MN21','MN22','MN27','MN25','MN19','MN15','MN17','MN50','MN20','MN24','MN28','MN23','MN13'] for x in cplxpknta['NTACode']]]
# (sum(cplxpknta['PK202009'])-sum(cplxpknta['PK201909']))/sum(cplxpknta['PK201909'])












# # AM Peak for DOHMH
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201909']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202009']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='left',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201909','E202009']].reset_index(drop=True)
# cplxamcp.to_csv('C:/Users/mayij/Desktop/cplxamcp.csv',index=False)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp=cplxamcp.to_crs('epsg:6539')
# cplxamcp['geometry']=cplxamcp.buffer(2640)
# cplxamcp=cplxamcp.to_crs('epsg:4326')
# nta=gpd.read_file(path+'ntaclippedadj.shp')
# nta.crs='epsg:4326'
# cplxamcp=gpd.sjoin(cplxamcp,nta,how='left',op='intersects')
# cplxamcp.to_csv('C:/Users/mayij/Desktop/cplxamcpnta.csv',index=False)


















# # Con Ed
# # AM Peak Mapbox
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['10/07/2019','10/08/2019','10/09/2019','10/10/2019','10/11/2019','10/15/2019','10/16/2019','10/17/2019','10/18/2019',
#           '10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020','10/16/2020',
#            '10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201910']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
#                        np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
#                        np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
#                        np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
#                        '<=-80%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxamconed.geojson',driver='GeoJSON')




# # PM Peak Mapbox
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['10/07/2019','10/08/2019','10/09/2019','10/10/2019','10/11/2019','10/15/2019','10/16/2019','10/17/2019','10/18/2019',
#           '10/21/2019','10/22/2019','10/23/2019','10/24/2019','10/25/2019','10/28/2019','10/29/2019','10/30/2019','10/31/2019']
# postdates=['10/05/2020','10/06/2020','10/07/2020','10/08/2020','10/09/2020','10/13/2020','10/14/2020','10/15/2020','10/16/2020',
#            '10/19/2020','10/20/2020','10/21/2020','10/22/2020','10/23/2020','10/26/2020','10/27/2020','10/28/2020','10/29/2020']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],pmlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','E201910']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],pmlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','E202010']
# cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamcp['Time']=cplxamcp['PreTime'].copy()
# cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
# cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
# cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
# cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>-0.5,'>-50%',
#                        np.where(cplxamcp['DiffPct']>-0.6,'-59%~-50%',
#                        np.where(cplxamcp['DiffPct']>-0.7,'-69%~-60%',
#                        np.where(cplxamcp['DiffPct']>-0.8,'-79%~-70%',
#                        '<=-80%'))))
# cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
# cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
#                    'Diff','DiffPct','DiffPctCat']].reset_index(drop=True)
# cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
# cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/cplxpmconed.geojson',driver='GeoJSON')










# AM Peak Pre and Post by PUMA
# dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
# predates=['11/18/2019','11/19/2019','11/20/2019','11/21/2019','11/22/2019','11/25/2019','11/26/2019','11/27/2019']
# postdates=['11/16/2020','11/17/2020','11/18/2020','11/19/2020','11/20/2020','11/23/2020','11/24/2020','11/25/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
# cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
# cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
# cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampre.columns=['CplxID','PreTime','PreEntries']
# cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
# cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
# cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
# cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
# cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
# cplxampost.columns=['CplxID','PostTime','PostEntries']
# cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
# cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
# cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
# cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
# cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
# cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
# cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
# puma=gpd.read_file(path+'nycpuma.shp')
# puma.crs='epsg:4326'
# cplxamdiffpuma=gpd.sjoin(puma,cplxamdiff,how='left',op='intersects')
# cplxamdiffpuma=cplxamdiffpuma.groupby(['puma'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
# cplxamdiffpuma=cplxamdiffpuma[cplxamdiffpuma['PreEntries']!=0].reset_index(drop=True)
# cplxamdiffpuma.columns=['PUMA','E201911','E202011']
# cplxamdiffpuma['Diff']=cplxamdiffpuma['E202011']-cplxamdiffpuma['E201911']
# cplxamdiffpuma['DiffPct']=cplxamdiffpuma['Diff']/cplxamdiffpuma['E201911']
# cplxamdiffpuma['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
# cplxamdiffpuma['DiffPctCat']=np.where(cplxamdiffpuma['DiffPct']>-0.6,'>-60%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.65,'-64%~-60%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.7,'-69%~-65%',
#           np.where(cplxamdiffpuma['DiffPct']>-0.75,'-74%~-70%',
#           '<=-75%'))))
# cplxamdiffpuma.to_csv(path+'OUTPUT/amdiffpuma.csv',index=False)










# Nadir AM
# AM Peak Mapbox
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
postdates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','E201910']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','E202010']
cplxamcp=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamcp['Time']=cplxamcp['PreTime'].copy()
cplxamcp=cplxamcp[['CplxID','Time','E201910','E202010']].reset_index(drop=True)
cplxamcp['Diff']=cplxamcp['E202010']-cplxamcp['E201910']
cplxamcp['DiffPct']=cplxamcp['Diff']/cplxamcp['E201910']
cplxamcp['DiffPct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['DiffPctCat']=np.where(cplxamcp['DiffPct']>=-0.8,'-80%~-77%',
                       np.where(cplxamcp['DiffPct']>=-0.85,'-85%~-81%',
                       np.where(cplxamcp['DiffPct']>=-0.9,'-90%~-86%',
                       np.where(cplxamcp['DiffPct']>=-0.95,'-95%~-91%',
                       '-99%~-96%'))))
cplxamcp['Pct']=cplxamcp['E202010']/cplxamcp['E201910']
cplxamcp['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamcp['PctCat']=np.where(cplxamcp['Pct']<=0.05,'1%~5%',
                   np.where(cplxamcp['Pct']<=0.1,'6%~10%',
                            '11%~23%'))
cplxamcp=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamcp,how='inner',on='CplxID')
cplxamcp=cplxamcp[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Time','E201910','E202010',
                   'Diff','DiffPct','DiffPctCat','Pct','PctCat']].reset_index(drop=True)
cplxamcp=gpd.GeoDataFrame(cplxamcp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamcp['CplxLong'],cplxamcp['CplxLat'])],crs='epsg:4326')
cplxamcp.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nadiram.geojson',driver='GeoJSON')







# Reopening
# AM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/16/2019','09/17/2019','09/18/2019','09/19/2019','09/20/2019']
postdates=['09/14/2020','09/15/2020','09/16/2020','09/17/2020','09/18/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E201909','E202009']
cplxamhed=cplxamdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
        '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
cplxampre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxampre=cplxampre[np.isin(cplxampre['time'],amlist)].reset_index(drop=True)
cplxampre=cplxampre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampre=pd.merge(cplxampre,rc,how='left',left_on='unit',right_on='Remote')
cplxampre=cplxampre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampre.columns=['CplxID','PreTime','PreEntries']
cplxampost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxampost=cplxampost[np.isin(cplxampost['time'],amlist)].reset_index(drop=True)
cplxampost=cplxampost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxampost=pd.merge(cplxampost,rc,how='left',left_on='unit',right_on='Remote')
cplxampost=cplxampost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxampost.columns=['CplxID','PostTime','PostEntries']
cplxamdiff=pd.merge(cplxampre,cplxampost,how='inner',on='CplxID')
cplxamdiff['Time']=cplxamdiff['PreTime'].copy()
cplxamdiff['Diff']=cplxamdiff['PostEntries']-cplxamdiff['PreEntries']
cplxamdiff['DiffPct']=cplxamdiff['Diff']/cplxamdiff['PreEntries']
cplxamdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxamdiff,how='left',on='CplxID')
cplxamdiff=cplxamdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxamdiff=gpd.GeoDataFrame(cplxamdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxamdiff['CplxLong'],cplxamdiff['CplxLat'])],crs='epsg:4326')
cplxamdiff=cplxamdiff.to_crs('epsg:6539')
cplxamdiff['geometry']=cplxamdiff.buffer(2640)
cplxamdiff=cplxamdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxamdiffnta=gpd.sjoin(nta,cplxamdiff,how='left',op='intersects')
cplxamdiffnta=cplxamdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxamdiffnta=cplxamdiffnta[cplxamdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxamdiffnta.columns=['NTACode','E202004','E202006']
cplxamhed=pd.merge(cplxamhed,cplxamdiffnta,how='inner',on='NTACode')
cplxamhed['Diff1']=cplxamhed['E202009']-cplxamhed['E201909']
cplxamhed['DiffPct1']=cplxamhed['Diff1']/cplxamhed['E201909']
cplxamhed['Diff2']=cplxamhed['E202009']-cplxamhed['E202004']
cplxamhed['DiffPct2']=cplxamhed['Diff2']/cplxamhed['E202004']
cplxamhed['Diff3']=cplxamhed['E202009']-cplxamhed['E202006']
cplxamhed['DiffPct3']=cplxamhed['Diff3']/cplxamhed['E202006']
cplxamhed=pd.merge(nta,cplxamhed,how='inner',on='NTACode')
cplxamhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat1']=np.where(cplxamhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxamhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxamhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxamhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxamhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat2']=np.where(cplxamhed['DiffPct2']<=2,'79%~200%',
          np.where(cplxamhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxamhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxamhed['DiffPct2']<=3.5,'301%~350%',
          '351%~581%'))))
cplxamhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['DiffPctCat3']=np.where(cplxamhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxamhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxamhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxamhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxamhed['Pct']=cplxamhed['E202009']/cplxamhed['E201909']
cplxamhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxamhed['PctCat']=np.where(cplxamhed['Pct']<=0.25,'13%~25%',
                    np.where(cplxamhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxamhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxamhed['Pct']<=0.4,'36%~40%',
                             '41%~51%'))))
cplxamhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/reopenntaam.geojson',driver='GeoJSON')







# PM Peak Pre and Post by NTA for HED
dfunitentry=pd.read_csv(path+'OUTPUT/dfunitentry.csv',dtype=str,converters={'entries':float,'gooducs':float,'flagtime':float,'flagentry':float})
predates=['09/16/2019','09/17/2019','09/18/2019','09/19/2019','09/20/2019']
postdates=['09/14/2020','09/15/2020','09/16/2020','09/17/2020','09/18/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E201909','E202009']
cplxpmhed=cplxpmdiffnta.copy()
predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
postdates=['06/01/2020','06/02/2020','06/03/2020','06/04/2020','06/05/2020','06/08/2020','06/09/2020','06/10/2020','06/11/2020','06/12/2020',
            '06/15/2020','06/16/2020','06/17/2020','06/18/2020','06/19/2020','06/22/2020','06/23/2020','06/24/2020','06/25/2020','06/26/2020',
            '06/29/2020','06/30/2020']
pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
        '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
cplxpmpre=dfunitentry[np.isin(dfunitentry['firstdate'],predates)].reset_index(drop=True)
cplxpmpre=cplxpmpre[np.isin(cplxpmpre['time'],pmlist)].reset_index(drop=True)
cplxpmpre=cplxpmpre.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpre=pd.merge(cplxpmpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpre=cplxpmpre.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpre.columns=['CplxID','PreTime','PreEntries']
cplxpmpost=dfunitentry[np.isin(dfunitentry['firstdate'],postdates)].reset_index(drop=True)
cplxpmpost=cplxpmpost[np.isin(cplxpmpost['time'],pmlist)].reset_index(drop=True)
cplxpmpost=cplxpmpost.groupby(['unit','time'],as_index=False).agg({'entries':'mean'}).reset_index(drop=True)
cplxpmpost=pd.merge(cplxpmpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpmpost=cplxpmpost.groupby(['CplxID'],as_index=False).agg({'time':lambda x:'|'.join(sorted(x.unique())),'entries':'sum'}).reset_index(drop=True)
cplxpmpost.columns=['CplxID','PostTime','PostEntries']
cplxpmdiff=pd.merge(cplxpmpre,cplxpmpost,how='inner',on='CplxID')
cplxpmdiff['Time']=cplxpmdiff['PreTime'].copy()
cplxpmdiff['Diff']=cplxpmdiff['PostEntries']-cplxpmdiff['PreEntries']
cplxpmdiff['DiffPct']=cplxpmdiff['Diff']/cplxpmdiff['PreEntries']
cplxpmdiff=pd.merge(rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),cplxpmdiff,how='left',on='CplxID')
cplxpmdiff=cplxpmdiff[['CplxID','CplxLat','CplxLong','PreEntries','PostEntries']].reset_index(drop=True)
cplxpmdiff=gpd.GeoDataFrame(cplxpmdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxpmdiff['CplxLong'],cplxpmdiff['CplxLat'])],crs='epsg:4326')
cplxpmdiff=cplxpmdiff.to_crs('epsg:6539')
cplxpmdiff['geometry']=cplxpmdiff.buffer(2640)
cplxpmdiff=cplxpmdiff.to_crs('epsg:4326')
nta=gpd.read_file(path+'ntaclippedadj.shp')
nta.crs='epsg:4326'
cplxpmdiffnta=gpd.sjoin(nta,cplxpmdiff,how='left',op='intersects')
cplxpmdiffnta=cplxpmdiffnta.groupby(['NTACode'],as_index=False).agg({'PreEntries':'sum','PostEntries':'sum'}).reset_index(drop=True)
cplxpmdiffnta=cplxpmdiffnta[cplxpmdiffnta['PreEntries']!=0].reset_index(drop=True)
cplxpmdiffnta.columns=['NTACode','E202004','E202006']
cplxpmhed=pd.merge(cplxpmhed,cplxpmdiffnta,how='inner',on='NTACode')
cplxpmhed['Diff1']=cplxpmhed['E202009']-cplxpmhed['E201909']
cplxpmhed['DiffPct1']=cplxpmhed['Diff1']/cplxpmhed['E201909']
cplxpmhed['Diff2']=cplxpmhed['E202009']-cplxpmhed['E202004']
cplxpmhed['DiffPct2']=cplxpmhed['Diff2']/cplxpmhed['E202004']
cplxpmhed['Diff3']=cplxpmhed['E202009']-cplxpmhed['E202006']
cplxpmhed['DiffPct3']=cplxpmhed['Diff3']/cplxpmhed['E202006']
cplxpmhed=pd.merge(nta,cplxpmhed,how='inner',on='NTACode')
cplxpmhed['DiffPct1'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat1']=np.where(cplxpmhed['DiffPct1']>-0.6,'>-60%',
          np.where(cplxpmhed['DiffPct1']>-0.65,'-64%~-60%',
          np.where(cplxpmhed['DiffPct1']>-0.7,'-69%~-65%',
          np.where(cplxpmhed['DiffPct1']>-0.75,'-74%~-70%',
          '<=-75%'))))
cplxpmhed['DiffPct2'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat2']=np.where(cplxpmhed['DiffPct2']<=2,'79%~200%',
          np.where(cplxpmhed['DiffPct2']<=2.5,'201%~250%',
          np.where(cplxpmhed['DiffPct2']<=3,'251%~300%',
          np.where(cplxpmhed['DiffPct2']<=3.5,'301%~350%',
          '351%~581%'))))
cplxpmhed['DiffPct3'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['DiffPctCat3']=np.where(cplxpmhed['DiffPct3']<=0.7,'<=70%',
          np.where(cplxpmhed['DiffPct3']<=0.8,'71%~80%',
          np.where(cplxpmhed['DiffPct3']<=0.9,'81%~90%',
          np.where(cplxpmhed['DiffPct3']<=1,'91%~100%',
          '>100%'))))
cplxpmhed['Pct']=cplxpmhed['E202009']/cplxpmhed['E201909']
cplxpmhed['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxpmhed['PctCat']=np.where(cplxpmhed['Pct']<=0.25,'16%~25%',
                    np.where(cplxpmhed['Pct']<=0.3,'26%~30%',
                    np.where(cplxpmhed['Pct']<=0.35,'31%~35%',
                    np.where(cplxpmhed['Pct']<=0.4,'36%~40%',
                             '41%~55%'))))
cplxpmhed.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/reopenntapm.geojson',driver='GeoJSON')








# predates=['04/15/2019','04/16/2019','04/17/2019','04/18/2019']
# predates=['09/03/2019','09/04/2019','09/05/2019','09/06/2019','09/09/2019','09/10/2019','09/11/2019','09/12/2019']
# predates=['04/14/2020','04/15/2020','04/16/2020','04/17/2020']
# postdates=['08/31/2020','09/01/2020','09/02/2020','09/03/2020','09/04/2020','09/08/2020','09/09/2020','09/10/2020']
# amlist=['05:00:00-09:00:00','05:30:00-09:30:00','06:00:00-10:00:00','06:30:00-10:30:00','07:00:00-11:00:00',
#         '07:22:00-11:22:00','07:30:00-11:30:00','08:00:00-12:00:00','08:22:00-12:22:00','08:30:00-12:30:00']
# pmlist=['13:00:00-17:00:00','13:30:00-17:30:00','14:00:00-18:00:00','14:30:00-18:30:00','15:00:00-19:00:00',
#         '15:22:00-19:22:00','15:30:00-19:30:00','16:00:00-20:00:00','16:22:00-20:22:00','16:30:00-20:30:00']
# dtoplist=['09:00:00-13:00:00','09:30:00-13:30:00','10:00:00-14:00:00','10:30:00-14:30:00','11:00:00-15:00:00',
#           '11:22:00-15:22:00','11:30:00-15:30:00','12:00:00-16:00:00','12:22:00-16:22:00','12:30:00-16:30:00']
# ntoplist=['17:00:00-21:00:00','17:30:00-21:30:00','18:00:00-22:00:00','18:30:00-22:30:00','19:00:00-23:00:00',
#           '19:22:00-23:22:00','19:30:00-23:30:00','20:00:00-00:00:00','20:22:00-00:22:00','20:30:00-00:30:00',
#           '21:00:00-01:00:00','21:30:00-01:30:00','22:00:00-02:00:00','22:30:00-02:30:00','23:00:00-03:00:00',
#           '23:22:00-03:22:00','23:30:00-03:30:00','00:00:00-04:00:00','00:22:00-04:22:00','00:30:00-04:30:00',
#           '01:00:00-05:00:00','01:30:00-05:30:00','02:00:00-06:00:00','02:30:00-06:30:00','03:00:00-07:00:00',
#           '03:22:00-07:22:00','03:30:00-07:30:00','04:00:00-08:00:00','04:22:00-08:22:00','04:30:00-08:30:00']