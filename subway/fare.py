import urllib.request
import shutil
import os
import pandas as pd
import numpy as np
import datetime
import pytz
import geopandas as gpd
import shapely
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go


pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SUBWAY/FARE/'



rc=pd.read_csv(path+'RemoteComplex.csv',dtype=str,converters={'CplxID':float,'CplxLat':float,'CplxLong':float,'Hub':float})



## Download data
#dl=datetime.datetime(2020,4,4)
#for i in range(0,150):
#    dl=dl-datetime.timedelta(days=7)
#    url='http://web.mta.info/developers/data/nyct/fares/fares_'+datetime.datetime.strftime(dl,'%y%m%d')+'.csv'
#    req=urllib.request.urlopen(url)
#    file = open(path+'DATA/'+datetime.datetime.strftime(dl,'%y%m%d')+'.csv', "wb")
#    shutil.copyfileobj(req,file)
#    file.close()



# Compile data
tp=pd.DataFrame()
for i in sorted(os.listdir(path+'DATA')):
    wk=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=1,nrows=1,header=None).loc[0,1]
    dt=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=2,header=0)
    for j in dt.columns[2:]:
        dt[j]=pd.to_numeric(dt[j],errors='coerce')
    dt=dt.fillna(0)
    dt['fare']=dt.iloc[:,2:].sum(axis=1)
    dt['unit']=dt['REMOTE'].copy()
    dt['week']=wk
    dt=dt[['unit','week','fare']].reset_index(drop=True)
    tp=pd.concat([tp,dt],ignore_index=True)
tp=pd.merge(tp,rc[['Remote']].drop_duplicates(keep='first'),how='inner',left_on='unit',right_on='Remote')
tp['date']=[datetime.datetime.strptime(x[0:10],'%m/%d/%Y') for x in tp['week']]
tp=tp.sort_values(['unit','date']).reset_index(drop=True)
tp=tp[['unit','week','fare']].reset_index(drop=True)
tp.to_csv(path+'fare.csv',index=False)






# Compile data by type
tp=pd.DataFrame()
for i in sorted(os.listdir(path+'DATA'))[52:]:
    wk=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=1,nrows=1,header=None).loc[0,1]
    dt=pd.read_csv(path+'DATA/'+str(i),dtype=str,skiprows=2,header=0)
    for j in dt.columns[2:]:
        dt[j]=pd.to_numeric(dt[j],errors='coerce')
    dt=dt.fillna(0)
    dt['unit']=dt['REMOTE'].copy()
    dt['week']=wk
    dt['30-Day Unlimited']=dt['30-D UNL'].copy()
    dt['7-Day Unlimited']=dt['7-D UNL'].copy()
    dt['7-Day Express Bus Pass']=dt['7D-XBUS PASS'].copy()
    dt['AirTrain 10-Trip']=dt['AIRTRAIN 10-T'].copy()
    dt['AirTrain 30-Day']=dt['AIRTRAIN 30-D'].copy()
    dt['AirTrain Full Fare']=dt['AIRTRAIN FF'].copy()
    dt['AirTrain Monthly']=dt['AIRTRAIN MTHLY'].copy()
    dt['CUNY-120']=dt['CUNY-120'].copy()
    dt['CUNY-60']=dt['CUNY-60'].copy()
    dt['Full Fare']=dt['FF'].copy()
    dt['Full Fare 30-Day']=dt['FF 30-DAY'].copy()
    dt['Full Fare 7-Day']=dt['FF 7-DAY'].copy()
    dt['Full Fare Value']=dt['FF VALUE'].copy()
    dt['Joint Rail Road Ticket']=dt['JOINT RR TKT'].copy()
    dt['Mail and Ride Easy Pay Express']=dt['MR EZPAY EXP'].copy()
    dt['Mail and Ride Easy Pay Unlimited']=dt['MR EZPAY UNL'].copy()
    dt['NICE 2-Trip']=dt['NICE 2-T'].copy()
    dt['PATH 2-Trip']=dt['PATH 2-T'].copy()
    dt['Rail Road Unlimited No Trade']=dt['RR UNL NO TRADE'].copy()
    dt['Reduced Fare 2-Trip']=dt['RF 2 TRIP'].copy()
    dt['Senior Citizen/Disabled']=dt['SEN/DIS'].copy()
    dt['Students']=dt['STUDENTS'].copy()
    dt['Transit Check Annual MetroCard']=dt['TCMC ANNUAL MC'].copy()
    dt=dt[['unit','week','30-Day Unlimited','7-Day Unlimited','7-Day Express Bus Pass','AirTrain 10-Trip',
           'AirTrain 30-Day','AirTrain Full Fare','AirTrain Monthly','CUNY-120','CUNY-60','Full Fare',
           'Full Fare 30-Day','Full Fare 7-Day','Full Fare Value','Joint Rail Road Ticket',
           'Mail and Ride Easy Pay Express','Mail and Ride Easy Pay Unlimited','NICE 2-Trip','PATH 2-Trip',
           'Rail Road Unlimited No Trade','Reduced Fare 2-Trip','Senior Citizen/Disabled','Students',
           'Transit Check Annual MetroCard']].reset_index(drop=True)
    dt=dt.groupby(['week'],as_index=False).agg({'30-Day Unlimited':'sum',
                                                '7-Day Unlimited':'sum',
                                                '7-Day Express Bus Pass':'sum',
                                                'AirTrain 10-Trip':'sum',
                                                'AirTrain 30-Day':'sum',
                                                'AirTrain Full Fare':'sum',
                                                'AirTrain Monthly':'sum',
                                                'CUNY-120':'sum',
                                                'CUNY-60':'sum',
                                                'Full Fare':'sum',
                                                'Full Fare 30-Day':'sum',
                                                'Full Fare 7-Day':'sum',
                                                'Full Fare Value':'sum',
                                                'Joint Rail Road Ticket':'sum',
                                                'Mail and Ride Easy Pay Express':'sum',
                                                'Mail and Ride Easy Pay Unlimited':'sum',
                                                'NICE 2-Trip':'sum',
                                                'PATH 2-Trip':'sum',
                                                'Rail Road Unlimited No Trade':'sum',
                                                'Reduced Fare 2-Trip':'sum',
                                                'Senior Citizen/Disabled':'sum',
                                                'Students':'sum',
                                                'Transit Check Annual MetroCard':'sum'}).reset_index(drop=True)
    tp=pd.concat([tp,dt],ignore_index=True)
tp['weekstart']=[datetime.datetime.strptime(x.split('-')[0],'%m/%d/%Y') for x in tp['week']]
tp.to_csv(path+'faretype.csv',index=False)


# Plot chart
pio.renderers.default = "browser"

fig=go.Figure()
fig=fig.add_trace(go.Scattergl(name='',
                               x=tp['weekstart'],
                               y=tp['CUNY-60'],
                               opacity=0,
                               showlegend=False,
                               hovertext='<b>'+tp['week']+'</b>',
                               hoverinfo='text'))
for i in ['30-Day Unlimited','7-Day Unlimited','Full Fare','Senior Citizen/Disabled','Students']:
    fig=fig.add_trace(go.Scattergl(name=i,
                                   mode='lines',
                                   x=tp['weekstart'],
                                   y=tp[i],
                                   line={'width':2},
                                   hovertext=[i+': '+'{:,}'.format(x) for x in tp[i]],
                                   hoverinfo='text',
                                   visible=True))    
for i in ['7-Day Express Bus Pass','AirTrain 10-Trip','AirTrain 30-Day','AirTrain Full Fare','AirTrain Monthly',
          'CUNY-120','CUNY-60','Full Fare 30-Day','Full Fare 7-Day','Full Fare Value','Joint Rail Road Ticket',
          'Mail and Ride Easy Pay Express','Mail and Ride Easy Pay Unlimited','NICE 2-Trip','PATH 2-Trip',
          'Rail Road Unlimited No Trade','Reduced Fare 2-Trip','Transit Check Annual MetroCard']:
    fig=fig.add_trace(go.Scattergl(name=i,
                                   mode='lines',
                                   x=tp['weekstart'],
                                   y=tp[i],
                                   line={'width':2},
                                   hovertext=[i+': '+'{:,}'.format(x) for x in tp[i]],
                                   hoverinfo='text',
                                   visible='legendonly'))
fig.update_layout(
     template='plotly_white',
     title={'text':'<b>Weekly MetroCard Swipes by Card Type</b>',
            'font_size':20,
            'x':0.5,
            'xanchor':'center',
            'y':0.98,
            'yanchor':'top'},
     legend={'orientation':'v',
             'title_text':'',
             'font_size':14,
             'x':1,
             'xanchor':'left',
             'y':1,
             'yanchor':'top'},
     xaxis={'title':{'text':'<b>Week</b>',
                     'font_size':14},
            'tickfont_size':12,
            'dtick':'M1',
            'fixedrange':True,
            'showgrid':False},
     yaxis={'title':{'text':'<b>Weekly MetroCard Swipes</b>',
                     'font_size':14},
            'tickfont_size':12,
            'fixedrange':False,
            'showgrid':False},
     hoverlabel={'font_size':14},
     font={'family':'Arial',
           'color':'black'},
     dragmode=False,
     hovermode='x unified',
     )
fig.write_html('C:/Users/mayij/Desktop/DOC/GITHUB/td-mtatracker/faretype.html',
               include_plotlyjs='cdn',
               config={'displaylogo':False,'modeBarButtonsToRemove':['select2d','lasso2d']})
























# Compare the data
df=pd.read_csv(path+'fare.csv',dtype=str,converters={'fare':float})
preweek='04/20/2019-04/26/2019'
postweek='04/18/2020-04/24/2020'
cplxpre=df[np.isin(df['week'],preweek)].reset_index(drop=True)
cplxpre=pd.merge(cplxpre,rc,how='left',left_on='unit',right_on='Remote')
cplxpre=cplxpre.groupby(['CplxID'],as_index=False).agg({'fare':'sum'}).reset_index(drop=True)
cplxpre.columns=['CplxID','PreEntries']
cplxpost=df[np.isin(df['week'],postweek)].reset_index(drop=True)
cplxpost=pd.merge(cplxpost,rc,how='left',left_on='unit',right_on='Remote')
cplxpost=cplxpost.groupby(['CplxID'],as_index=False).agg({'fare':'sum'}).reset_index(drop=True)
cplxpost.columns=['CplxID','PostEntries']
cplxdiff=pd.merge(cplxpre,cplxpost,how='inner',on='CplxID')
cplxdiff['Pct']=cplxdiff['PostEntries']/cplxdiff['PreEntries']
cplxdiff['Pct'].describe(percentiles=np.arange(0.2,1,0.2))
cplxdiff['PctCat']=np.where(cplxdiff['Pct']<=0.1,'1%~10%',
                   np.where(cplxdiff['Pct']<=0.15,'11%~15%',
                            '16%~38%'))
cplxdiff=pd.merge(cplxdiff,rc.drop('Remote',axis=1).drop_duplicates(keep='first').reset_index(drop=True),how='left',on='CplxID')
cplxdiff=cplxdiff[['CplxID','Borough','CplxName','Routes','CplxLat','CplxLong','Hub','PreEntries','PostEntries','Pct','PctCat']].reset_index(drop=True)
cplxdiff=gpd.GeoDataFrame(cplxdiff,geometry=[shapely.geometry.Point(x,y) for x,y in zip(cplxdiff['CplxLong'],cplxdiff['CplxLat'])],crs='epsg:4326')
cplxdiff.to_file('C:/Users/mayij/Desktop/DOC/GITHUB/td-covid19/subway/nadirfare.geojson',driver='GeoJSON')





# By Borough
df=pd.read_csv(path+'fare.csv',dtype=str,converters={'fare':float})
df=pd.merge(df,rc,how='left',left_on='unit',right_on='Remote')
df=df.groupby(['Borough','week'],as_index=False).agg({'fare':'sum'}).reset_index(drop=True)
df.to_csv('C:/Users/mayij/Desktop/fareboro.csv',index=False)

