import geopandas as gpd
import pandas as pd
import numpy as np
import re
from geosupport import Geosupport



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/TRAFFIC/TRANSCOM/'

transcom=pd.DataFrame()
for i in ['0301','0308','0315','0322']:
    df=pd.read_excel(path+'GROUP1/'+str(i)+'.XLS',sheetname='Comparison(In Seconds)',skiprows=14,dtype=str)
    tp=pd.DataFrame(index=range(0,30),columns=['segment','length','time','sec'])
    tp['segment']=np.repeat(list(df.loc[range(1,61,4),'Unnamed: 0']),2)
    tp['segment']=[re.sub('Trip Description : ','',x) for x in tp['segment']]
    tp['onstreet']=[str(x).split(' from ')[0] for x in tp['segment']]
    tp['onstreet']=[re.sub(' EB','',x) for x in tp['onstreet']]
    tp['onstreet']=[re.sub(' WB','',x) for x in tp['onstreet']]
    tp['onstreet']=[re.sub(' NB','',x) for x in tp['onstreet']]
    tp['onstreet']=[re.sub(' SB','',x) for x in tp['onstreet']]
    tp['onstreet']=[' '.join(x.split()).upper() for x in tp['onstreet']]
    tp['fromstreet']=[str(x).split(' from ')[1].split(' to ')[0] for x in tp['segment']]
    tp['fromstreet']=[' '.join(x.split()).upper() for x in tp['fromstreet']]
    tp['tostreet']=[str(x).split(' from ')[1].split(' to ')[1] for x in tp['segment']]
    tp['tostreet']=[re.sub(' \(EB\)','',x) for x in tp['tostreet']]
    tp['tostreet']=[re.sub(' \(WB\)','',x) for x in tp['tostreet']]
    tp['tostreet']=[re.sub(' \(NB\)','',x) for x in tp['tostreet']]
    tp['tostreet']=[re.sub(' \(SB\)','',x) for x in tp['tostreet']]
    tp['tostreet']=[' '.join(x.split()).upper() for x in tp['tostreet']]
    tp['length']=np.repeat(list(df.loc[range(1,61,4),'Unnamed: 6']),2)
    tp['mile']=pd.to_numeric([re.sub('Trip Length in miles : ','',x) for x in tp['length']])
    tp['time']=list(df.loc[sorted(list(range(2,62,4))+list(range(3,63,4))),'Unnamed: 0'])
    tp['week']=[str(x)[0:5]+'-'+str(x)[13:18] for x in tp['time']]
    tp['year']=[str(x)[6:10] for x in tp['time']]
    tp['sec']=pd.to_numeric(list(df.loc[sorted(list(range(2,62,4))+list(range(3,63,4))),'Unnamed: 4']))
    tp['mph']=tp['mile']/tp['sec']*3600
    tp=tp[['segment','onstreet','fromstreet','tostreet','length','mile','time','week','year','sec','mph']].reset_index(drop=True)
    transcom=pd.concat([transcom,tp],axis=0,ignore_index=True)
transcom.to_csv(path+'GROUP1/CLEAN.csv',index=False)

segment=pd.read_csv(path+'GROUP1/CLEAN.csv',dtype=str,converters={'mile':float,'sec':float,'mph':float})
segment=segment[['segment','onstreet','fromstreet','tostreet']].drop_duplicates(keep='first').reset_index(drop=True)
g = Geosupport()
for i in segment.index:
    borocode=str(1)
    onstreet=str(segment.loc[i,'onstreet'])
    fromstreet=str(segment.loc[i,'fromstreet'])
    tostreet=str(segment.loc[i,'tostreet'])
    try:
        strech=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet})
        tp=pd.DataFrame(strech['LIST OF INTERSECTIONS'])
        tp=tp[[x!=[] for x in tp['List of Cross Streets at this Intersection']]].reset_index(drop=True)
        weekendwalk.loc[i,'fromnode']=pd.to_numeric(tp.loc[0,'Node Number'])
        weekendwalk.loc[i,'tonode']=pd.to_numeric(tp.loc[len(tp)-1,'Node Number'])
        for j in range(0,len(tp)-1):
            fromstreet=list(snd.loc[snd['streetcode']==pd.to_numeric(tp.loc[j,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
            tostreet=list(snd.loc[snd['streetcode']==pd.to_numeric(tp.loc[j+1,'List of Cross Streets at this Intersection'][0][0:6]),'streetname'])
            segment=pd.concat([weekendwalk.loc[[i]]]*len(fromstreet)*len(tostreet),ignore_index=True)
            segment['segfromstreet']=[x for x in fromstreet for y in tostreet]
            segment['segtostreet']=[y for x in fromstreet for y in tostreet]
            segment['segfromnode']=pd.to_numeric(tp.loc[j,'Node Number'])
            segment['segtonode']=pd.to_numeric(tp.loc[j+1,'Node Number'])
            segment['segmentid']=np.nan
            for k in segment.index:
                try:
                    segment.loc[k,'segmentid']=pd.to_numeric(g['3']({'borough_code':borocode,'on':onstreet,'from':segment.loc[k,'segfromstreet'],'to':segment.loc[k,'segtostreet']})['Segment Identifier'])
                except:
                    print(segment.loc[[k]])
            segment=segment[pd.notna(segment['segmentid'])].reset_index(drop=True)
            segment=segment.drop(['segfromstreet','segtostreet'],axis=1).drop_duplicates(keep='first').reset_index(drop=True)
            df=pd.concat([df,segment],axis=0)
    except:
        print(weekendwalk.loc[[i]])
df.to_csv(path1+'weekendwalk/df.csv',index=False)


df=pd.read_csv(path1+'weekendwalk/df.csv')
lion=gpd.read_file(path2+'LION/LION.shp')
lion=lion.to_crs({'init':'epsg:4326'})
lion=lion[['SegmentID','geometry']].drop_duplicates('SegmentID',keep='first').reset_index(drop=True)
lion['SegmentID']=pd.to_numeric(lion['SegmentID'])
df=pd.merge(lion,df,how='right',left_on='SegmentID',right_on='segmentid')
df=df.dissolve(by=['On Street','From Street','To Street','Borough']).reset_index(drop=False)
df=df[['On Street','From Street','To Street','Borough','geometry']].reset_index(drop=True)
df.to_file(path1+'weekendwalk/weekendwalk2.shp')

