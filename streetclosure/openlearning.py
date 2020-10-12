import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport
import usaddress



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/STREET CLOSURE/'



g = Geosupport()



# School District
sd=gpd.read_file(path+'openlearning/school_district.shp')
sd.crs='epsg:4326'
sd['DISTRICT']=pd.to_numeric(sd['school_dis'])
sd=sd[['DISTRICT','geometry']].reset_index(drop=True)



# # School
# school=pd.read_csv(path+'openlearning/2019_-_2020_School_Locations.csv',dtype=str)
# school['DBN']=[str(x).strip().upper() for x in school['system_code']]
# for i in school.index:
#     try:
#         housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(school.loc[i,'primary_address_line_1']) if re.search('AddressNumber',x[1])])
#         streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(school.loc[i,'primary_address_line_1']) if re.search('StreetName',x[1])])
#         boroughcode=np.where(school.loc[i,'DBN'][2]=='M',1,np.where(school.loc[i,'DBN'][2]=='X',2,
#                     np.where(school.loc[i,'DBN'][2]=='K',3,np.where(school.loc[i,'DBN'][2]=='Q',4,
#                     np.where(school.loc[i,'DBN'][2]=='R',5,0))))).tolist()
#         addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#         if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#             school.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#             school.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#             school.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#         else:
#             print(school.loc[i,'DBN']+' not geocoded!')
#     except:
#         print(school.loc[i,'DBN']+' not geocoded!')
# school['LAT']=np.where(pd.notna(school['LAT']),school['LAT'],pd.to_numeric(school['LATITUDE']))
# school['LONG']=np.where(pd.notna(school['LONG']),school['LONG'],pd.to_numeric(school['LONGITUDE']))
# school['BBL']=np.where(pd.notna(school['BBL']),school['BBL'],pd.to_numeric(school['Borough_block_lot']))
# school['TYPE']=[str(x).strip().upper() for x in school['Managed_by_name']]
# school=school[['DBN','TYPE','LAT','LONG','BBL']].drop_duplicates(keep='first').reset_index(drop=True)



# LCGMS
lcgms=pd.read_excel(path+'openlearning/LCGMS_SchoolData_20201006_1523.xlsx',dtype=str)
lcgms['DBN']=[str(x).strip().upper() for x in lcgms['ATS System Code']]
lcgms['TYPE']=[str(x).strip().upper() for x in lcgms['Managed By Name']]
lcgms['ADDRESS']=[str(x).strip().upper() for x in lcgms['Primary Address']]
lcgms['ZIP']=[str(x).strip().upper() for x in lcgms['Zip']]
lcgms['BORO']=[str(x).strip().upper() for x in lcgms['City']]
lcgms['BBL1']=pd.to_numeric(lcgms['Borough Block Lot'])
lcgms['BBL2']=np.nan
lcgms['LAT']=np.nan
lcgms['LONG']=np.nan
lcgms=lcgms[['DBN','TYPE','ADDRESS','ZIP','BORO','BBL1','BBL2','LAT','LONG']].drop_duplicates(keep='first').reset_index(drop=True)
for i in lcgms.index:
    if pd.isna(lcgms.loc[i,'BBL2']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(lcgms.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(lcgms.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=lcgms.loc[i,'ZIP']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                lcgms.loc[i,'BBL2']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(lcgms.loc[i,'DBN'])+' not geocoded with zipcode!')
        except:
            print(str(lcgms.loc[i,'DBN'])+' not geocoded with zipcode!')
for i in lcgms.index:
    if pd.isna(lcgms.loc[i,'BBL2']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(lcgms.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(lcgms.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(lcgms.loc[i,'BORO']=='MANHATTAN',1,np.where(lcgms.loc[i,'BORO']=='NEW YORK',1,
                        np.where(lcgms.loc[i,'BORO']=='NEW YORK CITY',1,np.where(lcgms.loc[i,'BORO']=='BRONX',2,
                        np.where(lcgms.loc[i,'BORO']=='BROOKLYN',3,np.where(lcgms.loc[i,'BORO']=='STATEN ISLAND',5,
                        np.where(lcgms.loc[i,'BORO']=='STATEN IS',5,4))))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                lcgms.loc[i,'BBL2']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(lcgms.loc[i,'DBN'])+' not geocoded with borough!')
        except:
            print(str(lcgms.loc[i,'DBN'])+' not geocoded with borough!')
lcgms['BBL']=np.where(pd.notna(lcgms['BBL2']),lcgms['BBL2'],pd.to_numeric(lcgms['BBL1']))
for i in lcgms.index:
    try:
        boroughcode=np.where(lcgms.loc[i,'BORO']=='MANHATTAN',1,np.where(lcgms.loc[i,'BORO']=='NEW YORK',1,
                np.where(lcgms.loc[i,'BORO']=='NEW YORK CITY',1,np.where(lcgms.loc[i,'BORO']=='BRONX',2,
                np.where(lcgms.loc[i,'BORO']=='BROOKLYN',3,np.where(lcgms.loc[i,'BORO']=='STATEN ISLAND',5,
                np.where(lcgms.loc[i,'BORO']=='STATEN IS',5,4))))))).tolist()
        bbl=str(lcgms.loc[i,'BBL'])
        addr=g['BL']({'BBL':bbl,'borough_code':boroughcode})
        lcgms.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
        lcgms.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
    except:
        print(str(lcgms.loc[i,'DBN'])+' not geocoded with bbl!')
lcgms=lcgms[pd.notna(lcgms['LAT'])].reset_index(drop=True)
lcgms=lcgms[['DBN','TYPE','BBL','LAT','LONG']].drop_duplicates(keep='first').reset_index(drop=True)



# School Yard Only 714/714
syo=pd.read_excel(path+'openlearning/OL.MasterApprovals_1007.xlsx',sheet_name='School Yard Only',dtype=str,keep_default_na=False)
syo['DBN']=[str(x).strip().upper() for x in syo['DBN (if known)']]
syo['TYPE']=[str(x).strip().upper() for x in syo['My school program classification is:']]
syo['ADDRESS']=[str(x).strip().upper() for x in syo['ADDRESS']]
syo['APPROVED']=pd.to_numeric(syo['APPROVED'])
syo['DISAPPROVED']=pd.to_numeric(syo['DISAPPROVED'])
for i in syo.index:
    if syo.loc[i,'APPROVED']==1:
        syo.loc[i,'ID']=syo.loc[i,'ID']+'(Y)'
    else:
        syo.loc[i,'ID']=syo.loc[i,'ID']+'(N)' 
syo=syo[['ID','DBN','TYPE','ADDRESS','APPROVED','DISAPPROVED']].reset_index(drop=True)
syo=pd.merge(syo,lcgms,how='left',on='DBN')
for i in syo.index:
    if pd.isna(syo.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('ZipCode',x[1])])
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syo.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                syo.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syo.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(syo.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(syo.loc[i,'ID'])+' not geocoded with zipcode!')
for i in syo.index:
    if pd.isna(syo.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(syo.loc[i,'DBN'][2]=='M',1,np.where(syo.loc[i,'DBN'][2]=='X',2,
                        np.where(syo.loc[i,'DBN'][2]=='K',3,np.where(syo.loc[i,'DBN'][2]=='Q',4,
                        np.where(syo.loc[i,'DBN'][2]=='R',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syo.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                syo.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syo.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(syo.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(syo.loc[i,'ID'])+' not geocoded with borough!')
syo=syo[pd.notna(syo['BBL'])].reset_index(drop=True)
syo['TYPE']=np.where(pd.notna(syo['TYPE_y']),syo['TYPE_y'],syo['TYPE_x'])
syo=syo.groupby(['BBL','LAT','LONG'],as_index=False).agg({'DBN': lambda x: '/'.join(x),'TYPE': lambda x: '/'.join(x),'ADDRESS':'count','APPROVED':'sum','DISAPPROVED':'sum','ID': lambda x: '/'.join(x)}).reset_index(drop=True)
syo['DBN']=['/'.join(sorted(set(x.split('/')))) for x in syo['DBN']]
syo['DBNCOUNT']=[len(x.split('/')) for x in syo['DBN']]
syo['TYPE']=['/'.join(sorted(set(x.split('/')))) for x in syo['TYPE']]
syo['APPCOUNT']=syo['ADDRESS'].copy()
syo['IDS']=syo['ID'].copy()
syo=gpd.GeoDataFrame(syo,geometry=[shapely.geometry.Point(x,y) for x,y in zip(syo['LONG'],syo['LAT'])],crs='epsg:4326')
syo=gpd.sjoin(syo,sd,how='left',op='intersects')
syo['DISTRICT']=np.where(pd.notna(syo['DISTRICT']),syo['DISTRICT'],2)
syo=syo[['BBL','DISTRICT','DBN','DBNCOUNT','TYPE','APPCOUNT','APPROVED','DISAPPROVED','IDS','geometry']].reset_index(drop=True)
syo.to_file(path+'openlearning/school_yard_only.shp')



# School Yard + Pending 711/711
syp=pd.read_excel(path+'openlearning/OL.MasterApprovals_1007.xlsx',sheet_name='School Yard + Pending',dtype=str,keep_default_na=False)
syp['DBN']=[str(x).strip().upper() for x in syp['DBN (if known)']]
syp['TYPE']=[str(x).strip().upper() for x in syp['TYPE']]
syp['ADDRESS']=[str(x).strip().upper() for x in syp['ADDRESS']]
syp['APPROVED']=pd.to_numeric(syp['APPROVED'])
syp['DISAPPROVED']=pd.to_numeric(syp['DISAPPROVED'])
for i in syp.index:
    if syp.loc[i,'APPROVED']==1:
        syp.loc[i,'ID']=syp.loc[i,'ID']+'(Y)'
    else:
        syp.loc[i,'ID']=syp.loc[i,'ID']+'(N)' 
syp=syp[['ID','DBN','TYPE','ADDRESS','APPROVED','DISAPPROVED']].reset_index(drop=True)
syp=pd.merge(syp,lcgms,how='left',on='DBN')
for i in syp.index:
    if pd.isna(syp.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('ZipCode',x[1])])
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syp.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                syp.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syp.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(syp.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(syp.loc[i,'ID'])+' not geocoded with zipcode!')
for i in syp.index:
    if pd.isna(syp.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(syp.loc[i,'DBN'][2]=='M',1,np.where(syp.loc[i,'DBN'][2]=='X',2,
                        np.where(syp.loc[i,'DBN'][2]=='K',3,np.where(syp.loc[i,'DBN'][2]=='Q',4,
                        np.where(syp.loc[i,'DBN'][2]=='R',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syp.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                syp.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syp.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(syp.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(syp.loc[i,'ID'])+' not geocoded with borough!')
syp=syp[pd.notna(syp['BBL'])].reset_index(drop=True)
syp['TYPE']=np.where(pd.notna(syp['TYPE_y']),syp['TYPE_y'],syp['TYPE_x'])
syp=syp.groupby(['BBL','LAT','LONG'],as_index=False).agg({'DBN': lambda x: '/'.join(x),'TYPE': lambda x: '/'.join(x),'ADDRESS':'count','APPROVED':'sum','DISAPPROVED':'sum','ID': lambda x: '/'.join(x)}).reset_index(drop=True)
syp['DBN']=['/'.join(sorted(set(x.split('/')))) for x in syp['DBN']]
syp['DBNCOUNT']=[len(x.split('/')) for x in syp['DBN']]
syp['TYPE']=['/'.join(sorted(set(x.split('/')))) for x in syp['TYPE']]
syp['APPCOUNT']=syp['ADDRESS'].copy()
syp['IDS']=syp['ID'].copy()
syp=gpd.GeoDataFrame(syp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(syp['LONG'],syp['LAT'])],crs='epsg:4326')
syp=gpd.sjoin(syp,sd,how='left',op='intersects')
syp['DISTRICT']=np.where(pd.notna(syp['DISTRICT']),syp['DISTRICT'],2)
syp=syp[['BBL','DISTRICT','DBN','DBNCOUNT','TYPE','APPCOUNT','APPROVED','DISAPPROVED','IDS','geometry']].reset_index(drop=True)
syp.to_file(path+'openlearning/school_yard_pending.shp')



# Streets Point 157/157
stspt=pd.read_excel(path+'openlearning/OL.MasterApprovals_1007.xlsx',sheet_name='Streets',dtype=str,keep_default_na=False)
stspt['DBN']=[str(x).strip().upper() for x in stspt['DBN # (DOE ONLY)']]
stspt['TYPE']=[str(x).strip().upper() for x in stspt['TYPE']]
stspt['ADDRESS']=[str(x).strip().upper() for x in stspt['Address']]
stspt['ZIPCODE']=[str(x).strip().upper() for x in stspt['Zip']]
stspt['BORO']=[str(x).strip().upper() for x in stspt['Borough']]
stspt['APPROVED']=pd.to_numeric(stspt['APPROVED'])
stspt['DISAPPROVED']=pd.to_numeric(stspt['DISAPPROVED'])
for i in stspt.index:
    if stspt.loc[i,'APPROVED']==1:
        stspt.loc[i,'ID']=stspt.loc[i,'ID']+'(Y)'
    else:
        stspt.loc[i,'ID']=stspt.loc[i,'ID']+'(N)' 
stspt=stspt[['ID','DBN','TYPE','ADDRESS','ZIPCODE','BORO','APPROVED','DISAPPROVED']].reset_index(drop=True)
stspt=pd.merge(stspt,lcgms,how='left',on='DBN')
for i in stspt.index:
    if pd.isna(stspt.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=stspt.loc[i,'ZIPCODE']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                stspt.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                stspt.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                stspt.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(stspt.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(stspt.loc[i,'ID'])+' not geocoded with zipcode!')
for i in stspt.index:
    if pd.isna(stspt.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(stspt.loc[i,'BORO']=='MANHATTAN',1,np.where(stspt.loc[i,'BORO']=='BRONX',2,
                        np.where(stspt.loc[i,'BORO']=='BROOKLYN',3,np.where(stspt.loc[i,'BORO']=='QUEENS',4,
                        np.where(stspt.loc[i,'BORO']=='STATEN ISLAND',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                stspt.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
                stspt.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                stspt.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            else:
                print(str(stspt.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(stspt.loc[i,'ID'])+' not geocoded with borough!')
stspt=stspt[pd.notna(stspt['BBL'])].reset_index(drop=True)
stspt['TYPE']=np.where(pd.notna(stspt['TYPE_y']),stspt['TYPE_y'],stspt['TYPE_x'])
stspt=stspt.groupby(['BBL','LAT','LONG'],as_index=False).agg({'DBN': lambda x: '/'.join(x),'TYPE': lambda x: '/'.join(x),'ADDRESS':'count','APPROVED':'sum','DISAPPROVED':'sum','ID': lambda x: '/'.join(x)}).reset_index(drop=True)
stspt['DBN']=['/'.join(sorted(set(x.split('/')))) for x in stspt['DBN']]
stspt['DBNCOUNT']=[len(x.split('/')) for x in stspt['DBN']]
stspt['TYPE']=['/'.join(sorted(set(x.split('/')))) for x in stspt['TYPE']]
stspt['APPCOUNT']=stspt['ADDRESS'].copy()
stspt['IDS']=stspt['ID'].copy()
stspt=gpd.GeoDataFrame(stspt,geometry=[shapely.geometry.Point(x,y) for x,y in zip(stspt['LONG'],stspt['LAT'])],crs='epsg:4326')
stspt=gpd.sjoin(stspt,sd,how='left',op='intersects')
stspt=stspt[['BBL','DISTRICT','DBN','DBNCOUNT','TYPE','APPCOUNT','APPROVED','DISAPPROVED','IDS','geometry']].reset_index(drop=True)
stspt.to_file(path+'openlearning/streets_point.shp')



# Streets Segment 157/157
stsseg=pd.read_excel(path+'openlearning/OL.MasterApprovals_1007.xlsx',sheet_name='Streets',dtype=str)
stsseg['BORO']=[str(x).strip().upper() for x in stsseg['Borough']]
stsseg['BORO']=np.where(stsseg['BORO']=='MANHATTAN',1,np.where(stsseg['BORO']=='BRONX',2,
               np.where(stsseg['BORO']=='BROOKLYN',3,np.where(stsseg['BORO']=='QUEENS',4,
               np.where(stsseg['BORO']=='STATEN ISLAND',5,0)))))
stsseg['ON']=[str(x).strip().upper() for x in stsseg['on_street']]
stsseg['FROM']=[str(x).strip().upper() for x in stsseg['from_street']]
stsseg['TO']=[str(x).strip().upper() for x in stsseg['to_street']]
stsseg['APPROVED']=pd.to_numeric(stsseg['APPROVED'])
stsseg['DISAPPROVED']=pd.to_numeric(stsseg['DISAPPROVED'])
stsseg=stsseg[['ID','BORO','ON','FROM','TO','APPROVED','DISAPPROVED']].reset_index(drop=True)
stsseggeocode=[]
for i in stsseg.index:
    borocode=str(stsseg.loc[i,'BORO'])
    onstreet=str(stsseg.loc[i,'ON'])
    fromstreet=str(stsseg.loc[i,'FROM'])
    tostreet=str(stsseg.loc[i,'TO'])
    for j,k in [(x,y) for x in ['E','S','W','N'] for y in ['E','S','W','N']]:
        try:
            stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction_1':j,'compass_direction_2':k})
            stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
            stretch['SEGFROMNODE']=pd.to_numeric(stretch['Node Number'])
            stretch['SEGTONODE']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
            stretch=stretch.loc[0:len(stretch)-2,['SEGFROMNODE','SEGTONODE']].reset_index(drop=True)
            segment=pd.concat([stsseg.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
            segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
            stsseggeocode+=[segment]
        except:
            print(str(stsseg.loc[i,'ID'])+' not geocoded!')
            continue
        break
stsseggeocode=pd.concat(stsseggeocode,axis=0,ignore_index=True)
stsseggeocode=stsseggeocode.drop_duplicates(keep='first').reset_index(drop=True)
stsseggeocoderev=stsseggeocode.copy()
stsseggeocoderev['TEMP']=stsseggeocoderev['SEGFROMNODE'].copy()
stsseggeocoderev['SEGFROMNODE']=stsseggeocoderev['SEGTONODE'].copy()
stsseggeocoderev['SEGTONODE']=stsseggeocoderev['TEMP'].copy()
stsseggeocoderev=stsseggeocoderev.drop('TEMP',axis=1).reset_index(drop=True)
stsseggeocode=pd.concat([stsseggeocode,stsseggeocoderev],axis=0,ignore_index=True)
lion=gpd.read_file(path+'sidewalk/input/lion/lion.shp')
lion.crs='epsg:4326'
lion['SEGMENTID']=pd.to_numeric(lion['SegmentID'],errors='coerce')
lion['STREET']=[x.strip().upper() for x in lion['Street']]
lion['SEGFROMNODE']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
lion['SEGTONODE']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
lion=lion[['SEGMENTID','STREET','SEGFROMNODE','SEGTONODE','geometry']].reset_index(drop=True)
lion=lion.drop_duplicates(['SEGMENTID','STREET','SEGFROMNODE','SEGTONODE'],keep='first').reset_index(drop=True)
stsseggeocode=pd.merge(lion,stsseggeocode,how='inner',on=['SEGFROMNODE','SEGTONODE'])
stsseggeocode=stsseggeocode.sort_values('SEGMENTID').reset_index(drop=True)
stsseggeocode=stsseggeocode.drop_duplicates(['ID','SEGFROMNODE','SEGTONODE'],keep='first').reset_index(drop=True)
stsseggeocode=stsseggeocode[['SEGMENTID','ID','APPROVED','DISAPPROVED']].drop_duplicates(keep='first').reset_index(drop=True)
stsegadd=stsseg.loc[[26,36,54,88,95,95,133,134],['ID','APPROVED','DISAPPROVED']].copy()
stsegadd.loc[26,'SEGMENTID']=268
stsegadd.loc[36,'SEGMENTID']=22566
stsegadd.loc[54,'SEGMENTID']=21925
stsegadd.loc[88,'SEGMENTID']=33146
stsegadd.loc[95,'SEGMENTID']=[164398,33158]
stsegadd.loc[133,'SEGMENTID']=79838
stsegadd.loc[134,'SEGMENTID']=115724
stsseggeocode=pd.concat([stsseggeocode,stsegadd],axis=0,ignore_index=True)
stsseggeocode=stsseggeocode.groupby(['SEGMENTID'],as_index=False).agg({'ID':lambda x:'/'.join(x),'APPROVED':'sum','DISAPPROVED':'sum'}).reset_index(drop=True)
stsseggeocode['APPCOUNT']=stsseggeocode['APPROVED']+stsseggeocode['DISAPPROVED']
stsseggeocode['IDS']=stsseggeocode['ID'].copy()
lionsp=lion.drop_duplicates(['SEGMENTID'],keep='first').reset_index(drop=True)
stsseggeocode=pd.merge(lionsp,stsseggeocode,how='inner',on='SEGMENTID')
stsseggeocode=stsseggeocode[['SEGMENTID','STREET','APPCOUNT','APPROVED','DISAPPROVED','IDS','geometry']].reset_index(drop=True)
stsseggeocode.to_file(path+'openlearning/streets_segment.shp')



# Parks School 310/310
pkssc=pd.read_excel(path+'openlearning/OL.MasterApprovals_1007.xlsx',sheet_name='Parks',dtype=str,keep_default_na=False)
pkssc['DBN']=[str(x).strip().upper() for x in pkssc['DBN']]
pkssc['TYPE']=[str(x).strip().upper() for x in pkssc['TYPE']]
pkssc['ADDRESS']=[str(x).strip().upper() for x in pkssc['ADDRESS']]
pkssc['BORO']=[str(x).strip().upper() for x in pkssc['Borough']]
pkssc['APPROVED']=pd.to_numeric(pkssc['APPROVED'])
pkssc['DISAPPROVED']=pd.to_numeric(pkssc['DISAPPROVED'])
for i in pkssc.index:
    if pkssc.loc[i,'APPROVED']==1:
        pkssc.loc[i,'ID']=pkssc.loc[i,'ID']+'(Y)'
    else:
        pkssc.loc[i,'ID']=pkssc.loc[i,'ID']+'(N)' 
pkssc=pkssc[['ID','DBN','TYPE','ADDRESS','BORO','APPROVED','DISAPPROVED']].reset_index(drop=True)
pkssc=pd.merge(pkssc,lcgms,how='left',on='DBN')
for i in pkssc.index:
    if pd.isna(pkssc.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(pkssc.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(pkssc.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(pkssc.loc[i,'ADDRESS']) if re.search('ZipCode',x[1])])
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                pkssc.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                pkssc.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                pkssc.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(pkssc.loc[i,'ID'])+' not geocoded with zipcode!')
        except:
            print(str(pkssc.loc[i,'ID'])+' not geocoded with zipcode!')
for i in pkssc.index:
    if pd.isna(pkssc.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(pkssc.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(pkssc.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(pkssc.loc[i,'BORO']=='MN',1,np.where(pkssc.loc[i,'BORO']=='BX',2,
                        np.where(pkssc.loc[i,'BORO']=='BK',3,np.where(pkssc.loc[i,'BORO']=='QN',4,
                        np.where(pkssc.loc[i,'BORO']=='SI',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                pkssc.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                pkssc.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                pkssc.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(pkssc.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(pkssc.loc[i,'ID'])+' not geocoded with borough!')
pkssc=pkssc[pd.notna(pkssc['BBL'])].reset_index(drop=True)
pkssc['TYPE']=np.where(pd.notna(pkssc['TYPE_y']),pkssc['TYPE_y'],pkssc['TYPE_x'])
pkssc=pkssc.groupby(['BBL','LAT','LONG'],as_index=False).agg({'DBN': lambda x: '/'.join(x),'TYPE': lambda x: '/'.join(x),'ADDRESS':'count','APPROVED':'sum','DISAPPROVED':'sum','ID': lambda x: '/'.join(x)}).reset_index(drop=True)
pkssc['DBN']=['/'.join(sorted(set(x.split('/')))) for x in pkssc['DBN']]
pkssc['DBNCOUNT']=[len(x.split('/')) for x in pkssc['DBN']]
pkssc['TYPE']=['/'.join(sorted(set(x.split('/')))) for x in pkssc['TYPE']]
pkssc['APPCOUNT']=pkssc['ADDRESS'].copy()
pkssc['IDS']=pkssc['ID'].copy()
pkssc=gpd.GeoDataFrame(pkssc,geometry=[shapely.geometry.Point(x,y) for x,y in zip(pkssc['LONG'],pkssc['LAT'])],crs='epsg:4326')
pkssc=gpd.sjoin(pkssc,sd,how='left',op='intersects')
pkssc=pkssc[['BBL','DISTRICT','DBN','DBNCOUNT','TYPE','APPCOUNT','APPROVED','DISAPPROVED','IDS','geometry']].reset_index(drop=True)
pkssc.to_file(path+'openlearning/parks_school.shp')



# # Parks Park
# pkspk=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='Parks',dtype=str)
# pkspk['DBN']=[str(x).strip().upper() for x in pkspk['DBN']]

# pkssc['ADDRESS']=[str(x).strip().upper() for x in pkssc['Address (include zipcode)']]
# pkssc['BORO']=[str(x).strip().upper() for x in pkssc['Borough']]
# pkssc=pkssc[['ID','DBN','SCHOOL','ADDRESS','BORO']].reset_index(drop=True)
# pkssc=pd.merge(pkssc,school,how='left',on='DBN')


# for i in pkssc.index:
#     if pd.isna(pkssc.loc[i,'BBL']):
#         try:
#             streetname=pkssc.loc[i,'SCHOOL']
#             streetname='BRONX PARK MIDDLE SCHOOL'
#             boroughcode=np.where(pkssc.loc[i,'BORO']=='MN',1,np.where(pkssc.loc[i,'BORO']=='BX',2,
#                         np.where(pkssc.loc[i,'BORO']=='BK',3,np.where(pkssc.loc[i,'BORO']=='QN',4,
#                         np.where(pkssc.loc[i,'BORO']=='SI',5,0))))).tolist()
#             addr=g['1B']({'house_number':'','street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 pkssc.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 pkssc.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 pkssc.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#                 print(str(pkssc.loc[i,'ID'])+' is geocoded with borough!')
#             else:
#                 print(str(pkssc.loc[i,'ID'])+' not geocoded with borough!')
#         except:
#             print(str(pkssc.loc[i,'ID'])+' not geocoded with borough!')











# # JOP 246/263
# jop=pd.read_excel(path+'openlearning/JOP_LIST.xlsx',dtype=str)
# jop['ADDRESS']=[str(x).strip().upper() for x in jop['ADDRESS']]
# jop['BOROUGH']=[str(x).strip().upper() for x in jop['BOROUGH']]
# jop['SIGNNAME']=[str(x).strip().upper() for x in jop['SIGNNAME']]
# jop['LAT']=np.nan
# jop['LONG']=np.nan
# jop['BBL']=np.nan
# for i in jop.index:
#     if pd.isna(jop.loc[i,'BBL']):
#         try:
#             housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(jop.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
#             streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(jop.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
#             boroughcode=np.where(jop.loc[i,'BOROUGH']=='M',1,np.where(jop.loc[i,'BOROUGH']=='X',2,
#                         np.where(jop.loc[i,'BOROUGH']=='B',3,np.where(jop.loc[i,'BOROUGH']=='Q',4,
#                         np.where(jop.loc[i,'BOROUGH']=='R',5,0))))).tolist()
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 jop.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 jop.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 jop.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#             else:
#                 print(str(jop.loc[i,'ID'])+' not geocoded with borough!')
#         except:
#             print(str(jop.loc[i,'ID'])+' not geocoded with borough!')
# for i in jop.index:
#     if pd.isna(jop.loc[i,'BBL']):
#         try:
#             streetname=jop.loc[i,'SIGNNAME']
#             boroughcode=np.where(jop.loc[i,'BOROUGH']=='M',1,np.where(jop.loc[i,'BOROUGH']=='X',2,
#                         np.where(jop.loc[i,'BOROUGH']=='B',3,np.where(jop.loc[i,'BOROUGH']=='Q',4,
#                         np.where(jop.loc[i,'BOROUGH']=='R',5,0))))).tolist()
#             addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
#             if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
#                 jop.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
#                 jop.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
#                 jop.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
#             else:
#                 print(str(jop.loc[i,'ID'])+' not geocoded with name!')
#         except:
#             print(str(jop.loc[i,'ID'])+' not geocoded with name!')
# jop=jop[pd.notna(jop['BBL'])].reset_index(drop=True)
# jop=gpd.GeoDataFrame(jop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(jop['LONG'],jop['LAT'])],crs='epsg:4326')
# jop.to_file(path+'openlearning/jop.shp')
