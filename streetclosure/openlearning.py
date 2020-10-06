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

school=pd.read_csv(path+'openlearning/2019_-_2020_School_Locations.csv',dtype=str)
school['DBN']=[str(x).strip().upper() for x in school['system_code']]
for i in school.index:
    try:
        housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(school.loc[i,'primary_address_line_1']) if re.search('AddressNumber',x[1])])
        streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(school.loc[i,'primary_address_line_1']) if re.search('StreetName',x[1])])
        boroughcode=np.where(school.loc[i,'DBN'][2]=='M',1,np.where(school.loc[i,'DBN'][2]=='X',2,
                    np.where(school.loc[i,'DBN'][2]=='K',3,np.where(school.loc[i,'DBN'][2]=='Q',4,
                    np.where(school.loc[i,'DBN'][2]=='R',5,0))))).tolist()
        addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
        if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
            school.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
            school.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
            school.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
        else:
            print(school.loc[i,'DBN']+' not geocoded!')
    except:
        print(school.loc[i,'DBN']+' not geocoded!')
school['LAT']=np.where(pd.notna(school['LAT']),school['LAT'],pd.to_numeric(school['LATITUDE']))
school['LONG']=np.where(pd.notna(school['LONG']),school['LONG'],pd.to_numeric(school['LONGITUDE']))
school['BBL']=np.where(pd.notna(school['BBL']),school['BBL'],pd.to_numeric(school['Borough_block_lot']))
school=school[['DBN','LAT','LONG','BBL']].drop_duplicates(keep='first').reset_index(drop=True)



# School Yard Only 712/714
syo=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='School Yard Only',dtype=str)
syo['DBN']=[str(x).strip().upper() for x in syo['DBN (if known)']]
syo['ADDRESS']=[str(x).strip().upper() for x in syo['Address (include zipcode)']]
syo=syo[['ID','DBN','ADDRESS']].reset_index(drop=True)
syo=pd.merge(syo,school,how='left',on='DBN')
for i in syo.index:
    if pd.isna(syo.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syo.loc[i,'ADDRESS']) if re.search('ZipCode',x[1])])
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syo.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syo.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                syo.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
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
                syo.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syo.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                syo.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(syo.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(syo.loc[i,'ID'])+' not geocoded with borough!')
syo=syo[pd.notna(syo['BBL'])].reset_index(drop=True)
syo=syo.groupby(['BBL','LAT','LONG'],as_index=False).agg({'ID': lambda x: '/'.join(x)}).reset_index(drop=True)
syo=gpd.GeoDataFrame(syo,geometry=[shapely.geometry.Point(x,y) for x,y in zip(syo['LONG'],syo['LAT'])],crs='epsg:4326')
syo.to_file(path+'openlearning/school_yard_only.shp')



# School Yard + Pending 710/716
syp=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='School Yard + Pending',dtype=str)
syp['DBN']=[str(x).strip().upper() for x in syp['DBN (if known)']]
syp['ADDRESS']=[str(x).strip().upper() for x in syp['Address (include zipcode)']]
syp=syp[['ID','DBN','ADDRESS']].reset_index(drop=True)
syp=pd.merge(syp,school,how='left',on='DBN')
for i in syp.index:
    if pd.isna(syp.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(syp.loc[i,'ADDRESS']) if re.search('ZipCode',x[1])])
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                syp.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syp.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                syp.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
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
                syp.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                syp.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                syp.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(syp.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(syp.loc[i,'ID'])+' not geocoded with borough!')
syp=syp[pd.notna(syp['BBL'])].reset_index(drop=True)
syp=syp.groupby(['BBL','LAT','LONG'],as_index=False).agg({'ID': lambda x: '/'.join(x)}).reset_index(drop=True)
syp=gpd.GeoDataFrame(syp,geometry=[shapely.geometry.Point(x,y) for x,y in zip(syp['LONG'],syp['LAT'])],crs='epsg:4326')
syp.to_file(path+'openlearning/school_yard_pending.shp')



# Streets Point 152/156
stspt=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='Streets',dtype=str)
stspt['DBN']=[str(x).strip().upper() for x in stspt['DBN # (DOE ONLY)']]
stspt['BORO']=[str(x).strip().upper() for x in stspt['Borough']]
stspt['ADDRESS']=[str(x).strip().upper() for x in stspt['Address']]
stspt['ZIPCODE']=[str(x).strip().upper() for x in stspt['Zip']]
stspt=stspt[['ID','DBN','ADDRESS','ZIPCODE','BORO']].reset_index(drop=True)
stspt=pd.merge(stspt,school,how='left',on='DBN')
for i in stspt.index:
    if pd.isna(stspt.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(stspt.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            zipcode=stspt.loc[i,'ZIPCODE']
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'zip_code':zipcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                stspt.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                stspt.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                stspt.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
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
                stspt.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                stspt.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                stspt.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(stspt.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(stspt.loc[i,'ID'])+' not geocoded with borough!')
stspt=stspt[pd.notna(stspt['BBL'])].reset_index(drop=True)
stspt=stspt.groupby(['BBL','LAT','LONG'],as_index=False).agg({'ID': lambda x: '/'.join(x)}).reset_index(drop=True)
stspt=gpd.GeoDataFrame(stspt,geometry=[shapely.geometry.Point(x,y) for x,y in zip(stspt['LONG'],stspt['LAT'])],crs='epsg:4326')
stspt.to_file(path+'openlearning/streets_point.shp')



# Streets Segment 126/156
stsseg=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='Streets',dtype=str)
stsseg['BORO']=[str(x).strip().upper() for x in stsseg['Borough']]
stsseg['BORO']=np.where(stsseg['BORO']=='MANHATTAN',1,np.where(stsseg['BORO']=='BRONX',2,
               np.where(stsseg['BORO']=='BROOKLYN',3,np.where(stsseg['BORO']=='QUEENS',4,
               np.where(stsseg['BORO']=='STATEN ISLAND',5,0)))))
stsseg['ON']=[str(x).strip().upper() for x in stsseg['on_street']]
stsseg['FROM']=[str(x).strip().upper() for x in stsseg['from_street']]
stsseg['TO']=[str(x).strip().upper() for x in stsseg['to_street']]
stsseg=stsseg[['ID','BORO','ON','FROM','TO']].reset_index(drop=True)
stsseggeocode=[]
for i in stsseg.index:
    borocode=str(stsseg.loc[i,'BORO'])
    onstreet=str(stsseg.loc[i,'ON'])
    fromstreet=str(stsseg.loc[i,'FROM'])
    tostreet=str(stsseg.loc[i,'TO'])
    try:
        stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'E'})
        stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
        stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
        stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
        stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
        segment=pd.concat([stsseg.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
        segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
        stsseggeocode+=[segment]
    except:
        try:
            stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'S'})
            stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
            stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
            stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
            stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
            segment=pd.concat([stsseg.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
            segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
            stsseggeocode+=[segment]
        except:
            try:
                stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'W'})
                stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
                stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
                stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
                stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
                segment=pd.concat([stsseg.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
                segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
                stsseggeocode+=[segment]
            except:
                try:
                    stretch=g['3S']({'borough_code':borocode,'on':onstreet,'from':fromstreet,'to':tostreet,'compass_direction':'N'})
                    stretch=pd.DataFrame(stretch['LIST OF INTERSECTIONS'])
                    stretch['segfromnode']=pd.to_numeric(stretch['Node Number'])
                    stretch['segtonode']=pd.to_numeric(np.roll(stretch['Node Number'],-1))
                    stretch=stretch.loc[0:len(stretch)-2,['segfromnode','segtonode']].reset_index(drop=True)
                    segment=pd.concat([stsseg.loc[[i]]]*len(stretch),axis=0,ignore_index=True)
                    segment=pd.concat([segment,stretch],axis=1,ignore_index=False)
                    stsseggeocode+=[segment]
                except:
                    print(str(i))
stsseggeocode=pd.concat(stsseggeocode,axis=0,ignore_index=True) # 193/197
stsseggeocode=stsseggeocode.drop_duplicates(keep='first').reset_index(drop=True)
stsseggeocoderev=stsseggeocode.copy()
stsseggeocoderev['temp']=stsseggeocoderev['segfromnode'].copy()
stsseggeocoderev['segfromnode']=stsseggeocoderev['segtonode'].copy()
stsseggeocoderev['segtonode']=stsseggeocoderev['temp'].copy()
stsseggeocoderev=stsseggeocoderev.drop('temp',axis=1).reset_index(drop=True)
stsseggeocode=pd.concat([stsseggeocode,stsseggeocoderev],axis=0,ignore_index=True)
lion=gpd.read_file(path+'sidewalk/input/lion/lion.shp')
lion.crs='epsg:4326'
lion['segmentid']=pd.to_numeric(lion['SegmentID'],errors='coerce')
lion['street']=[x.strip().upper() for x in lion['Street']]
lion['segfromnode']=pd.to_numeric(lion['NodeIDFrom'],errors='coerce')
lion['segtonode']=pd.to_numeric(lion['NodeIDTo'],errors='coerce')
lion=lion[['segmentid','street','segfromnode','segtonode','geometry']].reset_index(drop=True)
lion=lion.drop_duplicates(['segmentid','street','segfromnode','segtonode'],keep='first').reset_index(drop=True)
stsseggeocode=pd.merge(lion,stsseggeocode,how='inner',on=['segfromnode','segtonode'])
stsseggeocode=stsseggeocode.sort_values('segmentid').reset_index(drop=True)
stsseggeocode=stsseggeocode.drop_duplicates(['ID','BORO','ON','FROM','TO','segfromnode','segtonode'],keep='first').reset_index(drop=True)
stsseggeocode=stsseggeocode[['segmentid','ID','BORO','ON','FROM','TO']].drop_duplicates(keep='first').reset_index(drop=True)
stsseggeocode=stsseggeocode.groupby(['segmentid'],as_index=False).agg({'ID':lambda x:'/'.join(x)}).reset_index(drop=True)
lion=lion.drop_duplicates(['segmentid'],keep='first').reset_index(drop=True)
stsseggeocode=pd.merge(lion,stsseggeocode,how='inner',on='segmentid')
stsseggeocode=stsseggeocode[['segmentid','street','ID','geometry']].reset_index(drop=True) # 193/197
stsseggeocode.to_file(path+'openlearning/streets_segment.shp')



# Parks School 270/310
pkssc=pd.read_excel(path+'openlearning/OL.MasterApprovals_1005.xlsx',sheet_name='Parks',dtype=str)
pkssc['DBN']=[str(x).strip().upper() for x in pkssc['DBN']]
pkssc['SCHOOL']=[str(x).strip().upper() for x in pkssc['School Name']]
pkssc['ADDRESS']=[str(x).strip().upper() for x in pkssc['Address (include zipcode)']]
pkssc['BORO']=[str(x).strip().upper() for x in pkssc['Borough']]
pkssc=pkssc[['ID','DBN','SCHOOL','ADDRESS','BORO']].reset_index(drop=True)
pkssc=pd.merge(pkssc,school,how='left',on='DBN')
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
pkssc=pkssc.groupby(['BBL','LAT','LONG'],as_index=False).agg({'ID': lambda x: '/'.join(x)}).reset_index(drop=True)
pkssc=gpd.GeoDataFrame(pkssc,geometry=[shapely.geometry.Point(x,y) for x,y in zip(pkssc['LONG'],pkssc['LAT'])],crs='epsg:4326')
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











# JOP 246/263
jop=pd.read_excel(path+'openlearning/JOP_LIST.xlsx',dtype=str)
jop['ADDRESS']=[str(x).strip().upper() for x in jop['ADDRESS']]
jop['BOROUGH']=[str(x).strip().upper() for x in jop['BOROUGH']]
jop['SIGNNAME']=[str(x).strip().upper() for x in jop['SIGNNAME']]
jop['LAT']=np.nan
jop['LONG']=np.nan
jop['BBL']=np.nan
for i in jop.index:
    if pd.isna(jop.loc[i,'BBL']):
        try:
            housenumber=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(jop.loc[i,'ADDRESS']) if re.search('AddressNumber',x[1])])
            streetname=' '.join([x[0].replace(',','').upper() for x in usaddress.parse(jop.loc[i,'ADDRESS']) if re.search('StreetName',x[1])])
            boroughcode=np.where(jop.loc[i,'BOROUGH']=='M',1,np.where(jop.loc[i,'BOROUGH']=='X',2,
                        np.where(jop.loc[i,'BOROUGH']=='B',3,np.where(jop.loc[i,'BOROUGH']=='Q',4,
                        np.where(jop.loc[i,'BOROUGH']=='R',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                jop.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                jop.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                jop.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(jop.loc[i,'ID'])+' not geocoded with borough!')
        except:
            print(str(jop.loc[i,'ID'])+' not geocoded with borough!')
for i in jop.index:
    if pd.isna(jop.loc[i,'BBL']):
        try:
            streetname=jop.loc[i,'SIGNNAME']
            boroughcode=np.where(jop.loc[i,'BOROUGH']=='M',1,np.where(jop.loc[i,'BOROUGH']=='X',2,
                        np.where(jop.loc[i,'BOROUGH']=='B',3,np.where(jop.loc[i,'BOROUGH']=='Q',4,
                        np.where(jop.loc[i,'BOROUGH']=='R',5,0))))).tolist()
            addr=g['1B']({'house_number':housenumber,'street_name':streetname,'borough_code':boroughcode})
            if addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']!='':
                jop.loc[i,'LAT']=pd.to_numeric(addr['Latitude'])
                jop.loc[i,'LONG']=pd.to_numeric(addr['Longitude'])
                jop.loc[i,'BBL']=pd.to_numeric(addr['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)'])
            else:
                print(str(jop.loc[i,'ID'])+' not geocoded with name!')
        except:
            print(str(jop.loc[i,'ID'])+' not geocoded with name!')
jop=jop[pd.notna(jop['BBL'])].reset_index(drop=True)
jop=gpd.GeoDataFrame(jop,geometry=[shapely.geometry.Point(x,y) for x,y in zip(jop['LONG'],jop['LAT'])],crs='epsg:4326')
jop.to_file(path+'openlearning/jop.shp')
