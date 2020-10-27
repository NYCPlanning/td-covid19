# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 12:55:14 2020

@author: mayij
"""


import geopandas as gpd
import pandas as pd
import sqlalchemy as sal
path='C:/Users/mayij/Desktop/DOC/DCP2020/COVID19/SAFEGRAPH/'
eg=pd.read_csv(path+'engine.csv').loc[0,'engine']
engine=sal.create_engine(str(eg))


gdf=gpd.read_file('C:/Users/mayij/Desktop/or_cafe_zn_el_wd.geojson')
gdf.crs='epsg:4326'
gdf.to_postgis('test',engine,if_exists='append',schema='api')


sql="""
    SELECT * FROM quadstatebkgpclipped
    WHERE blockgroup='360610247001'
    """
sql="""
    SELECT * FROM api.test
    """
k=gpd.read_postgis(sql,engine,geom_col='geometry')
