import pandas as pd

pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/COVID19/SURFACE/'

pdf=pd.read_csv(path+'psam_p36.csv',dtype=str)
p=pdf[['SERIALNO','SPORDER','PUMA','ST','NAICSP','JWTR','RAC1P','HISP','JWMNP','PWGTP']]
p.to_csv(path+'p.csv',index=False)


hdf=pd.read_csv(path+'psam_h36.csv',dtype=str)
h=hdf[['SERIALNO','HINCP']]
h.to_csv(path+'h.csv',index=False)


