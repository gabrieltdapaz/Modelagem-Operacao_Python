import xarray as xr
import pandas as pd
from datetime import datetime, timedelta


cities = pd.read_csv('file.csv', sep=';')
var = "total_precipitation"

for index, coluna in cities.iterrows():

    city = coluna['cidade']
    uf = coluna['UF']

    print(city)

    file_in =  f"test/{uf}_{city.replace(' ','_')}_{var}.csv"
    file_out = f"extract_556/{uf}_{city.replace(' ','_')}_{var}.csv"

    csv = pd.read_csv(file_in, sep=',', header=None, names=["latitude", "longitude", "values(mm)"])

    csv.index = pd.to_datetime(csv.index)

    csv1 = csv.resample("D").sum()
    csv1['latitude'] = csv['latitude'][0]
    csv1['longitude'] = csv['longitude'][0]

    csv1['datetime'] = csv1.index.strftime('%Y-%m-%d 00:00:00')
    csv1 = csv1[['datetime','latitude','longitude','values(mm)']]

    csv1['latitude'] = csv1['latitude'].map('{:,.2f}'.format)
    csv1['longitude'] = csv1['longitude'].map('{:,.2f}'.format)
    csv1['values(mm)'] = csv1['values(mm)'].map('{:,.2f}'.format)

    csv1.to_csv(file_out, sep=',', index=None)



