import pandas as pd
import numpy as np
import os
import datetime as dt

# import geopandas as gpd
# from geopandas.tools import sjoin

import folium
from folium.plugins import MarkerCluster

import sqlalchemy


def load_elevator_report(location, conn):
    # get list of all unique terminals from the trxdata
    clause = '''SELECT
                    "location" as locationcode,
                    "trxdate",
                    'doy',
                    'yr'
                    SUM ( netweight::integer ) AS snetweight,
                    AVG ( a_screens::float ) AS ascreens,
                    AVG ( a_protein::float ) AS aprotein,
                    AVG ( a_qs::float ) AS aqs,
                    AVG ( a_percent_phys_damage::float ) AS apdamage,
                    AVG ( "a_plump 6/64"::float ) AS aplump,
                    AVG ( a_immature::float ) AS aimmature,
                    yr AS harvestyear 
                FROM
                    trx_raw 
                WHERE
                    "location"!='' and "location"!='DD'
                GROUP BY
                    "location",
                    "trxdate",
                  yr	
                ORDER BY
                    "location",
                    "trxdate",
                    yr;'''

    #read in the database table
    df = pd.read_sql(clause,conn, parse_dates=['trxdate'])

    # make sure columns are coverted from sql to posgress capitalization
    # df.columns = [i.lower().strip() for i in df.columns.tolist()]

    #add harvest year, month, doy for calculations
    # df['yr'] = df['trxdate'].dt.year
    # df['mnth'] = df['trxdate'].dt.month
    # df['doy'] = df['trxdate'].dt.dayofyear
    df['nloads']=1


    # group the data for year over year comparison one location
    groupbys = ['locationcode','doy', 'yr']
    cols = ['locationcode', 'snetweight','ascreens','aprotein','aqs','apdamage','aplump','aimmature', 'doy', 'yr']
    df = df[cols]

    agg = {
        'snetweight' : np.sum,
        'ascreens' : np.average,
        'aprotein' : np.average,
        'aqs' : np.average,
        'apdamage' : np.average,
        'aplump' : np.average,
        'aimmature': np.average
    }

    df = df.groupby(groupbys).agg(agg)
    df = df.reset_index()

    df_16 = df.loc[df['yr'] == 2016]
    df_17 = df.loc[df['yr'] == 2017]
    return df_16, df_17




def connect(user, password, db, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con)
    return con, meta



def load_fields():
    # get list of all unique terminals from the trxdata
    clause = "SELECT distinct lat, lon, partnername, trim(locationcode) as locationcode from grower_latlon"

    # read in the database table
    df = pd.read_sql(clause, conn)
    df.columns = [i.lower().strip() for i in df.columns.tolist()]
    df.lat = df.lat.astype('float')
    df.lon = df.lon.astype('float')
    return df



def make_regional_map(jf,loc, yr, output_file='firstmap.html'):

    print(jf.head())

    df = jf     #[jf['locationcode']==loc]

    # generate a simple map html page from df lat/lon and folium module
    map = folium.Map(location=[df['lat'].mean(), df['lon'].mean()],
                     zoom_start = 6,
                     tiles='https://api.mapbox.com/styles/v1/mapbox/satellite-streets-v10/tiles/256/{z}/{x}/{y}?access_token='+api_key,
                     attr='Mapbox')
    #Loop through the df location and add them to the map point by point
    fg = folium.FeatureGroup(name="{} Field Locations, {} Region".format(yr, loc))
    for lat, lon, name, in zip(df['lat'],df['lon'],df['partnername']):
        fg.add_child(folium.Marker(location=[lat,lon],
                                   popup=(folium.Popup('Partner: ' + name + " \n\n " +str(( np.round(lat,2),np.round(lon,2)))))))
    map.add_child(fg)
    map.save(outfile=output_file)
    return



if __name__ == '__main__':

    dbs= {
        "user" : 'wadere',
        "password" : 'Guntherbob1',
        "host" : 'testdb.cyhvcrbdndgh.us-east-1.rds.amazonaws.com',
        "port" : 5433,
        "db" : 'BBrain'
    }

    api_key = 'pk.eyJ1Ijoid2FkZXJlIiwiYSI6ImNqMDV0eXUzYzBrNGYzM2xzM3VoaWc2cTEifQ.A7rIBh-H-_Fw-IJET0L9CA'

    # Setup connection to database
    conn, meta = connect(dbs['user'], dbs['password'], dbs['db'], dbs['host'] )

    # load elevator report trxdata current and prior year
    # xf16, xf17 = load_elevator_report('LL', conn)
    # print(xf17.head())

    # load 2016 fields and lat/longs for those fields.....
    df = load_fields()


    make_regional_map(df, "WW", 2017)


# ==============================================================================
# ==============================================================================


# ==============================================================================
# ==============================================================================
