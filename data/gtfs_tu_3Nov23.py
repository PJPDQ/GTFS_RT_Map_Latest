from google.transit import gtfs_realtime_pb2
import urllib
import urllib.request
import time
import datetime
import os
import multiprocessing
import re
import pandas as pd
from datetime import date
from zipfile import ZipFile
from pyproj import Geod
from google.transit import gtfs_realtime_pb2
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson
import requests
import json
import pandas as pd
from collections import OrderedDict
import datetime
from protobuf_to_dict import protobuf_to_dict
import mysql.connector
from sqlalchemy import create_engine
from pandas.io import sql
import MySQLdb
from IPython.display import clear_output
import pika
import sys
import logging

wgs84_geod = Geod(ellps='WGS84') #Distance will be measured on this ellipsoid - more accurate than a spherical method
def monthname(mydate):
    mydate = datetime.datetime.now()
    m = mydate.strftime("%B")
    return(m)

def Brisbane(epoch):
    a = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(epoch))
    return(a)

def createCSVfile(inputlist , name):
    with open( name , 'w') as f:
        for i in inputlist:
            k = 0
            for item in i:  
                f.write(str(item) + ',')
                k = k+1
            f.write('\n')
        return(f)
def inputCSVfile(csvfile):
    list1= []
    with open(csvfile, 'r') as f:
        for i in f:
            j = i.split(',')
            
            le = len(j)
            j[le - 1] = (j[le- 1]).strip()
            list1.append(j)
        return(list1)

def createCSVfileWCD(inputlist , name):
    with open( name , 'w') as f:
        for i in inputlist:
            f.write(str(i))
            f.write('\n')
        return(f)
def Distance(lat1,lon1,lat2,lon2):
    az12 ,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
    return (dist)

Working_Directory = r"Y:\\Data\\GTFS_NEW\\"
Saved_Directory = r"Y:\\Sentosa\\1Dec22\\Static_Preprocessing\\"
local_path = r".\\data"
if not os.path.exists(Working_Directory):
    working_path = f"{local_path}\\GTFS_NEW\\"
    if not os.path.exists(working_path):
        os.makedirs(working_path)
    saving_path = f"{local_path}\\Static_Preprocessing\\"
    if not os.path.exists(saving_path):
        os.makedirs(saving_path)
        saving_path = f"{saving_path}\\1Dec22\\"
        os.makedirs(saving_path)
    Working_Directory = working_path
    Saved_Directory = saving_path
Static_Folder = Working_Directory + os.sep + 'GTFS Static/'
Realtime_folder  = Working_Directory+ os.sep + 'GTFS Realtime/'
if not os.path.exists(Static_Folder):
    os.makedirs(Static_Folder)
if not os.path.exists(Realtime_folder):
    os.makedirs(Realtime_folder)

gtfs_static_link = os.environ.get("GTFS")
gtfs_realtime_link = os.environ.get("GTFS_RL_TU")
EXCHANGE_NAME = os.environ.get("EXCHANGE_NAME")
EXCHANGE_TYPE = os.environ.get("EXCHANGE_TYPE")
def producer_init():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ.get("HOST")))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)
    return channel, connection
def producer_start(channel, conn, df):
    def data_prep():
        msg = df.to_json()
        return msg.encode('ascii')
    message = data_prep()
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body=message)
    
t1 = 0
datee = datetime.datetime.strptime(str(Brisbane(t1)),"%d-%m-%Y %H:%M:%S" )
m = datee.month
y = datee.year
d = datee.day
m_name = monthname(Brisbane(t1))
date_name1 = str(d)+"-"+str(m)+"-"+str(y)
TableName = str(m_name)+"_"+str(y)
TableName = TableName.lower()
channel, conn = producer_init()
while True:
    try:
        #Directories for a new Day
        t = int(time.time())
        datee = datetime.datetime.strptime(str(Brisbane(t)),"%d-%m-%Y %H:%M:%S" )
        m = datee.month
        y = datee.year
        d = datee.day
        m_name = monthname(Brisbane(t))
        date_name = str(d)+"-"+str(m)+"-"+str(y)
        Month_year = str(m_name)+" ,"+str(y)
        if date_name!= date_name1:
            index_o = 0
            datee = datetime.datetime.strptime(str(Brisbane(t)),"%d-%m-%Y %H:%M:%S" )
            m = datee.month
            y = datee.year
            d = datee.day
            m_name = monthname(Brisbane(t))
            date_name = str(d)+"-"+str(m)+"-"+str(y)
            Month_year = str(m_name)+" ,"+str(y)
            TableName = str(m_name)+"_"+str(y)
            TableName = TableName.lower()
            TUdirectory = Realtime_folder + "TripUpdate entity" +"/" + 'TU '+str(Month_year)
            VPdirectory = Realtime_folder + "VehiclePosition entity" +"/" + 'VP '+str(Month_year)
            if not os.path.exists(TUdirectory):
                os.makedirs(TUdirectory)
            TUdirectory2= TUdirectory+ "/"+ 'TU '+str(date_name)
            if not os.path.exists(TUdirectory2):
                os.makedirs(TUdirectory2) 
            TUdirectory3= TUdirectory2+ "/"+ 'TU feeds '+str(date_name)
            TUdirectory4= TUdirectory2+ "/"+ 'TU Speed Analysis'+str(date_name)
            if not os.path.exists(TUdirectory3):
                os.makedirs(TUdirectory3)
            if not os.path.exists(TUdirectory4):
                os.makedirs(TUdirectory4)
            if not os.path.exists(VPdirectory):
                os.makedirs(VPdirectory)
            VPdirectory2= VPdirectory+ "/"+ 'VP '+str(date_name)
            if not os.path.exists(VPdirectory2):
                os.makedirs(VPdirectory2)
#Downloading and processing static file

            GTFS_Static = urllib.request.urlretrieve(gtfs_static_link, Static_Folder + '/GTFS Static ' +date_name + '.zip')
            zip_file = ZipFile(Static_Folder + '/GTFS Static ' +date_name + '.zip')
            trips = pd.read_csv(zip_file.open('trips.txt'))
            stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
            shapes = pd.read_csv(zip_file.open('shapes.txt'))
            shapes['shape_pt_sequence'] = shapes['shape_pt_sequence'].astype(str)
            Route_Shape_stop = stop_times.merge(trips, on = 'trip_id', how = 'left')
            Route_Shape_stop = Route_Shape_stop[['shape_id', 'stop_id', 'stop_sequence', 'route_id', 'trip_id']].drop_duplicates(keep = 'first')
            Route_Shape_stop['stop_sequence'] = Route_Shape_stop['stop_sequence'].astype(int)
            Route_Shape_stop['stop_id'] = Route_Shape_stop['stop_id'].astype(str)
            shapes2 = shapes.copy(deep = True)
            shapes2['shape_pt_sequence_next'] = (shapes2['shape_pt_sequence'].astype(int) + 1).astype(str)
            shapes2['stop_seq_1'] = (shapes2['shape_pt_sequence'].astype(int)/10000).astype(int)
            shapes2['stop_seq_2'] = (shapes2['shape_pt_sequence_next'].astype(int)/10000).astype(int)
            shapes3 = shapes2.merge(shapes, left_on = ['shape_id', 'shape_pt_sequence_next'], right_on = ['shape_id', 'shape_pt_sequence'], how = 'left')
            shapes3['distance'] = Distance(shapes3['shape_pt_lat_x'].tolist(),shapes3['shape_pt_lon_x'].tolist(),shapes3['shape_pt_lat_y'].tolist(),shapes3['shape_pt_lon_y'].tolist())
            shapes3['distance'] = shapes3['distance']/1000
            shapes3 = shapes3.loc[shapes3['stop_seq_1'] == shapes3['stop_seq_2']]
            shapes3['stop_seq_2'] = shapes3['stop_seq_2'] + 1
            shapes4 = pd.DataFrame(shapes3.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'], as_index = False)['distance'].sum())
            distance_file = shapes4
            date_name1 = date_name
#             distance_file.to_csv('distance_file.csv')
        
        feed = gtfs_realtime_pb2.FeedMessage()
        response = urllib.request.urlopen(gtfs_realtime_link)
        feed.ParseFromString(response.read())
        dict_obj = protobuf_to_dict(feed)
        r = json.dumps(dict_obj)
        loaded_r = json.loads(r)
        kppa = pd.DataFrame(loaded_r['entity'])
        kt = kppa.trip_update.apply(pd.Series)
        kt = pd.concat([kppa, kt], axis= 1)
        kt2 = kt['stop_time_update'].apply(pd.Series).reset_index().melt(id_vars='index').dropna()[['index', 'value']].set_index('index')
        kt2 = kt.merge(kt2, left_index=True, right_index=True, how = 'left')
        kt2 = kt2[[ 'id', 'trip_update', 'vehicle',  'stop_time_update', 'timestamp', 'trip', 'value']]
        kt3 = kt2.value.apply(pd.Series)
        kt3_1 = kt3.arrival.apply(pd.Series)
        kt3_1 = kt3_1[['delay', 'time', 'uncertainty']]
        kt3_1.columns = [ 'arrival_delay', 'arrival_time', 'arrival_uncertainty']
        kt3_2 = kt3.departure.apply(pd.Series)
        kt3_2 = kt3_2[['delay', 'time', 'uncertainty']]
        kt3_2.columns = ['departure_delay', 'departure_time', 'departure_uncertainty']
        kt3_f = pd.concat([kt3, kt3_1,kt3_2], axis = 1)
        kt3_f = kt3_f[[ 'schedule_relationship', 'stop_id', 'stop_sequence',
                              'arrival_delay', 'arrival_time', 'arrival_uncertainty', 'departure_delay',
                            'departure_time', 'departure_uncertainty']]
        kt4 = kt2.trip.apply(pd.Series)
        kt5 = kt2.vehicle.apply(pd.Series)
        KT_F = pd.concat([kt2, kt3_f, kt4, kt5], axis=1)
        KT_F = KT_F[['trip_id', 'start_time', 'start_date', 'route_id', 'stop_id', 'stop_sequence', 'arrival_delay', 'arrival_time', 'arrival_uncertainty','departure_delay' , 'departure_time',  'departure_uncertainty', 'schedule_relationship', 'id', 'timestamp']]
        KT_F = KT_F.loc[~KT_F['timestamp'].isna()]
        KT_F = KT_F.loc[:,~KT_F.columns.duplicated()]
        KT_F = KT_F.merge(Route_Shape_stop, on = ['stop_sequence', 'stop_id', 'route_id', 'trip_id'], how = 'left')
        producer_start(channel, conn, KT_F)
        KT_F_2 = KT_F.copy(deep = True)
        KT_F_copy = KT_F.copy(deep = True).loc[~KT_F['stop_sequence'].isna()][['shape_id', 'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
        KT_F_copy['stop_sequence2'] =  KT_F_copy['stop_sequence']-1
        KT_F_copy = KT_F_copy.merge(KT_F_copy, left_on = ['trip_id', 'stop_sequence'], right_on = ['trip_id', 'stop_sequence2'], how = 'inner')
        KT_F_copy[['stop_sequence_x', 'stop_sequence_y']] = KT_F_copy[['stop_sequence_x', 'stop_sequence_y']].astype(int)
        distance_file[['stop_seq_1', 'stop_seq_2']] = distance_file[['stop_seq_1', 'stop_seq_2']].astype(int)
        KT_F_copy = KT_F_copy.merge(distance_file, left_on = ['shape_id_x', 'stop_sequence_x', 'stop_sequence_y'], right_on = ['shape_id', 'stop_seq_1', 'stop_seq_2'], how = 'inner')
        KT_F_copy['Travel_Speed_DtoA'] = 3600*KT_F_copy['distance']/(KT_F_copy['arrival_time_y'] - KT_F_copy['departure_time_x'])
        response.close()
    except:
        pass
    
GTFS_Static = urllib.request.urlretrieve(gtfs_static_link, Static_Folder + '/GTFS Static ' +date_name + '.zip')
zip_file = ZipFile(Static_Folder + '/GTFS Static ' +date_name + '.zip')
trips = pd.read_csv(zip_file.open('trips.txt'))
stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
shapes = pd.read_csv(zip_file.open('shapes.txt'))
shapes['shape_pt_sequence'] = shapes['shape_pt_sequence'].astype(str)
Route_Shape_stop = stop_times.merge(trips, on = 'trip_id', how = 'left')
Route_Shape_stop = Route_Shape_stop[['shape_id', 'stop_id', 'stop_sequence', 'route_id', 'trip_id']].drop_duplicates(keep = 'first')
Route_Shape_stop['stop_sequence'] = Route_Shape_stop['stop_sequence'].astype(int)
Route_Shape_stop['stop_id'] = Route_Shape_stop['stop_id'].astype(str)
shapes2 = shapes.copy(deep = True)
shapes2['shape_pt_sequence_next'] = (shapes2['shape_pt_sequence'].astype(int) + 1).astype(str)
shapes2['stop_seq_1'] = (shapes2['shape_pt_sequence'].astype(int)/10000).astype(int)
shapes2['stop_seq_2'] = (shapes2['shape_pt_sequence_next'].astype(int)/10000).astype(int)
shapes3 = shapes2.merge(shapes, left_on = ['shape_id', 'shape_pt_sequence_next'], right_on = ['shape_id', 'shape_pt_sequence'], how = 'left')
shapes3['distance'] = Distance(shapes3['shape_pt_lat_x'].tolist(),shapes3['shape_pt_lon_x'].tolist(),shapes3['shape_pt_lat_y'].tolist(),shapes3['shape_pt_lon_y'].tolist())
shapes3['distance'] = shapes3['distance']/1000
shapes3 = shapes3.loc[shapes3['stop_seq_1'] == shapes3['stop_seq_2']]
shapes3['stop_seq_2'] = shapes3['stop_seq_2'] + 1
shapes4 = pd.DataFrame(shapes3.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'], as_index = False)['distance'].sum())
distance_file = shapes4
date_name1 = date_name
distance_file.to_csv('distance_file.csv')