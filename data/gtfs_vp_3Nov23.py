from google.transit import gtfs_realtime_pb2
import urllib
import urllib.request
import time
import datetime
import os
import multiprocessing
import re
import pandas as pd
from datetime import date, timedelta
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
from pyproj import Geod
import geopandas as gpd
from shapely.geometry import Point, LineString
from gtfs_functions import speed_trip_trajectory_preprocessing_analysis, ckdnearest, flatten_link_names, high_frequency_buses, shaper_mapper, bus_route_path
from IPython.display import clear_output
import pika
import sys
import logging

wgs84_geod = Geod(ellps='WGS84') #Distance will be measured on this ellipsoid - more accurate than a spherical method
def monthname(mydate):
    ##Generate the today's month name instead of the selected date
    #mydate = datetime.datetime.now()
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
gtfs_realtime_link = os.environ.get("GTFS_RL_VP")
HFS_DIR = r"Y:\\Sentosa\\FINAL_STATIC_GDF_26-8-2021\\"
if not os.path.exists(HFS_DIR):
    HFS_DIR = f".\\FINAL_STATIC_GDF_26-8-2021\\"
gdf = gpd.read_file(HFS_DIR + "FINAL_STATIC_GDF_26-8-2021.shp")
EXCHANGE_NAME = 'logs'

def producer_init():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("HOST")))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=os.environ.get("EXCHANGE_TYPE"))
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
m_name = monthname(datee)
date_name1 = str(d)+"-"+str(m)+"-"+str(y)
TableName = str(m_name)+"_"+str(y)
TableName = TableName.lower()
index_o = 0
curr_date = date_name1
########################Between 12:30am to 4:30am Operating Hours#############################
end_time = datetime.time(0,30,0)
start_time = datetime.time(4,30,0)
static_curr_date = date_name1
static_end_time = datetime.time(12,20,0)
static_start_time = datetime.time(13,30,0)
i = 1
flags = 0
#### Producer created ######
channel, conn = producer_init()
# print("producer is initialized!")
while True:
    try:
        start = time.time()
        clear_output(wait=True)
        #Directories for a new Day
        t = int(time.time())
        datee = datetime.datetime.strptime(str(Brisbane(t)),"%d-%m-%Y %H:%M:%S" )
        m = datee.month
        y = datee.year
        d = datee.day
        m_name = monthname(datee)
        date_name = str(d)+"-"+str(m)+"-"+str(y)
        Month_year = str(m_name)+" ,"+str(y)
        if date_name!= date_name1:
            index_o = 0
            datee = datetime.datetime.strptime(str(Brisbane(t)),"%d-%m-%Y %H:%M:%S" )
            m = datee.month
            y = datee.year
            d = datee.day
            m_name = monthname(datee)
            date_name = str(d)+"-"+str(m)+"-"+str(y)
            TableName = str(m_name)+"_"+str(y)
            TableName = TableName.lower()
            Month_year = str(m_name)+" ,"+str(y)
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
            
            HFSdirectory = f"{Saved_Directory}HFS\\{Month_year}"
            if not os.path.exists(HFSdirectory):
                os.makedirs(HFSdirectory)
            shapesdirectory = f"{Saved_Directory}shapes4\\{Month_year}"
            if not os.path.exists(shapesdirectory):
                os.makedirs(shapesdirectory)
            #Downloading and processing static file
            print("starting to process static!")
            GTFS_Static = urllib.request.urlretrieve(gtfs_static_link, Static_Folder + '/GTFS Static ' +date_name + '.zip')
            zip_file = ZipFile(Static_Folder + '/GTFS Static ' +date_name + '.zip')
            trips = pd.read_csv(zip_file.open('trips.txt'))
            stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
            shapes = pd.read_csv(zip_file.open('shapes.txt'))
            shapes['shape_pt_sequence'] = shapes['shape_pt_sequence'].astype(str)
            Route_Shape_stop = stop_times.merge(trips, on = 'trip_id', how = 'left')
            Route_Shape_stop = Route_Shape_stop[['shape_id', 'stop_id', 'stop_sequence', 'route_id', 'trip_id', 'direction_id', 'arrival_time']].drop_duplicates(keep = 'first')
            Route_Shape_stop['stop_sequence'] = Route_Shape_stop['stop_sequence'].astype(int)
            Route_Shape_stop['stop_id'] = Route_Shape_stop['stop_id'].astype(str)
            Route_Shape_stop['bus_num'] = Route_Shape_stop['route_id'].apply(lambda x: x.split('-')[0])
            print(f"starting to compute shapes...")
            #######FOR DEMO ONLY FOCUS ON HFS##############
            route_shp_stp = Route_Shape_stop[Route_Shape_stop['bus_num'].isin(high_frequency_buses)]
            shapes2 = shapes.copy(deep = True)
            shapes2['shape_pt_sequence_next'] = (shapes2['shape_pt_sequence'].astype(int) + 1).astype(str)
            shapes2['stop_seq_1'] = (shapes2['shape_pt_sequence'].astype(int)/10000).astype(int)
            shapes2['stop_seq_2'] = (shapes2['shape_pt_sequence_next'].astype(int)/10000).astype(int)
            shapes3 = shapes2.merge(shapes, left_on = ['shape_id', 'shape_pt_sequence_next'], right_on = ['shape_id', 'shape_pt_sequence'], how = 'left')
            shapes3['distance'] = Distance(shapes3['shape_pt_lat_x'].tolist(),shapes3['shape_pt_lon_x'].tolist(),shapes3['shape_pt_lat_y'].tolist(),shapes3['shape_pt_lon_y'].tolist())
            shapes3['distance'] = shapes3['distance']/1000
            shapes3 = shapes3.loc[shapes3['stop_seq_1'] == shapes3['stop_seq_2']]
            shapes3['stop_seq_2'] = shapes3['stop_seq_2'] + 1
#             shapes4 = pd.DataFrame(shapes3.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'], as_index = False)['distance'].sum())
            print("shaper mapper is about to start....")
#             date_name1 = date_name
            print(date_name)
            hfs_route_fname = f"{HFSdirectory}\\HFS_Route_Shape_stop_{date_name}.csv"
            print(f"hfs path = {hfs_route_fname}")
            shapes4_fname = f"{shapesdirectory}\\shapes4_combine_{date_name}.csv"
            print(f"shapes4 path = {shapes4_fname}")
            if not (os.path.isfile(hfs_route_fname) and os.path.isfile(shapes4_fname)):
                print(f"file exists? {os.path.isfile(hfs_route_fname)}, {os.path.isfile(shapes4_fname)}")
                try:
                    timer_st = time.time()
                    shapes4 = shapes3.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2']).apply(shaper_mapper)
                    timer_end = time.time()
                    print(f"The duration of static shaper mapper = {(timer_end - timer_st)/60} mins")
                    print(f"shapes4 is computed with shape = {shapes4.shape}")
                    shapes4 = shapes4.reset_index()
                    distance_file = shapes4.copy(deep=True)
                    shapes4['shape_id'] = shapes4['shape_id'].astype(str)
                    shapes4 = shapes4.reset_index()
                    shapes4_geo = gpd.GeoDataFrame(shapes4, crs="EPSG:4376", geometry=[Point(x) for x in shapes4['routes_list']])
                    timer_st = time.time()
                    shapes4_geo = ckdnearest(shapes4_geo, gdf, ['Name'])
                    timer_end = time.time()
                    print(f"The duration of static ckdnearest = {(timer_end - timer_st)/60} mins")
                    print("geo computed!")
                    #########PROCESS DONE BETWEEN 1:30AM TO 4:30AM ONCE######################################
                    shapes4_geo_non_hfs = shapes4_geo.loc[~(shapes4_geo['shape_id'].str[:-4].isin(high_frequency_buses))]
                    shapes4_geo_non_hfs['names_list'] = ['' for x in range(len(shapes4_geo_non_hfs))]
                    shapes4_geo_hfs = shapes4_geo.loc[shapes4_geo['shape_id'].str[:-4].isin(high_frequency_buses)]
                    print("hfs and non are separated!")
                    timer_st = time.time()
                    shapes4_geo_hfs['names_list'] = shapes4_geo_hfs.apply(lambda x: bus_route_path(x, gdf, ['Name']), axis=1)
                    timer_end = time.time()
                    print(f"The duration of static shaper mapper = {(timer_end - timer_st)/60} mins")
                    print("names list computed!")
                    shapes4_combine = pd.concat([shapes4_geo_hfs, shapes4_geo_non_hfs], ignore_index=True)
                    timer_st = time.time()
                    shapes4_combine.to_csv(shapes4_fname)
                    timer_end = time.time()
                    print(f"The duration of shapes4 combine saving = {(timer_end - timer_st)/60} mins")
                    timer_st = time.time()
                    route_shp_stp.to_csv(hfs_route_fname)
                    timer_end = time.time()
                    print(f"The duration of static hfs route shape stop saving = {(timer_end - timer_st)/60} mins")
                    print("both csv is stored!")
                    date_name1 = date_name
                except Exception as e:
                    print("an exception occurred")
                    print(str(e))
                    break
            else:
                print("files already exist!")
                date_name1 = date_name
        #Downloading the feed
        feed = gtfs_realtime_pb2.FeedMessage()
        response = urllib.request.urlopen(gtfs_realtime_link)
        feed.ParseFromString(response.read())
        dict_obj = protobuf_to_dict(feed)
        r = json.dumps(dict_obj)
        loaded_r = json.loads(r)
        if flags == 1:
            kppa = pd.DataFrame(loaded_r['entity'])
            if kppa.shape != kppa1.shape:
                vt = kppa.vehicle.apply(pd.Series)
                vt1 = vt.position.apply(pd.Series)
                vt2 = vt.trip.apply(pd.Series)
                vt4 = vt.vehicle.apply(pd.Series)
                VE = pd.concat([vt, vt1, vt2, vt4], axis=1)
                VE = VE[['current_status', 'stop_id', 'timestamp', 'trip_id','latitude', 'longitude' , 'route_id','id', 'label' ]]
                VE = VE.loc[~VE['timestamp'].isna()]
                end = time.time()
                start = end
                producer_start(channel, conn, VE)
                index_o = index_o + 1
                kppa1 = kppa.copy(deep = True)
                flags = 1
        if flags == 0:
            kppa = pd.DataFrame(loaded_r['entity'])
            vt = kppa.vehicle.apply(pd.Series)
            vt1 = vt.position.apply(pd.Series)
            vt2 = vt.trip.apply(pd.Series)
            vt4 = vt.vehicle.apply(pd.Series)
            VE = pd.concat([vt, vt1, vt2, vt4], axis=1)
            VE = VE[['current_status', 'stop_id', 'timestamp', 'trip_id','latitude', 'longitude' , 'route_id','id', 'label' ]]
            VE = VE.loc[~VE['timestamp'].isna()]
            end = time.time()
            print(f"duration flags 0 = {(end-start)/60} mins")
            start = end
            producer_start(channel, conn, VE)
            index_o = index_o + 1
            kppa1 = kppa.copy(deep = True)
            flags = 1
        response.close()
    except:
        pass