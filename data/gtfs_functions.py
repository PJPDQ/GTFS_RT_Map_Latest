# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 12:30:35 2023

@author: gozalid
"""
import pandas as pd
from zipfile import ZipFile
import glob
import datetime
import time
import os
pd.set_option('display.max_columns', None)
from pyproj import Geod
wgs84_geod = Geod(ellps='WGS84')
from tqdm import tqdm
# import osmnx as ox
# import networkx as nx
from datetime import timedelta
from shapely.geometry import Point, LineString
import geopandas as gpd
from ast import literal_eval
from multiprocessing import Process
import numpy as np

from scipy.spatial import cKDTree
import itertools
from operator import itemgetter
from scipy.interpolate import interp1d
from shapely.ops import nearest_points, linemerge
import matplotlib.pyplot as plt
import shapely.wkt
# import seaborn as sns; sns.set()
# from sklearn.preprocessing import MinMaxScaler
import os.path
from datetime import timedelta, datetime as dt
from scipy.spatial import distance_matrix
# from sklearn.preprocessing import normalize
# from sklearn.cluster import DBSCAN
# from sklearn.metrics import silhouette_score as ss
import sqlalchemy as sql

# Data_Directory = r"Y:\\Data\\GTFS_NEW\\"
Working_Directory = r"Y:\\Sentosa\\GTFS_preprocessed\\gtfs\\"   #ensure slash sign in the end "/" not "\"
# Static_Folder = Data_Directory + 'GTFS Static\\'
Saved_DIR = Working_Directory + 'Saved_Dirs\\'
Saved_Dir_Static = Saved_DIR + 'GTFS Static\\'
# if not os.path.exists(Static_Folder):
#     os.makedirs(Static_Folder)
if not os.path.exists(Saved_DIR):
    os.makedirs(Saved_DIR)
if not os.path.exists(Saved_Dir_Static):
    os.makedirs(Saved_Dir_Static)
    
high_frequency_buses = ['60', '61', '100', '111' ,'120', '130' ,'140', '150', '180', '196' , '199' ,'200' ,'222', '330', '333' ,'340', '345', '385', '412', '444', '555']
Acquired_Directory = r'Y:\\Data\\GTFS_NEW\\'
SELECTED_DATE = datetime.date(2022, 12, 9)
# duration_length = 3

# Working_Directory = r"C:\\Users\\gozalid\\Documents\\GTFS_NEW\\"
Working_Directory = r"Y:\\Data\\GTFS_NEW\\"
Static_Folder = Working_Directory + 'GTFS Static\\'
Realtime_folder  = Working_Directory + 'GTFS Realtime\\'
if not os.path.exists(Static_Folder):
    os.makedirs(Static_Folder)
if not os.path.exists(Realtime_folder):
    os.makedirs(Realtime_folder)
gtfs_realtime_link = os.environ.get("GTFS_RL_VP")
gtfs_static_link = os.environ.get("GTFS")  

# SELECTED_DATE = datetime(2021, 8, 26)
# duration_length = 7
# FROM DATA_ACQUISITION_TU_VP.py
# def monthname(mydate):
#     mydate = datetime.now()
#     m = mydate.strftime("%B")
#     return(m)
# def Brisbane(epoch):
#     a = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(epoch))
#     return(a)

def bus_separator(df, SELECTED_DATE):
    print("separating buses....")
    
    date_name = str(SELECTED_DATE.day)+"-"+str(SELECTED_DATE.month)+"-"+str(SELECTED_DATE.year)
    month_name = SELECTED_DATE.strftime("%B") + " ," +str(SELECTED_DATE.year)

    Static_Dir = Saved_Dir_Static + "Static " + SELECTED_DATE.strftime("%B") + " ," +str(SELECTED_DATE.year) + "\\"
    if not os.path.exists(Static_Dir):
        os.makedirs(Static_Dir)
    Static_month = Static_Dir + 'Static ' + month_name + '\\'
    if not os.path.exists(Static_month):
        os.makedirs(Static_month)
        
    ## concat all distance calculated and route generated
    all_static_df_5 = pd.DataFrame()
    all_static_df_6 = pd.DataFrame()
    all_static_df = pd.DataFrame()
    new_df = df.copy(deep=True)
    print("deep copy....")
    new_df['bus_num'] = new_df.shape_id.apply(lambda x: str(x)[:-4])
    unique_buses = list(new_df.bus_num.unique())
    Static_Date = Static_month + "Static " + date_name + "\\"
    if not os.path.exists(Static_Date):
        os.makedirs(Static_Date)
    for bus in unique_buses:
        temp_df = new_df.loc[new_df['bus_num'] == bus]
        ##temp_df = dist_calc(temp_df)
        temp_df = temp_df.apply(distance_calc, axis=1)
        temp_df.to_csv(Static_Dir+"temp_df_distance_route" + date_name + ".csv")
        temp_df = temp_df.loc[temp_df['stop_seq_1'] == temp_df['stop_seq_2']]
        temp_df['stop_seq_2'] = temp_df['stop_seq_2'] + 1
        shapes5 = temp_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'])['distance'].sum().reset_index()
        temp_df = temp_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2']).agg({'distance': 'sum', 'routes_list':'sum', 'shape_pt_lat_x': 'first', 'shape_pt_lon_x': 'first', 'shape_pt_lat_y': 'last', 'shape_pt_lon_y': 'last'}).reset_index()
        temp_df['shape_file_name'] = temp_df.apply(save_shapefile, axis=1)
        # temp_df['route_shape'] = temp_df['routes_list'].apply(lambda x: LineString(x))
        temp_df['shape_id'] = temp_df['shape_id'].astype(str)
        temp_df.to_csv(Static_Date+bus+"_full_static_data.csv")
        all_static_df_5 = all_static_df_5.append(shapes5, ignore_index=True)
    print("processed done")
    return all_static_df_6

# def distance_calc(x):
#     lat1 = x['shape_pt_lat_x']
#     lon1 = x['shape_pt_lon_x']
#     lat2 = x['shape_pt_lat_y']
#     lon2 = x['shape_pt_lon_y']
#     az12 ,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
#     x['distance'] = dist / 1000 ## convert from metre to km
#     x['routes_list'] = [(lon1, lat1)]
#     return x

# def save_shapefile(x):
#     routes = x['routes_list']
#     shape_id = str(x['shape_id'])
#     stop_start = str(x['stop_seq_1'])
#     stop_stop = str(x['stop_seq_2'])
#     # filename = shape_id + "_" + stop_start + "_" + stop_stop + "_routes"
#     # line_string_shape = LineString(routes)
#     filename = shape_id + "_" + stop_start + "_" + stop_stop + "_routes"
#     try:
#         line_string_shape = LineString(routes)
#     except:
#         print("an error occurred")
#         line_string_shape = Point(routes[0])
#     crs = {'init': 'epsg:4376'}
#     gdf = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[line_string_shape])
#     # dir_shape = shapefile_dir + "/" + shape_id
#     # if not os.path.exists(dir_shape):
#     #     os.makedirs(dir_shape)
#     # shapefile_folder = Static_Dir + "shapefiles\\"
#     # if not os.path.exists(shapefile_folder):
#     #     os.makedirs(shapefile_folder)
#     full_shape_file_path = "shapefiles\\" + filename
#     # gdf.to_file(shapefile_folder + filename)
#     return full_shape_file_path

def load_static(date_name, gtfs_static_link=gtfs_static_link):
    print("Processing Static GTFS for date = ", date_name)
    # GTFS_Static = urllib.request.urlretrieve(gtfs_static_link, Static_Folder + '/GTFS Static ' +date_name + '.zip')
    print("static data fetched..........")
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
    ## Distance calculation using the aerial distance per shape pt sequence
    #### [NEW]!!!! Distance calculation based on actual distance driven using OSM road network
    return bus_separator(shapes3, date_name)

def static_merger(date_name):
    # GTFS_Static = urllib.request.urlretrieve(gtfs_static_link, Static_Folder + '/GTFS Static ' +date_name + '.zip')
    print("static merger fetched..........")
    zip_file = ZipFile(Static_Folder + '/GTFS Static ' +date_name + '.zip')
    trips = pd.read_csv(zip_file.open('trips.txt'))
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    stops = pd.read_csv(zip_file.open('stops.txt'))
    routes = pd.read_csv(zip_file.open('routes.txt'))
    calendar = pd.read_csv(zip_file.open('calendar.txt'))
    calendar_dates = pd.read_csv(zip_file.open('calendar_dates.txt'))
    
    stop_times['stop_id'] = stop_times['stop_id'].astype(str)
    static_df = stop_times.merge(stops, on='stop_id', how='inner')
    static_df = trips.merge(static_df, on='trip_id', how='inner')
    static_df = static_df.merge(routes, on='route_id', how='inner')
    static_df = static_df.merge(calendar, on='service_id', how='inner')
    static_df = static_df.merge(calendar_dates, on='service_id', how='left')
    print(static_df.shape)
    return static_df

def get_all_routes(date_name):
    print("getting all routes.....")
    zip_file = ZipFile(Static_Folder + 'GTFS Static ' +date_name + '.zip')
    routes = pd.read_csv(zip_file.open('routes.txt'))
    unique_routes = list(routes.route_id.unique())
    print("unique routes = ", len(unique_routes))
    return unique_routes

def tu_preprocessing(sample_route, date_name, dir_path=None):
    ## Test route chosen by its shapeID and route path
    # bus_num = sample_route[:-5]
    
    TU_WORKING_DIR = Realtime_folder + "TripUpdate entity\\TU August ,2021"
    TU_SELECTED_DATE_DIR = TU_WORKING_DIR + "\\TU " + date_name
    tu_feed_path =  TU_SELECTED_DATE_DIR + "\\" + "TU feeds " + date_name
    # tu_speed_path = TU_SELECTED_DATE_DIR + "\\" + "TU Speed Analysis" + date_name
    
    tu_feed_csv_files = glob.glob(tu_feed_path + "\\*.csv")
    
    zip_file = ZipFile(Static_Folder + 'GTFS Static ' +date_name + '.zip')
    trips = pd.read_csv(zip_file.open('trips.txt'))
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    stops = pd.read_csv(zip_file.open('stops.txt'))
    
    trips_df = trips[['route_id', 'trip_id', 'shape_id', 'direction_id']]
    stop_times_df = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    stops_df = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    
    # Instantiate an empty DataFrame object
    sample_tu_trip_df = pd.DataFrame()
    
    # Loop through all the files in the files collection variable and then look for
    # shape_id that are included in our shape id list for a specific routes
    for file in tu_feed_csv_files:
        df = pd.read_csv(file)
        # newdf = df[(df.shape_id == shape_ids[shape_id_index])]
        newdf = df[(df.route_id == sample_route)]
        
        sample_tu_trip_df = pd.concat([sample_tu_trip_df, newdf], ignore_index=True)
    
    sample_tu_trip_df = sample_tu_trip_df.dropna()
    sample_tu_trip_df_copy = sample_tu_trip_df.copy(deep=True)
    
    ## Convert arrival and departure time into datetime format
    ## Convert arrival and departure time into datetime format
    sample_tu_trip_df_copy['arrival_time_dt'] = sample_tu_trip_df_copy['arrival_time'].apply(lambda x: datetime.fromtimestamp(x))
    sample_tu_trip_df_copy['departure_time_dt'] = sample_tu_trip_df_copy['departure_time'].apply(lambda x: datetime.fromtimestamp(x))
    
    if sample_tu_trip_df_copy['stop_id'].dtypes == 'float64':
        sample_tu_trip_df_copy['stop_id'] = sample_tu_trip_df_copy['stop_id'].astype(int)
        sample_tu_trip_df_copy['stop_id'] = sample_tu_trip_df_copy['stop_id'].astype(str)
    elif sample_tu_trip_df_copy['stop_id'].dtypes == 'int64':
        sample_tu_trip_df_copy['stop_id'] = sample_tu_trip_df_copy['stop_id'].astype(str)
    else:
        sample_tu_trip_df_copy['stop_id'] = sample_tu_trip_df_copy['stop_id'].astype(str)    
    list_unnameds = [s for s in sample_tu_trip_df_copy.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        sample_tu_trip_df_copy.drop(list_unnameds, axis=1, inplace=True)
    
    
    sample_tu_trip_df_copy['stop_sequence'] = sample_tu_trip_df_copy['stop_sequence'].astype(str)
    sample_tu_trip_df_non_duplicates = sample_tu_trip_df_copy.drop_duplicates(subset=['timestamp', 'id', 'stop_sequence'], keep='first')
    ### Merge the static dataset
    sample_tu_trip_df_non_duplicates = sample_tu_trip_df_non_duplicates.merge(trips_df, on=['route_id', 'trip_id', 'shape_id'], how='inner')
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
    sample_tu_trip_df_non_duplicates = sample_tu_trip_df_non_duplicates.merge(stop_times_df, on=['trip_id', 'stop_id'], how='inner')
    sample_tu_trip_df_non_duplicates = sample_tu_trip_df_non_duplicates.merge(stops_df, on=['stop_id'], how='inner')
    
    sample_tu_trip_df_non_duplicates.rename(columns={'arrival_time_y': 'expected_arrival_time', 'departure_time_y':'expected_departure_time', 'arrival_time_x':'actual_arrival_time', 'departure_time_x':'actual_departure_time', 'departure_time_dt': 'actual_departure_dt', 'arrival_time_dt': 'actual_arrival_dt'}, inplace=True)

    ##Actual
    sample_tu_trip_df_non_duplicates_sort = sample_tu_trip_df_non_duplicates.sort_values(by=['actual_departure_dt'])
    sample_tu_trip_df_non_duplicates_sort['actual_prev_departure_dt'] = sample_tu_trip_df_non_duplicates_sort.groupby(['timestamp', 'id'])['actual_departure_dt'].shift()
    sample_tu_trip_df_non_duplicates_sort['actual_travel_time'] = sample_tu_trip_df_non_duplicates_sort['actual_arrival_dt'] - sample_tu_trip_df_non_duplicates_sort['actual_prev_departure_dt']
    sample_tu_trip_df_non_duplicates_sort['actual_travel_time'] = sample_tu_trip_df_non_duplicates_sort.actual_travel_time.apply(lambda x: x.total_seconds())
    
    ## Expected
    sample_tu_trip_df_non_duplicates_sort['expected_arrival_dt'] = (pd.to_datetime(sample_tu_trip_df_non_duplicates_sort['start_date'], format='%Y%m%d') + pd.to_timedelta(sample_tu_trip_df_non_duplicates_sort['expected_arrival_time']))
    sample_tu_trip_df_non_duplicates_sort['expected_departure_dt'] = (pd.to_datetime(sample_tu_trip_df_non_duplicates_sort['start_date'], format='%Y%m%d') + pd.to_timedelta(sample_tu_trip_df_non_duplicates_sort['expected_departure_time']))
    ##Expected
    sample_tu_trip_df_non_duplicates_sort = sample_tu_trip_df_non_duplicates_sort.sort_values(by=['expected_departure_dt'])
    sample_tu_trip_df_non_duplicates_sort['expected_prev_departure_dt'] = sample_tu_trip_df_non_duplicates_sort.groupby(['timestamp', 'id'])['expected_departure_dt'].shift()
    sample_tu_trip_df_non_duplicates_sort['expected_travel_time'] = sample_tu_trip_df_non_duplicates_sort['expected_arrival_dt'] - sample_tu_trip_df_non_duplicates_sort['expected_prev_departure_dt']
    sample_tu_trip_df_non_duplicates_sort['expected_travel_time'] = sample_tu_trip_df_non_duplicates_sort.expected_travel_time.apply(lambda x: x.total_seconds())
    
    return sample_tu_trip_df_non_duplicates

def vp_generator(sample_route, date_name, dir_path=None):
    from datetime import datetime
    ## Test route chosen by its shapeID and route path

    zip_file = ZipFile(Static_Folder + 'GTFS Static ' +date_name + '.zip')
    trips = pd.read_csv(zip_file.open('trips.txt'))
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    stops = pd.read_csv(zip_file.open('stops.txt'))
    
    trips_df = trips[['route_id', 'trip_id', 'shape_id', 'direction_id']]
    stop_times_df = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    stops_df = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    
    
    # Vehicle Position RealTime Data
    VP_WORKING_DIR = Realtime_folder + "VehiclePosition entity\\VP August ,2021"
    vp_source_path = VP_WORKING_DIR + '\\VP ' + date_name
    vp_csv_files = glob.glob(vp_source_path + "\\*.csv")
    
    # Instantiate an empty DataFrame object
    sample_vp_trip_df = pd.DataFrame()
    
    # Loop through all the files in the files collection variable and then look for
    # shape_id that are included in our shape id list for a specific routes
    for file in vp_csv_files:
        df = pd.read_csv(file)
        # newdf = df[(df.shape_id == shape_ids[shape_id_index])]
        newdf = df[(df.route_id == sample_route)]
        
        sample_vp_trip_df = pd.concat([sample_vp_trip_df, newdf], ignore_index=True)
    
    sample_vp_trip_df = sample_vp_trip_df.dropna()
    sample_vp_trip_df_copy = sample_vp_trip_df.copy(deep=True)
    print(sample_vp_trip_df_copy.head(3))
    print(sample_vp_trip_df_copy.info())
    ## Convert arrival and departure time into datetime format
    sample_vp_trip_df_copy['timestamp_dt'] = sample_vp_trip_df_copy['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
    
    if sample_vp_trip_df_copy['stop_id'].dtypes == 'float64':
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(int)
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
    elif sample_vp_trip_df_copy['stop_id'].dtypes == 'int64':
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
    else:
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
        
    list_unnameds = [s for s in sample_vp_trip_df_copy.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        sample_vp_trip_df_copy.drop(list_unnameds, axis=1, inplace=True)
        
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_copy.drop_duplicates(subset=['timestamp', 'id'], keep='first')
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(trips_df, on=['route_id', 'trip_id'], how='inner')
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(stop_times_df, on=['trip_id', 'stop_id'], how='inner')
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(stops_df, on=['stop_id'], how='inner')
    
    return sample_vp_trip_df_non_duplicates

def fetch_data_from_hetrogen(date_name, month_name):
    from datetime import datetime
    data_path = f"{Realtime_folder}VehiclePosition entity\\VP {month_name}\\VP {date_name}"
    vp_csv_files = glob.glob(data_path + "\\*.csv")
    all_df = pd.DataFrame()
    for file in vp_csv_files:
        df = pd.read_csv(file)
        all_df = pd.concat([all_df, df], ignore_index=True)
    if len(all_df) < 1:
        return all_df
    all_df = all_df.dropna()
    sample_vp_trip_df_copy = all_df.copy(deep=True)
    print(sample_vp_trip_df_copy.head(3))
    print(sample_vp_trip_df_copy.info())
    
    ## Convert arrival and departure time into datetime format
    sample_vp_trip_df_copy['timestamp_dt'] = sample_vp_trip_df_copy['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
    
    if sample_vp_trip_df_copy['stop_id'].dtypes == 'float64':
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(int)
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
    elif sample_vp_trip_df_copy['stop_id'].dtypes == 'int64':
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
    else:
        sample_vp_trip_df_copy['stop_id'] = sample_vp_trip_df_copy['stop_id'].astype(str)
        
    list_unnameds = [s for s in sample_vp_trip_df_copy.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        sample_vp_trip_df_copy.drop(list_unnameds, axis=1, inplace=True)
        
    zip_file = ZipFile(Static_Folder + 'GTFS Static ' +date_name + '.zip')
    trips = pd.read_csv(zip_file.open('trips.txt'))
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    stops = pd.read_csv(zip_file.open('stops.txt'))
    
    trips_df = trips[['route_id', 'trip_id', 'shape_id', 'direction_id']]
    stop_times_df = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    stops_df = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    
    
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_copy.drop_duplicates(subset=['timestamp', 'id'], keep='first')
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(trips_df, on=['route_id', 'trip_id'], how='inner')
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(stop_times_df, on=['trip_id', 'stop_id'], how='inner')
    sample_vp_trip_df_non_duplicates = sample_vp_trip_df_non_duplicates.merge(stops_df, on=['stop_id'], how='inner')
    
    return sample_vp_trip_df_non_duplicates

def beneficial_normalized(col, col_max):
    return col_max and (col/col_max)
def nonbeneficial_normalized(x, col_min):
    return x and (col_min/x)
def custom_minmax_scaler(x, min_x, max_x):
    return (x - min_x) / (max_x - min_x)
    
def remove_unnamed(df):
    list_unnameds = [s for s in df.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        df.drop(list_unnameds, axis=1, inplace=True)
    return df


def vehicle_position_preprocessing(vp_df, shape_sample, route_sample, static_distance_df):
    vp = vp_df.copy(deep=True)
    vp = vp.loc[vp.shape_id.astype(str) == shape_sample]
    static_df = static_distance_df.loc[static_distance_df['shape_id'].astype(str) == shape_sample]
    vp_copy = vp.sort_values(by=['timestamp', 'trip_id'])
    
    vp_copy['next_stop_id'] = vp_copy['stop_id']
    vp_copy['o_latitude'] = vp_copy['latitude']
    vp_copy['d_latitude'] = vp_copy['latitude']
    vp_copy['d_longitude'] = vp_copy['longitude']
    vp_copy['o_longitude'] = vp_copy['longitude']
    vp_copy['o_timestamp'] = vp_copy['timestamp']
    vp_copy['d_timestamp'] = vp_copy['timestamp']
    
    a = vp_copy.groupby(['stop_id', 'trip_id'], sort=False).agg({'next_stop_id': 'first', 'o_latitude': 'first', 'o_longitude':'first', 'd_latitude':'first', 'd_longitude':'first', 'o_timestamp':'first', 'd_timestamp':'first'}).reset_index()
    a['next_stop_id'] = a.groupby(['trip_id'],sort=False)['next_stop_id'].shift(-1)
    a = a.apply(lambda x: vp_dest_columns_generator(x, vp_copy, static_df), axis=1)
    
    vp_copy.drop(labels=['d_latitude', 'd_longitude','d_timestamp', 'next_stop_id', 'o_latitude', 'o_longitude', 'o_timestamp'], inplace=True, axis=1)
    
    test_vp = vp_copy.merge(a, on=['trip_id', 'stop_id'], how='inner')
    test_vp = test_vp.sort_values(by=['timestamp', 'trip_id'])
    test_vp['next_latitude'] = test_vp.groupby(['trip_id'],sort=False)['latitude'].shift(-1)
    test_vp['next_longitude'] = test_vp.groupby(['trip_id'],sort=False)['longitude'].shift(-1)
    test_vp['distance_to_next_vp'] = distance_calc(test_vp['latitude'].tolist(), test_vp['longitude'].tolist(), test_vp['next_latitude'].tolist(), test_vp['next_longitude'].tolist())
    test_vp['distance_to_next_vp'] = test_vp['distance_to_next_vp'].apply(lambda x: x/1000)
    
    test_vp['next_timestamp_to_next_vp'] = test_vp.groupby(['trip_id'],sort=False)['timestamp'].shift(-1)
    test_vp['travel_time'] = test_vp['next_timestamp_to_next_vp'] - test_vp['timestamp']
    a = test_vp.groupby(['trip_id', 'stop_id'], sort=False).agg({'travel_time': 'sum'}).reset_index()
    test_vp = test_vp.merge(a, on=['trip_id', 'stop_id'], how='inner')
    test_vp = test_vp.rename(columns={'travel_time_x':'tt_to_next_vp', 'travel_time_y':'tt_all'})

    return test_vp

def trip_update_preprocessing(tu_df, shape_sample, route_sample, bus_num, static_distance_df):
    tu_df_sample = tu_df.loc[tu_df.shape_id.astype(str) == shape_sample]
    ## Aggregate dataset
    tu_df_sample = tu_df_sample.groupby(['timestamp', 'stop_sequence']).agg(lambda x: x.mean() if x.dtype in ['float64', 'float32'] else x.head(1)).reset_index()
    
    ### Timestamp
    tu_df_sample['dest_lat'] = tu_df_sample.groupby(['timestamp'])[['stop_lat']].shift(-1)
    tu_df_sample['dest_lon'] = tu_df_sample.groupby(['timestamp'])[['stop_lon']].shift(-1)
    tu_df_sample['actual_next_arrival_dt'] = tu_df_sample.groupby(['timestamp'])[['actual_arrival_time_dt']].shift(-1)
    tu_df_sample['expected_next_arrival_dt'] = tu_df_sample.groupby(['timestamp'])[['expected_arrival_dt']].shift(-1)
    
    ## Distance
    tu_static_merged = tu_df_sample.merge(static_distance_df, left_on=['stop_sequence'], right_on=['stop_seq_1'], how='inner')

    ###Travel Time
    # #### Actual
    # tu_static_merged['actual_travel_time_tu'] = tu_static_merged['actual_next_arrival_dt'] - tu_static_merged['actual_departure_time_dt']
    # tu_static_merged['actual_travel_time_tu'] = tu_static_merged.actual_travel_time_tu.apply(lambda x: x.total_seconds())
    # #### Expected
    # tu_static_merged['expected_travel_time_tu'] = tu_static_merged['expected_next_arrival_dt'] - tu_static_merged['expected_departure_dt']
    # tu_static_merged['expected_travel_time_tu'] = tu_static_merged.expected_travel_time_tu.apply(lambda x: x.total_seconds())
    
    #tu_static_merged = tu_static_merged[~tu_static_merged[['stop_seq_1', 'stop_seq_2']].isin([np.nan, np.inf, -np.inf]).any(1)]
    tu_static_merged['stop_sequence_id'] = tu_static_merged.apply(lambda x: str(int(x['stop_seq_1'])) + "-" + str(int(x['stop_seq_2'])), axis=1)
    
    #### Set the first timestamp to zero
    # df_merged.loc[df_merged.groupby('timestamp', as_index=False).head(1).index, 'actual_travel_time'] = 0
    
    list_unnameds = [s for s in tu_static_merged.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        tu_static_merged.drop(list_unnameds, axis=1, inplace=True)
        
    return tu_static_merged

# high_frequency_buses = ['60', '61', '100', '111' ,'120', '130' ,'140', '150', '180', '196' , '199' ,'200' ,'222', '330', '333' ,'340', '345', '385', '412', '444', '555']
# Acquired_Directory = r'Y:\\Data\\GTFS_NEW\\'
# SELECTED_DATE = datetime.date(2022, 4, 20)
# duration_length = 3
# HFS_DIR = r"C:\\Users\\n9502271\\OneDrive\\OneDrive - Queensland University of Technology\\Transit Dashboards\\Data used\\Rough Work\\Ultra final\\Shape File\\"


def distance_calc(x):
    lat1 = x['shape_pt_lat_x']
    lon1 = x['shape_pt_lon_x']
    lat2 = x['shape_pt_lat_y']
    lon2 = x['shape_pt_lon_y']
    az12 ,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) #Yes, this order is correct
    x['distance'] = dist / 1000 ## convert from metre to km
    x['routes_list'] = [(lon1, lat1)]
    return x

def to_float(result):
    newresult = []
    for tuple in result:
        temp = []
        for x in tuple:
            if x.isalpha():
                temp.append(x)
            elif x.isdigit():
                temp.append(int(x))
            else:
                temp.append(float(x))
        newresult.append((temp[0],temp[1]))
    return newresult

def save_shapefile(x):
    routes = x['routes_list']
    shape_id = x['shape_id']
    stop_start = x['stop_seq_1']
    stop_stop = x['stop_seq_2']
    # filename = shape_id + "_" + stop_start + "_" + stop_stop + "_routes"
    # line_string_shape = LineString(routes)
    # filename = shape_id + "_" + stop_start + "_" + stop_stop + "_routes"
    filename = f"{shape_id}_{stop_start}_{stop_stop}_routes"
    new_routes = to_float(routes)
    try:
        line_string_shape = LineString(new_routes)
    except:
        line_string_shape = Point(new_routes[0])
    # crs = {'epsg': '4376'}
    gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4376', geometry=[line_string_shape])
    # full_shape_file_path = "shapefiles\\" + filename
    full_shape_file_path = f"shapefiles\\{filename}"
    return full_shape_file_path

def load_new_static(bus_num, date_name, Static_month, Static_Folder):
    print("Processing Static GTFS for date = ", date_name)
    zip_file = ZipFile(Static_Folder + '/GTFS Static ' +date_name + '.zip')
    trips = pd.read_csv(zip_file.open('trips.txt'))
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    shapes = pd.read_csv(zip_file.open('shapes.txt'), sep=',', index_col=False, dtype='unicode')
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
    print("fetched static data......")
    df = shapes3.copy(deep=True)
    all_static_df_5 = pd.DataFrame()
    # all_static_df_6 = pd.DataFrame()
    # all_static_df = pd.DataFrame()
    new_df = df.copy(deep=True)
    new_df['bus_num'] = new_df.shape_id.apply(lambda x: str(x)[:-4])
    # unique_buses = list(new_df.bus_num.unique())
    Static_Date = Static_month + "Static " + date_name + "\\"
    if not os.path.exists(Static_Date):
        os.makedirs(Static_Date)
    for bus in bus_num:
        fname = Static_Date+bus+"_full_static_data.csv"
        if os.path.isfile(fname):
            pass
        else:
            temp_df = new_df.loc[new_df['bus_num'] == bus]
            if len(temp_df) < 1:
                continue
            ##temp_df = dist_calc(temp_df)
            temp_df = temp_df.apply(distance_calc, axis=1)
            temp_df.to_csv(Static_month+"temp_df_distance_route" + date_name + ".csv")
            temp_df = temp_df.loc[temp_df['stop_seq_1'] == temp_df['stop_seq_2']]
            temp_df['stop_seq_2'] = temp_df['stop_seq_2'] + 1
            # print("routes computing routes linestring....")
            # temp_df['routes_list'] = temp_df.apply(route_generator, axis=1)
            # print(f"routes computed for linestring from stop by stop for bus {bus}")
            # shapes4 = temp_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2']).agg({'distance': 'sum', 'routes_list': 'sum'}).reset_index()
            # print("shapes4....")
            # print(shapes4.head())
            shapes5 = temp_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'])['distance'].sum().reset_index()
            temp_df = temp_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2']).agg({'distance': 'sum', 'routes_list':'sum', 'shape_pt_lat_x': 'first', 'shape_pt_lon_x': 'first', 'shape_pt_lat_y': 'last', 'shape_pt_lon_y': 'last'}).reset_index()
            temp_df['shape_file_name'] = temp_df.apply(save_shapefile, axis=1)
            # temp_df['route_shape'] = temp_df['routes_list'].apply(lambda x: LineString(x))

            temp_df['shape_id'] = temp_df['shape_id'].astype(str)
            ## merge all static files together
            # static_df = all_static_data.loc[(all_static_data['shape_id'].str.slice(0,-4) == str(bus))]
            # static_df = static_df.merge(temp_df, on='shape_id', how='inner')
            # shapes6 = temp_df.merge(shapes4, on=['shape_id', 'stop_seq_1', 'stop_seq_2'], how='inner')
            # print(shapes6.head())
            temp_df.to_csv(Static_Date+bus+"_full_static_data.csv")
            # all_static_df_5 = all_static_df_5.append(shapes5, ignore_index=True)
            all_static_df_5 = pd.concat([all_static_df_5, shapes5], ignore_index=True)
    return all_static_df_5

def cut(line, distance):
    """
    https://shapely.readthedocs.io/en/stable/manual.html#object.interpolate
    """
    # Cuts a line in two at a distance from its starting point
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]
        
def compare_df_distance_minimum(df, coord):
    min_row = df.iloc[0]
    line = min_row.geometry
    min_idx = 0
    min_dist = line.distance(coord)
    for idx, row in df.iterrows():
        line = row.geometry
        dist = line.distance(coord)
        if (min_dist - dist) > 0:
            min_dist = dist
            min_idx = idx
            min_row = row
    return min_row, min_dist, min_idx

def link_name_generator(x, gdf):
    lat = x['shape_pt_lat_x']
    lon = x['shape_pt_lon_x']
    temp_df = gdf.distance(Point(lon,lat)) < 1e-6
    row = gdf.iloc[temp_df[temp_df].index]
    row = row.loc[row.direction_ == x.direction_id]
    name = ""
    if len(row) > 1:
        row, _, _ = compare_df_distance_minimum(row, Point(lon, lat))
        name = row['Name']
    elif len(row) == 1:
        name = row.iloc[0]['Name']
    else:
        direction_df = gdf.loc[gdf.direction_ == x.direction_id]
        row, _, _ = compare_df_distance_minimum(direction_df, Point(lon, lat))
        name = row['Name']
    return name

def run_in_parallel(*fns):
    procs = []
    result = 0
    for fn in fns:
        p = Process(target=fn)
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
        result += 1
    return result

def link_connector_generator(x, gdf):
    lat = x['shape_pt_lat_x']
    lon = x['shape_pt_lon_x']
    pt = Point(lon, lat)
    temp = gdf.distance(pt) < 1e-6
    row = gdf.iloc[temp[temp].index]
    row = row.loc[row.direction_ == x.direction_id]
    name = ""
    length = 0
    if len(row) > 1:
        row, _, _ = compare_df_distance_minimum(row, Point(lon, lat))
        name = row['Name']
        length = row['Length']
    elif len(row) == 1:
        name = row.iloc[0]['Name']
        length = row.iloc[0]['Length']
    else:
        direction_df = gdf.loc[gdf.direction_ == x.direction_id]
        row, _, _ = compare_df_distance_minimum(direction_df, Point(lon, lat))
        name = row['Name']
        length = row['Length']
    x['Link Name'] = name
    x['length'] = length
    return x

def vp_link_connector_generator(x, gdf):
    lat = x['latitude']
    lon = x['longitude']
    pt = Point(lon, lat)
    temp = gdf.distance(pt) < 1e-6
    row = gdf.iloc[temp[temp].index]
    row = row.loc[row.direction_ == x.direction_id]
    name = ""
    length = 0
    distance = 0
    if len(row) > 1:
        row, min_dist, min_idx = compare_df_distance_minimum(row, Point(lon, lat))
        name = row['Name']
        length = row['Length']
        distance = min_dist
    elif len(row) == 1:
        name = row.iloc[0]['Name']
        length = row.iloc[0]['Length']
        distance = 0
    else:
        direction_df = gdf.loc[gdf.direction_ == x.direction_id]
        row, min_dist, min_idx = compare_df_distance_minimum(direction_df, Point(lon, lat))
        name = row['Name']
        length = row['Length']
        distance = min_dist
    x['Link Name'] = name
    x['length'] = length
    x['distance'] = distance
    return x

def gtfs_compiler(date_name, route_sample, static_distance_df, TU_Saved_DIR2, VP_Saved_DIR2):
    bus_num = route_sample.split('-')[0]

    tu_df = pd.read_csv(TU_Saved_DIR2 + "TU " + date_name + "\\result_feed_" + route_sample +" " + date_name + ".csv")
    vp_df = pd.read_csv(VP_Saved_DIR2 + "VP " + date_name + "\\result_vp_" + route_sample +" " + date_name + ".csv")
    
    tu_df = remove_unnamed(tu_df)
    vp_df = remove_unnamed(vp_df)
    
    # tu_bus_df = trip_update_preprocessing(tu_df, shape_sample, route_sample, bus_num, static_distance_df)
    unique_shapes = list(static_distance_df.shape_id.unique())
    all_vps = pd.DataFrame()
    unique_vp_shapes= list(str(e) for e in list(vp_df.shape_id.unique()))
    
    shape_list = list(set(unique_shapes) & set(unique_vp_shapes))
    
    for shape in shape_list:
        vp_bus_df = vehicle_position_preprocessing(vp_df, shape, route_sample, static_distance_df)
        vp_bus_df['speed'] = vp_bus_df['stop_distance'] / (vp_bus_df['tt_all'] / 3600)
        all_vps = all_vps.append(vp_bus_df, ignore_index=True)
    
    all_vps['stop_sequence_id'] = all_vps['stop_sequence'].apply(lambda x: f"{x}-{x+1}")
    print("Process completed!")
    return all_vps

def ckdnearest(gdfA, gdfB, gdfB_cols):
    """
    https://gis.stackexchange.com/questions/222315/finding-nearest-point-in-other-geodataframe-using-geopandas
    Mapping the point to the closest linestring
    Parameters
    ----------
    gdfA : TYPE
        DESCRIPTION.
    gdfB : TYPE
        DESCRIPTION.
    gdfB_cols : TYPE, optional
        DESCRIPTION. The default is ['Name', 'direction_'].

    Returns
    -------
    gdf : TYPE
        DESCRIPTION.

    """
    A = np.concatenate(
        [np.array(geom.coords) for geom in gdfA.geometry.to_list()])
    B = [np.array(geom.coords) for geom in gdfB.geometry.to_list()]
    B_ix = tuple(itertools.chain.from_iterable(
        [itertools.repeat(i, x) for i, x in enumerate(list(map(len, B)))]))
    B = np.concatenate(B)
    ckd_tree = cKDTree(B)
    dist, idx = ckd_tree.query(A, k=1)    
    idx = itemgetter(*idx)(B_ix)
    gdf = pd.concat(
        [gdfA, gdfB.loc[idx, gdfB_cols].reset_index(drop=True),
         pd.Series(dist, name='dist')], axis=1)
    return gdf

def shaper_mapper(x):
    d = {}
    d['distance'] = x['distance'].sum()
    d['shape_pt_lat_x']=x['shape_pt_lat_x'].head(1).iloc[0]
    d['shape_pt_lon_x']=x['shape_pt_lon_x'].head(1).iloc[0]
    d['shape_pt_lat_y']=x['shape_pt_lat_y'].tail(2).iloc[0]
    d['shape_pt_lon_y']=x['shape_pt_lon_y'].tail(2).iloc[0]
    d['routes_list']=list(zip(x['shape_pt_lon_x'], x['shape_pt_lat_x']))
    return pd.Series(d, index=['distance', 'shape_pt_lat_x','shape_pt_lon_x', 'shape_pt_lat_y', 'shape_pt_lon_y', 'routes_list'])

def bus_route_path(x, gdf, cols):
    routes = x['routes_list']
    idxs = [i for i in range(1, len(routes))]
    tempdf = pd.DataFrame(list(zip(idxs, routes)), columns=['index', 'points'])
    geo = [Point(x) for x in tempdf['points']]
    temp_gpd = gpd.GeoDataFrame(tempdf, crs='EPSG:4376', geometry=geo)
    c_near = ckdnearest(temp_gpd, gdf, cols)
    return ' | '.join(c_near.iloc[:, 3].unique())

def fetch_data_from_sql_remote(query, db='gtfs_v'):
    start = time.time()
    print("fetching data from remote sql.....")
    print("query = ", query)
    try:
        data = pd.read_sql_query(query, 'mysql://sentosadg18_rw:lLGlN4wlMXCm.2QW@resmysql03.qut.edu.au/' + str(db))
        end = time.time()
        print(f"sql query fetching data executed in: {str((end - start) / 60)} mins")
        print("sql remote data = ", data.shape)
        return data
    except (pd.errors.DatabaseError, sql.exc.ProgrammingError):
        return pd.DataFrame()

def fetch_data_from_sql(query, db='gtfs_preprocessed'):
    start = time.time()
    print("fetching data from local sql...")
    data = pd.read_sql_query(query, 'mysql://root:password@127.0.0.1/' + str(db))
    end = time.time()
    print(f"sql query fetching data executed in: {str((end - start) / 60)} mins")
    print("local sql remote data = ", data.shape)
    return data

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta
        
def apply_lambda(static_sample, x):
    if len(list(static_sample.loc[static_sample.stop_sequence == x].stop_id.unique())) > 0:
        return list(static_sample.loc[static_sample.stop_sequence == x].stop_id.unique())[0]
    else:
        return None

def static_filtering(date_name, shape_sample, route_sample, bus_num, static_sample, Static_month):
#     print("starting static filtering.... for bus num {}".format(bus_num))
    try:
        distance_static = pd.read_csv(Static_month + "Static " + date_name + "\\" + bus_num + "_full_static_data.csv")
        static_sample = remove_unnamed(static_sample)
        distance_static = remove_unnamed(distance_static)

        distance_static['shape_id'] = distance_static['shape_id'].astype(str)
        distance_static = distance_static.loc[distance_static.shape_id == shape_sample]
#         print("distance static shape = ", distance_static.shape)
        ### Can be removed!!!!
        dist_df = distance_static.merge(static_sample[['stop_id', 'stop_sequence', 'direction_id', 'arrival_time']], left_on=['stop_seq_1'], right_on=['stop_sequence'], how='inner')
        dist_df_grouped = dist_df.groupby(['shape_id', 'stop_seq_1', 'stop_seq_2'])
        dist_df = dist_df_grouped.agg(shape_pt_lat_x=('shape_pt_lat_x', 'first'), 
                                      shape_pt_lon_x=('shape_pt_lon_x', 'first'), 
                                      shape_pt_lat_y=('shape_pt_lat_y', 'last'), 
                                      shape_pt_lon_y=('shape_pt_lon_y', 'last'), 
                                      distance=('distance', 'first'), 
                                      routes_list=('routes_list', 'first'), 
                                      shape_file_name=('shape_file_name', 'first'),
                                      direction_id=('direction_id', 'first'), 
                                      arrival_time=('arrival_time', 'first')).reset_index()
        #########################################################
        dist_df['stop_id'] = dist_df['stop_seq_1'].apply(lambda x: list(static_sample.loc[static_sample.stop_sequence == x].stop_id.unique())[0])
        dist_df['stop_name'] = dist_df['stop_seq_1'].apply(lambda x: list(static_sample.loc[static_sample.stop_sequence == x].stop_name.unique())[0])
        dist_df['from_stop_id'] = dist_df['stop_seq_1'].apply(lambda x: list(static_sample.loc[static_sample.stop_sequence == x].stop_id.unique())[0])
        dist_df['to_stop_id'] = dist_df['stop_seq_2'].apply(lambda x: apply_lambda(static_sample, x))
        
        return dist_df
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print("no data")
        return pd.DataFrame()
    

def dist_link_to_pt(x, gdf):
    first_pt = x['first_detection']
    last_pt = x['last_detection']
    row = gdf[gdf['Name'] == x['Link Name']]
    line = row['geometry']
    first_distance = line.project(first_pt).iloc[0]
    last_distance = line.project(last_pt).iloc[0]
    actual_distance = line.length.iloc[0]
    x['actual_distance'] = (last_distance * 100) - (first_distance * 100)
    ## error distance the distance of first detection and difference between the actual travelled and length of link
    x['error_distance'] = (actual_distance * 100) - (last_distance * 100) + (first_distance * 100)
    return x

def static_link_name_connector(x, all_static_high_freq_bus):
    shape_row = x['shape_id']    
    stop_row = x['stop_id']
    stop_seq_row = x['stop_sequence']
    static_bus = all_static_high_freq_bus.loc[all_static_high_freq_bus['shape_id'].astype(str) ==str(shape_row)]
    static_row = static_bus[(static_bus['stop_id'] == str(stop_row)) & (static_bus['stop_seq_1'] == stop_seq_row)]
    if len(static_row) < 1:
        static_row = all_static_high_freq_bus[(all_static_high_freq_bus['to_stop_id'] == str(stop_row)) & (all_static_high_freq_bus['stop_seq_2'] == stop_seq_row)]
    x['Actual_Link_Name'] = static_row.iloc[0]['Link Name']
    x['Actual_Length'] = static_row.iloc[0]['length']
    return x

def convert_to_float(x):
    return float(x[0]), float(x[1])

def bus_routes_paths_x(x, gdf, cols):
    routes = literal_eval(x['routes_list'])
    routes = tuple(map(convert_to_float, routes))
    bus_num = x['bus_num']
    idxs = [i for i in range(1, len(routes))]
    tempdf = pd.DataFrame(list(zip(idxs, routes)), columns=['index', 'points'])
    geo = [Point(x) for x in tempdf['points']]
    # geo = [Point(x) for x in tempdf['points']]
    # test = [Point(float(x)) for x in tempdf['points']]
    # print(test)
    temp_gpd = gpd.GeoDataFrame(tempdf, crs='EPSG:4376', geometry=geo)
    static_gdf = gdf.loc[gdf['Associate'].str.contains(bus_num)].reset_index()
    if len(static_gdf) < 1:
        static_gdf = gdf.copy(deep=True)
    # print("temp_gpd = ", temp_gpd.info())
    # print("static_gdf = ", static_gdf.info())
    c_near = ckdnearest(temp_gpd, static_gdf, cols)
    # print("c_near created!! ", c_near.columns)
    # print(c_near.info())
    # print(list(c_near.iloc[:, 3].unique()))
    # print(c_near.iloc[:, 3].nunique())
    return list(c_near.iloc[:, 3].unique())

def matrix_mapping(all_static_buses_df, gdf, cols, is_static=False):
    inbound_static_buses = all_static_buses_df.loc[all_static_buses_df.direction_id == 0]
    outbound_static_buses = all_static_buses_df.loc[all_static_buses_df.direction_id == 1]
    inbound_gdf = gdf.loc[gdf.direction_id == 0]
    outbound_gdf = gdf.loc[gdf.direction_id == 1]
    inbound_gdf = inbound_gdf.reset_index()
    inbound_static_buses = inbound_static_buses.reset_index()
    c_inbound = ckdnearest(inbound_static_buses, inbound_gdf, cols)
    outbound_static_buses = outbound_static_buses.reset_index()
    outbound_gdf = outbound_gdf.reset_index()
    c_outbound = ckdnearest(outbound_static_buses, outbound_gdf, cols)
    if is_static:
        c_inbound['names_list'] = c_inbound.apply(lambda x: bus_routes_paths_x(x, inbound_gdf, cols), axis=1)
        c_outbound['names_list'] = c_outbound.apply(lambda x: bus_routes_paths_x(x, outbound_gdf, cols), axis=1)
    c_all = pd.concat([c_inbound, c_outbound], ignore_index=True)
    # print(c_all)
    c_all_line = c_all.merge(gdf, on=['Name'], how='inner')
    return c_all_line

def static_matrix_mapping(all_static_buses_df, gdf):
#     print(all_static_buses_df.columns)
#     print(all_static_buses_df.shape)
    all_static_buses_df['bus_num'] = all_static_buses_df['shape_id'].apply(lambda x: x[:-4])
#     print("bus num generated...")
    all_static_buses_df['point'] = all_static_buses_df[['shape_pt_lon_x', 'shape_pt_lat_x']].apply(lambda x: Point(x['shape_pt_lon_x'], x['shape_pt_lat_x']), axis=1)
#     print("point is generated from lon and lat")
#     print("all_static_buses_df = ", all_static_buses_df.shape)
    geo = [Point(x) for x in all_static_buses_df['point']]
#     print(geo)
    all_static_gdf = gpd.GeoDataFrame(all_static_buses_df, crs='EPSG:4376', geometry=geo)
    all_static_gdf = gpd.GeoDataFrame(all_static_buses_df, crs='EPSG:4376', geometry=[Point(x) for x in all_static_buses_df['point']])
    gdf.rename(columns={'direction_':'direction_id', 'Associated':'Associate', 'Length' :'length'}, inplace=True)
    c_all_line = matrix_mapping(all_static_gdf, gdf, cols=['Name', 'direction_id'], is_static=True)
    # print("c_all_line = ", c_all_line)
    c_all_line = c_all_line.drop(['index', 'direction_id_x'], axis=1)
    # print("dropping index and dir_id = ")
    # print("c_all_line = ", c_all_line)
    new_gdf = c_all_line.groupby(['Name']).agg(Associat_1=('shape_id', set), Associate=('bus_num', set), length=('length', 'first'), direction_id=('direction_id_y', 'first'), geometry=('geometry_y', 'first')).reset_index()
    # print("new_gdf = ", new_gdf)
    new_gdf['Associate'] = new_gdf.Associate.apply(lambda xs: ' | '.join(str(x) for x in xs))
    new_gdf['Associat_1'] = new_gdf.Associat_1.apply(lambda xs: ' | '.join(str(x) for x in xs))
    gddf = gpd.GeoDataFrame(new_gdf, crs='EPSG:4376', geometry=[LineString(x) for x in new_gdf['geometry']])
    #gddf.to_file("HFS_new_" + date_name + " 24Sep")
    non_gdf = gdf[~gdf['Name'].isin(gddf['Name'])]
    non_gdf.rename(columns={'direction_':'direction_id', 'Associated':'Associate', 'Length' :'length'}, inplace=True)
    merged_gdf = gpd.GeoDataFrame(pd.concat([gddf, non_gdf], ignore_index=True))
    c_all_line.rename(columns={"geometry_x":"point_geometry", "geometry_y" : "linestring_geometry", "direction_id_x" : "direction_id", "length_x": "length"}, inplace=True)
#     print("ending matrix mapping....")
    return c_all_line, merged_gdf

def vp_matrix_mapping(vp_df, static_stops, static_gdf):
    vp_df['current_point'] = vp_df[['latitude', 'longitude']].apply(lambda x: Point(x['longitude'], x['latitude']), axis=1)
    vp_gdf = gpd.GeoDataFrame(vp_df, crs='EPSG:4376', geometry=[Point(x) for x in vp_df['current_point']])
    ######NOT WORKING FOR YOUR GDF!!!!!!!!!!!!!###############
    static_gdf.rename(columns={'direction_':'direction_id', 'Associated':'Associate', 'Length' :'length'}, inplace=True)
    ##############################################################
    vp_line = matrix_mapping(vp_gdf, static_gdf, cols=['Name', 'length'])
    vp_line.rename(columns={"Name": "Link Name", "direction_id_x": "direction_id", "length_x": "link_length", "dist": "link_dist"}, inplace=True)
    vp_line = vp_line.drop(['Associate', 'Associat_1', "geometry_y"], axis=1)
    static_stops.rename(columns={'direction_id_y': 'direction_id'}, inplace=True)
    vp_line['stop_id'] = vp_line['stop_id'].astype(str)
    vp_line['shape_id'] = vp_line['shape_id'].astype(str)
    static_stops['stop_id'] = static_stops['stop_id'].astype(str)
    static_stops['shape_id'] = static_stops['shape_id'].astype(str)
    ###############################CAN BE REMOVED!! REDUNDANT!!####################################
    vp_gdf_2 = vp_line.merge(static_stops[['shape_id', 'distance', 'stop_id', 'Name', 'names_list', 'length', 'direction_id', 'linestring_geometry']], on=['shape_id', 'stop_id', 'direction_id'], how='inner')
    # vp_line['stop_point'] = vp_line[['stop_lat', 'stop_lon']].apply(lambda x: Point(x['stop_lon'], x['stop_lat']), axis=1)
    # vp_gdf = gpd.GeoDataFrame(vp_line, crs={'init': 'epsg:4376'}, geometry=[Point(x) for x in vp_line['stop_point']])
    # vp_gdf = vp_gdf.drop(['stop_point'], axis=1)
    # vp_gdf_2 = matrix_mapping(vp_gdf, static_gdf, cols=["Name", "length"])
    # vp_gdf_2 = vp_gdf_2.drop(['level_0', 'index', 'dist', 'Associate', 'Associat_1', 'direction_id_y'], axis=1)
    vp_gdf_2.rename(columns={"distance": "stop_distance", "Name": "Actual_Link_Name", "length": "Actual_length"}, inplace=True)
    return vp_gdf_2

# static_stops['linestring_geometry'] = static_stops[['names_list']].apply(lambda x: generate_lines(x, gdf))

def speed_agg_link_analysis(date_name, static_stops, static_gdf, all_static_df, month_name): 
    start = time.time()
    month, year = month_name.split(' ,')
    month = month.lower()
    zip_file = ZipFile(Static_Folder + 'GTFS Static ' +date_name + '.zip')
    # data = fetch_data_from_sql(f"select * from `hfb_vp_{date_name}`")
    data = fetch_data_from_sql_remote(f"select * from `{month}_{year}` where DATE(FROM_UNIXTIME(`timestamp`)) = str_to_date('{date_name}', '%%d-%%m-%%Y')");
    stop_times = pd.read_csv(zip_file.open('stop_times.txt'))
    stop_times_df = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
    if len(data) > 0:
        data = data.drop(['index'], axis=1)
        data['timestamp_dt'] = data['timestamp'].apply(lambda x: dt.fromtimestamp(x))
        
        data = data.drop_duplicates(subset=['timestamp', 'id'], keep='first')
        
        trips = pd.read_csv(zip_file.open('trips.txt'))
        stops = pd.read_csv(zip_file.open('stops.txt'))
        
        trips_df = trips[['route_id', 'trip_id', 'shape_id', 'direction_id']]
        stops_df = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
        
        data = data.merge(trips_df, on=['route_id', 'trip_id'], how='inner')
        data = data.merge(stop_times_df, on=['trip_id', 'stop_id'], how='inner')
        data = data.merge(stops_df, on=['stop_id'], how='inner')
    else:
        data = fetch_data_from_hetrogen(date_name, month_name)    
    if len(data) < 1:
        return pd.DataFrame(columns=['trip_id', 'Link Name', 'link_length', 'cum_space', 'timestamp', 'scheduled_timestamp', 'actual_cum_tt', 'expected_cum_tt', 'travel_time', 'expected_tt', 'delay', 'speed', 'dt'])
    data_new = data.loc[(data['latitude'].notnull()) & (data['longitude'].notnull())]
    # data_new = data_new.drop_duplicates(subset=['trip_id'], keep='last')
    data_new = vp_matrix_mapping(data_new, static_stops, static_gdf)
    static_stops['stop_id'] = static_stops['stop_id'].astype(str)
    static_stops = static_stops.sort_values(by=['shape_id', 'stop_seq_1'])
    static_stops = static_stops.drop_duplicates(subset=['shape_id', 'stop_seq_1', 'stop_seq_2'], keep='first')
#     print("starting bus trajectory..........")
    stop_stats_df = static_stops.groupby(['shape_id']).apply(stop_geometry).reset_index()
    stop_stats_df = stop_stats_df.drop(['level_1'],axis=1)
    # data_new['trip_id_new'] = data_new['trip_id'].copy(deep=True)
    # print(data_new.trip_id.unique()[-1])
    ###############Stop-to-Stop Trajectory Buses##############################
    # sorted_df_merged = data.groupby(['trip_id']).apply(bus_trajectory)
    # print("trajectory generated..........")
    # end = time.time()
    # print(sorted_df_merged.columns)
    ##########################################################################
    #############Intersection-to-Intersection Trajectory Buses#################
    # data_merged = data.merge(stop_stats_df[['stop_id', 'shape_id', 'stop_sequence', 'distance', 'direction_id']], on=['stop_id', 'shape_id', 'stop_sequence', 'direction_id'], how='inner')
    # sorted_df_merged = data.groupby(['trip_id']).apply(lambda x: intersection_analysis_new(x, static_gdf))
    sorted_df_merged = data_new.groupby(['trip_id']).apply(intersection_analysis_new, gdf=static_gdf, stop_stats_df=stop_stats_df, all_static_df=stop_times_df, date_name=date_name)
    #sorted_df_merged = sorted_df_merged.interpolate()
    sorted_df_merged = sorted_df_merged.dropna()
    if len(sorted_df_merged) > 0:
        sorted_df_merged['dt'] = sorted_df_merged['timestamp'].apply(lambda x: dt.fromtimestamp(x))
    end = time.time()
    print(f"The duration it takes to execute for the whole day intersection analysis = {(end - start)/60} mins")
    return sorted_df_merged

# def length_rest(x):
#     line = x['linestring_geometry']
#     pt = x['current_point']
#     cut_pieces = cut(line, line.project(pt))
#     if len(cut_pieces) > 1:
#         print("first")
#         return (wgs84_geod.geometry_length(cut_pieces[0]) / 1000)
#     print("second")
#     return (wgs84_geod.geometry_length(LineString(nearest_points(line, pt))) / 1000)

def length_rest(pt_df, gdf):
    list_names = pt_df['names_list']
    pt = pt_df['current_point']
    stop_link = gdf.loc[gdf['Name'].isin(list_names)]
    stop_link['dist'] = stop_link.geometry.apply(lambda y: y.project(nearest_points(y, pt)[0]))
    test_stop = stop_link.reset_index()
    idx_min = test_stop['dist'].idxmin()
    closest_link = test_stop.loc[idx_min]
    dist = closest_link['dist']
    return test_stop.iloc[:idx_min+1]['length'].sum() + dist

def actual_distance_difference(sorted_data, stop_df):
    """
    IF SAME STOP = NEXT DIST - CURR_DIST ELSE (NEXT_DIST + STOP DIST) - CURR_DIST
    """
    next_stop = sorted_data['next_stop_seq']
    curr_stop = sorted_data['stop_sequence']
    if curr_stop == next_stop:
        sorted_data['next_dist'] = abs(sorted_data['next_distance'] - sorted_data['distance'])
    else:
        sorted_data['next_dist'] = abs((sorted_data['next_distance'] + sorted_data['stop_distance']) - sorted_data['distance'])
    return sorted_data

def bus_trajectory(x):
    data = x[['Link Name', 'timestamp', 'stop_id', 'stop_sequence', 'current_point', 'link_length', 'link_dist', 'Actual_length', 'linestring_geometry', 'names_list']]
    sorted_data = data.sort_values(by=['timestamp'])
    sorted_data['distance'] = sorted_data[['linestring_geometry', 'current_point']].apply(length_rest, axis=1)
    # sorted_data['next_dist'] = abs(sorted_data['distance'].shift(-1) - sorted_data['distance'])
    sorted_data['cumulative_space'] = sorted_data['next_dist'].cumsum()
    sorted_data['travel_time'] = abs(sorted_data['timestamp'].shift(-1) - sorted_data['timestamp'])
    sorted_data['cumulative_tt'] = sorted_data['travel_time'].cumsum()
    cumulative_plot = sorted_data.groupby(['timestamp']).agg(space=('cumulative_space','last'), tt=('cumulative_tt','last'))
    cumulative_plot = cumulative_plot.reset_index()
    if len(cumulative_plot) < 2:
        res = x[['Link Name', 'link_length', 'shape_id', 'latitude', 'longitude', 'current_status', 'timestamp', 'id', 'stop_id', 'current_point', 'stop_sequence', 'link_dist']]
        res['Space'] = 0
        res['travel_time'] = 0
        res['speed'] = 0
        return res.head(1)
    #####################################28Sep2022 19:00pm#############################################
    f = interp1d(cumulative_plot['timestamp'], cumulative_plot['space'])
    actual_df = x[['Link Name', 'link_length', 'shape_id', 'latitude', 'longitude', 'current_status', 'timestamp', 'id', 'stop_id', 'current_point', 'stop_sequence', 'link_dist', 'route_id', 'direction_id_x']]
    actual_df.rename(columns={'direction_id_x': 'direction_id'}, inplace=True)
    actual_df['route_id'] = actual_df['route_id'].apply(lambda x: x.split('-')[0])
    actual_df_min = actual_df.loc[(actual_df.groupby(['shape_id', 'id', 'stop_id'])['link_dist'].idxmin())]
    actual_df_max = actual_df.loc[(actual_df.groupby(['shape_id', 'id', 'stop_id'])['link_dist'].idxmax())]
    actual_combi = pd.concat([actual_df_min, actual_df_max], ignore_index=True)
    actual_combi = actual_combi.sort_values(by=['timestamp'])
    actual_combi['Space'] = f(actual_combi['timestamp'])
    actual_combi['travel_time'] = actual_combi.groupby(['stop_sequence'])['timestamp'].diff()
    actual_combi['Space'] = actual_combi.groupby(['stop_sequence'])['Space'].diff()
    actual_combi_drop = actual_combi.dropna(how='any', axis=0)

    actual_combi_drop['speed'] = actual_combi_drop['Space'] / (actual_combi_drop['travel_time'] / 3600)
    actual_combi_drop = actual_combi_drop.fillna(0)
    return actual_combi_drop

def flatten_link_names(x):
    """
    flatten the list of names lists to generate a single list of items
    """
    flat_list = []
    q_list = []
    a = x.copy()
    for sublist in a:
        if isinstance(sublist, str):
            sublist = literal_eval(sublist)
        if not sublist in q_list:
            q_list.append(sublist)
            for item in sublist[:-1]:
                if item not in flat_list:
                    flat_list.append(item)
    return flat_list

def intersection_analysis(x, gdf, date_name):
    """
    Generate the intersection cumulative plot for various tripID and output new dataframe for list of Link Name

    Parameters
    ----------
    x : Dataframe for a particular tripID
        DESCRIPTION.
    gdf : GeoDataFrame
        HFS intersection level shapefile.

    Returns Dataframe with full link-to-link from first stop to the last stop before terminus
    -------
    DataFrame
        Full Dataframe of Link-to-Link From start to end of trip based on interpolated values.

    """
    data = x[['Link Name', 'timestamp', 'stop_id', 'stop_sequence', 'current_point', 'link_length', 'link_dist', 'Actual_length', 'linestring_geometry', 'arrival_time', 'names_list']]
    sorted_data = data.sort_values(by=['timestamp'])
    sorted_data['distance'] = sorted_data[['linestring_geometry', 'current_point']].apply(length_rest, axis=1)
    sorted_data['next_link'] = sorted_data['Link Name'].shift(-1)
    sorted_data['next_distance'] = sorted_data['distance'].shift(-1)
    sorted_data = sorted_data.apply(lambda x: actual_distance_difference(x, gdf), axis=1)
    # sorted_data['next_dist'] = abs(sorted_data['distance'].shift(-1) - sorted_data['distance'])
    sorted_data['cumulative_space'] = sorted_data['next_dist'].cumsum()
    sorted_data['travel_time'] = abs(sorted_data['timestamp'].shift(-1) - sorted_data['timestamp'])
    sorted_data['cumulative_tt'] = sorted_data['travel_time'].cumsum()
    sorted_data['scheduled_arrival_dt'] = (pd.to_datetime(date_name, format='%d-%m-%Y') + pd.to_timedelta(sorted_data['arrival_time']))
    sorted_data['scheduled_timestamp'] = sorted_data['scheduled_arrival_dt'].apply(lambda x: datetime.timestamp(x))
    cumulative_plot = sorted_data.groupby(['timestamp', 'scheduled_timestamp']).agg(space=('cumulative_space','last'), tt=('cumulative_tt','last'))
    cumulative_plot = cumulative_plot.reset_index()
    ### Link to Link DataFrame Generator
    # data_linked = sorted_data[['Link Name', 'link_length']]
    # data_linked = data_linked.drop_duplicates()
    list_links = flatten_link_names(sorted_data['names_list'])
    link_df = pd.DataFrame(list_links, columns=['Name'])
    link_df = link_df.merge(gdf[['Name', 'length']], on=['Name'], how='inner')
    link_df.rename(columns={"Name": "Link Name", "length": "link_length"}, inplace=True)
    if len(cumulative_plot) < 2:
        link_df['timestamp'] = sorted_data.tail(1)['timestamp'].iloc[0]
        link_df['scheduled_timestamp'] = sorted_data.tail(1)['scheduled_timestamp'].iloc[0]
        link_df['travel_time'] = 0
        link_df['expected_tt'] = link_df['scheduled_timestamp'] - link_df['scheduled_timestamp'].shift(1)
        link_df['delay'] = link_df['timestamp'] - link_df['scheduled_timestamp']
        link_df['speed'] = 0
        return link_df
#     print("linear interpolation....")
    f_actual = interp1d(cumulative_plot['space'], cumulative_plot['timestamp'], fill_value='extrapolate')
    f_scheduled = interp1d(cumulative_plot['space'], cumulative_plot['scheduled_timestamp'], fill_value='extrapolate')
#     print("generating cumulative sum....")
    link_df['timestamp'] = f_actual(link_df['link_length'].cumsum())
    link_df['scheduled_timestamp'] = f_scheduled(link_df['link_length'].cumsum())
    link_df['travel_time'] = link_df['timestamp'] - link_df['timestamp'].shift(1)
    link_df['expected_tt'] = link_df['scheduled_timestamp'] - link_df['scheduled_timestamp'].shift(1)
    link_df['delay'] = link_df['timestamp'] - link_df['scheduled_timestamp']
    link_df_drop = link_df.dropna(how='any', axis=0)
    ################## this is not a space-mean speed####################################
    link_df_drop['speed'] = link_df_drop['link_length'] / (link_df_drop['travel_time'] / 3600)
    link_df_drop = link_df_drop.replace([np.inf, -np.inf], np.nan)
    return link_df_drop

def intersection_analysis_new(x, gdf, stop_stats_df, all_static_df, date_name):
    """
    Generate the intersection cumulative plot for various tripID and output new dataframe for list of Link Name

    Parameters
    ----------
    x : Dataframe for a particular tripID
        DESCRIPTION.
    gdf : GeoDataFrame
        HFS intersection level shapefile.

    Returns Dataframe with full link-to-link from first stop to the last stop before terminus
    -------
    DataFrame
        Full Dataframe of Link-to-Link From start to end of trip based on interpolated values.

    """
    # print("intersection_analysis_new starting....")
    trip = x['trip_id'].unique()[0]
    # print("trip is ")
    # print(trip)
    # print(x['trip_id_new'].unique())
    stop_df = stop_stats_df.loc[(stop_stats_df['shape_id'] == x['shape_id'].unique()[0]) & (stop_stats_df['direction_id'] == x['direction_id'].unique()[0])]
    # if 'arrival_time' not in stop_df.columns:
    # static_time = all_static_df[all_static_df['trip_id'] == trip].groupby(['stop_sequence'])['arrival_time'].last()
    # print("static shape = ", static_time.shape)
    # print("static = ", static_time[['stop_sequence', 'arrival_time']])
    # print("stop shape = ", stop_df.shape)
    # print("stop = ", stop_df)
    # stop_df['arrival_time'] = static_time.copy(deep=True).interpolate()
    # print("sorting data...")
    
    sorted_data, stop_df = get_sort_data(x, stop_df, gdf, date_name)
    # print("get sorted data.... starting cumulative....")
    # print(sorted_data.head())
    actual_cum_plot, sch_cum_plot = get_cumulative_plot(sorted_data, stop_df)
    # print("cumulative actual and sch created!!")
    # print("sch_cum = ", sch_cum_plot.head())
    if len(actual_cum_plot['stop_sequence'].unique()) < 2:
        return None
    list_links = flatten_link_names(stop_df['names_list'])
    # print("list_links = ", list_links)
    link_df = pd.DataFrame(list_links, columns=['Name'])
    link_df = link_df.merge(gdf[['Name', 'length']], on=['Name'], how='inner')
    # print("merge gdf link = ", link_df.head())
    link_df.rename(columns={"Name": "Link Name", "length": "link_length"}, inplace=True)
    link_df['cum_space'] = link_df['link_length'].cumsum()
    # print("link_df cuim_space = ", link_df.head())
    list_links = flatten_link_names(sorted_data['names_list'])
    link_df = link_df.loc[link_df['Link Name'].isin(list_links)]
    # plt.plot(actual_cum_plot['timestamp'], actual_cum_plot['space'], 'oc', label='actual_pts')
    # plt.plot(actual_cum_plot['timestamp'], actual_cum_plot['space'], '-b', label='actual_line')
    # plt.plot(sch_cum_plot['scheduled_timestamp'], sch_cum_plot['sch_space'], 'og', label='scheduled_pts')
    # plt.plot(sch_cum_plot['scheduled_timestamp'], sch_cum_plot['sch_space'], '-r', label='scheduled_line')
    # for i, row in sch_cum_plot.iterrows():
    #     if i % 2 == 0:
    #         plt.annotate('{}'.format(row['stop_sequence']), (row['scheduled_timestamp']-100, row['sch_space'] + 1))
    #     else:
    #         plt.annotate('{}'.format(row['stop_sequence']), (row['scheduled_timestamp']-60, row['sch_space'] - 2.5))
    # plt.xlabel('timestamp')
    # plt.ylabel('cumulative space (km)')
    # plt.title("Timestamp vs. Cumulative Space (km) for trip {}".format(trip))
    # plt.legend()
    # plt.show()
    # print(link_df['link_length'].cumsum())
    UNDEF = np.nan
    # print("link_df = ", link_df.head())
    link_df['timestamp'] = np.interp(link_df['cum_space'], actual_cum_plot['space'], actual_cum_plot['timestamp'], right=UNDEF)
    link_df['scheduled_timestamp'] = np.interp(link_df['cum_space'], sch_cum_plot['sch_space'], sch_cum_plot['scheduled_timestamp'], right=UNDEF)
    # if link_df.head(1)['timestamp'].isnull().values.any():
    #     f_actual_tp = interp1d(actual_cum_plot['space'], actual_cum_plot['timestamp'])
    #     link_df['timestamp'] = np.where(link_df['timestamp'].isnull(), f_actual_tp(link_df['link_length']), link_df['timestamp'])
    link_df['actual_cum_tt'] = np.interp(link_df['cum_space'], actual_cum_plot['space'], actual_cum_plot['tt'], right=UNDEF)
    link_df['expected_cum_tt'] = np.interp(link_df['cum_space'], sch_cum_plot['sch_space'], sch_cum_plot['sch_tt'], right=UNDEF)
    link_df = link_df.dropna()
    # print("link_df dropping null")
    if len(link_df) < 1:
        # print("len_df is less than 1")
        return None
    link_df['travel_time'] = link_df['actual_cum_tt'] - link_df['actual_cum_tt'].shift(1)
    link_df = link_df.fillna(link_df.iloc[0]['actual_cum_tt'])
    link_df['expected_tt'] = link_df['expected_cum_tt'] - link_df['expected_cum_tt'].shift(1)
    link_df = link_df.fillna(link_df.iloc[0]['expected_cum_tt'])
    # print("fillna with first value....")
    # print(link_df.head())
    link_df['delay'] = link_df['travel_time'] - link_df['expected_tt']
    link_df_drop = link_df.dropna(how='any', axis=0)
    # print("dropping link_df null...")
    # print(link_df_drop.info())
    ################## this is not a space-mean speed####################################
    link_df_drop['speed'] = link_df_drop['link_length'] / (link_df_drop['travel_time'] / 3600)
    # print("computed speed....")
    link_df_drop.loc[link_df_drop.speed > 120, ['speed', 'travel_time', 'delay']] = np.nan
    # print("remove 120km/h above null")
    link_df_drop['travel_time'] = link_df_drop['travel_time'].interpolate(method='linear')
    # print("linear interpolation...")
    link_df_drop['delay'] = link_df_drop['travel_time'] - link_df_drop['expected_tt']
    link_df_drop['speed'] = link_df_drop['link_length'] / (link_df_drop['travel_time'] / 3600)
    df = link_df_drop.drop(link_df_drop[link_df_drop.speed > 120].index)
    # print(df.head(3))
    return df

def get_sort_data(x, stop_df, gdf, date_name):
    data = x[['Link Name', 'timestamp', 'stop_id', 'stop_sequence', 'current_point', 'link_length', 'stop_distance', 'linestring_geometry', 'arrival_time', 'names_list']]
    sorted_data = data.sort_values(by=['timestamp'])
    actual_line = LineString(stop_df['routes_list'].apply(lambda x: tuple(map(convert_to_float, literal_eval(x)))).sum())
    sorted_data['distance'] = sorted_data['current_point'].apply(lambda x: actual_line.project(nearest_points(actual_line, x)[0]) * 100)
    sorted_data['cumulative_space'] = sorted_data['distance'].diff().cumsum()
    sorted_data['cumulative_space'] = sorted_data['cumulative_space'].fillna(0)
    sorted_data['travel_time'] = abs(sorted_data['timestamp'].shift(-1) - sorted_data['timestamp'])
    sorted_data['travel_time'] = sorted_data['travel_time'].shift(1)
    sorted_data.iloc[0, sorted_data.columns.get_loc('travel_time')] = 0
    sorted_data['cumulative_tt'] = sorted_data['travel_time'].cumsum()
    sorted_data['scheduled_arrival_dt'] = (pd.to_datetime(date_name, format='%d-%m-%Y') + pd.to_timedelta(sorted_data['arrival_time']))
    sorted_data['scheduled_timestamp'] = sorted_data['scheduled_arrival_dt'].apply(lambda x: dt.timestamp(x))
    stop_df['scheduled_arrival_dt'] = (pd.to_datetime(date_name, format='%d-%m-%Y') + pd.to_timedelta(stop_df['arrival_time']))
    stop_df['scheduled_timestamp'] = stop_df['scheduled_arrival_dt'].apply(lambda x: dt.timestamp(x))
    stop_df['scheduled_tt'] = abs(stop_df['scheduled_timestamp'].shift(-1) - stop_df['scheduled_timestamp'])
    stop_df['scheduled_tt'] = stop_df['scheduled_tt'].shift(1)
    stop_df.iloc[0, stop_df.columns.get_loc('scheduled_tt')] = 0
    stop_df['cumulative_scheduled_tt'] = stop_df['scheduled_tt'].cumsum()
    stop_df['cumulative_space'] = stop_df['distance'].cumsum()
    return sorted_data, stop_df

def get_cumulative_plot(sorted_data, stop_df):
#     print("getting cumulative plot....")
    actual_cum_plot = sorted_data[['timestamp', 'cumulative_space', 'cumulative_tt', 'stop_sequence']].copy(deep=True)
    actual_cum_plot.rename(columns={'cumulative_space':'space', 'cumulative_tt':'tt'}, inplace=True)
    sch_cum_plot = stop_df.groupby(['stop_sequence']).agg(sch_space=('distance', 'last'), sch_tt=('cumulative_scheduled_tt', 'last'), scheduled_timestamp=('scheduled_timestamp', 'last'), scheduled_arr_dt=('scheduled_arrival_dt', 'last'), bus_num=('bus_num', 'first'))
    sch_cum_plot = sch_cum_plot.reset_index()
    sch_cum_plot['sch_space'] = sch_cum_plot['sch_space'].cumsum()
    return actual_cum_plot, sch_cum_plot

def bus_trajectory_analysis(x, gdf, date_name):
    sorted_data = x.sort_values(by=['timestamp'])
    sorted_data['distance'] = sorted_data[['linestring_geometry', 'current_point']].apply(length_rest, axis=1)
    sorted_data['next_link'] = sorted_data['Link Name'].shift(-1)
    sorted_data['next_distance'] = sorted_data['distance'].shift(-1)
    sorted_data = sorted_data.apply(lambda x: actual_distance_difference(x, gdf), axis=1)
    # sorted_data['next_dist'] = abs(sorted_data['distance'].shift(-1) - sorted_data['distance'])
    sorted_data['cumulative_space'] = sorted_data['next_dist'].cumsum()
    sorted_data['travel_time'] = abs(sorted_data['timestamp'].shift(-1) - sorted_data['timestamp'])
    sorted_data['cumulative_tt'] = sorted_data['travel_time'].cumsum()
    sorted_data['scheduled_arrival_dt'] = (pd.to_datetime(date_name, format='%d-%m-%Y') + pd.to_timedelta(sorted_data['arrival_time']))
    sorted_data['scheduled_timestamp'] = sorted_data['scheduled_arrival_dt'].apply(lambda x: datetime.timestamp(x))
    sorted_test = sorted_data.groupby(['stop_id'])[['shape_id', 'Actual_Link_Name', 'Actual_length', 'latitude', 'longitude', 'current_status', 'timestamp', 'timestamp_dt', 'stop_sequence', 'stop_name', 'current_point', 'link_dist', 'cumulative_space', 'scheduled_arrival_dt', 'scheduled_timestamp']].last()
    return sorted_test

def static_flattening(x, gdf):
    temp_list = flatten_link_names(x)
    link_df = pd.DataFrame(temp_list, columns=['Name'])
    link_df = link_df.merge(gdf[['Name', 'length']], on=['Name'], how='inner')
    return link_df

# def stop_geometry(x):
#     new_stop = x[['stop_seq_2','shape_pt_lat_y','shape_pt_lon_y','distance', 'routes_list', 'shape_file_name', 'direction_id', 'stop_id','stop_name', 'bus_num','point_geometry', 'linestring_geometry', 'Name', 'names_list']]
#     a = x.iloc[[0]][['stop_seq_1','shape_pt_lat_x','shape_pt_lon_x','distance', 'routes_list', 'shape_file_name', 'direction_id', 'to_stop_id','stop_name', 'bus_num', 'point_geometry', 'linestring_geometry', 'Name', 'names_list']]
#     new_stop.rename(columns={'stop_seq_2':'stop_seq', 'shape_pt_lat_y':'shape_pt_lat','shape_pt_lon_y': 'shape_pt_lon'}, inplace=True)
#     a.rename(columns={'stop_seq_1':'stop_seq', 'shape_pt_lat_x':'shape_pt_lat', 'shape_pt_lon_x': 'shape_pt_lon', 'to_stop_id':'stop_id'}, inplace=True)
#     #a['geometry'] = Point(a['shape_pt_lon'], a['shape_pt_lat'])
#     new_stop = pd.concat([a, new_stop[:]]).reset_index(drop=True)
#     return new_stop

def stop_geometry(x):
    """
    NEEDS FIXING!! STOPNAME, NAME LAST STOP IS INCORRECT!!

    Parameters
    ----------
    x : TYPE
        DESCRIPTION.

    Returns
    -------
    new_stop : TYPE
        DESCRIPTION.

    """
    new_stop = x[['stop_seq_1','shape_pt_lat_x','shape_pt_lon_x','distance', 'routes_list', 'shape_file_name', 'direction_id', 'stop_id','stop_name', 'bus_num','point_geometry', 'linestring_geometry', 'Name', 'names_list', 'arrival_time']]
    a = x.iloc[[-1]][['stop_seq_2','shape_pt_lat_y','shape_pt_lon_y','distance', 'routes_list', 'shape_file_name', 'direction_id', 'to_stop_id','stop_name', 'bus_num', 'point_geometry', 'linestring_geometry', 'Name', 'names_list', 'arrival_time']]
    new_stop.rename(columns={'stop_seq_1':'stop_sequence', 'shape_pt_lat_x':'shape_pt_lat','shape_pt_lon_x': 'shape_pt_lon'}, inplace=True)
    a.rename(columns={'stop_seq_2':'stop_sequence', 'shape_pt_lat_y':'shape_pt_lat', 'shape_pt_lon_y': 'shape_pt_lon', 'to_stop_id': 'stop_id'}, inplace=True)
    new_stop = pd.concat([new_stop, a], ignore_index=True)
    new_stop_shift = new_stop.copy(deep=True)
    new_stop_shift[['distance', 'routes_list', 'shape_file_name', 'linestring_geometry']] = new_stop[['distance', 'routes_list', 'shape_file_name', 'linestring_geometry']].shift(1)
    new_stop_shift = new_stop_shift.fillna(new_stop_shift.iloc[1][['distance', 'routes_list', 'shape_file_name', 'linestring_geometry']])
    new_stop_shift.iloc[0, new_stop_shift.columns.get_loc('distance')] = 0
    new_stop_shift.iloc[-1, new_stop_shift.columns.get_loc('Name')] = new_stop.iloc[-1]['names_list'][-1]
    return new_stop_shift

def contains_number(string):
    return any(char.isdigit() for char in string)
#### 18312704-BT 21_22-AUG_FUL-Weekday-01
# df_non_grouped, df_grouped = speed_agg_link_analysis(date_name, static_stops, gdf, all_static_df)

def speed_trip_trajectory_preprocessing_analysis(SELECTED_DATE):
    date_name = str(SELECTED_DATE.day)+"-"+str(SELECTED_DATE.month)+"-"+str(SELECTED_DATE.year)
    month_name = SELECTED_DATE.strftime("%B") + " ," +str(SELECTED_DATE.year)

    Working_Directory = r"Y:\\Sentosa\\GTFS_preprocessed\\gtfs\\Saved_Dirs\\"
    Realtime_folder = Working_Directory + 'GTFS Realtime\\'
    Static_folder = Working_Directory + 'GTFS Static\\'
    Static_month = Static_folder + 'Static '+ month_name + '\\'
    if not os.path.exists(Static_month):
        os.makedirs(Static_month)
    ######## HIGH FREQUENCY BUSES ANALYSIS #################
    HFB_DIR = Working_Directory + "GTFS Realtime\\HFB\\"
    if not os.path.exists(HFB_DIR):
        os.makedirs(HFB_DIR)
    ######## TRIP UPDATE DIRECTORY #########################
    TU_Saved_DIR = HFB_DIR + 'TripUpdate entity\\'
    if not os.path.exists(TU_Saved_DIR):
        os.makedirs(TU_Saved_DIR)
    TU_Saved_DIR2 = TU_Saved_DIR + 'TU ' + month_name + '\\'
    if not os.path.exists(TU_Saved_DIR2):
        os.makedirs(TU_Saved_DIR2)
    ######## VEHICLE POSITION DIRECTORY ####################
    VP_Saved_DIR = HFB_DIR + 'VehiclePosition entity\\'
    if not os.path.exists(VP_Saved_DIR):
        os.makedirs(VP_Saved_DIR)
    VP_Saved_DIR2 = VP_Saved_DIR + 'VP ' + month_name + '\\'
    if not os.path.exists(VP_Saved_DIR2):
        os.makedirs(VP_Saved_DIR2)
    
    HFS_DIR = r"C:\\Users\\gozalid\\Documents\\OneDrive_2022-09-12\\Transit Dashboards\\Data used\\Rough Work\\Ultra final\\Shape file\\"
    print("hello world!!!")
    gdf = gpd.read_file(HFS_DIR + "HFS.shp")
    print(Static_Folder)
    routes = get_all_routes(date_name)
    buses = []
    print("iterating routes...")
    for route in routes:
        bus_num = route.split('-')[0]
        if contains_number(bus_num):
            buses.append(bus_num)
    print("calculating dist file....")
    # dist_file = load_new_static(bus_num=high_frequency_buses, date_name=date_name)
    dist_file = load_new_static(buses, date_name, Static_month, Static_Folder)
    # def static_link_generator(date_name):
    start = time.time()
    all_static_high_freq_bus = pd.DataFrame()
    print("static merger is starting....")
    all_static_df = static_merger(date_name)
    print("populating the high frequency buses....")
    print("exists a file called static stops = ", os.path.exists(f".\\static_stops_{date_name}.csv"))
    # if not os.path.exists(f".\\static_stops_{date_name}.csv"):
    for route in routes:
        bus_num = route.split('-')[0]
        # if bus_num in high_frequency_buses:
        if bus_num in buses:
            static_df = all_static_df.loc[all_static_df.route_id == route]
            unique_shapes = list(static_df.shape_id.unique())
            static_df_stops_shapes = pd.DataFrame()
            for shape in unique_shapes:
                static_sample = static_df.loc[static_df['shape_id'].astype(str) == str(shape)]
                if len(static_sample) < 1:
                    pass
                static_distance_df = static_filtering(date_name, shape, route, bus_num, static_sample, Static_month)
                static_df_stops_shapes = pd.concat([static_df_stops_shapes, static_distance_df], ignore_index=True)
            # all_static_high_freq_bus = all_static_high_freq_bus.append(static_df_stops_shapes, ignore_index=True)
            all_static_high_freq_bus = pd.concat([all_static_high_freq_bus, static_df_stops_shapes], ignore_index=True)
    end = time.time()
    print(f"The duration it takes to execute for the whole day static mapping = {(end - start)/60} mins")

    print("routing is done....")
    print(f"The duration it takes to execute for the whole day static preprocessing = {(end - start)/60} mins")
    print("static_matrix mapping is starting....")
    static_stops, static_gdf = static_matrix_mapping(all_static_high_freq_bus, gdf)
    # static_stops.to_csv(f"static_stops_{date_name}.csv")
    # else:
    #     static_stops = pd.read_csv(f".\\static_stops_{date_name}.csv")
    #     if static_stops.columns.str.contains('Unnamed: ').any():
    #         remove_unnamed(static_stops)
    #     print(static_stops.info())
    #     static_gdf = gdf.copy(deep=True)
#     print('-'*100)
    start = time.time()
    print("starting speed agg link analysis....")
    df_grouped = speed_agg_link_analysis(date_name, static_stops, gdf, all_static_df, month_name)
    end = time.time()
    print(f"The duration it takes to execute for the whole day new_whole day analysis = {(end - start)/60} mins")
    print('-'*100)
    Saved_Worked = r"C:\\Users\\gozalid\\Documents\\15Apr23_Bluetooth_GTFS\\trip_trajectory_Sep21\\"
    if len(df_grouped) < 1:
        df_grouped.to_csv(Saved_Worked + "trip_trajectory_" + date_name + ".csv")
        return static_stops, static_gdf, df_grouped
    df_grouped_reset = df_grouped.reset_index()
    if 'index' in df_grouped_reset:
        df_grouped_reset = df_grouped_reset.drop(['index'], axis=1)
    if 'level_1' in df_grouped_reset:
        df_grouped_reset = df_grouped_reset.drop(['level_1'], axis=1)
#     print("df_reset is saving....")
#     print(Saved_Worked + "trip_trajectory_" + date_name + ".csv")
    df_grouped_reset.to_csv(Saved_Worked + "trip_trajectory_" + date_name + ".csv")
#     print("reset saved!")
#     print(Saved_Worked + "trip_trajectory_" + date_name + ".csv")
    return static_stops, static_gdf, df_grouped_reset

# static_stops, static_gdf, df_grouped = speed_trip_trajectory_preprocessing_analysis(SELECTED_DATE)
# duration_length = 8
# historical_days = []
# # all_groups = pd.DataFrame()
# for i in range(1, duration_length):
#     new_date = SELECTED_DATE + timedelta(-i)
#     _, _, new_group = speed_trip_trajectory_preprocessing_analysis(new_date)
#     historical_days.append(new_group.copy(deep=True))
#     new_date_name = str(new_date.day)+"-"+str(new_date.month)+"-"+str(new_date.year)
#     print(f"Processing date {new_date_name}......")
    
#     # dist_file = load_new_static(bus_num=buses, date_name=new_date_name)

#     # # def static_link_generator(date_name):
#     # routes = get_all_routes(new_date_name)
#     # all_static_high_freq_bus = pd.DataFrame()
#     # all_static_df = static_merger(new_date_name)
#     # for route in routes:
#     #     bus_num = route.split('-')[0]
#     #     # if bus_num in high_frequency_buses:
#     #     if bus_num in buses:
#     #         static_df = all_static_df.loc[all_static_df.route_id == route]
#     #         unique_shapes = list(static_df.shape_id.unique())
#     #         static_df_stops_shapes = pd.DataFrame()
#     #         for shape in unique_shapes:
#     #             static_sample = static_df.loc[static_df['shape_id'].astype(str) == str(shape)]
#     #             static_distance_df = static_filtering(new_date_name, shape, route, bus_num, static_sample)
#     #             static_df_stops_shapes = static_df_stops_shapes.append(static_distance_df, ignore_index=True)
#     #         all_static_high_freq_bus = all_static_high_freq_bus.append(static_df_stops_shapes, ignore_index=True)

#     # static_stops, static_gdf = static_matrix_mapping(all_static_high_freq_bus, gdf)
    
#     # new_group = speed_agg_link_analysis(new_date_name, static_stops, gdf, all_static_df, month_name)
#     new_group = speed_trip_trajectory_preprocessing_analysis(new_date_name)
#     historical_days.append(new_group.copy(deep=True))
#     all_groups = all_groups.append(new_group, ignore_index=True)
 
def ranking_algorithm(df_grouped, all_grouped, date_name):
    agg_df = df_grouped.set_index('dt')[['link_length', 'Link Name', 'travel_time', 'expected_tt', 'delay']].groupby(['Link Name']).resample('15T').mean()
    agg_df['speed(SMS)'] = agg_df['link_length'] / (agg_df['travel_time'] / 3600)
    agg_df = agg_df.replace([np.inf, -np.inf], np.nan)
    print("executing linear interpolation...........")
    agg_link_interpolate = agg_df.interpolate(method='linear')
    # agg_link_interpolate['speed(SMS)'] = agg_link_interpolate['link_length'] / (agg_link_interpolate['travel_time'] / 3600)
    all_grouped = all_grouped.sort_values(by=['timestamp'])
    all_grouped['time'] = all_grouped['dt'].apply(lambda x: x.time())
    all_grouped['new_dt'] = all_grouped['time'].apply(lambda x: dt.combine(pd.to_datetime(date_name, format='%d-%m-%Y'), x))
    all_group_link = all_grouped.set_index('new_dt')[['link_length', 'Link Name', 'travel_time', 'expected_tt', 'delay']].groupby(['Link Name']).resample('15T').quantile(0.95)
    all_group_link['speed(SMS)'] = all_group_link['link_length'] / (all_group_link['travel_time'] / 3600)
    all_group_link = all_group_link.replace([np.inf, -np.inf], np.nan)
    all_group_interpolate = all_group_link.interpolate(method='linear')
    # all_group_agg = all_grouped.set_index('dt')[['link_length', 'Link Name', 'travel_time', 'expected_tt', 'delay']].groupby(['Link Name']).resample('15T').quantile(0.95)
    # all_group_agg = all_group_agg.reset_index()
    
    # all_group_interpolate['speed(SMS)'] = all_group_interpolate['link_length'] / (all_group_interpolate['travel_time'] / 3600)
    agg_all_reset = all_group_interpolate.reset_index()
    agg_all_reset.rename(columns={"new_dt": "dt", 'travel_time': 'ff_tt', 'expected_tt': 'ff_expected_tt', 'speed(SMS)': 'ff_speed_SMS', 'delay': 'ff_delay'}, inplace=True)
    agg_df_reset = agg_link_interpolate.reset_index()
    df_merged = agg_df_reset.merge(agg_all_reset, left_on=['Link Name', 'dt', 'link_length'], right_on=['Link Name', 'dt', 'link_length'], how='inner')
    
    df_merged['ratio_travel_time'] = df_merged['travel_time'] / df_merged['expected_tt']
    df_merged['ratio_expected_tt'] = df_merged['ff_tt'] / df_merged['ff_expected_tt']
    df_merged['ratio_tt'] = df_merged['ratio_travel_time'] / df_merged['ratio_expected_tt']
    df_merged['ratio_speed'] = df_merged['speed(SMS)'] / df_merged['ff_speed_SMS']
    df_merged = df_merged.loc[(df_merged['link_length'] > 0)]
    ### Beneficial => higher value is desired => for congested => higher travel time required to be observed
    ### Non-beneficial => lower value is desired => for congested => lower speed required to be observed
    df_merged['norm_speed'] = df_merged[['speed(SMS)', 'Link Name']].apply(lambda x: nonbeneficial_normalized(x['speed(SMS)'], df_merged.loc[df_merged['Link Name'] == x['Link Name']]['speed(SMS)'].min()), axis=1)
    df_merged['norm_tt'] = df_merged[['travel_time', 'Link Name']].apply(lambda x: beneficial_normalized(x['travel_time'], df_merged.loc[df_merged['Link Name'] == x['Link Name']]['travel_time'].max()), axis=1)
    df_merged['norm_ratio_speed'] = df_merged[['ratio_speed', 'Link Name']].apply(lambda x:  nonbeneficial_normalized(x['ratio_speed'], df_merged.loc[df_merged['Link Name'] == x['Link Name']]['ratio_speed'].min()), axis=1)
    df_merged['norm_ratio_travel'] = df_merged[['ratio_travel_time', 'Link Name']].apply(lambda x: beneficial_normalized(x['ratio_travel_time'], df_merged.loc[df_merged['Link Name'] == x['Link Name']]['ratio_travel_time'].max()), axis=1)
    df_merged['norm_delay'] = df_merged['delay'].apply(np.abs, axis=1)
    df_merged['norm_delay'] = df_merged[['delay', 'Link Name']].apply(lambda x: nonbeneficial_normalized(x['norm_delay'], df_merged.loc[df_merged['Link Name'] == x['Link Name']]['norm_delay'].min()), axis=1)
    
    df_merged['weighted_speed'] = df_merged['norm_speed'].apply(lambda x: x * 0.4)
    df_merged['weighted_travel'] = df_merged['norm_tt'].apply(lambda x: x * 0.1)
    df_merged['weighted_ratio_speed'] = df_merged['norm_ratio_speed'].apply(lambda x: x * 0.1)
    df_merged['weighted_ratio_tt'] = df_merged['norm_ratio_travel'].apply(lambda x: x * 0.1)
    df_merged['weighted_delay'] = df_merged['norm_delay'].apply(lambda x: x * 0.3)
    ## sum of criterias => higher value shows congested link while lower value shows uncongested link
    # df_merged.to_csv("df_merged_weighted_norm_" + date_name + ".csv")
    weighted_cols = [col for col in df_merged.columns if 'weighted' in col]
    ##########RANK BASED ON ONLY THE LINKNAME OR DT##########################
    new_df = df_merged.groupby(['Link Name', 'dt'])[weighted_cols].mean()
    new_df['Total'] = new_df[weighted_cols].sum(axis=1)
    new_df = new_df.reset_index()
    new_df['Hierarchy_Rank'] = new_df['Total'].rank(ascending=False)
    new_df['Hierarchy_Rank'] = new_df.groupby(['Link Name'])['Total'].rank('first')
    
    df_merged_new = df_merged.merge(new_df[['Link Name', 'dt', 'Total', 'Hierarchy_Rank']], on=['Link Name', 'dt'], how='inner')
    # df_merged_new.to_csv("FULL_DATA_" + date_name + ".csv")
    return new_df, df_merged_new

# print("Ranking is applying.....")    
# rank_df, rank_merged_new = ranking_algorithm(df_grouped, all_groups, date_name)
# print("Ranked applied.")

def flattening_static_links(x, gdf):
    list_links = flatten_link_names(x['names_list'])
    link_df_333_0 = pd.DataFrame(list_links, columns=['Name'])
    link_df_333_0 = link_df_333_0.merge(gdf[['Name', 'length']], on=['Name'], how='inner')
    link_df_333_0.rename(columns={"Name": "Link Name", "length": "link_length"}, inplace=True)
    link_df_333_0['cum_space'] = link_df_333_0['link_length'].cumsum()
    return link_df_333_0

def route_based_analysis(df_grouped, all_groups, static_stops, static_gdf, route_choice, dir_id, date):
    date_name = str(date.year)+"-"+str(date.month)+"-"+str(date.day)
    month_name = date.strftime("%B") + "_" +str(date.year)
    month_name = month_name.lower()
    data = fetch_data_from_sql_remote(f"select * from `{month_name}` where date(from_unixtime(`timestamp`)) >= '{date_name}';")
    data = data.drop(['index'], axis=1)
    data = vp_matrix_mapping(data, static_stops, static_gdf)
    print("data columns.............")
    print(data.columns)
    print("-"*50)
    df_grouped_reset = df_grouped.reset_index()
    static_stops = static_stops.sort_values(by=['shape_id', 'stop_seq_1'])
    print("starting bus trajectory..........")
    stop_stats_df = static_stops.groupby(['shape_id']).apply(stop_geometry).reset_index()
    stop_stats_df = stop_stats_df.drop(['level_1'],axis=1)

    shape_route = stop_stats_df.loc[(stop_stats_df.bus_num == route_choice) & (stop_stats_df.direction_id == dir_id)]
    trips_route = data.loc[data['shape_id'].astype(str).isin(shape_route['shape_id'].unique())][['trip_id', 'shape_id']]
    trips_route = trips_route.drop_duplicates(subset=['trip_id'])

    ######################################merge the cumulative space#######################################
    # link_df_333_0_t = shape_333.groupby(['shape_id']).apply(lambda x: flattening_static_links(x))
    # link_df_333_0_t = link_df_333_0_t.reset_index()
    # link_df_333_0_t.drop(columns={'level_1'}, inplace=True)
    #######################################################################################################
    ## Get all trips that travel at the intersection travelled by the route
    df_grouped_route = df_grouped_reset.loc[df_grouped_reset['trip_id'].isin(trips_route['trip_id'].unique())]
    df_grouped_merged = df_grouped_route.merge(trips_route, on=['trip_id'], how='inner')

    agg_df = df_grouped_merged.set_index('dt')[['link_length', 'Link Name', 'travel_time', 'expected_tt', 'delay', 'cum_space', 'shape_id']].groupby(['Link Name']).resample('15T').mean()
    agg_df['speed(SMS)'] = agg_df['link_length'] / (agg_df['travel_time'] / 3600)
    agg_df = agg_df.replace([np.inf, -np.inf], np.nan)
    print("executing linear interpolation...........")
    agg_link_interpolate = agg_df.interpolate(method='linear')

    agg_link_interpolate = agg_link_interpolate.reset_index()
    agg_link_interpolate['hour'] = agg_link_interpolate['dt'].apply(lambda x: x.strftime('%H:%M:%S'))
    new_df = agg_link_interpolate.groupby(['cum_space', 'hour'])['speed(SMS)'].mean().unstack('hour')
    new_df.columns = pd.DatetimeIndex(new_df.columns).strftime('%H:%M')
    new_df.index = np.round(new_df.index, 3)
    xticks_spacing = int(pd.Timedelta('1h')/pd.Timedelta('15T'))

    ### Generate Heatmap of overall trip
    ax = sns.heatmap(new_df, xticklabels=xticks_spacing, yticklabels=5)
    ax.invert_yaxis()
    plt.yticks(rotation=0)
    bound = "Outbound" if dir_id else "Inbound"
    plt.title("Hour (HH:MM) vs. cum_space (km) for {} {}".format(route_choice, bound))

    return new_df

def score_calc_pts(i, j):
    #dt_diff = dtA - dtB
    return (i - j)**2

def distance_calc_pts(i, j):
    _, _, dist = wgs84_geod.inv(i.x, i.y, j.x, j.y)
    return dist / 1000

def time_calc_pts(i, j):
    #dt_diff = dtA - dtB
    return abs(i - j.to_pydatetime()).total_seconds()

def difference_calc(one, other, indicator=0):
    list_elements = []
    for i in one:
        temp_list = []
        for j in other:
            if indicator == 1: ##spatial
                temp_list.append(distance_calc_pts(i, j))
            elif indicator == 2: ## temporal 
                temp_list.append(time_calc_pts(i, j))
            else: ##multicriterions
                temp_list.append(score_calc_pts(i, j))
        list_elements.append(temp_list)
    return pd.DataFrame(list_elements)

def square_matrix_similarity(df_merged_new, static_gdf, SELECTED_DATE):
    df_cluster = df_merged_new.merge(static_gdf[['Name', 'length', 'direction_id', 'geometry']], left_on=['Link Name', 'link_length'], right_on=['Name', 'length'], how='inner')
    df_cluster = df_cluster[['Link Name', 'link_length', 'dt', 'Total', 'direction_id', 'geometry']]
    start_time = datetime.combine(SELECTED_DATE, datetime.time(6,0,0))
    end_time = datetime.combine(SELECTED_DATE, datetime.time(9,0,0))
    temp_cluster = df_cluster.loc[(df_cluster['dt'] >= start_time & df_cluster['dt'] <= end_time)]
    
    remove_unnamed(temp_cluster)
    ## Spatio-temporal Criterions
    distance_df = temp_cluster[['Link Name', 'centre_pt']].copy(deep=True)
    time_df = temp_cluster[['Link Name', 'dt']].copy(deep=True)
    score_df = temp_cluster[['Link Name', 'Total']].copy(deep=True)
    ## Square Matrix Formation
    dist_matrix = difference_calc(distance_df.centre_pt, distance_df.centre_pt, 1)
    time_matrix = difference_calc(time_df['dt'], time_df['dt'], 2)
    score_matrix = difference_calc(score_df['Total'], score_df['Total'])
    ## Spatial-Temporal Normalization
    dist_normed = normalize(dist_matrix, axis=1, norm='l2')
    time_normed = normalize(time_matrix, axis=1, norm='l2')
    ## Similarity Computation
    total_similarity_mat = np.add(dist_normed, time_normed)
    total_similarity_mat = np.add(total_similarity_mat, score_matrix)
    total_similarity_mat_euclidean = np.sqrt(total_similarity_mat)
    # import seaborn as sns
    # sns.heatmap(total_similarity_mat_euclidean, annot=True)
    return total_similarity_mat_euclidean

# total_similarity_mat_euclidean = square_matrix_similarity(df_merged_new, static_gdf, SELECTED_DATE)
# best_labels = dbscan_clustering(total_similarity_mat_euclidean)

def dbscan_clustering(df):
    epsilons = np.linspace(0.01, 1, num=20)
    
    min_samples = np.arange(20, 500, step=15)
    
    import itertools
    combinations = list(itertools.product(epsilons, min_samples))
    N = len(combinations)
    print(N)
    
    scores = []
    all_labels = []
    
    for i, (eps, min_sam) in enumerate(combinations):
        db = DBSCAN(eps=eps, min_samples=min_sam).fit(df)
        labels = db.labels_
        labels_set = set(labels)
        num_clusters = len(labels_set)
        
        if -1 in labels_set:
            num_clusters -= 1
            
        if num_clusters < 2 or num_clusters > 50:
            scores.append(-10)
            all_labels.append('bad')
            c = (eps, min_sam)
            print(f"Combination {c} on iteration {i+1} of {N} has {num_clusters} clusters. Next...")
            continue
        
        scores.append(ss(df, labels))
        all_labels.append(labels)
        print(f"Index: {i}, Score: {scores[-1]}, Labels: {all_labels[-1]}, NumClusters: {num_clusters}")
        
    best_index = np.argmax(scores)
    best_param = combinations[best_index]
    best_labels = all_labels[best_index]
    best_score = scores[best_index]
    
    return {'best_epsilon': best_param[0], 
            'best_min_samples': best_param[1],
            'best_labels': best_labels,
            'best_score': best_score}


def spatial_auto(df_merged_new, static_gdf, start, end):
    import os
    import esda
    from esda.moran import Moran, Moran_Local
    
    import splot
    from splot.esda import moran_scatterplot, plot_moran, lisa_cluster,plot_moran_simulation
    import libpysal as lps
    import contextily as ctx
    
    # Graphics
    import matplotlib.pyplot as plt
    import plotly.express as px
    from shapely import wkt
    date_name = f"{start.date().day}-{start.date().month}-{start.date().year}"
    print(date_name)
    start_time_name = f"{start.time().hour}_{start.time().minute}"
    end_time_name = f"{end.time().hour}_{end.time().minute}"
    # For spatial statistics
    dir_path = f"Y:\\Sentosa\\24Feb23\\{date_name}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    minutes = start.time().minute
    time = start.time().hour
    dir_path_time = dir_path + f"\\{start_time_name}_{end_time_name}"
    if not os.path.exists(dir_path_time):
        os.makedirs(dir_path_time)
    if "Unnamed: " in df_merged_new:
        remove_unnamed(df_merged_new)
        df_merged_new['dt'] = pd.to_datetime(df_merged_new['dt'])
        df_merged_new['centre_pt'] = df_merged_new['centre_pt'].apply(wkt.loads)
        df_merged_new['geometry'] = df_merged_new['geometry'].apply(wkt.loads)
    df_cluster = df_merged_new.merge(static_gdf[['Name', 'length', 'direction_id', 'geometry']], left_on=['Link Name', 'link_length'], right_on=['Name', 'length'], how='inner')
    df_cluster = df_cluster[['Link Name', 'link_length', 'dt', 'Total', 'direction_id', 'geometry']]
    # start_time = datetime.time(7,0,0)
    # end_time = datetime.time(7,15,0)
    # start = datetime.datetime.combine(SELECTED_DATE, start_time)
    # end = datetime.datetime.combine(SELECTED_DATE, end_time)
    test = df_cluster.loc[(df_cluster['dt'] >= start) & (df_cluster['dt'] < end)]
    
    print(f"time selected = {start.time()} to {end.time()}")
    
    test_gdf = gpd.GeoDataFrame(test, crs="EPSG:4376", geometry=test['geometry'])
    test_gdf.sort_values(by='Total').tail()
    fig,ax = plt.subplots(figsize=(12,10))
    test_gdf.sort_values(by='Total',ascending=False)[:20].plot(ax=ax,
                                                                     color='red',
                                                                     edgecolor='white',
                                                                     alpha=0.5,legend=True)
    
    
    # title
    ax.set_title(f'Top 20 Congested Links (9 December 2022) {start.time()} to {end.time()}')
    
    # no axis
    ax.axis('off')
    # add a basemap
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    # plot it!
    fig, ax = plt.subplots(figsize=(12,12))
    
    test_gdf.plot(ax=ax,
             color='black', 
             edgecolor='white',
             lw=0.5,
             alpha=0.4)
    
    # no axis
    ax.axis('off')
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    fig,ax = plt.subplots(figsize=(15,15))
    
    test_gdf.plot(ax=ax,
            column='Total', # this makes it a choropleth
            legend=True,
            alpha=0.8,
            cmap='RdYlGn_r', # a diverging color scheme
            scheme='quantiles') # how to break the data into bins
    
    ax.axis('off')
    ax.set_title(f'9 December 2022 Transit Congestion Total {start.time()} to {end.time()}',fontsize=22)
    # plt.savefig(dir_path_time + f"\\{date_name}_TransitCong_Total_{start_time_name}_to_{end_time_name}")
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    # calculate spatial weight
    wq =  lps.weights.KNN.from_dataframe(test_gdf,k=8)
    
    # Row-standardization
    wq.transform = 'r'
    
    # create a new column for the spatial lag
    test_gdf['Total_lag'] = lps.weights.lag_spatial(wq, test_gdf['Total'])
    # sample gives us 10 random rows
    test_gdf.sample(10)[['Link Name','link_length','Total','Total_lag']]
    
    test_gdf['total_lag_diff'] = test_gdf['Total'] - test_gdf['Total_lag']
    test_gdf.sort_values(by='total_lag_diff')
    
    # Donut
    ### A Block rate that has a low congestion metric surrounded by block groups with really high congestion rate
    test_gdf.sort_values(by='total_lag_diff').head(1)
    
    gdf_donut = test_gdf.sort_values(by='total_lag_diff').head(1)
    # subset donut, project to WGS84, and get its centroid
    # gdf_donut = gdf_donut.to_crs('epsg:4326')
    
    # what's the centroid?
    minx, miny, maxx, maxy = gdf_donut.geometry.total_bounds
    center_lat_donut = (maxy-miny)/2+miny
    center_lon_donut = (maxx-minx)/2+minx
    
    # Diamond
    ### A single census block group with a high congestion that is surrounded by links that have low or significantly lower total congested. 
    gdf_diamond = test_gdf.sort_values(by='total_lag_diff').tail(1)
    # gdf_diamond = gdf_diamond.to_crs('epsg:4326')
    # what's the centroid?
    minx, miny, maxx, maxy = gdf_diamond.geometry.total_bounds
    center_lat_diamond = (maxy-miny)/2+miny
    center_lon_diamond = (maxx-minx)/2+minx
    
    px.choropleth_mapbox(gdf_donut, 
                     geojson=gdf_donut.geometry, 
                     locations=gdf_donut.index, 
                     mapbox_style="open-street-map",
                     zoom=14, 
                     center = {"lat": center_lat_donut, "lon": center_lon_donut},
                     hover_data=['Link Name','Total','Total_lag'],
                     opacity=0.4,
                     title='The Donut')
    
    px.choropleth_mapbox(gdf_diamond, 
                     geojson=gdf_diamond.geometry, 
                     locations=gdf_diamond.index, 
                     mapbox_style="open-street-map",
                     zoom=12, 
                     center = {"lat": center_lat_diamond, "lon": center_lon_diamond},
                     hover_data=['Link Name','Total','Total_lag'],
                     opacity=0.4,
                     title='The Diamond')
    
    gdf_web = test_gdf.copy(deep=True)

    # what's the centroid?
    minx, miny, maxx, maxy = gdf_web.geometry.total_bounds
    center_lat_gdf_web = (maxy-miny)/2+miny
    center_lon_gdf_web = (maxx-minx)/2+minx
    
    # some stats
    gdf_web.Total_lag.describe()
    # some stats
    gdf_web.Total_lag.describe()
    median = gdf_web.Total_lag.median()
    
    fig = px.choropleth_mapbox(gdf_web, 
                         geojson=gdf_web.geometry, # the geometry column
                         locations=gdf_web.index, # the index
                         mapbox_style="carto-darkmatter",
                         zoom=9,
                         color=gdf_web.Total_lag,
                         color_continuous_scale='viridis',
                         color_continuous_midpoint =median, # put the median as the midpoint
                         range_color =(0,median*2),
                         featureidkey="properties['Link Name']",
                        center = {"lat": center_lat_gdf_web, "lon": center_lon_gdf_web})
    fig.update_traces(marker_line_width=0.1, marker_line_color='white')
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    
    # use subplots that make it easier to create multiple layered maps
    fig, ax = plt.subplots(1,1,figsize=(15, 15))
    
    # spatial lag choropleth
    test_gdf.plot(ax=ax,
             figsize=(15,15),
             column='Total_lag',
             legend=True,
             alpha=0.8,
             cmap='RdYlGn_r',
             scheme='quantiles')
    
    ax.axis('off')
    ax.set_title(f'9 December 2022 Transit Congestion Total Lag {start.time()} to {end.time()}',fontsize=22)
    # plt.savefig(dir_path_time + f"\\{date_name}_TransitCong_lag_{start_time_name}_to_{end_time_name}")
    
    # create the 1x2 subplots
    fig, ax = plt.subplots(1, 2, figsize=(15, 8))
    
    # two subplots produces ax[0] (left) and ax[1] (right)
    
    # regular count map on the left
    test_gdf.plot(ax=ax[0], # this assigns the map to the left subplot
             column='Total', 
             cmap='RdYlGn_r', 
             scheme='quantiles',
             k=5,  
             linewidth=1, 
             alpha=0.75, 
               )
    
    
    ax[0].axis("off")
    ax[0].set_title("Total")
    
    # spatial lag map on the right
    test_gdf.plot(ax=ax[1], # this assigns the map to the right subplot
             column='Total_lag', 
             cmap='RdYlGn_r', 
             scheme='quantiles',
             k=1, 
             linewidth=1, 
             alpha=0.75)
    
    ax[1].axis("off")
    ax[1].set_title("Total - Spatial Lag")
    
    plt.show()
    
    # Global Spatial autocorrelation
    #### the overall geographical pattern present in the data.
    #### Statistics designed to measure this trend thus characterize a map in terms of its degree of clustering and summarize it.
    ## Moran Plot
    #### The Moran Plot is a way of visualizing a spatial dataset to explore the nature and strength of spatial autocorrelation. It is essentially a traditional scatter plot in which the variables of interest is displayed against its spatial lag. In order to be able to interpret values as above or below the mean, and their quantities in terms of standard deviations, the variable of interest is usually standardized by substracting its mean and dividing it by its standard deviation.
    y = test_gdf.Total
    moran = Moran(y, wq)
    ## Global Moran's value
    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-norm = ", moran.z_norm)
    print("p-norm = ", moran.p_norm)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)

    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-random = ", moran.z_rand)
    print("p-random = ", moran.p_rand)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)


    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-sim = ", moran.z_sim)
    print("p-sim = ", moran.p_sim)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)
    W_plot = lps.weights.Queen.from_dataframe(test_gdf)
    y = test_gdf.Total
    moran = Moran(y, W_plot)
    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-norm = ", moran.z_norm)
    print("p-norm = ", moran.p_norm)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)

    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-random = ", moran.z_rand)
    print("p-random = ", moran.p_rand)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)


    print("weights = ", moran.w)
    print("moran I = ", moran.I)
    print("z-sim = ", moran.z_sim)
    print("p-sim = ", moran.p_sim)
    print("expected value = ", moran.EI)
    print("number of permutations = ", moran.permutations)
    # The plot displays a positive relationship between both variables. This is associated with 
    # * the presence of *positive* spatial autocorrelation: similar values tend to be located close to each other. 
    # *This means that the *overall trend* is for high values to be close to other high values, and for low values to be surrounded by other low values. 
    # This however does not mean that this is only situation in the dataset: there can of course be particular cases where high values are surrounded by low ones, and viceversa.
    
    # In the context of the example, this can be interpreted along the lines of: local intersection links display positive spatial autocorrelation on the congestion level. This means that local intersection links with high percentage of congested tend to be located nearby other local links where a significant share of the congested links appearing, and viceversa.
    fig, ax = moran_scatterplot(moran, aspect_equal=True)
    ax.set_title(f"{date_name}_moran_scatterplot_{start_time_name}_to_{end_time_name}")
    # plt.savefig(dir_path_time + f"\\{date_name}_moran_scatterplot_{start_time_name}_to_{end_time_name}")
    plt.show()
    
    plot_moran_simulation(moran,aspect_equal=False)
    ### P-value
    print(f"p_sim = {moran.p_sim}")
    
    # The value is calculated as an empirical P-value that represents the proportion of realisations in the simulation under spatial randomness that are more extreme than the observed value. The p-value of 0.001 (0.1%) associated with the Moran's I of a map allows to reject the hypothesis that the map is random and concluded that the map displays more spatial pattern than we would expect if the values had been randomly allocated to a locations. 
    # Setup the figure and axis
    f, ax = plt.subplots(1, figsize=(9, 9))
    # Plot values
    sns.regplot(x='Total', y='Total_lag', data=test_gdf, ci=None)
    # Add vertical and horizontal lines
    plt.axvline(0, c='k', alpha=0.5)
    plt.axhline(0, c='k', alpha=0.5)
    # Display
    plt.show()
    
    # Local Spatial Autocorrelation
    ### Local Indicators of Spatial Association (LISA) is used to classifies areas into four groups: high values near to high values (HH), Low values with nearby Low Values (LL), Low values with high values in its neighbours (LH) and High Values with low values in its neighbours (HL)
    # * HH: High congested link near other high congested link neighbours
    # * LL: Low congested Link near other low congested link neighbours
    # * HL: High congested link near low congested link neighbours
    # * LH: Low congested link near high congested link neighbours
    
    # calculate local moran values
    lisa = esda.moran.Moran_Local(y, wq)
    
    # Plot
    fig,ax = plt.subplots(figsize=(10,15))
    
    moran_scatterplot(lisa, ax=ax, p=0.05)
    ax.set_xlabel("Total")
    ax.set_ylabel('Spatial Lag of Total')
    
    # add some labels
    plt.text(1.95, 0.5, "HH", fontsize=25)
    plt.text(1.95, -1, "HL", fontsize=25)
    plt.text(-2, 1, "LH", fontsize=25)
    plt.text(-1, -1, "LL", fontsize=25)
    plt.show()
    
    fig, ax = plt.subplots(figsize=(14,12))
    lisa_cluster(lisa, test_gdf,ax=ax)
    # plt.savefig(dir_path_time + f"\\moran_local_scatterplot_{date_name}_{start_time_name}_{end_time_name}")
    plt.show()
    
    # create the 1x2 subplots
    fig, ax = plt.subplots(1, 2, figsize=(15, 8))
    
    # regular count map on the left
    lisa_cluster(lisa, test_gdf, p=0.05, ax=ax[0])
    
    ax[0].axis("off")
    ax[0].set_title("P-value: 0.05")
    
    # spatial lag map on the right
    lisa_cluster(lisa, test_gdf, p=0.01, ax=ax[1])
    ax[1].axis("off")
    ax[1].set_title("P-value: 0.01")
    
    # plt.savefig(dir_path_time + f"\\lisa_cluster_{date_name}_significance_{start_time_name}_{end_time_name}")
    plt.show()
    
    sig = lisa.p_sim < 0.05
    hotspot = sig * lisa.q==1
    coldspot = sig * lisa.q==3
    doughnut = sig * lisa.q==2
    diamond = sig * lisa.q==4
    
    spots = ['n.sig.', 'hot spot']
    labels = [spots[i] for i in hotspot*1]
    
    from matplotlib import colors
    hmap = colors.ListedColormap(['red', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'cold spot']
    labels = [spots[i] for i in coldspot*1]
    hmap = colors.ListedColormap(['blue', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'doughnut']
    labels = [spots[i] for i in doughnut*1]
    
    hmap = colors.ListedColormap(['yellow', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'diamond']
    labels = [spots[i] for i in diamond*1]
    
    hmap = colors.ListedColormap(['orange', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    sig = 1 * (lisa.p_sim < 0.05)
    hotspot = 1 * (sig * lisa.q==1)
    coldspot = 3 * (sig * lisa.q==3)
    doughnut = 2 * (sig * lisa.q==2)
    diamond = 4 * (sig * lisa.q==4)
    spots = hotspot + coldspot + doughnut + diamond
    
    spot_labels = [ '0 ns', '1 hot spot', '2 doughnut', '3 cold spot', '4 diamond']
    labels = [spot_labels[i] for i in spots]
    
    hmap = colors.ListedColormap([ 'lightgrey', 'red', 'yellow', 'blue', 'orange'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    ax.set_title(f"{date_name}_spot_cluster_labels_{start_time_name}_to_{end_time_name}")
    # plt.savefig(dir_path_time + f"\\{date_name}_spot_cluster_labels_{start_time_name}_to_{end_time_name}")
    plt.show()
    
    f, ax = plt.subplots(1,2, figsize=(15, 9))
    
    test_gdf.plot(ax=ax[0],
             figsize=(15,15),
             column='Total_lag',
             legend=True,
             alpha=0.8,
             cmap='RdYlGn_r',
             scheme='quantiles')
    ax[0].axis("off")
    ax[0].set_title(f"{date_name} Transit Congestion Performance Level between {start_time_name} to {end_time_name}")
        
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax[1], legend=True)
    ax[1].set_axis_off()
    ax[1].set_title(f"{date_name}_spot_cluster_labels_{start_time_name}_to_{end_time_name}")
    # plt.savefig(dir_path_time + f"\\{date_name}_Actual_TransitCog_vs_Hotspots_{start_time_name}_to_{end_time_name}")
    plt.show()
    
    new_gdf = test_gdf.assign(cl=labels)
    return new_gdf

def spatial_autocorrelation(df_merged_new, static_gdf, start, end):
    # For spatial statistics
    import esda
    from esda.moran import Moran, Moran_Local
    
    import splot
    from splot.esda import moran_scatterplot, plot_moran, lisa_cluster,plot_moran_simulation
    import libpysal as lps
    import contextily as ctx
    
    # Graphics
    import matplotlib.pyplot as plt
    import plotly.express as px
    from shapely import wkt

    dir_path = f"Y:\\Sentosa\\24Feb23\\{start.date()}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    dir_path_time = dir_path + f"\\{start.time()}"
    if not os.path.exists(dir_path_time):
        os.makedirs(dir_path_time)
    
    #import gtfs_functions
    if "Unnamed: " in df_merged_new:
        remove_unnamed(df_merged_new)
        df_merged_new['dt'] = pd.to_datetime(df_merged_new['dt'])
        df_merged_new['centre_pt'] = df_merged_new['centre_pt'].apply(wkt.loads)
        df_merged_new['geometry'] = df_merged_new['geometry'].apply(wkt.loads)
    df_cluster = df_merged_new.merge(static_gdf[['Name', 'length', 'direction_id', 'geometry']], left_on=['Link Name', 'link_length'], right_on=['Name', 'length'], how='inner')
    df_cluster = df_cluster[['Link Name', 'link_length', 'dt', 'Total', 'direction_id', 'geometry']]
    # start_time = datetime.time(7,0,0)
    # end_time = datetime.time(7,15,0)
    # start = datetime.datetime.combine(SELECTED_DATE, start_time)
    # end = datetime.datetime.combine(SELECTED_DATE, end_time)
    test = df_cluster.loc[(df_cluster['dt'] >= start) & (df_cluster['dt'] < end)]
    
    print(f"time selected = {start.time()} to {end.time()}")
    
    test_gdf = gpd.GeoDataFrame(test, crs="EPSG:4376", geometry=test['geometry'])
    test_gdf.sort_values(by='Total').tail()
    fig,ax = plt.subplots(figsize=(12,10))
    test_gdf.sort_values(by='Total',ascending=False)[:20].plot(ax=ax,
                                                                     color='red',
                                                                     edgecolor='white',
                                                                     alpha=0.5,legend=True)
    
    
    # title
    ax.set_title(f'Top 20 Congested Links (9 December 2022) {start.time()} to {end.time()}')
    
    # no axis
    ax.axis('off')
    # add a basemap
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    # plot it!
    fig, ax = plt.subplots(figsize=(12,12))
    
    test_gdf.plot(ax=ax,
             color='black', 
             edgecolor='white',
             lw=0.5,
             alpha=0.4)
    
    # no axis
    ax.axis('off')
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    fig,ax = plt.subplots(figsize=(15,15))
    
    test_gdf.plot(ax=ax,
            column='Total', # this makes it a choropleth
            legend=True,
            alpha=0.8,
            cmap='RdYlGn_r', # a diverging color scheme
            scheme='quantiles') # how to break the data into bins
    
    ax.axis('off')
    ax.set_title(f'9 December 2022 Transit Congested between {start.time()} to {end.time()}',fontsize=22)
    ctx.add_basemap(ax,source=ctx.providers.CartoDB.Positron)
    
    # calculate spatial weight
    wq =  lps.weights.KNN.from_dataframe(test_gdf,k=8)
    
    # Row-standardization
    wq.transform = 'r'
    
    # create a new column for the spatial lag
    test_gdf['Total_lag'] = lps.weights.lag_spatial(wq, test_gdf['Total'])
    # sample gives us 10 random rows
    test_gdf.sample(10)[['Link Name','link_length','Total','Total_lag']]
    
    test_gdf['total_lag_diff'] = test_gdf['Total'] - test_gdf['Total_lag']
    test_gdf.sort_values(by='total_lag_diff')
    
    # Donut
    ### A Block rate that has a low congestion metric surrounded by block groups with really high congestion rate
    test_gdf.sort_values(by='total_lag_diff').head(1)
    
    gdf_donut = test_gdf.sort_values(by='total_lag_diff').head(1)
    # subset donut, project to WGS84, and get its centroid
    # gdf_donut = gdf_donut.to_crs('epsg:4326')
    
    # what's the centroid?
    minx, miny, maxx, maxy = gdf_donut.geometry.total_bounds
    center_lat_donut = (maxy-miny)/2+miny
    center_lon_donut = (maxx-minx)/2+minx
    
    # Diamond
    ### A single census block group with a high congestion that is surrounded by links that have low or significantly lower total congested. 
    gdf_diamond = test_gdf.sort_values(by='total_lag_diff').tail(1)
    # gdf_diamond = gdf_diamond.to_crs('epsg:4326')
    # what's the centroid?
    minx, miny, maxx, maxy = gdf_diamond.geometry.total_bounds
    center_lat_diamond = (maxy-miny)/2+miny
    center_lon_diamond = (maxx-minx)/2+minx
    
    px.choropleth_mapbox(gdf_donut, 
                     geojson=gdf_donut.geometry, 
                     locations=gdf_donut.index, 
                     mapbox_style="open-street-map",
                     zoom=14, 
                     center = {"lat": center_lat_donut, "lon": center_lon_donut},
                     hover_data=['Link Name','Total','Total_lag'],
                     opacity=0.4,
                     title='The Donut')
    
    px.choropleth_mapbox(gdf_diamond, 
                     geojson=gdf_diamond.geometry, 
                     locations=gdf_diamond.index, 
                     mapbox_style="open-street-map",
                     zoom=12, 
                     center = {"lat": center_lat_diamond, "lon": center_lon_diamond},
                     hover_data=['Link Name','Total','Total_lag'],
                     opacity=0.4,
                     title='The Diamond')
    
    gdf_web = test_gdf.copy(deep=True)

    # what's the centroid?
    minx, miny, maxx, maxy = gdf_web.geometry.total_bounds
    center_lat_gdf_web = (maxy-miny)/2+miny
    center_lon_gdf_web = (maxx-minx)/2+minx
    
    # some stats
    gdf_web.Total_lag.describe()
    # some stats
    gdf_web.Total_lag.describe()
    median = gdf_web.Total_lag.median()
    
    fig = px.choropleth_mapbox(gdf_web, 
                         geojson=gdf_web.geometry, # the geometry column
                         locations=gdf_web.index, # the index
                         mapbox_style="carto-darkmatter",
                         zoom=9,
                         color=gdf_web.Total_lag,
                         color_continuous_scale='viridis',
                         color_continuous_midpoint =median, # put the median as the midpoint
                         range_color =(0,median*2),
                         featureidkey="properties['Link Name']",
                        center = {"lat": center_lat_gdf_web, "lon": center_lon_gdf_web})
    fig.update_traces(marker_line_width=0.1, marker_line_color='white')
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    
    # use subplots that make it easier to create multiple layered maps
    fig, ax = plt.subplots(1,1,figsize=(15, 15))
    
    # spatial lag choropleth
    test_gdf.plot(ax=ax,
             figsize=(15,15),
             column='Total_lag',
             legend=True,
             alpha=0.8,
             cmap='RdYlGn_r',
             scheme='quantiles')
    
    ax.axis('off')
    ax.set_title(f'9 December 2022 Transit Congestion {start.time()} to {end.time()}',fontsize=22)
    # create the 1x2 subplots
    fig, ax = plt.subplots(1, 2, figsize=(15, 8))
    
    # two subplots produces ax[0] (left) and ax[1] (right)
    
    # regular count map on the left
    test_gdf.plot(ax=ax[0], # this assigns the map to the left subplot
             column='Total', 
             cmap='RdYlGn_r', 
             scheme='quantiles',
             k=5,  
             linewidth=1, 
             alpha=0.75, 
               )
    
    
    ax[0].axis("off")
    ax[0].set_title("Total")
    
    # spatial lag map on the right
    test_gdf.plot(ax=ax[1], # this assigns the map to the right subplot
             column='Total_lag', 
             cmap='RdYlGn_r', 
             scheme='quantiles',
             k=1, 
             linewidth=1, 
             alpha=0.75)
    
    ax[1].axis("off")
    ax[1].set_title("Total - Spatial Lag")
    
    plt.show()
    
    # Global Spatial autocorrelation
    #### the overall geographical pattern present in the data.
    #### Statistics designed to measure this trend thus characterize a map in terms of its degree of clustering and summarize it.
    ## Moran Plot
    #### The Moran Plot is a way of visualizing a spatial dataset to explore the nature and strength of spatial autocorrelation. It is essentially a traditional scatter plot in which the variables of interest is displayed against its spatial lag. In order to be able to interpret values as above or below the mean, and their quantities in terms of standard deviations, the variable of interest is usually standardized by substracting its mean and dividing it by its standard deviation.
    y = test_gdf.Total
    moran = Moran(y, wq)
    ## Global Moran's value
    print(moran.I)
    
    # The plot displays a positive relationship between both variables. This is associated with 
    # * the presence of *positive* spatial autocorrelation: similar values tend to be located close to each other. 
    # *This means that the *overall trend* is for high values to be close to other high values, and for low values to be surrounded by other low values. 
    # This however does not mean that this is only situation in the dataset: there can of course be particular cases where high values are surrounded by low ones, and viceversa.
    
    # In the context of the example, this can be interpreted along the lines of: local intersection links display positive spatial autocorrelation on the congestion level. This means that local intersection links with high percentage of congested tend to be located nearby other local links where a significant share of the congested links appearing, and viceversa.
    fig, ax = moran_scatterplot(moran, aspect_equal=True)
    plt.show()
    
    plot_moran_simulation(moran,aspect_equal=False)
    # plt.savefig(dir_path_time + f"\\moran_scatterplot_{start.time()}_to_{end.time()}")
    ### P-value
    moran.p_sim
    
    # The value is calculated as an empirical P-value that represents the proportion of realisations in the simulation under spatial randomness that are more extreme than the observed value. The p-value of 0.001 (0.1%) associated with the Moran's I of a map allows to reject the hypothesis that the map is random and concluded that the map displays more spatial pattern than we would expect if the values had been randomly allocated to a locations. 
    # Setup the figure and axis
    f, ax = plt.subplots(1, figsize=(9, 9))
    # Plot values
    sns.regplot(x='Total', y='Total_lag', data=test_gdf, ci=None)
    # Add vertical and horizontal lines
    plt.axvline(0, c='k', alpha=0.5)
    plt.axhline(0, c='k', alpha=0.5)
    # Display
    plt.show()
    
    # Local Spatial Autocorrelation
    ### Local Indicators of Spatial Association (LISA) is used to classifies areas into four groups: high values near to high values (HH), Low values with nearby Low Values (LL), Low values with high values in its neighbours (LH) and High Values with low values in its neighbours (HL)
    # * HH: High congested link near other high congested link neighbours
    # * LL: Low congested Link near other low congested link neighbours
    # * HL: High congested link near low congested link neighbours
    # * LH: Low congested link near high congested link neighbours
    
    # calculate local moran values
    lisa = esda.moran.Moran_Local(y, wq)
    
    # Plot
    fig,ax = plt.subplots(figsize=(10,15))
    
    moran_scatterplot(lisa, ax=ax, p=0.05)
    ax.set_xlabel("Total")
    ax.set_ylabel('Spatial Lag of Total')
    
    # add some labels
    plt.text(1.95, 0.5, "HH", fontsize=25)
    plt.text(1.95, -1, "HL", fontsize=25)
    plt.text(-2, 1, "LH", fontsize=25)
    plt.text(-1, -1, "LL", fontsize=25)
    plt.show()
    
    fig, ax = plt.subplots(figsize=(14,12))
    lisa_cluster(lisa, test_gdf,ax=ax)
    plt.show()
    
    # create the 1x2 subplots
    fig, ax = plt.subplots(1, 2, figsize=(15, 8))
    
    # regular count map on the left
    lisa_cluster(lisa, test_gdf, p=0.05, ax=ax[0])
    
    ax[0].axis("off")
    ax[0].set_title("P-value: 0.05")
    
    # spatial lag map on the right
    lisa_cluster(lisa, test_gdf, p=0.01, ax=ax[1])
    ax[1].axis("off")
    ax[1].set_title("P-value: 0.01")
    
    plt.show()
    
    sig = lisa.p_sim < 0.05
    hotspot = sig * lisa.q==1
    coldspot = sig * lisa.q==3
    doughnut = sig * lisa.q==2
    diamond = sig * lisa.q==4
    
    spots = ['n.sig.', 'hot spot']
    labels = [spots[i] for i in hotspot*1]
    
    from matplotlib import colors
    hmap = colors.ListedColormap(['red', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'cold spot']
    labels = [spots[i] for i in coldspot*1]
    hmap = colors.ListedColormap(['blue', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'doughnut']
    labels = [spots[i] for i in doughnut*1]
    
    hmap = colors.ListedColormap(['yellow', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    spots = ['n.sig.', 'diamond']
    labels = [spots[i] for i in diamond*1]
    
    hmap = colors.ListedColormap(['orange', 'lightgrey'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    
    sig = 1 * (lisa.p_sim < 0.05)
    hotspot = 1 * (sig * lisa.q==1)
    coldspot = 3 * (sig * lisa.q==3)
    doughnut = 2 * (sig * lisa.q==2)
    diamond = 4 * (sig * lisa.q==4)
    spots = hotspot + coldspot + doughnut + diamond
    
    spot_labels = [ '0 ns', '1 hot spot', '2 doughnut', '3 cold spot', '4 diamond']
    labels = [spot_labels[i] for i in spots]
    
    hmap = colors.ListedColormap([ 'lightgrey', 'red', 'yellow', 'blue', 'orange'])
    f, ax = plt.subplots(1, figsize=(9, 9))
    test_gdf.assign(cl=labels).plot(column='cl', categorical=True, \
            k=2, cmap=hmap, linewidth=1, ax=ax, legend=True)
    ax.set_axis_off()
    plt.show()
    # plt.savefig(dir_path_time + f"\\spot_cluster_labels_{start.time()}_to_{end.time()}")
# best_dict = dbscan_clustering()
    
##def apply_mf(new_df):
##    # R = np.array([
##    #     [5, 3, 0, 1],
##    #     [4, 0, 0, 1],
##    #     [1, 1, 0, 5],
##    #     [1, 0, 0, 4],
##    #     [0, 1, 5, 4],
##    # ])  
##    scaler = MinMaxScaler(feature_range=(-1,1))
##    new_df_0 = new_df.fillna(0)
##    scaled_new_df = scaler.fit_transform(new_df_0)
##    mf = MF(scaled_new_df, K=2, alpha=0.1, beta=0.01, iterations=100)
##    training_process = mf.train()
##    print()
##    print("T x D:")
##    print(mf.full_matrix())
##    print()
##    print("Global bias:")
##    print(mf.b)
##    print()
##    print("Trips bias:")
##    print(mf.b_u)
##    print()
##    print("Cumulative Distance bias:")
##    print(mf.b_i)
##  
##    x = [x for x, y in training_process]
##    y = [y for x, y in training_process]
##    plt.figure(figsize=((16,4)))
##    plt.plot(x, y)
##    plt.xticks(x, x)
##    plt.xlabel("Iterations")
##    plt.ylabel("Mean Square Error")
##    plt.grid(axis="y")
##    
##    new_scaled_new_df = mf.full_matrix()
##    new_new_df = scaler.inverse_transform(new_scaled_new_df)
##    new_new_df_df = pd.DataFrame(new_new_df, columns=new_df.columns, index=new_df.index)
##    xticks_spacing = int(pd.Timedelta('1h')/pd.Timedelta('15T'))
##    ax = sns.heatmap(new_new_df_df, xticklabels=xticks_spacing, yticklabels=5)
##    ax.invert_yaxis()
##    plt.yticks(rotation=0)
##    plt.title("Hour (HH:MM) vs. cum_space (km) for 333 Outbound")

