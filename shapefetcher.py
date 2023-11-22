import geopandas as gpd
import pandas as pd
import datetime
import time
from shapely.ops import nearest_points, linemerge
from shapely.geometry import Point
import os
from ast import literal_eval
def Brisbane(epoch):
    a = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(epoch))
    return(a)

def monthname(mydate):
    ##Generate the today's month name instead of the selected date
    m = mydate.strftime("%B")
    return(m)

def remove_unnamed(df):
    list_unnameds = [s for s in df.columns if 'Unnamed:' in s]
    if len(list_unnameds) > 0:
        df.drop(list_unnameds, axis=1, inplace=False)
    return df

def convert_to_float(x):
    return float(x[0]), float(x[1])

def coords_gen(x, actual_line):
    duration = int(x['travel_time_sec']) #sec
    speed = x['speed_sms'] #back to metre
    end_dist = x['distance_from_line']
    start_dist = x['distance_travelled_from']
    res = [x['nearest_pt']]
    res_time = [0]
    for i in range(1, duration):
        res_time.append(i)
        temp_dist = start_dist - ((speed * (i/3600))/100) if start_dist > end_dist else start_dist + ((speed * (i/3600))/100) #hour
        res.append(actual_line.interpolate(temp_dist))
    return res, res_time

def point_geojson_generator(data, line):
    """
    This function would generate a geojson that helps to generate point shapes
    """
    data['distance_travelled_from'] = data['distance_from_line'].shift(1)
    data = data.fillna(0)
    data['coords'], data['diff_time'] = zip(*data[['nearest_pt', 'speed_sms', 'distance_from_line', 'distance_travelled_from', 'travel_time_sec']].apply(coords_gen, actual_line = line, axis=1))
    new_data = data.explode(['coords', 'diff_time'])
    new_data['actual_timestamp'] = new_data[['datetime', 'diff_time']].apply(lambda x: x.datetime + datetime.timedelta(seconds=x.diff_time), axis=1)
    
    new_data['actual_timestamp'] = new_data['actual_timestamp'].astype(str)
    new_data = new_data[['stop_id', 'actual_timestamp', 'trip_id', 'route_id', 'id', 'latitude', 'longitude', 'speed_sms', 'distance_from_line', 'distance_travelled_km', 'travel_time_sec', 'diff_time', 'coords']]
    new_data_geo = gpd.GeoDataFrame(new_data, crs="EPSG:4376", geometry=new_data['coords'])
    res = new_data_geo[['stop_id', 'actual_timestamp', 'trip_id', 'route_id', 'id', 'latitude', 'longitude', 'speed_sms', 'distance_travelled_km', 'travel_time_sec', 'diff_time', 'geometry']]
    res_msg = res.to_json()
    return res_msg

def static_mapper(data):
    """
    A function which map the datapoints into the route linestring to interpolate the vehicle trajectory from current to two prior epochs
    params:
        data: selected vehicles to display (a list of markers containing two prior, one prior and current)
        route_shape: the linestring shapefile of the data
    return:
        result: a geojson containing a linestring of an linearly interpolated 1-sec interval from current to two prior
    """
    data['stop_id'] = data['stop_id'].astype(int)
    data = data.sort_values(by=["timestamp"])
    data['datetime'] = data['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(round(x/1000)))
    cols = data.columns
    data['travel_time'] = data['datetime'].diff()
    data['travel_time_sec'] = data['travel_time'].apply(lambda x: x.total_seconds())
    cols = [*cols, 'travel_time_sec']
    data = data[cols]
    data['travel_time_hour'] = (data['travel_time_sec'] / 3600) ## sec to hour
    data_route_static = routes_table[(routes_table['trip_id'] == data['trip_id'].unique()[0]) & (routes_table['stop_id'].isin(data['stop_id'].unique()))] ## TEST2
    data_shape = shapes_table[(shapes_table['shape_id'] == data_route_static['shape_id'].unique()[0]) & (shapes_table['stop_seq_1'].isin(data_route_static['stop_sequence'].tolist()))] ##SHAPES4 TEST3
    res = {'type': 'FeatureCollection', 'crs': {"type": "name", "properties": {"name": "EPSG:4326"}}, 'features': []}
    if len(data_shape) < 3 and len(data_shape) > 0:
        lists_list = data_shape.groupby(['shape_id'])['names_list'].apply(" | ".join).str.rsplit(" | ").to_list()
        link_list = lists_list[0]
        selected_shape = traj_shp[(traj_shp['Name'].isin(link_list)) & (traj_shp['direction_'] == data_route_static.direction_id.unique()[0])] ##GDF
        line_selected_shape = linemerge((line for line in selected_shape.geometry))
        data['nearest_pt'] = data[['latitude', 'longitude']].apply(lambda x: nearest_points(line_selected_shape, Point((x['longitude'], x['latitude'])))[0], axis=1)
        data['distance_from_line'] = data['nearest_pt'].apply(lambda x: line_selected_shape.project(x)) #km
        data['actual_distance_km'] = data['distance_from_line'].apply(lambda x: x*100) ## to km
        data['distance_travelled_km'] = abs(data['actual_distance_km'].diff())
        data['speed_sms'] = data['distance_travelled_km'] / (data['travel_time_hour']) ## km/h
        data = data.fillna(0)
        data2 = data[['stop_id', 'datetime', 'trip_id', 'route_id', 'id', 'latitude', 'longitude', 'nearest_pt', 'distance_travelled_km', 'distance_from_line', 'speed_sms', 'travel_time_sec']]
        pt_gjson = point_geojson_generator(data2, line_selected_shape)
        res['features'] = pt_gjson
    else:
        new_data = data.copy(deep=True)
        new_data.rename(columns={"timestamp": "actual_timestamp"}, inplace=True)
        new_data['point'] = new_data.apply(lambda x: (x['longitude'], x['latitude']), axis=1)
        new_data_geo = gpd.GeoDataFrame(new_data, crs="EPSG:4376", geometry=[Point(x) for x in new_data['point']])
        new_data_geo = new_data_geo[['stop_id', 'actual_timestamp', 'trip_id', 'route_id', 'id', 'latitude', 'longitude', 'geometry']]
        res['features'] = new_data_geo.to_json()
    return res

###################DATA SHAPEFILES AND SHAPES4 AND HFS ROUTE DETAILS#######################################
##REMOTE OPTIONS
WORKING_DIR = "Y:\\Sentosa\\"
local_path = ".\\data"
if not os.path.exists(WORKING_DIR):
    WORKING_DIR = local_path ## LOCAL OPTIONS
daily_static_dir = f"{WORKING_DIR}\\1Dec22\\Static_Preprocessing\\"
shapefile_dir = f"{WORKING_DIR}\\FINAL_STATIC_GDF_26-8-2021\\"

t = int(time.time())
datee = datetime.datetime.strptime(str(Brisbane(t)),"%d-%m-%Y %H:%M:%S" )
m = datee.month
y = datee.year
d = datee.day
m_name = f"{monthname(datee)} ,{datee.year}"
date_name = str(d)+"-"+str(m)+"-"+str(y)
############################################################################################################

traj_shp = gpd.read_file(shapefile_dir + "FINAL_STATIC_GDF_26-8-2021.shp")
traj_shp = traj_shp.set_crs("EPSG:4326")
stop_dir = f"{WORKING_DIR}\\stop_gdf"
stop_shp = gpd.read_file(stop_dir+"\\stop_gdf.shp")
stop_shp = stop_shp.set_crs("EPSG:4326")
hfs_stop_shp = gpd.read_file(shapefile_dir + "Stops.shp")
hfs_stop_shp = hfs_stop_shp.set_crs("EPSG:4326")
hfs_stop_shp['Route id'] = hfs_stop_shp['Route id'].astype(str)

fetched_shapes_table = pd.read_csv(f"{daily_static_dir}shapes4\\{m_name}\\shapes4_combine_{date_name}.csv")
routes_stop_table = pd.read_csv(f"{daily_static_dir}HFS\\{m_name}\\HFS_Route_Shape_stop_{date_name}.csv")

shapes_table = remove_unnamed(fetched_shapes_table)
shapes_table['routes_list'] = shapes_table['routes_list'].apply(lambda x: tuple(map(convert_to_float, literal_eval(x))))
routes_table = remove_unnamed(routes_stop_table)
routes_table['shape_id'] = routes_table['shape_id'].astype(str)
high_frequency_buses = ['60', '61', '100', '111' ,'120', '130' ,'140', '150', '180', '196' , '199' ,'200' ,'222', '330', '333' ,'340', '345', '385', '412', '444', '555']
