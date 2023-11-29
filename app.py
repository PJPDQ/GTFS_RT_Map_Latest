import os
import sys
import uuid
from typing import Dict, Generator
from queue import Queue
from threading import Thread
import pandas as pd
from flask import Flask, Response, request, render_template
from consumer_broker import MessageBroker
from datetime import datetime
import json
from shapefetcher import traj_shp, stop_shp, hfs_stop_shp, static_mapper, high_frequency_buses

app = Flask(__name__)
broker = MessageBroker()
conns: Dict[uuid.UUID, Queue] = {}
hfs: Dict[str, Queue] = {}

def bus_checker(hfs_data):
    hfs_storage = hfs_data.copy(deep=True)
    hfs_storage = hfs_storage.drop_duplicates(subset=['trip_id', 'latitude', 'longitude', 'timestamp'], keep='last')
    hfs_storage = hfs_storage.sort_values(by=['timestamp', 'bus_num'])
    n_hfs = hfs_storage.groupby('trip_id').filter(lambda x: len(x) == 3)
    hfs_storage = hfs_storage.groupby('trip_id').filter(lambda x: len(x) != 3)
    if (len(n_hfs) > 1):
        new_n_hfs = n_hfs.reset_index().groupby('trip_id').last().reset_index()
        new_n_hfs = new_n_hfs.set_index(keys='index')
        hfs_storage = pd.concat([hfs_storage, new_n_hfs])
        return hfs_storage
    else:
        return hfs_storage

def consume() -> None:
    for eid, content in broker.subscribe():
        for q in conns.values():
            q.put((eid, content))

def event_stream(user_id: uuid.UUID) -> Generator[str, None, None]:
    q = Queue()
    conns[user_id] = q
    try:
        while True:
            eid, content = q.get()
            data = pd.read_json(content)
            print(f"shape of data = {data.shape}")
            if data.shape[1] > 9:
                new_data = data.groupby('trip_id').first().reset_index()
                json_data = new_data.to_json()
                json_data.encode('ascii')
                yield 'data:{0}\n\n'.format(json_data)
            else:
                data = data.sort_values(by=["timestamp"])
                data['timestamp_dt'] = data['timestamp'].apply(lambda x: x.tz_localize(tz='utc').tz_convert('Australia/Brisbane'))
                hfs_data = data.copy(deep=True)
                hfs_data['bus_num'] = hfs_data['route_id'].apply(lambda x: x.split("-")[0])
                hfs = hfs_data[hfs_data['bus_num'].isin(high_frequency_buses)]
                non_hfs = hfs_data[~(hfs_data['bus_num'].isin(high_frequency_buses))]
                new_hfs = bus_checker(hfs)
                data = pd.concat([hfs, non_hfs])
                data = data[['stop_id', 'timestamp', 'timestamp_dt','trip_id', 'latitude', 'longitude', 'route_id', 'id']]
                for i in range(len(data)):
                    msg = data.iloc[i].to_json()
                    msg.encode('ascii')
                    yield 'data:{0}\n\n'.format(msg)
    finally:
        conns.pop(user_id)

def filter_trajectory(selected_routes, new_traj, hfs_stop_shp):
    """
    This function would filter the shapefile trajectory based on user prompt
    """
    filtered_routes = new_traj.Associate.str.split(" | ").map(lambda x: bool(set(selected_routes) & set(x)))
    new_traj = new_traj[filtered_routes]
    traj_msg = new_traj.to_json()
    new_stop = hfs_stop_shp[hfs_stop_shp['Route id'].isin(selected_routes)]
    stop_msg = new_stop.to_json()
    return traj_msg, stop_msg

@app.route("/gtfs_data")
def events() -> Response:
    user_id = uuid.uuid4()
    return Response(event_stream(user_id), mimetype="text/event-stream")

@app.route("/", methods=["GET", "POST"])
def viewer() -> str:
    new_traj = traj_shp.set_crs("EPSG:4326")
    if request.method == 'POST':
        user_in_json = request.get_json()
        if 'markers' in user_in_json:
            data = pd.DataFrame.from_records(user_in_json['markers'])
            response = static_mapper(data)
            return response
        else:
            selected_routes = request.get_json()['routes']
            traj_msg, stop_msg = filter_trajectory(selected_routes, new_traj, hfs_stop_shp)
            response = {
                'traj_shp': traj_msg,
                'stop_shp': stop_msg
            }
            return response
    traj_msg = new_traj.to_json()
    stop_msg = hfs_stop_shp.to_json()
    response = {
        "traj_shp": traj_msg,
        "stop_shp": stop_msg
    }
    return render_template("map.html", res_data=response)

if __name__ == "__main__":
    try:
        print("Starting consumer...")
        consumer_thread = Thread(target=consume, name="RabbitMQ Consumer")
        consumer_thread.daemon = True
        consumer_thread.start()
        print("Starting server...")
        # app.run(port=5003, debug=True, use_reloader=False)
        app.run(debug=False, port=3005, host='0.0.0.0', use_reloader=False)
    except KeyboardInterrupt:
        consumer_thread.join()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)