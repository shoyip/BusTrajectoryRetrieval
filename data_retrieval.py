#!/usr/bin/env python
# coding: utf-8

# importing libraries

# these libraries are required to read protobuf binaries
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format as pbj

# standard libraries
import os
from dotenv import load_dotenv, find_dotenv
import requests
import time
from datetime import datetime
from io import StringIO

# pandas for data munging and psycopg2 for db
import pandas as pd
import psycopg2

# definitions of functions

def protobuf_to_dict_df(buffer, dataset):
    """
    This function takes a protobuf buffer and the dataset name as an input
    and a dictionary or a DataFrame, depending on the dataset, as output.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(buffer)
    d = pbj.MessageToDict(feed)
    if dataset == 'vehicle_positions':
        df = pd.json_normalize(d['entity']).set_index('id')
        return df
    else:
        return d

def write_df_to_buffer(df):
    """
    This function takes the vehicle positions DataFrame as input and writes
    a StringIO buffer containing the csv dataset.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='NaN', sep='\t')
    buffer.seek(0)
    return buffer

def write_d_to_buffer(d, req_time):
    """
    This function takes the trip updates dictionary as input and writes
    a StringIO buffer containing the csv dataset.
    """
    buffer = StringIO()
    lines = []
    for trip in d['entity']:
        trip_id = trip['tripUpdate']['trip']['tripId']
        for update in trip['tripUpdate']['stopTimeUpdate']:
            seq = update['stopSequence']
            if 'arrival' in update.keys():
                arrival = update['arrival']
            elif 'departure' in update.keys():
                arrival = update['departure']
            else:
                continue
            try:
                delay, timestamp, uncertainty = arrival.values()
                time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            except:
                if 'delay' not in arrival.keys():
                    delay = 'NaN'
                if 'uncertainty' not in arrival.keys():
                    uncertainty = 'NaN'
                time = arrival['time']
            row = (trip_id, seq, delay, time, uncertainty, req_time)
            lines.append('\t'.join(map(str, row)) + '\n')
    buffer.writelines(lines)
    buffer.seek(0)
    return buffer

# configuration variables
load_dotenv(find_dotenv())
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
vehicle_positions_table = os.environ.get("VEHICLE_POSITIONS_TABLE")
trip_updates_table = os.environ.get("TRIP_UPDATES_TABLE")

# urls of data sources
url_vehicle_positions = 'https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/d2b123d6-8d2d-4dee-9792-f535df3dc166/download/rome_vehicle_positions.pb'
url_trip_updates = 'https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/bf7577b5-ed26-4f50-a590-38b8ed4d2827/download/rome_trip_updates.pb'

# setting current time of request, downloading data
req_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
req_vehicle_positions = requests.get(url=url_vehicle_positions)
req_trip_updates = requests.get(url=url_trip_updates)

# transforming protobuf to dataframe or dict
df_vehicle_positions = protobuf_to_dict_df(req_vehicle_positions.content, dataset='vehicle_positions')
d_trip_updates = protobuf_to_dict_df(req_trip_updates.content, dataset='trip_updates')

# defining a correspondence map between old and new column names
columns_dict_vehicle_positions = {'isDeleted': 'is_deleted',
    'vehicle.trip.tripId': 'trip_id',
    'vehicle.trip.startTime': 'trip_start_time',
    'vehicle.trip.startDate': 'trip_start_date',
    'vehicle.trip.routeId': 'route_id',
    'vehicle.trip.directionId': 'dir_id',
    'vehicle.position.latitude': 'latitude',
    'vehicle.position.longitude': 'longitude',
    'vehicle.currentStopSequence': 'seq',
    'vehicle.currentStatus': 'status',
    'vehicle.timestamp': 'timestamp',
    'vehicle.stopId': 'stop_id',
    'vehicle.vehicle.id': 'vehicle_id',
    'vehicle.vehicle.label': 'vehicle_label',
    'vehicle.position.odometer': 'odometer',
    'vehicle.occupancyStatus': 'occupancy',
    'vehicle.trip.scheduleRelationship': 'schedule'}

# setting new column names
df_vehicle_positions.columns = [columns_dict_vehicle_positions[column] for column in df_vehicle_positions.columns]

# selecting columns, adding column for request time and transforming timestamp in readable format
columns_vehicle_positions = ['trip_id', 'latitude', 'longitude', 'seq', 'status', 'timestamp', 'stop_id']
df_vehicle_positions = df_vehicle_positions[columns_vehicle_positions]
df_vehicle_positions['req_time'] = req_time
df_vehicle_positions['timestamp'] = df_vehicle_positions.timestamp.apply(lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S'))

# writing the dataframe to a csv buffer
buffer_vehicle_positions = write_df_to_buffer(df_vehicle_positions)

# selecting columns and writing the dictionary to a csv buffer
columns_trip_updates = ['trip_id', 'seq', 'delay', 'time', 'uncertainty', 'req_time']
buffer_trip_updates = write_d_to_buffer(d=d_trip_updates, req_time=req_time)

# connecting to database, creating the table if it does not exist
sql_string = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password='{db_pass}' sslmode=require"
con = psycopg2.connect(sql_string)
cur = con.cursor()
cur.execute(f"""
CREATE TABLE IF NOT EXISTS {vehicle_positions_table} (trip_id text, latitude float, longitude float, seq int, status text, timestamp text, stop_id text, req_time text);
CREATE TABLE IF NOT EXISTS {trip_updates_table} (trip_id text, seq int, delay int, time text, uncertainty int, req_time text);
""")
con.commit()

# copying the data in sql tables
cur.copy_from(buffer_vehicle_positions, vehicle_positions_table, columns=columns_vehicle_positions + ['req_time'], sep='\t', null='NaN')
cur.copy_from(buffer_trip_updates, trip_updates_table, columns=columns_trip_updates, sep='\t', null='NaN')
con.commit()

# closing the database connection
con.close()
