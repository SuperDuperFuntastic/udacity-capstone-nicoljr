from email import header
import logging
import os
import pandas as pd
import requests
from meteostat import Stations
from pathlib import Path
from config.definitions import ROOT_DIR

logging.basicConfig(level=logging.INFO)

def prepare_bicycle_metadata():
    '''
    Walks through the data folder and parses information from its contents
    
    Returns:
        bicycle_metadata (list): a collection of metadata dictionaries
    '''
    logging.info('Preparing bicycle metadata')
    data_folder = os.path.join(ROOT_DIR, 'data/bicycle_counters')
    bicycle_metadata = []
    # Reduces number of API calls needed
    last_city = ''
    last_state = ''
    for root, dir, files in os.walk(data_folder):
        for file in files:
            file_path = Path(os.path.join(root, file))
            city = file_path.parent.name
            state = file_path.parent.parent.name
            country = file_path.parent.parent.parent.name
            # Reduces number of API calls needed
            if city != last_city and state != last_state:
                latitude, longitude = get_lat_long(city, state)
                weather_station_code = get_nearby_weather_station(latitude,
                                                                  longitude)

            bicycle_dict = {"file_path": file_path,
                            "weather_station_code": weather_station_code,
                            "city": city, "state": state,
                            "country": country}
            bicycle_metadata.append(bicycle_dict)
            last_city = city
            last_state = state
            
    return bicycle_metadata

def get_lat_long(city, state):
    '''
    Requests location data from API, parses the .JSON, and extracts the
    latitude and longitude
    Parameters:
        city (str)
        state (str)
    Returns:
        latitude, longitude (float)
    '''
    r = requests.get(f'https://nominatim.openstreetmap.org/search?city=\
                     {city}&state={state}&format=json').json()[0]
    latitude, longitude = float(r['lat']), float(r['lon'])
    return latitude, longitude
    
def get_nearby_weather_station(latitude, longitude):
    '''
    Finds the nearest weather station and returns its identifier
    Parameters:
        latitude (float)
        longitude (float)
    Returns:
        nearby_station (str): the ID of the nearest weather station
    '''
    stations = Stations()
    stations = stations.nearby(lat=latitude, lon=longitude)
    nearby_station = stations.fetch(1).index[0]
    return nearby_station
    

def create_fact_dataframe(bicycle_metadata_item):
    '''
    '''
    logging.info(f'Creating dataframe for {bicycle_metadata_item}')
    df = pd.read_csv(bicycle_metadata_item['file_path'],
                     header=0,
                     names=['date', 'bicycle_count'],
                     usecols=[0,1],
                     parse_dates=['date'])
    df['time'] = df['date'].dt.time
    df['date'] = df['date'].dt.date
    df['counter_location'] = bicycle_metadata_item['file_path'].stem
    df['weather_station_code'] = bicycle_metadata_item['weather_station_code']
    df = df[['date', 'time', 'counter_location', 'weather_station_code', 'bicycle_count']]
    
    logging.info(f'Dataframe created, shape: {df.shape}')
    create_output_csv(df, 'bicycle_fact.csv')
    
    
    
def create_output_csv(dataframe, output_name):
    output_destination = os.path.join(ROOT_DIR, 'data/output')
    logging.info(f'Creating (or appending) data to {output_name} in folder {output_destination}')
    if os.path.exists(os.path.join(output_destination, output_name)):
        dataframe.to_csv(os.path.join(output_destination, output_name), mode='a', header=False)
    else:
        dataframe.to_csv(os.path.join(output_destination, output_name))

def main():
    work_locally = True
    bicycle_metadata = prepare_bicycle_metadata()
    
    if work_locally:
        logging.info('Working locally')
        for item in bicycle_metadata:
            create_fact_dataframe(item)
    

if __name__ == '__main__':
    main()