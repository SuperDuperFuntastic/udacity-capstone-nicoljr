import logging
import os
import pandas as pd
import requests
from meteostat import Stations
from pathlib import Path
from config.definitions import ROOT_DIR

# TODO remove when no longer needed
pd.set_option('display.max_columns', None)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-4s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

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
                weather_station_code, time_zone = get_nearby_weather_station(
                    latitude,
                    longitude)

            bicycle_dict = {"file_path": file_path,
                            "weather_station_code": weather_station_code,
                            "time_zone": time_zone,
                            "city": city,
                            "state": state,
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
    Finds the 3 closest stations, compares their completeness, then returns
        the closest station with the most data (because Middleton is too
        close to Madison and only recently started collecting data, whereas
        the Madison regional airport has been collecting since the 1940's)
    Parameters:
        latitude (float)
        longitude (float)
    Returns:
        nearby_station (str): the ID of the nearest weather station with the
            most data
        time_zone (str): the time zone of the weather station, which we'll
            use later to account for local time vs. UTC
    '''
    stations = Stations()
    stations = stations.nearby(lat=latitude, lon=longitude)
    df = stations.fetch(3).sort_values(by=['hourly_start', 'distance'])
    nearby_station = df.index[0]
    station_data = df.iloc[0]
    time_zone = station_data['timezone']
    return nearby_station, time_zone
    
def create_fact_dataframe(bicycle_metadata_item):
    '''
    Creates a dataframe that contains information from both the metadata
        and the contents of the bicycle count source files
    Parameters:
        bicycle_metadata_item (dict): A dictionary containing info about
            the files containing data on bicycle counts. Metadata include
            things like filepath, city, state, etc. based on info from
            the directory they're stored in and API calls
    '''
    logging.info(f'Creating dataframe for {bicycle_metadata_item}')
    df = pd.read_csv(bicycle_metadata_item['file_path'],
                     header=0,
                     names=['date', 'bicycle_count'],
                     usecols=[0,1],
                     parse_dates=['date'])
    df['local_time'] = df['date'].dt.time
    df['local_date'] = df['date'].dt.date
    df['timezone'] = bicycle_metadata_item['time_zone']
    df['utc_datetime'] = df['date'].dt.tz_localize(
        bicycle_metadata_item['time_zone'],
        ambiguous=True, nonexistent='shift_backward').dt.tz_convert('utc')
    df['utc_date'] = df['utc_datetime'].dt.date
    df['utc_time'] = df['utc_datetime'].dt.time
    df['counter_location'] = bicycle_metadata_item['file_path'].stem
    df['weather_station_code'] = bicycle_metadata_item\
        ['weather_station_code']
    df = df[['local_date', 'local_time', 'utc_date', 'utc_time',
             'counter_location', 'weather_station_code', 'bicycle_count']]
    
    logging.info(f'Dataframe created, shape: {df.shape}')
    create_output_csv(df, 'bicycle_fact.csv') 
    
def create_output_csv(dataframe, output_name):
    output_destination = os.path.join(ROOT_DIR, 'data/output')
    logging.info(f'Creating (or appending) data to {output_name} in folder\
                 {output_destination}')
    if os.path.exists(os.path.join(output_destination, output_name)):
        dataframe.to_csv(os.path.join(output_destination, output_name),
                        mode='a',
                        header=False,
                        index=False)
    else:
        dataframe.to_csv(os.path.join(output_destination, output_name),
                        index=False)

def fetch_weather_data(bicycle_metadata):
    '''
    Retrieves data from the meteostat API based on the nearby weather
        station codes found in prior processing
    Parameters:
        bicycle_metadata (list): A list of dictionaries containing metadata
            related to the source files for bicycle counts
    '''
    logging.info(f"Fetching weather data for the weather stations below:")
    unique_weather_stations = set()
    for item in bicycle_metadata:
        unique_weather_stations.add(item['weather_station_code'])
    logging.info(unique_weather_stations)
    
    for station in unique_weather_stations:
        url = f"https://bulk.meteostat.net/v2/hourly/{station}.csv.gz"
        output_destination = os.path.join(ROOT_DIR, 'data/output')
        output_file = os.path.join(output_destination, url.split('/')[-1])
        with open(output_file, 'wb') as f:
            r = requests.get(url)
            f.write(r.content)

def transform_weather_data():
    '''
    Reads the compressed weather .csv files, creates dataframes, and then
        creates (or appends to) a .csv data file
    '''
    output_destination = os.path.join(ROOT_DIR, 'data/output')
    col_names = ['utc_date', 'utc_hour', 'temperature_c', 'dew_point_c',
            'relative_humidity_pct', 'hourly_precipitation_mm',
            'snow_mm', 'wind_direction_deg', 'avg_wind_spd_kmh',
            'peak_wind_gust_kmh', 'air_pressure_hpa',
            'hourly_sunshine_min', 'weather_condition_code']
    for root, dir, files in os.walk(output_destination):
        for file in files:
            if file.endswith('.gz'):
                df = pd.read_csv(os.path.join(root, file),
                                compression='gzip',
                                names=col_names)
                df['weather_station_code'] = file.split('.')[0]
                df['utc_hour'] = df['utc_hour'].astype(str).str.zfill(2)\
                    + ':00:00'
                logging.info(f"Dataframe created, {df.shape}")
                create_output_csv(df, 'weather_dim.csv')

def main():
    # TODO: Clean this up, i'm just using logic to bypass stuff that i don't need to run again
    
    get_bike_data = False
    get_weather_data = False
    do_weather_data = False
    bicycle_metadata = prepare_bicycle_metadata()
    
    if get_bike_data:
        logging.info('Working locally')
        for item in bicycle_metadata:
            create_fact_dataframe(item)
    
    if get_weather_data:
        fetch_weather_data(bicycle_metadata)

    if do_weather_data:
        transform_weather_data()

if __name__ == '__main__':
    main()