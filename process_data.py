from enum import unique
import boto3
import configparser
import logging
import os
import pandas as pd
import psycopg2
import re
import requests
from meteostat import Stations
from pathlib import Path
from config.definitions import ROOT_DIR
from sql_queries import create_table_queries, copy_table_queries, target_control_queries, dim_uniqueness_queries, bicycle_no_blanks

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
    
def create_fact_dataframe(bicycle_metadata_item) -> pd.DataFrame:
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
    # new UTC-based columns by converting from local time 
    df['utc_datetime'] = df['date'].dt.tz_localize(
        bicycle_metadata_item['time_zone'],
        ambiguous=True, nonexistent='shift_backward').dt.tz_convert('utc').dt.round('H')
    df['utc_date'] = df['utc_datetime'].dt.date
    df['utc_time'] = df['utc_datetime'].dt.time
    df['city'] = bicycle_metadata_item['city']
    df['state'] = bicycle_metadata_item['state']
    df['counter_location'] = bicycle_metadata_item['file_path'].stem
    df['weather_station_code'] = bicycle_metadata_item\
        ['weather_station_code']
    df = df[['local_date', 'local_time', 'utc_date', 'utc_time',
             'city', 'state', 'counter_location', 'weather_station_code',
             'bicycle_count']]
    
    logging.info(f'Dataframe created, shape: {df.shape}')
    return df
    
def create_output_csv(dataframe, output_name):
    '''
    Creates (or appends to) a .csv file in the specified location
    Parameters:
        dataframe (Pandas dataframe)
        output_name (str): the full path of the to-be-created .csv
    '''
    output_destination = os.path.join(ROOT_DIR, 'data\\output')
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

def download_weather_data(bicycle_metadata):
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
        output_destination = os.path.join(ROOT_DIR, 'data/download')
        output_file = os.path.join(output_destination, url.split('/')[-1])
        with open(output_file, 'wb') as f:
            r = requests.get(url)
            f.write(r.content)

def transform_weather_data():
    '''
    Reads the compressed weather .csv files, creates dataframes, and then
        creates (or appends to) a .csv data file
    '''
    download_path = os.path.join(ROOT_DIR, 'data/download')
    output_destination = os.path.join(ROOT_DIR, 'data/output')
    col_names = ['utc_date', 'utc_hour', 'temperature_c', 'dew_point_c',
            'relative_humidity_pct', 'hourly_precipitation_mm',
            'snow_mm', 'wind_direction_deg', 'avg_wind_spd_kmh',
            'peak_wind_gust_kmh', 'air_pressure_hpa',
            'hourly_sunshine_min', 'weather_condition_code']
    for root, dir, files in os.walk(download_path):
        i=0
        for file in files:
            if file.endswith('.gz'):
                df = pd.read_csv(os.path.join(root, file),
                                compression='gzip',
                                names=col_names)
                df['weather_station_code'] = file.split('.')[0]
                df['utc_hour'] = df['utc_hour'].astype(str).str.zfill(2)\
                    + ':00:00'
                logging.info(f"Dataframe created, shape: {df.shape}")
                create_output_csv(df, os.path.join(output_destination, f'weather_d-{i}.csv'))
                i += 1

def copy_to_s3(source_path, s3_bucket):
    '''
    Copies files from a local path to an S3 bucket. You'll need to
    use the AWS CLI to set your ID, secret, and region
    Parameters:
        source_path (str): path to a set of files to upload
        s3_bucket (str): short S3 bucket name
    Returns:
        source_counts (dict): {'file_name':row_count,...}
    '''
    s3 = boto3.resource('s3')
    i = 0
    for root, dir, files in os.walk(source_path):
        logging.info(f"Found {files.count} files, preparing to upload")
        for file in files:
            logging.info(f"Uploading '{file}' to bucket '{s3_bucket}'")
            s3.meta.client.upload_file(
                os.path.join(source_path, file),
                s3_bucket,
                file
            )
            logging.info("Upload complete")
            i += 1
    logging.info(f"Finished uploading {i} files")

def create_tables(cur, con):
    '''
    Drops/recreates fact and dim tables on the target database
    Parameters:
        cur (psycopg2 cursor object)
        con (psycopg2 connection object)
    '''
    for query in create_table_queries:
        logging.info(f"Executing query {query[:64]}...")
        try:
            cur.execute(query)
            con.commit()
        except Exception as e:
            logging(e)
    logging.info("Done with creating tables")

def load_redshift_tables(cur, con):
    '''
    Performs a copy of data from the source S3 bucket to
        a set of staging tables using the cursor and connection
        objects provided
    Parameters:
        cur (psycopg2 cursor)
        con (psycopg2 connection)
    '''
    logging.info("Loading redshift tables")
    for query in copy_table_queries:
        logging.info(f"{query[:24]}...")
        cur.execute(query)
        con.commit()
    logging.info("finished loading redshift tables")
    
def target_control_totals(cur, query_list):
    '''
    Performs basic row counts on the target tables for comparison with
        similar counts on the source files
    Parameters:
        cur (psycopg2 cursor)
        query_list (a list of queries)
    Returns:
        target_counts (dict): {'table_name':row_count,...}
    '''
    target_counts = {}
    for query in query_list:
        # split and drop \n
        table_name = query.split('FROM ',1)[-1][:-1]
        logging.info(f"Counting rows on {table_name}")
        cur.execute(query)
        count = cur.fetchone()[0]
        target_counts[table_name]=count
    return target_counts

def fast_row_count(file):
    '''
    Quickly counts rows in a file, excludes header
    Parameter:
        file (str): a file path
    Returns: integer
    '''
    with open(file) as f:
        return sum(1 for line in f) - 1

def source_control_totals(source_path):
    '''
    Performs basic row counts on the source files for comparison with
        similar counts on the target files
    Parameters:
        source_path (path-like): the directory that contains the files you
        wish to retrieve control totals for
    Returns:
        source_counts (dict): {'short_file_name':row_count,...}
    '''
    last_file_short = ''
    last_count = 0
    source_counts = {}
    for root, dir, files in os.walk(source_path):
        logging.info(f"Preparing source control totals on files found in {source_path}")
        for file in files:
            # accounts for multi-part sources
            file_short = re.split('-|\.',str(file))[0]
            count = fast_row_count(os.path.join(source_path, file))
            if file_short == last_file_short:
                logging.info(f"File prefix {file_short} matches last file, \
                    combining counts")
                count += last_count
            source_counts[file_short] = count
            last_file_short = file_short
            last_count = count
    logging.info("Source control totals complete")
    return source_counts

def check_dim_uniqueness(cur, total_dim_queries, unique_dim_queries):
    '''
    Compares target control totals to unique counts on the dimension tables
        as defined by what makes a unique record in the corresponding query
        in sql_queries.py and indicates if the two counts are equal
    Parameters:
        cur (psycopg2 cursor)
        total_dim_queries (list): a list of queries that will return counts
        unique_dim_queries (list): a list of queries that will, based on the
            columns specified in each query on sql_queries.py, return a
            count of their unique values
    Returns: True or False
    '''
    unique_counts = {}
    target_counts = target_control_totals(cur, total_dim_queries)
    for query in unique_dim_queries:
        # split and drop \n
        table_name = query.split('FROM ')[-1][:-1]
        logging.info(f"Counting unique values on {table_name}")
        cur.execute(query)
        count = cur.fetchone()[0]
        unique_counts[table_name]=count
    logging.info(f"Total dim counts: {target_counts}")
    logging.info(f"Unique dim counts: {unique_counts}")
    if unique_counts == target_counts:
        logging.info("Dim uniqueness check passed")
        return True
    else:
        logging.info("Dim uniqueness check failed")
        return False

def no_fk_blanks_on_bicycle_fact(cur):
    '''
    Checks the columns used as FK's on the bicycle_fact table for nulls
        and returns the count of any nulls found
    Parameters:
        cur (psycopg2 cursor)
    Returns:
        True or False
    '''
    logging.info(f"Checking for nulls in key columns on fact table")
    cur.execute(bicycle_no_blanks)
    should_be_zero = cur.fetchone()[0]
    logging.info(f"Total nulls found (expecting 0): {should_be_zero}")
    if should_be_zero == 0:
        logging.info("Fact table null check passed")
        return True
    else:
        logging.info("Fact table null check failed")
        return False
            
def main():
    config = configparser.ConfigParser()
    config.read('config\config.cfg')
    
    # s3 variables
    capstone_bucket = config['S3']['CAPSTONE_BUCKET']
    source_path = os.path.join(ROOT_DIR, 'data\\output')
    
    bicycle_metadata = prepare_bicycle_metadata()
    
    # build df's and stage .csv's in data/output folder
    i=0
    for item in bicycle_metadata:
        df = create_fact_dataframe(item)
        create_output_csv(df, f'bicycle_fact-{i}.csv')
        i += 1
    
    download_weather_data(bicycle_metadata)
    transform_weather_data()
    copy_to_s3(source_path, capstone_bucket)
    
    # S3 to Redshift
    con = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
        .format(*config['CLUSTER'].values()))
    cur = con.cursor()
    
    create_tables(cur, con)
    load_redshift_tables(cur, con)
    
    # Control totals
    target_counts = target_control_totals(cur, target_control_queries)
    source_counts = source_control_totals(source_path)
        
    logging.info(f"Comparing control total dicts (remember, order doesn't matter)")
    logging.info(source_counts)
    logging.info(target_counts)
    
    # Control totals test
    if target_counts == source_counts:
        logging.info("Control totals match!")
    else:
        logging.info("Control totals do NOT match!")
    
    # Dim uniqueness testing, excludes fact table
    dim_count_queries = target_control_queries[1:]
    uniqueness_check = check_dim_uniqueness(cur, dim_count_queries,
                                            dim_uniqueness_queries)
    
    null_fact_check = no_fk_blanks_on_bicycle_fact(cur)
    con.close()
    
    if all([uniqueness_check, null_fact_check]):
        logging.info("Additional data integrity checks passed!")
    else:
        logging.info("One or more data integrity checks failed, see the logs for details")
    
if __name__ == '__main__':
    main()