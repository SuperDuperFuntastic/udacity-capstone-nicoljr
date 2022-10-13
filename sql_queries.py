import configparser

config = configparser.ConfigParser()
config.read('config\config.cfg')

s3_bucket = config['S3']['CAPSTONE_BUCKET']
credentials = config['ARN']['ARN_ROLE']

# Modified from of sunnycmf's date dimension definition
# https://gist.github.com/sunnycmf/131a10a17d226e2ffb69
date_dimension_create = (
'''
DROP TABLE IF EXISTS date_d;
CREATE TABLE date_d (
date_id          BIGINT GENERATED ALWAYS AS IDENTITY,
date             DATE NOT NULL,
year             INT,
month            CHAR(10),
month_of_year    CHAR(2),
day_of_month     INT,
day              CHAR(10),
day_of_week      INT,
weekend          CHAR(10) NOT NULL,
day_of_year      INT,
week_of_year     CHAR(2),
quarter          INT,
previous_day     DATE NOT NULL,
next_day         DATE NOT NULL
);             
'''
)

# Modified from sunnycmf's time dimension definition
# https://gist.github.com/sunnycmf/2e8c2b5aa72d77a0dd96
time_dimension_create = (
'''
DROP TABLE IF EXISTS time_d;
CREATE TABLE time_d  (
    time_id     INT GENERATED ALWAYS AS IDENTITY,
    fulltime    TIME,
    hour        INT,
    ampm        VARCHAR(2)
);
'''    
)

bicycle_fact_create = (
'''
DROP TABLE IF EXISTS bicycle_fact;
CREATE TABLE bicycle_fact (
    id                      INT GENERATED ALWAYS AS IDENTITY,
    local_date              DATE NOT NULL,
    local_time              VARCHAR(9) NOT NULL,
    utc_date                DATE NOT NULL,
    utc_time                VARCHAR(9) NOT NULL,
    city                    VARCHAR NOT NULL,
    state                   VARCHAR NOT NULL,
    counter_location        VARCHAR,
    weather_station_code    VARCHAR,
    bicycle_count           NUMERIC
);
'''
)

weather_dimension_create = (
'''
DROP TABLE IF EXISTS weather_d;
CREATE TABLE weather_d (
    weather_id              INT GENERATED ALWAYS AS IDENTITY,
    utc_date                DATE NOT NULL,
    utc_hour                VARCHAR(9) NOT NULL,
    temperature_c           NUMERIC,
    dew_point_c             NUMERIC,
    relative_humidity_pct   NUMERIC,
    hourly_precipitation_mm NUMERIC,
    snow_mm                 NUMERIC,
    wind_direction_deg      NUMERIC,
    avg_wind_spd_kmh        NUMERIC,
    peak_wind_gust_kmh      NUMERIC,
    air_pressure_hpa        NUMERIC,
    hourly_sunshine_min     NUMERIC,
    weather_condition_code  VARCHAR,
    weather_station_code    VARCHAR NOT NULL
);
'''
)

bicycle_fact_copy = (
f'''
COPY bicycle_fact FROM 's3://{s3_bucket}/bicycle_fact'
CREDENTIALS 'aws_iam_role={credentials}'
CSV
IGNOREHEADER 1;
'''
)

weather_d_copy = (
f'''
COPY weather_d FROM 's3://{s3_bucket}/weather_d'
CREDENTIALS 'aws_iam_role={credentials}'
CSV
IGNOREHEADER 1;
'''
)

date_d_copy = (
f'''
COPY date_d FROM 's3://{s3_bucket}/date_d'
CREDENTIALS 'aws_iam_role={credentials}'
CSV
IGNOREHEADER 1;
'''
)

time_d_copy = (
f'''
COPY time_d FROM 's3://{s3_bucket}/time_d'
CREDENTIALS 'aws_iam_role={credentials}'
CSV
IGNOREHEADER 1;
'''
)

bicycle_fact_count = (
'''
SELECT COUNT(*) FROM bicycle_fact
'''
)

weather_d_count = (
'''
SELECT COUNT(*) FROM weather_d
'''
)

date_d_count = (
'''
SELECT COUNT(*) FROM date_d
'''
)

time_d_count = (
'''
SELECT COUNT(*) FROM time_d
'''
)

bicycle_no_blanks = (
'''
SELECT COUNT(*)
FROM bicycle_fact
WHERE
    local_time IS NULL
OR  local_date IS NULL
OR  weather_station_code IS NULL
'''
)

weather_uniqueness = (
'''
SELECT COUNT(*)
FROM
	(SELECT DISTINCT
        utc_date,
        utc_hour,
        weather_station_code
	FROM weather_d)'''
)

date_uniqueness = (
'''
SELECT COUNT(DISTINCT(date))
FROM date_d
'''
)
time_uniqueness = (
'''
SELECT COUNT(DISTINCT(fulltime))
FROM time_d
'''
)

# Query Lists
create_table_queries = [bicycle_fact_create,
                        time_dimension_create,
                        date_dimension_create,
                        weather_dimension_create]

copy_table_queries = [bicycle_fact_copy,
                      weather_d_copy,
                      date_d_copy,
                      time_d_copy]

target_control_queries = [bicycle_fact_count,
                          weather_d_count,
                          date_d_count,
                          time_d_count]

dim_uniqueness_queries = [weather_uniqueness,
                          date_uniqueness,
                          time_uniqueness]