
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
    counter_location        VARCHAR,
    weather_station_code    VARCHAR,
    bicycle_count           INT
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

# Query Lists
create_table_queries = [bicycle_fact_create,
                        time_dimension_create,
                        date_dimension_create,
                        weather_dimension_create]