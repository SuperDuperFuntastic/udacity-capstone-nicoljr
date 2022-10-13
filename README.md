# Jason's Udacity Capstone Project / Write Up
## Cycling Analysis

According to Andrew Van Dam's work for the Washington Post's weekly
"Department of Data" column, the following 20 US cities with populations
over 100,000 have the greatest number of bicycle commuters:

![bar chart depicting the top 20 US cities for bicycle commuters](
images/large_city_bicycle_commuters.png)
https://www.washingtonpost.com/business/2022/09/09/films-assigned-college/

That got me thinking about how weather and other factors may impact workers'
decisions to ride a bike into work vs. driving. The annual census data is a
nice place to start, but weather over the course of the year isn't all that
compelling to look at. What if we could find data on daily bicycle usage?
Or better yet, hourly data?

I knew Madison had a pair of bicycle counters since I live here and ride my
bike frequently, but what about other cities on the list? Unfortunately, it
turns out most don't. And of the handful that do, some don't make their data
readily available (I'm looking at you, Portland). Thankfully, Seattle also
had several counters installed, and they made all that data (and more)
available! So we'll just be looking at those two cities and their
corresponding hourly bicycle counts, mashed together with weather, date, and
time data. Which city's residents ride more in the rain? Or the cold? What
about weekends vs. weekdays?

The `ride_bikes_have_fun.pbix` file in the reporting directory contains an
import of the data landed in Redshift for analysis. Attempting to refresh
when the Redshift cluster is offline or if you don't have the proper
credentials will result in an error.

## Installation

1. Install all required libraries via the requirements.txt file
2. Create your own S3 buckets, IAM users, IAM roles, and Redshift cluster
3. Update the `config\definitions.py` file for your own environment
4. Use the AWS CLI to set your credentials (used in boto3)

## Running the Pipeline
After completing the steps outlined in the Installation section, run
`process_data.py` to create the .csv's that contain the data for all
the Redshift tables and stage them out in your S3 bucket.

Once the files are out in the S3 bucket, the process will drop/create any S3
tables (an actual implementation would omit the drop/create after the
tables were created) and perform a set of Redshift COPY queries to load
the data from the .csv's into their respective Redshift tables.

Record integrity is enforced through definitions on the Redshift tables, and a
control total comparison is performed on the files staged in the
data/output folder and the 

## How It Works
1. Bicycle count .csv's were placed in their respective country, state,
and city folders (see the 'Manually Downloaded Data' section for links)
2. Pandas dataframes are created using the .csv's. Additional columns
required are added using API calls to get the lat/lon based on city
and state using the directory metadata. The Meteostat python library is
used to find the 3 closest weather stations to that lat/lon and returns
the most complete set of data (this avoids scenarios where a more recently
established weather station is built closer than one that had been
collecting data for years). That weather station's code is added to the
dataframe.
3. The dataframes are then used to create a collection of .csv files that
are uploaded to an AWS S3 location using the boto3 library.
4. Once the .csv's are in S3, is used to perform COPY's
to the Redshift destination.
5. Control totals are created by querying the target database and building
a dictionary of table name keys to count values. Then a similar process is
performed on the data/output directory, taking care to handle "multi-part"
.csv's

## Datasource Types
* .CSV's for the source files that eventually become the facts/dims
* .CSV.GZ via Meteostat bulk data API calls
* .JSON via nominatim location API calls (used directly in df's)
* Pandas dataframe via Meteostat weather station library

## APIs Used
Finding latitude/longitude (used for finding nearby weather stations) of a city:
https://nominatim.org

Free weather data .csv.gz files downloaded from:
https://dev.meteostat.net

## Libraries Used
Meteostat for finding nearby weather stations
boto3 for copying files to S3
Pandas for data wrangling


## Manually Downloaded Data

Madison Bicycle Counter Data
https://data-cityofmadison.opendata.arcgis.com/

Seattle Bicycle Counter Data
https://data.seattle.gov/browse?q=bicycle%20counter&sortBy=relevance


## Data Model
![Bicycle fact table with M:1 relationships to dates, time, and weather dims](
images/redshift_data_model.png)

## Defending Data Model Decisions
I opted for a star schema as the design for the data model because it is well
suited for reporting. A centralized fact table that has a many-to-one
relationship with various dimensions (like time, date, and weather) means
less complicated joins when being used by downstream users. Additionally, a
star schema like this one allows for quick and easy aggregation (in this
case, aggregating hourly bicycle counts by day, week, month, year, city,
state, temperature, etc.), which allows end users to quickly view sums,
averages, mins, and maxes of the bicycle count data at various levels.

Since there is very little need for insert, update, or delete operations
on this reporting data, a traditional OLTP-style design isn't necessary.
Plus, designing this as an OLTP-style database would've meant far more
complicated queries and joins for end users.

## Defending Technology Decisions
Since the data that populate the fact table and the weather dim were spread
among multiple files, it made sense to stage them in S3 and use a Redshift
cluster to copy the data to their respective tables. Additionally, reading
the data from Redshift and loading it into the included Power BI model was
extremely quick, especially compared to loading from .csv's.

Because the files themselves weren't overly large, I was comfortable doing
most of the transformation using Pandas locally. This also allowed me to
quickly address data quality issues by simply opening the staged .csv's in
my workspace and then adjusting whichever Pandas transformation needing
fixing. Had I been using S3 as my workspace staging area from the get-go,
I would have spent additional time troubleshooting AWS settings that I
could have messed up, or wait additional time to copy/download files as
needed.

Once I knew the files were in the state they needed to be in to be loaded
via a Redshift COPY from S3, it was just a matter of using boto3 to push
out the files and then handle the copy.

A fun aside, I had initially tried to use the Redshift destination as
part of the "to_sql" method for a Pandas dataframe, and the resulting
long-tailed processing time (and eventual failure) was what convinced
me to stage the .csv's in S3 first and embrace working from there.
## Addressing Other Scenarios
### What if the data was increased by 100x?
The longest step is creating the Pandas dataframes and .csv's, so
I'd look into storing the .csv's that make up the bicycle fact and
weather data directly in S3 and try to use Spark dataframes to
shape the data and then continue using Redshift as the destination.

### What if the pipelines needed to run on a daily basis by 7AM?
In addition to using something like Apache Airflow to handle
scheduled runs and only returning a subset of bicycle or weather-
station data based on the current date, I'd also want to rewrite
much of the file copy/download/staging logic to clean up files after
a run has completed. Right now, the entire pipeline is effectively a
truncate-and-load, with some manual steps to clear the local and S3
files when needed.

### What if the database needed to be accessed by 100+ people?
I'd definitely keep the destination in the cloud, but since
Redshift isn't a high concurrency/connectivity database, it might
be worth investigating using S3 as the final target instead. It
could also be worth looking into Redshift Serverless, as according
to this AWS resource, it would allow up to 2,000 connections within
a single workspace.
https://docs.amazonaws.cn/en_us/redshift/latest/mgmt/amazon-redshift-limits.html

## Evidence of Successful Completion of ETL
The included `ride_bikes_have_fun.pbix` file is a Power BI model and
accompanying dashboard that loaded its data directly from the Redshift
database upon the completion of the ETL process. Below are some
screenshots of the data presented in the dashboard, as well as what the
equivalent filter criteria would be when looking at the database. The
screenshots are designed to show the different filters (at the top) and
the levels of aggregation (time-based line chart by year, month, day, and
time by hour at the bottom) the data could be presented at. The line chart
can be "drilled down" into to show aggregations across time.

Additionally, you can open the .pbix file and play around with the
visualizations yourself as well as long as you don't mind installing
Power BI Desktop (it's free!)

### Total Number of Rainy, September Weekend Morning Riders in Madison in 2020
Rainy weather:
- weather_d.hourly_precipitation_mm > 0.1

September/Weekend/2020:
- date_d.month_of_year = '09',
- date_d.weekend = 'Weekend' 
- date_d.year = 2020 (drilled down in visualizion, only one day fit that criteria so I went all the way down to the hourly level)

Morning:
- time_d.ampm = 'AM'

Madison:
- bicycle_fact.state = 'wi'
- bicycle_fact.city = 'madison'

![Dashboard depicting rainy morning Madison riders](
images/rainy_madison_riders_dashboard.png)

### Total Number of Hot Summer Night Riders in Seattle in 2020
Hot weather:
- weather_d > 26

Summer/2020:
- date_d.month_of_year in ('06, '07', '08')
- date_d.year = 2020 (drilled down in visualization)

Night
- time_d.hour > 16

Seattle:
- bicycle_fact.state = 'wa'
- bicycle_fact.city = 'seattle'

![Dashboard depicting hot Seattle summer night riders](
images/hot_seattle_night_riders_dashboard.png)

## Connection Info
![Screenshot depecting the Redshift connection info for Jason's capstone](
images/redshift_connection_info.png)

## Fact Table Contents Example after Loading to Power BI
![Screenshot depecting bicycle data loaded from Redshift to Power BI](
images/bicycle_fact_loaded_to_power_bi.png)

## Weather Dim Table Contents Example after Loading to Power BI
![Screenshot depecting weather data loaded from Redshift to Power BI](
images/weather_dim_loaded_to_power_bi.png)