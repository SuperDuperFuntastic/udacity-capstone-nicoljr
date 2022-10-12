# Jason's Udacity Capstone Project
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
corresponding hourly bicycle counts, mashed together with weather data.

## Installation

1. Install all required libraries via the requirements.txt file
2. Update the `config\definitions.py` file for your own environment

```python format
hey look instructions
```

## APIs Used

Finding latitude/longitude (used for finding nearby weather stations) of a city:
https://nominatim.org

Free weather data:
https://dev.meteostat.net

## Packages Used
Meteostat (and any depdendencies)

## Manually Downloaded Data

Madison Bicycle Counter Data
https://data-cityofmadison.opendata.arcgis.com/

Seattle Bicycle Counter Data
https://data.seattle.gov/browse?q=bicycle%20counter&sortBy=relevance


## Usage

```python format
whatever
```