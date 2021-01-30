# NYCTaxiTrip

## content
 - background
 - environmental dependence
 - datasource
 - usage

## background
The goal of this project is to find the most popular boroug in NYC.

## environmental dependence
scalaVersion := "2.11.12"

sparkVersion := "2.4.7"

Other settings are in sbt file.

## datasource
taxi data: https://www.kaggle.com/c/nyc-taxi-trip-duration

geometry data: https://github.com/haghard/streams-recipes/blob/master/nyc-borough-boundaries-polygon.geojson

## usage
The taxi orgianl data is huge, this project choose fisrt 5000 rows as dataset.

taxiTrip.scala is the main file. Firstly, it creates the case class for row object, converts csv file to dataframe, and cleans the data. Then analyze the data to get result. 

Features.scala is used to extract geometry information from the geojson file. In order to determine which borough the taxi is in.
