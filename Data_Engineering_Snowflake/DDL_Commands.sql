//Snowflake: SnowSQL

//Create WareHouse in SnowSQL

create or replace warehouse sf_tuts_wh with
  warehouse_size='X-SMALL'
  auto_suspend = 180
  auto_resume = true
  initially_suspended=true;

//----------------------------------------------------------  
  
//Create DataBase in SnowSQL

create or replace database sf_tuts_db;

//---------------------------------------------------------- 

//Create Schema in SnowSQL

create schema sf_tuts_sch comment='this is comment1';

show schemas like '%tuts%';

//---------------------------------------------------------- 

use warehouse sf_tuts_wh;
use database sf_tuts_db;

//---------------------------------------------------------- 

//Create Temporary/Staging Tables in SnowSQL

create or replace temp table sf_tuts_sch.airlines
(
	iata_code varchar, 
	airline varchar
)
comment = 'Airlines table use to keep airline relevant details.';

create or replace temp table sf_tuts_sch.airports
(
	iata_code varchar,
	airport varchar,
	city varchar,
	state varchar,
	country varchar,
	latitude number,
	longitude number
)
comment = 'Airport table use to keep airport relevant details.';

create or replace temp table sf_tuts_sch.flights
(
	year number,
	month number,
	day number,
	day_of_week number,
	airline varchar,
	flight_number varchar,
	tail_number varchar,
	origin_airport varchar,
	destination_airport varchar,
	scheduled_departure varchar,
	departure_time varchar,
	departure_delay number, 
	wheels_off varchar,
	scheduled_time number, 
	elapsed_time number,
	air_time number,
	distance number,
	wheels_on number,
	taxi_in number,
	scheduled_arrival number,
	arrival_time varchar,
	arrival_delay varchar,
	diverted number,
	cancelled number,
	cancellation_reason varchar,
	air_system_delay number,
	security_delay number,
	airline_delay number,
	late_aircraft_delay number,
	weather_delay number
)
comment = 'Flights table use to keep flights relevant details.';

//----------------------------------------------------------

//Create EXTERNAL Tables in SnowSQL contains complete snapshot data


create or replace external table sf_tuts_sch.airlines_snapshot
(
	iata_code varchar, 
	airline varchar
)
with LOCATION = 'azure://myaccount.blob.core.windows.net/mycontainer/mypath/airline/'
auto_refresh = true
file_format = (type = parquet)
comment = 'Airlines table use to keep airline relevant details.';

create or replace external table sf_tuts_sch.airports_snapshot
(
	iata_code varchar, 
	airport varchar,
	city varchar,
	state varchar,
	country varchar,
	latitude number,
	longitude number
)
with LOCATION = 'azure://myaccount.blob.core.windows.net/mycontainer/mypath/airports/'
auto_refresh = true
file_format = (type = parquet)
comment = 'Airport external table use to keep airport relevant details.';

create or replace external table sf_tuts_sch.flights_snapshot
(
	year number,
	month number,
	day number,
	date number,
	day_of_week number,
	airline varchar,  
	flight_number varchar,
	tail_number varchar,
	origin_airport varchar,
	destination_airport varchar,
	scheduled_departure varchar,
	departure_time varchar,
	departure_delay number,   
	wheels_off varchar,
	scheduled_time number, 
	elapsed_time number,
	air_time number,
	distance number,
	wheels_on number,
	taxi_in number,
	scheduled_arrival number,
	arrival_time varchar,
	arrival_delay varchar,
	diverted number,
	cancelled number,
	cancellation_reason varchar,
	air_system_delay number,
	security_delay number,
	airline_delay number,
	late_aircraft_delay number,
	weather_delay number,
	delay_reasons varchar // derived values by cancatination of all delay resons
)
partition by (date)
with LOCATION = 'azure://myaccount.blob.core.windows.net/mycontainer/mypath/flights/'
auto_refresh = true
file_format = (type = parquet)
comment = 'Flights external table use to keep flights relevant details.';

//---------------------------------------------------------- 

  
drop database if exists sf_tuts_db;

drop warehouse if exists sf_tuts_wh;