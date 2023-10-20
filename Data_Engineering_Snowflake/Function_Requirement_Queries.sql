//----------------------------------------------------------------------

//Create summary views to get aggregate data in SnowSQL

//----------------------------------------------------------------------

use warehouse sf_tuts_wh;
use database mydb;

// 1. Total number of flights by airline and airport on a monthly basis

create or replace view sf_tuts_sch.flight_airport_mnth_smry_vw
as select 
	count(a.flight_number), 
	a.airline,  
	b.airport, 
	concat(year, month)
from flights_snapshot a, 
		airports_snapshot b
where 
	a.iata_code = b.origin_airport
group by 
	a.airline,  
	b.airport, 
	concat(year, month);



// 2. On-time percentage of each airline for the year 2015

create or replace view sf_tuts_sch.ontime_airline_smry_vw
as 	select 
		(airln.cnt/on_time.null_cnt)*100 as cnt_ontime_arrv, 
		airln.airline airline_cd, 
		airln.airline airline
	from 
		(select count(*) as cnt, airline, 
			from flights_snapshot 
			where year = 2015 
			group by airline) tot_airln,
		(select count(*) as null_cnt, airline
			from flights_snapshot 
			where year = 2015
			and elapsed_time is null 
			group by airline) on_time,
		airlines_snapshot airln
where 
	tot_airln.airline = on_time.airline
and tot_airln.airline = airln.iata_code;

// 3. Airlines with the largest number of delays

create or replace view sf_tuts_sch.delay_airline_smry_vw
as select 
	max(arrival_delay) max_arrival_delay,
	airline.airline airline_cd, 
	airln.airline airline  
from 
	flights_snapshot flt_snp,
	airlines_snapshot airln
where 
	flt_snp.airline = airln.iata_code
group by 
	airline.airline airline_cd, 
	airln.airline airline ;

// 4. Cancellation reasons by airport

create or replace view sf_tuts_sch.cancelled_airport_smry_vw
as select 
	count(*), 
	flt_snp.cancellation_reason cancellation_reason, 
	prt_smry.iata_code airport_cd, 
	prt_smry.airport airport
from 
	flights_snapshot flt_snp,
	airports_snapshot prt_smry
where 
	prt_smry.iata_code = flt_snp.origin_airport
and flt_snp.cancelled != 0
group by 
	flt_snp.cancellation_reason cancellation_reason, 
	prt_smry.iata_code airport_cd, 
	prt_smry.airport airpor;
	
	
// 5. Delay reasons by airport

create or replace view sf_tuts_sch.delay_reason_airport_smry_vw
as select 
	count(*), 
	flt_snp.delay_reasons, //derived values
	prt_smry.iata_code airport_cd, 
	prt_smry.airport airport
from 
	flights_snapshot flt_snp,
	airports_snapshot prt_smry
where prt_smry.iata_code = flt_snp.origin_airport
group by 
	flt_snp.delay_reasons,
	prt_smry.iata_code airport_cd, 
	prt_smry.airport airpor;


// 6. Airline with the most unique routes
create or replace view sf_tuts_sch.delay_reason_airport_smry_vw
as select 
	min(flt_snp.tail_number) min_route_frq, 
	airln.airline airline_cd, 
	airln.airline airline 
	flt_snp.flight_number
from 
	flights_snapshot flt_snp,
	airlines_snapshot airln
where 
	flt_snp.airline = on_time.airline
group by 
	airln.airline airline_cd, 
	airln.airline airline 
	flt_snp.flight_number;

//----------------------------------------------------------  