WITH origin_count AS (
	SELECT origin AS airport, -- determine all departures
		   COUNT(*) AS departures_per_day
	FROM {{ref('prep_flights')}}
	GROUP BY origin
),
dest_count AS (
	SELECT dest AS airport, -- determine all arrivals
		   COUNT(*) AS arrivals_per_day
	FROM {{ref('prep_flights')}}
	GROUP BY dest
),
actual_flights AS (
SELECT origin AS airport, -- determine number of flights that were not diverted or cancelled (all planned)
	count(*)AS actual_flight_num
FROM {{ref('prep_flights')}}
WHERE cancelled = 0 AND diverted = 0
GROUP BY airport
),
sorted_flights AS 
(
SELECT origin AS airport, -- determine number of cancelled and diverted flights
	SUM(cancelled) AS cancelled_num,
	SUM(diverted) AS diverted_num,
	count(*) AS all_flight_num
FROM {{ref('prep_flights')}}
GROUP BY airport
),
airports_table AS 
(SELECT *
FROM {{ref('prep_airports')}}
)
SELECT
		at.COUNTRY y,
		at.region,
		at.CITY,
		at.name,
		oc.airport,
		arrivals_per_day,
		departures_per_day,
		cancelled_num,
		diverted_num,
		all_flight_num,
		actual_flight_num
FROM origin_count oc
LEFT JOIN dest_count dc ON oc.airport = dc.airport
LEFT JOIN sorted_flights sf ON oc.airport = sf.airport
LEFT JOIN actual_flights af ON oc.airport = af.airport
LEFT JOIN airports_table at ON oc.airport = at.faa