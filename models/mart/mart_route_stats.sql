WITH table_mart_route AS
(
	SELECT
		tail_number,
		airline,
		origin,
		dest,
		count(*) AS total_num_route,
		round(avg(f.actual_elapsed_time),2) AS avg_elapsed_time,
		round(avg(f.arr_delay),1) AS avg_arr_delay_time,
		min(arr_delay) AS min_delay,
		max(arr_delay) AS max_delay,
		Sum(f.cancelled) AS cancelled_flights,
		Sum(f.diverted) AS diverted_flights
	FROM {{ref('prep_flights')}} f
	GROUP BY tail_number,
		airline,
		origin,
		dest
)
SELECT 
		tmr.tail_number,
		tmr.airline,
		ao.country AS origin_country,
		ao.city AS origin_city,
		ao.name AS origin_airport,
		tmr.origin AS origin,
		ad.country AS dest_country,
		ad.city AS dest_city,
		ad.name AS dest_airport,
		tmr.dest AS dest,
		total_num_route,
		avg_elapsed_time,
		avg_arr_delay_time,
		min_delay,
		max_delay,
		cancelled_flights,
		diverted_flights
FROM table_mart_route tmr
JOIN airports ao ON tmr.origin = ao.faa
JOIN airports ad ON tmr.dest = ad.faa