SELECT airport_code,
		DATE_PART('week', date) AS week,
		DATE_PART('year', date) AS year,
		round(avg(AVG_TEMP_C),1) AS AVG_TEMP_C,
		min(MIN_TEMP_C) AS MIN_TEMP_C,
		max(MAX_TEMP_C) AS MAX_TEMP_C,
		max(precipitation_mm) AS precipitation_mm,
		max(MAX_SNOW_MM) AS MAX_SNOW_MM,
		avg(AVG_WIND_DIRECTION) AS AVG_WIND_DIRECTION
FROM {{ref('prep_weather_daily')}}
GROUP BY DATE_PART('week', date),
		DATE_PART('year', date),
		airport_code
ORDER BY week, AIRPORT_CODE	