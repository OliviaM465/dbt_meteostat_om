WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *,
        DATE_PART('day', date) AS date_day,            -- day of month
        DATE_PART('month', date) AS date_month,        -- month number
        DATE_PART('year', date) AS date_year,          -- year number
        DATE_PART('week', date) AS cw,                 -- calendar week
        TO_CHAR(date, 'FMMonth') AS month_name,        -- full month name (no padding)
        TO_CHAR(date, 'FMDay') AS weekday              -- full weekday name (no padding)
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name IN ('November', 'December', 'January') THEN 'winter'
            WHEN month_name IN ('February', 'March', 'May') THEN 'spring'
            WHEN month_name IN ('April', 'June', 'July') THEN 'summer'
            WHEN month_name IN ('August', 'September', 'October') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date