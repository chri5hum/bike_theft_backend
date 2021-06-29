-- Credit: Thomas Cerqueus - Scripts to populate the date dimension in a PostgreSQL data warehouse

-- Create Table
CREATE TABLE IF NOT EXISTS prod.dim_date (
	id integer PRIMARY KEY,
	date timestamp NOT NULL,
	-- Formatted dates
	date_us_format text NOT NULL, 		-- mm/dd/yyyy
	date_us_short_format text NOT NULL, -- m/d/yyyy
	date_iso_format text NOT NULL, 		-- yyyy-mm-dd
	-- Year
	num_year integer NOT NULL,
	-- Month
	num_month_in_year integer NOT NULL,
	-- Week
	num_week_in_year integer NOT NULL,
	num_week_in_month integer NOT NULL,
	-- Day
	num_day_in_year integer NOT NULL,
	num_day_in_month integer NOT NULL,
	num_day_in_week integer NOT NULL,
	-- Names
	name_month_en text NOT NULL,
	name_month_abbreviated_en text NOT NULL,
	name_day_en text NOT NULL,
	name_day_abbreviated_en text NOT NULL
);

-- get_date_primary_key(‘2017-08-01’::timestamp) returns 20170801
CREATE OR REPLACE FUNCTION get_date_primary_key(ts anyelement) RETURNS integer AS $$
BEGIN
	RETURN 10000 * EXTRACT(YEAR FROM ts) +
		100 * EXTRACT(MONTH FROM ts) +
		EXTRACT(DAY FROM ts);
END;
$$ LANGUAGE plpgsql;

-- populate_dim_date
-- 1. generate all dates
-- 2. extract main attributes
-- 3. holidays (skip)
-- 4. names from numbers
-- 5. abbreviated names from names
CREATE OR REPLACE PROCEDURE populate_dim_date()
LANGUAGE SQL
AS $$
WITH date1 AS (
	SELECT generate_series('2008-01-01'::timestamp, '2021-12-31'::timestamp, '1 day') AS ts
), date2 AS (
	SELECT get_date_primary_key(ts) AS key,
		ts AS date,
		to_char(ts, 'MM/DD/YYYY') AS date_us_format, 
		EXTRACT(MONTH FROM ts) || '/' || EXTRACT(DAY FROM ts) || '/' || EXTRACT(YEAR FROM ts) AS date_us_short_format,
		to_char(ts, 'YYYY-MM-DD') AS date_iso_format,
		EXTRACT(YEAR FROM ts) AS num_year,
		EXTRACT(MONTH FROM ts) AS num_month_in_year,
		EXTRACT(WEEK FROM ts) AS num_week_in_year,
		EXTRACT(WEEK FROM ts) - EXTRACT(WEEK FROM date(date_trunc('MONTH', ts))) + 1 AS num_week_in_month,
		EXTRACT(DOY FROM ts) AS num_day_in_year,
		EXTRACT(DAY FROM ts) AS num_day_in_month,
		EXTRACT(ISODOW FROM ts) AS num_day_in_week
	FROM date1
), date3 AS (
	SELECT *
-- 	,
-- 		num_month_in_year = 1 AND num_day_in_month = 1 OR 
-- 		num_month_in_year = 7 AND num_day_in_month = 4 OR 
-- 		num_month_in_year = 12 AND num_day_in_month = 25 AS is_holiday_us
	FROM date2
), date4 AS (
	SELECT
		*,
		CASE
			WHEN num_month_in_year = 1 THEN 'January'
			WHEN num_month_in_year = 2 THEN 'February'
			WHEN num_month_in_year = 3 THEN 'March'
			WHEN num_month_in_year = 4 THEN 'April'
			WHEN num_month_in_year = 5 THEN 'May'
			WHEN num_month_in_year = 6 THEN 'June'
			WHEN num_month_in_year = 7 THEN 'July'
			WHEN num_month_in_year = 8 THEN 'August'
	 		WHEN num_month_in_year = 9 THEN 'September'
			WHEN num_month_in_year = 10 THEN 'October'
			WHEN num_month_in_year = 11 THEN 'November'
			WHEN num_month_in_year = 12 THEN 'December'
		END AS name_month_en,
		CASE
			WHEN num_day_in_week = 1 THEN 'Monday'
			WHEN num_day_in_week = 2 THEN 'Tuesday'
			WHEN num_day_in_week = 3 THEN 'Wednesday'
			WHEN num_day_in_week = 4 THEN 'Thursday'
			WHEN num_day_in_week = 5 THEN 'Friday'
			WHEN num_day_in_week = 6 THEN 'Saturday'
			WHEN num_day_in_week = 7 THEN 'Sunday'
		END AS name_day_en
	FROM date3
), date5 AS (
	SELECT *, 
		substring(name_month_en from 1 for 3) AS name_month_abbreviated_en,
		substring(name_day_en from 1 for 2) AS name_day_abbreviated_en
	FROM date4
)
INSERT INTO prod.dim_date
	SELECT key, date,
		date_us_format, date_us_short_format, date_iso_format, 
		num_year, 
		num_month_in_year,
		num_week_in_year,num_week_in_month,
		num_day_in_year, num_day_in_month, num_day_in_week,
		name_month_en, name_month_abbreviated_en, name_day_en, name_day_abbreviated_en
	FROM date5;
$$;

DO $$
DECLARE date_dim_not_empty INTEGER;
BEGIN
SELECT count(*) FROM (SELECT 1 FROM prod.dim_date LIMIT 1) as _ INTO date_dim_not_empty;
IF date_dim_not_empty = 0 THEN
CALL populate_dim_date();
END IF;
END $$;
