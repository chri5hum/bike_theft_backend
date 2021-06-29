INSERT INTO prod.fact_bike
			(title, manufacturer_key, location_key, date_key, model, occurred_at)
SELECT e.title, e.manufacturer_key, e.location_key, e.date_key, e.model, e.occurred_at
FROM staging.fact_bike e;
TRUNCATE TABLE staging.fact_bike;
SELECT * from prod.fact_bike