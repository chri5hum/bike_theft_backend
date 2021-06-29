CREATE OR REPLACE FUNCTION get_last_timestamp() RETURNS integer AS $$
BEGIN
	RETURN (
		SELECT CASE WHEN MAX(loaded_until) IS NULL
				THEN 1
				ELSE MAX(loaded_until)
				END
		FROM source.transactions
		WHERE source.transactions.success = True
	);
END
$$ LANGUAGE plpgsql;

INSERT INTO prod.dim_manufacturer (manufacturer_name, manufacturer_company_url, manufacturer_frame_maker,manufacturer_description,manufacturer_short_name,manufacturer_slug)
SELECT 
	e.manufacturer_name,
	e.manufacturer_company_url,
	e.manufacturer_frame_maker,
	e.manufacturer_description,
	e.manufacturer_short_name,
	e.manufacturer_slug
FROM source.bikes e
WHERE e.occurred_at >= get_last_timestamp()
ON CONFLICT (manufacturer_name) DO nothing;

INSERT INTO prod.dim_location (postal, city, country, lat, long)
SELECT 
	e.address_postal,
	e.address_city,
	e.address_country,
	e.address_lat,
	e.address_lon
FROM source.bikes e
WHERE e.occurred_at >= get_last_timestamp()
ON CONFLICT (postal) DO nothing;

DROP TABLE IF EXISTS staging.fact_bike;

CREATE TABLE staging.fact_bike(
    id serial PRIMARY KEY,
    title VARCHAR,
    manufacturer_key INT,
	location_key INT,
 	date_key INT,
    model VARCHAR,
    occurred_at INT,
	CONSTRAINT fk_manufacturer
		FOREIGN KEY(manufacturer_key)
		REFERENCES prod.dim_manufacturer(id),
	CONSTRAINT fk_location
		FOREIGN KEY(location_key)
		REFERENCES prod.dim_location(id),
	CONSTRAINT fk_date
		FOREIGN KEY(date_key)
		REFERENCES prod.dim_date(id)
);

INSERT INTO staging.fact_bike(title, manufacturer_key, location_key, date_key, model, occurred_at)
select x.title, prod.dim_manufacturer.id, prod.dim_location.id, get_date_primary_key(to_timestamp(CAST(x.occurred_at AS int))), x.model, x.occurred_at
FROM source.bikes as x
left join prod.dim_manufacturer
on x.manufacturer_name = prod.dim_manufacturer.manufacturer_name
left join prod.dim_location
on x.address_postal = prod.dim_location.postal
WHERE x.occurred_at >= get_last_timestamp();
