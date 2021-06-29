-- DROP TABLE IF EXISTS prod.dim_manufacturer CASCADE;
CREATE TABLE IF NOT EXISTS prod.dim_manufacturer (
	id serial PRIMARY KEY,
	manufacturer_name VARCHAR( 50 ) UNIQUE NOT NULL,
	manufacturer_company_url VARCHAR ( 255 ),
	manufacturer_frame_maker BOOLEAN,
	manufacturer_description TEXT,
	manufacturer_short_name VARCHAR( 50 ),
	manufacturer_slug VARCHAR( 50 )
);

-- DROP TABLE IF EXISTS prod.dim_location CASCADE;
CREATE TABLE IF NOT EXISTS prod.dim_location (
	id serial PRIMARY KEY,
	postal VARCHAR( 3 ) UNIQUE NOT NULL,
	city VARCHAR ( 255 ),
	country VARCHAR ( 255 ),
	lat VARCHAR,
	long VARCHAR
);


-- DROP TABLE IF EXISTS prod.fact_bike;
CREATE TABLE IF NOT EXISTS prod.fact_bike(
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