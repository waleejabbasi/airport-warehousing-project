USE ROLE ACCOUNTADMIN
USE SCHEMA AIRPORTDB.MYSCHEMA


--CREATING STREAM
--CREATE OR REPLACE STREAM raw_stream 
--ON TABLE raw_data_table

--COPY STREAM TO STAGING TABLE
CREATE OR REPLACE TEMP TABLE raw_staging AS 
SELECT * FROM raw_stream

--MERGING INTO CUSTOMER DIMENSION NEW RECORDS
MERGE INTO cust_dim AS target USING(
    SELECT DISTINCT passenger_id,
    first_name,
    last_name,
    Gender,
    Age,
    Nationality,
    arrival_airport
    FROM raw_staging
) as s
on target.passenger_id=s.passenger_id
WHEN NOT MATCHED THEN
INSERT(passenger_id,first_name,last_name,Gender,Age,Nationality,arrival_airport)
VALUES(s.passenger_id,s.first_name,s.last_name,s.Gender,s.Age,s.Nationality,s.arrival_airport)

--MERGING INTO airport dimesnion table
MERGE INTO airport_dim AS target USING(
    SELECT DISTINCT airport_name,
    airport_country_code,
    country_name,
    continents
    FROM raw_staging
) as source
on target.airport_name=source.airport_name
AND target.airport_country_code=source.airport_country_code
WHEN NOT MATCHED THEN
INSERT(airport_name, airport_country_code, country_name, Continents)
VALUES(source.airport_name, source.airport_country_code, source.country_name, source.Continents);

--MERGING INTO pilot dimension table
MERGE INTO pilot_dim AS target
USING (
    SELECT DISTINCT pilot_name
    FROM raw_staging
) AS source
ON target.pilot_name = source.pilot_name
WHEN NOT MATCHED THEN
  INSERT (pilot_name)
  VALUES (source.pilot_name);

  --MERGING INTO DATE DIMESNION TABLE
  MERGE INTO date_dim AS target
USING (
    SELECT DISTINCT departure_date,
           YEAR(departure_date) AS year,
           MONTHNAME(departure_date) AS month,
           DAY(departure_date) AS day
    FROM raw_staging
) AS source
ON target.departure_date = source.departure_date
WHEN NOT MATCHED THEN
  INSERT (departure_date, year, month, day)
  VALUES (source.departure_date, source.year, source.month, source.day);


  --INCREMENTAL LOADING IN FACT TABLE
  INSERT INTO airport_fact (cust_dim_key, airport_dim_key, pilot_dim_key, date_dim_key, flight_status)
SELECT c.cust_dim_key,
       a.airport_dim_key,
       p.pilot_dim_key,
       d.date_dim_key,
       r.flight_status
FROM raw_staging r
INNER JOIN cust_dim c ON r.passenger_id = c.passenger_id
INNER JOIN airport_dim a 
    ON r.airport_name = a.airport_name
   AND r.airport_country_code = a.airport_country_code
INNER JOIN pilot_dim p ON r.pilot_name = p.pilot_name
INNER JOIN date_dim d ON r.departure_date = d.departure_date;

/*
TESTING INCREMENTAL LOADING
INSERT INTO raw_data_table
(passenger_id, first_name, last_name, Gender, Age, Nationality,
 airport_name, airport_country_code, country_name,airport_continent,
 Continents, departure_date, arrival_airport, pilot_name, flight_status)
VALUES
('P1001','Ali','Khan','Male',30,'Pakistan',
 'JFK','US','United States','North America',
 'North America','2026-03-16','LHR','John Smith','On Time')
 
SELECT *
FROM airport_fact f
INNER JOIN cust_dim c
on c.cust_dim_key=f.cust_dim_key
WHERE c.passenger_id='P1001'
/*