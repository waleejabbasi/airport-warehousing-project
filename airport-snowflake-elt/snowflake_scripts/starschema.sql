USE ROLE ACCOUNTADMIN

USE SCHEMA AIRPORTDB.MYSCHEMA

--CREATE CUSTOMER DIMENSION TABLE
CREATE OR REPLACE TABLE cust_dim(
    cust_dim_key INT AUTOINCREMENT,
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    Gender STRING,
    Age INT,
    Nationality STRING,
    arrival_airport STRING
);

select * from cust_dim
INSERT INTO cust_dim 
(passenger_id,first_name,last_name,Gender,Age,Nationality,arrival_airport)
SELECT DISTINCT passenger_id,
first_name,
last_name,
Gender,
Age,
Nationality,
arrival_airport
FROM raw_data_table


--CREATING AIRPORT DIMENSION TABLE
CREATE OR REPLACE TABLE airport_dim(
    airport_dim_key INT AUTOINCREMENT,
    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    Continents STRING
)

INSERT INTO airport_dim
(airport_name,airport_country_code,country_name,Continents)
SELECT DISTINCT airport_name,
airport_country_code,
country_name,
Continents
FROM raw_data_table


--CREATING DIMESNIONAL TABLE FOR PILOT
CREATE OR REPLACE TABLE pilot_dim(
pilot_dim_key INT AUTOINCREMENT,
pilot_name STRING
)

INSERT INTO pilot_dim
(pilot_name)
SELECT  DISTINCT pilot_name
FROM raw_data_table


--CREATE DATE DIMENSIONAL TABLE

CREATE OR REPLACE TABLE date_dim(
    date_dim_key INT AUTOINCREMENT,
    departure_date DATE,
    year INT,
    month STRING,
    day INT
)

INSERT INTO date_dim
(departure_date,year,month,day)
SELECT DISTINCT departure_date,
YEAR(departure_date) AS year,
MONTHNAME(departure_date) AS month,
DAY(departure_date) AS day
FROM raw_data_table


--CREATING FACT TABLE

CREATE OR REPLACE TABLE airport_fact(
    cust_dim_key INT,
    airport_dim_key INT,
    pilot_dim_key INT,
    date_dim_key INT,
    flight_status STRING
)

INSERT INTO airport_fact
SELECT c.cust_dim_key,
a.airport_dim_key,
p.pilot_dim_key,
d.date_dim_key,
r.flight_status
FROM
raw_data_table r 
INNER JOIN cust_dim c
ON r.passenger_id=c.passenger_id
INNER JOIN airport_dim_clean a
ON a.airport_name=r.airport_name
AND a.airport_country_code=r.airport_country_code
INNER JOIN date_dim d
ON d.departure_date=r.departure_date
INNER JOIN pilot_dim p
ON r.pilot_name=p.pilot_name





