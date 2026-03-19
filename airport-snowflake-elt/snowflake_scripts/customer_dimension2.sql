--MERGING INTO CUSTOMER DIMENSION NEW RECORDS

USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRPORTDB;
USE SCHEMA MYSCHEMA;
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
VALUES(s.passenger_id,s.first_name,s.last_name,s.Gender,s.Age,s.Nationality,s.arrival_airport);