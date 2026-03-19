--INCREMENTAL LOADING IN FACT TABLE
  USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRPORTDB;
USE SCHEMA MYSCHEMA;
  
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
