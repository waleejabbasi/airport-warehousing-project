--MERGING INTO DATE DIMESNION TABLE
 USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRPORTDB;
USE SCHEMA MYSCHEMA;
 
 
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
