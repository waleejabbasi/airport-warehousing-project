--MERGING INTO airport dimesnion table
USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRPORTDB;
USE SCHEMA MYSCHEMA;

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