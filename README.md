Project Title: Airport Warehousing Project
Project Overview

This project demonstrates a complete end-to-end ETL pipeline for airline and airport data. Raw CSV datasets are ingested, cleaned, and transformed using Python (Pandas), then stored in AWS S3. Data is loaded into Snowflake using Snowpipe for continuous ingestion. A star schema is implemented for analytics, and incremental loading is handled using Streams & Merge in Snowflake. The entire process is orchestrated using Apache Airflow, enabling automated, scheduled execution of the ETL pipeline.

ETL Process Flow

Extract:

Raw CSV files are read using Pandas.

Data is checked for null values, duplicates, and incorrect data types.

Transform:

Data is cleaned and normalized (standardized dates, removed duplicates).

Dimension and fact table transformations are applied.

Load:

Cleaned CSVs are uploaded to AWS S3.

Snowpipe continuously loads data into Snowflake staging tables.

Incremental loading is performed using Streams & Merge, updating dimension and fact tables efficiently.

Star Schema Design

Fact Table: airport_fact

Measures: flight_count, passenger_count, cargo_weight

Foreign Keys: customer_id, pilot_id, date_id, airport_id

Dimension Tables:

customer_dim – Customer details

pilot_dim – Pilot details

date_dim – Calendar details

airport_dim – Airport details

Architecture Diagram
![ETL Architecture](architecturediagram2.png)

Tools & Technologies

Python (Pandas) – Data extraction, cleaning, and transformation

AWS S3 – Cloud storage for raw and staged data

Snowflake – Data warehouse with:

Snowpipe for continuous ingestion

Streams & Merge for incremental loading

Apache Airflow – ETL orchestration and scheduling

SQL – Table creation, star schema design, incremental merges

Power BI / SQL Queries – Data visualization and analytics

Key Features

Fully automated end-to-end ETL pipeline

Incremental loading for efficient data updates

Star schema optimized for analytics

Orchestrated and monitored via Airflow
