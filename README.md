# Azure NYC-Taxi data

## Introduction
This project serves as a comprehensive implementation to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing Azure Cloud Services and Databricks Lakehouse Architecture as a core.

## System Architecture
![System Architecture](https://github.com/maihuy-dataguy/Azure-NYC-Taxi-project/blob/main/pics/flow.png)

The project is designed with the following components:

- **Data Source**: We use 2 files taxi_zone_lookup.csv and trip_type.csv alongside with NYC-Taxi trip data API from `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page` for our pipeline.
- **Azure Data Factory (ADF)**: Responsible for ingesting data and storing fetched data in azure data lakehouse at bronze layer.
- **Databricks Lakehouse**: Using medallion architecture, we transfer our data between these layers including bronze, silver, gold layers
    - Bronze layer: Used for storing raw data ingested from ADF.
    - silver layer: used for storing transformed data (parquet format) through spark using databrick notebook.
    - gold layer: Used for storing transformed (delta format), creating delta tables on top of data, thanks to transactional log created from delta lake, using sql (sparkSQL) to query data for  auditing, reports in combination with data versioning and time travel to enhance ACID features and data integrity in azure data lake.
- **Power BI**: For creating dashboards to support related reports. 


