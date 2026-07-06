# Smart Energy Data Platform

This project is an end-to-end Azure Data Engineering solution built using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, and dbt.

The goal of the project is to build a modern Medallion Architecture that ingests smart meter energy data, transforms it into analytics-ready datasets, and prepares it for downstream forecasting and reporting.

---

## Architecture

<p align="center">
  <img src="screenshots/architecture.png" alt="Smart Energy Data Platform Architecture" width="1000">
</p>

---

## Tech Stack

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- dbt
- Git & GitHub

---

## Data Sources

The project combines multiple datasets:

- Smart meter household energy consumption
- Household metadata
- Historical weather data
- UK bank holidays

---

## Project Structure

```text
adf/            Azure Data Factory artifacts
databricks/     PySpark notebooks
dbt/            Gold models, tests and documentation
screenshots/    Project screenshots
```

---

## Pipeline

### 1. Data Ingestion (Azure Data Factory)

A metadata-driven Azure Data Factory pipeline loads data into the Raw layer.

Current workflow:

- Reads ingestion configuration from JSON
- Iterates through configured datasets
- Copies source files into the Raw layer
- Archives processed files
- Deletes successfully processed source files

*(ADF pipeline screenshot will be added here.)*

---

### 2. Silver Layer (Azure Databricks)

The Silver layer standardizes and cleans the raw datasets before they are used for analytics.

Current transformations include:

- Column standardization
- Data type conversion
- Basic data quality checks
- Delta Lake conversion
- Unity Catalog registration

*(Databricks notebook screenshot will be added here.)*

---

### 3. Gold Layer (dbt)

dbt reads the Silver Delta tables and builds a dimensional model using a Star Schema.

Current models include:

**Dimensions**

- dim_household
- dim_date

**Facts**

- fact_energy_daily
- fact_weather_daily

The project also includes dbt tests, documentation, and model lineage.

*(dbt lineage screenshot will be added here.)*

---

## Current Status

### Completed

- ✅ Metadata-driven Azure Data Factory pipeline
- ✅ Raw, Silver and Gold architecture
- ✅ Azure Databricks transformations
- ✅ Delta Lake implementation
- ✅ Unity Catalog integration
- ✅ dbt Gold models
- ✅ dbt tests
- ✅ dbt documentation

### Planned

- Watermark-based incremental ingestion
- Weather enrichment
- Forecasting using Prophet
- Power BI dashboard
- Performance optimization

---

## Repository

```text
adf/            Data ingestion
databricks/     Silver layer transformations
dbt/            Gold layer models
screenshots/    Documentation images
```

---

## Notes

This repository focuses on the engineering implementation rather than the datasets themselves. Public datasets are used for learning and portfolio purposes.