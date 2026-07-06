# Smart Energy Data Platform

This project demonstrates an end-to-end Azure Data Engineering pipeline that ingests smart meter energy data, transforms it using Databricks, models it into a star schema with dbt, and prepares it for analytics and forecasting.

The project was built to gain hands-on experience with a modern Azure data stack while following industry practices such as metadata-driven ingestion, Medallion Architecture, Delta Lake, and modular data modeling.

---

## Architecture

*(Architecture diagram will be added here.)*

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
adf/           Azure Data Factory artifacts
databricks/    PySpark notebooks
dbt/           Gold models, tests and documentation
screenshots/   Project screenshots
```

---

## Pipeline

### 1. Ingestion (ADF)

- Reads ingestion configuration from JSON
- Loads source files into the Raw layer
- Archives processed files
- Removes processed source files

*(ADF screenshot will be added.)*

---

### 2. Silver Layer (Databricks)

The Silver layer standardizes and cleans the raw datasets.

Current transformations include:

- Column standardization
- Data type conversion
- Delta Lake conversion
- Unity Catalog registration

*(Databricks screenshot will be added.)*

---

### 3. Gold Layer (dbt)

dbt creates analytics-ready models from the Silver Delta tables.

Current models:

**Dimensions**

- dim_household
- dim_date

**Facts**

- fact_energy_daily
- fact_weather_daily

The project also includes dbt tests and documentation.

*(dbt lineage screenshot will be added.)*

---

## Current Status

Completed

- Metadata-driven Azure Data Factory pipeline
- Raw, Silver and Gold architecture
- Databricks transformations
- Delta Lake implementation
- dbt models
- dbt tests
- dbt documentation

Planned

- Watermark-based incremental loading
- Weather enrichment
- Forecasting using Prophet
- Power BI dashboard
- Performance optimization

---

## Repository

```
adf/           Data ingestion
databricks/    Silver transformations
dbt/           Gold layer
```

---

## Notes

This repository focuses on the engineering implementation rather than the datasets themselves. Public datasets are used for learning and portfolio purposes.