# Medicare Medallion Architecture Data Pipeline

An end-to-end data engineering pipeline built in **Databricks** using **PySpark**, applying the **Medallion Architecture** (Bronze → Silver → Gold) to real CMS Medicare synthetic claims data.

## Overview

This project ingests, cleans, and joins multi-table healthcare claims data to answer a real analytical question:

> **Does an Alzheimer's disease diagnosis affect average inpatient hospital cost?**

## Dataset

[CMS DE-SynPUF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf) — a synthetic (fake, privacy-safe) dataset released by the U.S. Centers for Medicare & Medicaid Services, built to mirror the structure and complexity of real Medicare claims data.

Files used:
- Beneficiary Summary File (patient demographics and chronic condition flags)
- Inpatient Claims (hospital stay records and payment amounts)
- Outpatient Claims (clinic visit records)

## Architecture

### Bronze — Raw Ingestion
Raw CSV files loaded as-is into Databricks Volumes and read into Spark DataFrames, with no transformations applied. This preserves an untouched copy of the source data at all times.

### Silver — Cleaning & Standardization
- Converted numerically-encoded dates (e.g., `19230501`) into proper date types
- Flagged missing/null claim dates with a dedicated boolean column, rather than silently dropping or guessing values
- Translated coded categorical fields (e.g., sex: `1`/`2`) into human-readable labels (`Male`/`Female`)
- Each cleaned table persisted as its own permanent Delta table

### Gold — Analytical Summary
- Joined Beneficiary and Inpatient tables on patient ID
- Translated the Alzheimer's diagnosis flag into a readable `Yes`/`No` column
- Aggregated average inpatient cost, grouped by Alzheimer's diagnosis status
- Result persisted as a final summary table

## Key Engineering Decisions

- **Missing data was flagged, not deleted** — preserving all other patient information rather than discarding rows with incomplete date fields, since deletion is a one-way, irreversible operation.
- **Each pipeline stage is saved as its own permanent table** — Bronze data is never modified in place, ensuring the raw source is always recoverable and every transformation step is auditable.

## Tech Stack
`Databricks` · `PySpark` · `Delta Lake` · `Python`

## Results

| Alzheimer's Diagnosis | Average Inpatient Cost |
|---|---|
| Yes | $9,427.08 |
| No | $9,692.99 |

## Future Work
- Incorporate Carrier Claims and Prescription Drug Event data for a more complete cost picture
- Build a Delta Live Tables pipeline with declarative data quality expectations
- Add Unity Catalog-based access control and lineage tracking
