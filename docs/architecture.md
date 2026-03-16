# Architecture

## Purpose

This document describes the implemented architecture for the Financial Signals Lakehouse repo as it exists in the current Databricks bundle and job wiring.

## Design Goals

- keep ingestion, standardization, and analytics responsibilities separate
- use simple public data sources to demonstrate practical lakehouse patterns
- centralize table naming and environment conventions
- keep audit logging isolated from business-facing schemas
- make orchestration understandable from the repo without hidden notebook logic

## High-Level Flow

Public APIs
-> Bronze ingestion jobs
-> Silver standardization jobs
-> Gold analytical outputs
-> Validation and audit review

## Medallion Layers

### Bronze

Purpose:

- land source records with minimal transformation
- preserve source fidelity where practical
- attach ingestion metadata
- support repeatable append-oriented loads

Implemented domains:

- market prices
- FX rates
- macro indicators

Key files:

- `src/bronze/ingest_market_data.py`
- `src/bronze/ingest_fx_data.py`
- `src/bronze/ingest_macro_data.py`

### Silver

Purpose:

- enforce data types
- remove obviously invalid records
- deduplicate source rows
- standardize fields for downstream joins and analytics

Implemented domains:

- `src/silver/transform_market_data.py`
- `src/silver/transform_fx_data.py`
- `src/silver/transform_macro_data.py`

### Gold

Purpose:

- create reporting- and signal-ready outputs
- derive change metrics and trend indicators
- publish stable analytical tables

Currently active in the gold runner:

- `daily_market_snapshot`
- `fx_trend_signals`
- `macro_indicator_trends`
- `cross_signal_summary`

## Catalog And Schemas

The current project uses the `fin_signals_dev` catalog with these schemas:

- `bronze`
- `silver`
- `gold`
- `audit`

Names are defined in `src/common/config.py` and should be treated as the primary source of truth.

## Orchestration

### Bundle Entry

- `databricks.yml`

### Bootstrap Job

- Job name: `bootstrap_environment`
- Purpose: create catalog, schemas, and audit objects

### Daily Pipeline Job

- File: `resources/jobs/daily_pipeline_job.yml`
- Job name: `financial_signals_daily_pipeline`

Current task flow:

1. Price Bronze ingest
2. Price Silver transform
3. FX Bronze ingest
4. FX Silver transform
5. Macro Bronze ingest
6. Macro Silver transform
7. Gold analytics
8. Validation

Dependency notes:

- each Silver task depends on its matching Bronze task
- Gold depends on Price Silver, FX Silver, and Macro Silver
- validation depends on Gold

There is also a separate historical backfill workflow for Price and FX so the daily job can stay in snapshot mode.

## Audit Pattern

Audit logging is intentionally isolated under `fin_signals_dev.audit`.

Current audit implementation:

- writer: `src/common/audit.py`
- main table used in code: `fin_signals_dev.audit.pipeline_runs`

The audit layer is part of the operational control plane and should not be merged into Bronze, Silver, or Gold schemas.

## Reality Checks

- Python pipeline code is more complete than some SQL and notebook assets.
- Validation now performs data freshness, duplicate-key, history-depth, and Gold output checks, with macro freshness assessed against lower-frequency public indicator cadence.
- Some configured tables exist before their full SQL DDL coverage or active job usage is complete.
