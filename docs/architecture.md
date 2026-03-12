# Architecture

## Purpose
This document describes the architecture for the Financial Signals Lakehouse project.

## Design Principles
- simple, repeatable, governed
- secure by default
- clear separation of raw, standardised, and curated data
- reusable transformation logic
- auditable pipeline runs

## High-Level Flow

External Sources
    ->
Bronze Ingestion
    ->
Silver Standardisation
    ->
Gold Analytics
    ->
SQL / dashboards / downstream consumers

## Layers

### Bronze
Purpose:
- land raw source data with minimal transformation
- preserve source fidelity
- support replay and traceability

Characteristics:
- append-only where possible
- includes ingestion timestamp
- includes source name
- includes batch or run identifier

### Silver
Purpose:
- apply schema
- standardise column names and data types
- deduplicate and validate
- conform dates, symbols, and codes

Characteristics:
- business-ready but not presentation-ready
- supports joins across source domains
- enforces quality checks

### Gold
Purpose:
- publish curated analytics tables
- support reporting and consulting-style business questions

Characteristics:
- denormalised where appropriate
- metric-driven
- stable table contracts for downstream users

## Initial Domains
- market prices
- FX rates
- macroeconomic indicators

## Audit Layer
The audit schema stores:
- pipeline run status
- row counts
- start and end timestamps
- source system name
- error details where relevant

## Orchestration
Initial orchestration will be daily batch.
Later phases may add more frequent incremental ingestion if justified by source freshness and business value.
