# Financial Signals Lakehouse

## Overview
Financial Signals Lakehouse is a Databricks project designed to demonstrate secure, governed, and production-style ingestion of market and macroeconomic data using a medallion architecture.

The project is intended to:
- showcase Databricks engineering patterns for a financial consultancy audience
- reinforce Databricks Associate exam topics through hands-on implementation
- provide a secure and well-documented example of external data ingestion, transformation, and analytics delivery

## Objectives
- Ingest public financial and macroeconomic datasets into Bronze tables
- Clean and standardise data into Silver tables
- Publish analytics-ready Gold tables for reporting and downstream use
- implement source control, documentation, audit logging, and security controls from day one

## Architecture
The project follows a Bronze / Silver / Gold medallion pattern:

- Bronze: raw landed source data with ingestion metadata
- Silver: validated, typed, deduplicated, standardised data
- Gold: curated business-facing analytics tables

## Initial Scope
Phase 1 includes:
- market data ingestion
- FX data ingestion
- macroeconomic indicator ingestion
- one scheduled pipeline
- one audit logging model
- initial Gold analytics tables

## Security Principles
- no secrets in source code
- credentials retrieved from secret scopes
- least-privilege access model
- governed data structure using catalog/schema/table separation
- audit logging for ingestion and transformation runs

## Repository Structure
See `/docs/architecture.md` and `/docs/data_model.md` for details.

## Status
Phase: Foundation / project bootstrap
