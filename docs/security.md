# Security

## Objective
This project uses public data but is designed according to production-minded security standards suitable for a financial consultancy demonstration.

## Principles
- least privilege
- no credentials in code
- governed access to datasets
- separation of environments
- auditable execution

## Secrets Management
- all API keys and sensitive connection values must be stored in Databricks secret scopes
- secrets must never be committed to Git
- local development must use ignored environment files if required

## Access Model
Suggested roles:

### Data Engineer
- read/write: bronze
- read/write: silver
- read: gold
- read/write: audit

### Analyst
- read: gold
- no write access to pipeline tables
- no access to secrets

### Demo Consumer / Hiring Review Audience
- read-only access to selected Gold outputs only

## Data Classification
Initial source classification:
- market data: public
- FX data: public
- macroeconomic indicators: public

Despite using public data, the project should still apply:
- controlled access
- documented retention
- auditability
- secure credential handling

## Prohibited Practices
- hardcoded secrets in notebooks or Python files
- direct credentials in job definitions
- uncontrolled personal workspace copies of production-like data
- ad hoc manual edits to managed tables

## Audit Requirements
Each pipeline run should capture:
- pipeline name
- run identifier
- source name
- row count
- status
- error message if failed
- started timestamp
- completed timestamp
