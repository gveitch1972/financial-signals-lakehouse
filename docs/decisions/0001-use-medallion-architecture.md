# ADR 0001: Use Medallion Architecture

## Status
Accepted

## Context
The project requires a clear and auditable pattern for handling raw source ingestion, standardisation, and business-facing analytics outputs.

## Decision
Use Bronze, Silver, and Gold layers.

## Consequences
Positive:
- better traceability
- cleaner separation of concerns
- easier debugging
- aligns with common Databricks patterns

Negative:
- slightly more objects to manage
- requires naming discipline
