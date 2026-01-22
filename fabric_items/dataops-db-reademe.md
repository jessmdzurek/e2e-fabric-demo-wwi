# DataOps Database (Fabric SQL DB)

## Purpose
The DataOps database acts as the control plane for the platform:
- Registers source systems
- Defines datasets to ingest
- Applies lightweight data contracts
- Logs ingestion executions

## Key Tables
- ref.SourceSystem – source systems and gateway association
- cfg.Dataset – dataset-level ingestion configuration
- dc.DataContract – lightweight schema expectations per dataset
- log.IngestRun – execution logging per dataset per run

## Source Control Model
- Database objects are authored in a Fabric SQL Database (Dev)
- Objects are source-controlled via Fabric Git integration
- The generated `.SQLDatabase` project is the deployable artifact
- Ad-hoc or seed scripts live outside the project and are not auto-deployed

## Design Notes
- This is intentionally lightweight to support rapid onboarding
- Bronze is source-aligned; Silver is optionally curated
- Contracts enforce required columns only (WARN or BLOCK)
- Column mappings and advanced data quality rules can be added later
