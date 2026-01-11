# Finance Unified Base

## Pipeline Steps

### Banco Estado Credit Card Statements (MVP)

Steps summary:
- extract_statements: n8n automation that pulls statements from email sources.
- remove_password: Python endpoint that removes PDF passwords when present.
- parse_document: Python endpoint that converts PDF documents to JSON.
- structure_data: Python endpoint that turns semi-structured JSON into tables.
- process_data: Python endpoint that builds denormalized sheets for analytics.

Sources and targets:
- Source: Gmail, bronze.landing or bronze.raw depending on step.
- Target: bronze.landing/raw for early stages; silver.discovery for outputs.

Formats and partitions:
- Formats: pdf for ingestion, parquet for processed data.
- Partitions: reception year, provider/charge type, and provider name.

Parameters and filenames:
- Parameters: start_date/end_date or start_year/end_year depending on step.
- Filenames: YYYYMMDD for raw; YYYY-{table} for processed outputs.

Table names:
- statements, items, upcoming-dues.

Output paths:
- /banco-estado/credit-card-statements/locked-pdf/automated/card_name=smart-visa
- /banco-estado/credit-card-statements/pdf/card_name=smart-visa
- /banco-estado/credit-card-statements/json/card_name=smart-visa
- /banco-estado/credit-card-statements/csv/card_name=smart-visa
- /banco-estado/credit-card-items/csv/card_name=smart-visa
- /banco-estado/credit-card-upcoming-dues/csv/card_name=smart-visa
- /banco-estado/credit-card-statements/card_name=smart-visa/
- /banco-estado/credit-card-items/card_name=smart-visa/
- /banco-estado/credit-card-unified-base/card_name=smart-visa/
- /banco-estado/unified-base/

## Datalake Paths

General processes:
- extract_statements: email ingestion for billing documents.
- remove_password: decrypt PDFs when a password is present.
- parse_document: convert PDFs to JSON for downstream use.
- structure_data: normalize semi-structured JSON to tabular outputs.
- process_data: transform tables without changing row counts; add columns.
- unify_providers: group providers by type (cards, utilities, HOA, etc.).
- create_unified_base: unify data across providers to common entities.
- create_analytics_table: produce denormalized sheets for dashboards.

Sources:
- Gmail, bronze.landing, bronze.raw, silver.discovery, gold.discovery.

Targets:
- bronze.landing/raw for early steps; silver/gold for processed outputs.

Formats:
- pdf for ingestion; parquet for transformed data.

Partitions:
- Reception year, provider/charge type, provider.

Parameters:
- provider type, provider, date range or year range as required.

Filenames and tables:
- YYYYMMDD and YYYY-{table} filename formats.
- Tables: documents, items, upcoming-dues, consumptions, entities, charges,
  payments, domains, finance-unified-base.

### Bronze

Domain: banco-estado. Tables: credit-card-statements, credit-card-items. Partition: smart-visa.

1) PDF files with password:
	/landing/banco-estado/credit-card-statements/locked-pdf/automated/card_name=smart-visa

2) PDF files without password:
	/raw/banco-estado/credit-card-statements/pdf/card_name=smart-visa

3) JSON files:
	/raw/banco-estado/credit-card-statements/json/card_name=smart-visa

4) Standard CSV files:
	- /raw/banco-estado/credit-card-statements/csv/card_name=smart-visa
	- /raw/banco-estado/credit-card-items/csv/card_name=smart-visa
	- /raw/banco-estado/credit-card-upcoming-dues/csv/card_name=smart-visa

### Silver

Discovery tables:
- /discovery/banco-estado/credit-card-statements/card_name=smart-visa/
- /discovery/banco-estado/credit-card-items/card_name=smart-visa/
- /discovery/banco-estado/credit-card-upcoming-dues/card_name=smart-visa/
- /discovery/banco-estado/unified-base

## Google Storage Commands

Copy files to Cloud Storage:
`gsutil -m cp -r "${LOCAL_DIR}"/* "gs://${BUCKET_NAME}/"`

## Environment Variables (English)

The app reads environment variables via python-dotenv for local runs.
In Docker, pass variables with `--env-file .env` or individual `-e NAME=value`.

Required variables:
- OPENAI_API_KEY: API key for OpenAI requests used in PDF parsing.
- API_KEY: API key to protect Flask endpoints (header X-API-Key).
- BUCKET_NAME: Primary GCS bucket name for bronze/raw paths.
- PREFIX: GCS prefix for locked PDFs when applicable.
- PREFIX_PDF_UNLOCKED: GCS prefix for unlocked PDFs.

Service account fields (per-field configuration):
- GCP_SERVICE_ACCOUNT_TYPE
- GCP_SERVICE_ACCOUNT_PROJECT_ID
- GCP_SERVICE_ACCOUNT_PRIVATE_KEY_ID
- GCP_SERVICE_ACCOUNT_PRIVATE_KEY
- GCP_SERVICE_ACCOUNT_CLIENT_EMAIL
- GCP_SERVICE_ACCOUNT_CLIENT_ID
- GCP_SERVICE_ACCOUNT_AUTH_URI
- GCP_SERVICE_ACCOUNT_TOKEN_URI
- GCP_SERVICE_ACCOUNT_AUTH_PROVIDER_X509_CERT_URL
- GCP_SERVICE_ACCOUNT_CLIENT_X509_CERT_URL
- GCP_SERVICE_ACCOUNT_UNIVERSE_DOMAIN

Optional variables:
- PASSWORD_PDF: Password used for locked statements, when needed.
- SILVER_BUCKET_NAME: Secondary bucket for silver outputs (defaults to BUCKET_NAME).
- SOURCE_TYPE: Source type hint (e.g., gcs, local, gmail).
- LOCAL_PATH: Local input folder (default cuentas_pdf).
- OUTPUT_LOCAL_PATH: Local output folder (default unlocked_pdf).

Docker note:
- Do not commit secrets. Pass Docker-only overrides at runtime, e.g.
  -e SERVICE_ACCOUNT_JSON_DOCKER=/app/secrets/service_account.json

