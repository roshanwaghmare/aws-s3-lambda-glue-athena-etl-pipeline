# AWS Serverless JSON-to-Parquet ETL Pipeline

A serverless, event-driven ETL pipeline that automatically reads nested JSON order data from Amazon S3, transforms it into a tabular structure, converts it to Apache Parquet, catalogs the output with AWS Glue, and makes it queryable using Amazon Athena.

The pipeline uses deterministic output filenames so that processing the same source file again overwrites its existing Parquet output instead of creating duplicate files.

# AWS Services Used
Amazon S3 for raw JSON and curated Parquet storage.
AWS Lambda for serverless ETL processing.
AWS Glue Crawler for schema discovery.
AWS Glue Data Catalog for table metadata.
Amazon Athena for SQL-based data analysis.
Amazon CloudWatch Logs for monitoring and troubleshooting.
AWS IAM for permissions and access control.

---

## Architecture

```text
JSON file
   |
   v
Amazon S3 Raw Prefix
orders_json_incoming/
   |
   | S3 ObjectCreated event
   v
AWS Lambda
   |
   |-- Read JSON file
   |-- Flatten nested order and product data
   |-- Create Pandas DataFrame
   |-- Convert DataFrame to Parquet
   |-- Upload Parquet file
   v
Amazon S3 Curated Prefix
orders_parquet_datalake/
   |
   v
AWS Glue Crawler
   |
   v
AWS Glue Data Catalog
   |
   v
Amazon Athena

