# AWS Serverless JSON-to-Parquet ETL Pipeline

A serverless, event-driven ETL pipeline that processes nested JSON order data stored in Amazon S3, converts it into Apache Parquet using AWS Lambda, catalogs the Parquet schema with AWS Glue, and makes the transformed data queryable through Amazon Athena.

This project is designed as a practical AWS data engineering and serverless ETL learning project.

## Table of Contents

- [Architecture](#architecture)
- [AWS Services Used](#aws-services-used)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Step 1: Create the S3 Bucket](#step-1-create-the-s3-bucket)
- [Step 2: Create the Lambda Function](#step-2-create-the-lambda-function)
- [Step 3: Add the AWS SDK for pandas Layer](#step-3-add-the-aws-sdk-for-pandas-layer)
- [Step 4: Configure Lambda Permissions](#step-4-configure-lambda-permissions)
- [Step 5: Add the Lambda Code](#step-5-add-the-lambda-code)
- [Step 6: Configure the S3 Trigger](#step-6-configure-the-s3-trigger)
- [Step 7: Perform the Initial Test](#step-7-perform-the-initial-test)
- [Step 8: Verify CloudWatch Logs](#step-8-verify-cloudwatch-logs)
- [Step 9: Create an AWS Glue Database](#step-9-create-an-aws-glue-database)
- [Step 10: Create and Run the Glue Crawler](#step-10-create-and-run-the-glue-crawler)
- [Step 11: Query the Data in Athena](#step-11-query-the-data-in-athena)
- [Duplicate-Prevention Behavior](#duplicate-prevention-behavior)
- [Important Limitations](#important-limitations)
- [Troubleshooting](#troubleshooting)
- [Security Improvements](#security-improvements)
- [Cost Controls and Cleanup](#cost-controls-and-cleanup)
- [Future Enhancements](#future-enhancements)
- [Interview Explanation](#interview-explanation)

---

## Architecture

```text
orders_etl.json
       |
       v
Amazon S3
orders_json_incoming/
       |
       | S3 ObjectCreated event
       v
AWS Lambda
       |
       |-- Read JSON from S3
       |-- Flatten nested order data
       |-- Create Pandas DataFrame
       |-- Convert DataFrame to Parquet
       |-- Write Parquet to S3
       v
Amazon S3
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
       |
       v
SQL query results
```

---

## AWS Services Used

- **Amazon S3**: Stores incoming JSON files and generated Parquet files.
- **AWS Lambda**: Runs the JSON-to-Parquet transformation.
- **AWS SDK for pandas**: Provides Pandas and PyArrow dependencies to Lambda.
- **AWS Glue Crawler**: Discovers the Parquet schema.
- **AWS Glue Data Catalog**: Stores database and table metadata.
- **Amazon Athena**: Queries Parquet data directly from S3.
- **Amazon CloudWatch Logs**: Stores Lambda execution logs.
- **AWS IAM**: Controls access to S3, Glue, and CloudWatch Logs.

---

## How the Pipeline Works

1. A JSON file is uploaded to the `orders_json_incoming/` S3 prefix.
2. An S3 ObjectCreated event invokes the Lambda function.
3. Lambda reads the uploaded JSON object.
4. The `flatten()` function converts nested order, customer, and product data into rows.
5. Pandas creates a DataFrame from the flattened rows.
6. PyArrow converts the DataFrame to Parquet in memory.
7. Lambda writes the Parquet file to `orders_parquet_datalake/`.
8. The Glue Crawler scans the Parquet location and creates or updates a Glue table.
9. Athena queries the table using SQL.

One order can contain multiple products. Therefore, the Lambda creates one flattened row per product.

---

## Repository Structure

```text
aws-serverless-json-to-parquet-etl/
|
|-- lambda/
|   `-- lambda_function.py
|
|-- sample-data/
|   `-- orders_etl.json
|
|-- athena/
|   `-- sample_queries.sql
|
|-- screenshots/
|   |-- s3-structure.png
|   |-- s3-event-notification.png
|   |-- lambda-configuration.png
|   |-- cloudwatch-logs.png
|   |-- glue-crawler.png
|   `-- athena-results.png
|
|-- .gitignore
`-- README.md
```

---

## Prerequisites

Before starting, you need:

- An AWS account
- Permission to create and manage S3, Lambda, IAM, Glue, Athena, and CloudWatch resources
- A sample nested JSON order file
- Basic familiarity with the AWS Management Console

> This guide uses AWS Management Console steps instead of AWS CLI commands.

---

## Step 1: Create the S3 Bucket

Create an S3 bucket to store incoming JSON files and generated Parquet files.

Example bucket name:

```text
namaste-etl-project-2026
```

S3 bucket names must be globally unique. If this name is unavailable, choose another unique name and update the references in this guide.

Create these prefixes inside the bucket:

```text
namaste-etl-project-2026/
|
|-- orders_json_incoming/       # Incoming JSON files
`-- orders_parquet_datalake/    # Generated Parquet files
```

### Input location

```text
s3://namaste-etl-project-2026/orders_json_incoming/
```

### Output location

```text
s3://namaste-etl-project-2026/orders_parquet_datalake/
```

Do not manually upload JSON files into `orders_parquet_datalake/`. That prefix is reserved for Lambda-generated Parquet files.

---

## Step 2: Create the Lambda Function

Open the AWS Lambda console and select:

```text
Create function -> Author from scratch
```

Example configuration:

```text
Function name: json-to-parquet-etl
Runtime: A supported Python runtime compatible with the selected AWS SDK for pandas layer
Architecture: Match the architecture supported by the selected layer
```

You can choose a different function name.

> The Lambda Python runtime and AWS SDK for pandas layer version must be compatible. Check the layer list displayed in your AWS Region and choose the matching runtime-specific layer.

After creating the function, open its code editor.

---

## Step 3: Add the AWS SDK for pandas Layer

The standard Lambda Python runtime does not include Pandas and PyArrow. The function needs both libraries to create Parquet output.

Open:

```text
Lambda function -> Code -> Layers -> Add a layer
```

Select:

```text
AWS layers
```

Choose the AWS SDK for pandas layer that matches your Lambda Python runtime and architecture. For example, if your Region offers a layer named similar to the following for your selected runtime, choose it:

```text
AWSSDKPandas-Python3xx
```

The exact suffix depends on the runtime versions available in your AWS Region.

Without a compatible layer or deployment package, Lambda can return errors such as:

```text
No module named 'pandas'
```

or:

```text
No module named 'pyarrow'
```

---

## Step 4: Configure Lambda Permissions

Open:

```text
Lambda -> Configuration -> Permissions -> Execution role
```

For a learning environment, the following managed policies can be attached to the Lambda execution role:

```text
AmazonS3FullAccess
AWSGlueServiceRole
AWSLambdaBasicExecutionRole
```

### Policy purpose

- `AmazonS3FullAccess`: Allows Lambda to read JSON and write Parquet.
- `AWSGlueServiceRole`: Provides Glue-related permissions. Confirm that the role can call `glue:GetCrawler` and `glue:StartCrawler` if the crawler is started from Lambda.
- `AWSLambdaBasicExecutionRole`: Allows Lambda to write execution logs to CloudWatch Logs.

### Production recommendation

The managed policies above are broad. For production, replace them with a least-privilege policy limited to:

```text
s3:GetObject
s3:PutObject
glue:GetCrawler
glue:StartCrawler
logs:CreateLogGroup
logs:CreateLogStream
logs:PutLogEvents
```

Limit S3 resources to the required bucket and prefixes.

---

## Step 5: Add the Lambda Code

Save the following code as `lambda/lambda_function.py` in the repository and paste it into the Lambda code editor.

```python
import io
import json
import os
from urllib.parse import unquote_plus

import boto3
import pandas as pd


# Reuse AWS clients between Lambda invocations when the execution
# environment remains warm.
s3 = boto3.client("s3")
glue = boto3.client("glue")

# S3 prefix where Lambda writes Parquet files.
OUTPUT_PREFIX = "orders_parquet_datalake/"

# This must match the crawler name created in AWS Glue.
CRAWLER_NAME = "etl_pipeline_crawler"


def flatten(data):
    """
    Convert nested order JSON into a flat table.

    Each product becomes one output row. Order and customer fields
    are repeated for every product belonging to the order.
    """

    # Store every flattened order-product row in this list.
    orders_data = []

    # Loop through each order in the top-level JSON array.
    for order in data:

        # Loop through every product belonging to the current order.
        for product in order.get("products", []):

            # Combine order, customer, and product fields into one row.
            row_orders = {
                "order_id": order.get("order_id"),
                "order_date": order.get("order_date"),
                "total_amount": order.get("total_amount"),
                "customer_id": order.get("customer", {}).get("customer_id"),
                "customer_name": order.get("customer", {}).get("name"),
                "email": order.get("customer", {}).get("email"),
                "address": order.get("customer", {}).get("address"),
                "product_id": product.get("product_id"),
                "product_name": product.get("name"),
                "category": product.get("category"),
                "price": product.get("price"),
                "quantity": product.get("quantity")
            }

            # Add the flattened row to the output collection.
            orders_data.append(row_orders)

    # Convert the list of dictionaries into a Pandas DataFrame.
    return pd.DataFrame(orders_data)


def lambda_handler(event, context):
    """
    Lambda entry point.

    Reads S3 ObjectCreated event records, converts JSON files to
    Parquet, writes the output to S3, and starts the Glue Crawler.
    """

    print("Lambda execution started")
    print(f"Received event: {json.dumps(event)}")

    processed_files = []
    skipped_files = []

    # An S3 event can contain more than one record, so process all records.
    for record in event.get("Records", []):

        # Extract the source bucket from the S3 event.
        bucket_name = record["s3"]["bucket"]["name"]

        # S3 event keys can be URL-encoded. Decode the key before using it.
        source_key = unquote_plus(record["s3"]["object"]["key"])

        print(f"Received file: s3://{bucket_name}/{source_key}")

        # Process only JSON files.
        if not source_key.lower().endswith(".json"):
            print(f"Skipping non-JSON file: {source_key}")
            skipped_files.append(source_key)
            continue

        # Safety check to avoid processing anything from the output prefix.
        if source_key.startswith(OUTPUT_PREFIX):
            print(f"Skipping object from output prefix: {source_key}")
            skipped_files.append(source_key)
            continue

        # Download the JSON object from S3.
        response = s3.get_object(
            Bucket=bucket_name,
            Key=source_key
        )

        # Read bytes, decode them as UTF-8 text, and parse the JSON.
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)

        # This project expects a top-level JSON array of orders.
        if not isinstance(data, list):
            raise ValueError(
                f"Expected a JSON array in {source_key}, "
                f"but received {type(data).__name__}"
            )

        # Flatten nested order data into tabular rows.
        dataframe = flatten(data)

        # Skip files that contain no product rows.
        if dataframe.empty:
            print(f"No product records found in {source_key}")
            skipped_files.append(source_key)
            continue

        print(f"Total flattened records: {len(dataframe)}")

        # Create an in-memory binary buffer. No temporary disk file is needed.
        parquet_buffer = io.BytesIO()

        # Convert the DataFrame to Apache Parquet using PyArrow.
        dataframe.to_parquet(
            parquet_buffer,
            index=False,
            engine="pyarrow"
        )

        # Get only the filename from the full S3 key.
        # Example: orders_json_incoming/orders_etl.json -> orders_etl.json
        source_filename = os.path.basename(source_key)

        # Separate the filename from its extension.
        filename_without_extension = os.path.splitext(source_filename)[0]

        # Build a deterministic Parquet filename.
        # Example: orders_etl.json -> orders_etl.parquet
        parquet_filename = f"{filename_without_extension}.parquet"

        # Build the complete output S3 object key.
        output_key = f"{OUTPUT_PREFIX}{parquet_filename}"

        # Upload the Parquet bytes to S3.
        s3.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream"
        )

        print(f"Parquet file created: s3://{bucket_name}/{output_key}")

        processed_files.append(
            {
                "source_file": source_key,
                "parquet_file": output_key,
                "records_processed": len(dataframe)
            }
        )

    # Start the crawler only if at least one file was processed.
    if processed_files:
        try:
            crawler_response = glue.get_crawler(Name=CRAWLER_NAME)
            crawler_state = crawler_response["Crawler"]["State"]

            # A crawler can be started only when it is ready.
            if crawler_state == "READY":
                glue.start_crawler(Name=CRAWLER_NAME)
                print(f"Glue Crawler started: {CRAWLER_NAME}")
            else:
                print(
                    f"Glue Crawler not started because its current "
                    f"state is {crawler_state}"
                )

        except glue.exceptions.EntityNotFoundException:
            print(f"Glue Crawler not found: {CRAWLER_NAME}")
            raise

    # Return a summary that also appears in Lambda test results.
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "ETL processing completed",
                "processed_files": processed_files,
                "skipped_files": skipped_files
            }
        )
    }
```

Click **Deploy** after pasting or updating the code.

### Initial testing without Glue

If the Glue Crawler has not been created yet, temporarily comment out the Glue section near the end of the function. Otherwise, the Parquet upload can succeed and the invocation can then fail because the crawler does not exist.

After creating the crawler, restore the Glue code and deploy the Lambda again.

---

## Step 6: Configure the S3 Trigger

Open:

```text
S3 -> namaste-etl-project-2026 -> Properties -> Event notifications
```

Create an event notification:

```text
Name: orders-json-upload-event
Event type: All object create events
Prefix: orders_json_incoming/
Suffix: .json
Destination type: Lambda function
Destination: json-to-parquet-etl
```

This configuration invokes Lambda only for JSON files uploaded to the input prefix.

This object triggers Lambda:

```text
orders_json_incoming/orders_etl.json
```

This object does not match the trigger:

```text
orders_parquet_datalake/orders_etl.parquet
```

This separation prevents the output from recursively invoking the Lambda.

---

## Step 7: Perform the Initial Test

Upload the sample file:

```text
orders_etl.json
```

to:

```text
orders_json_incoming/orders_etl.json
```

Expected result:

```text
orders_parquet_datalake/orders_etl.parquet
```

The Parquet output should appear after the Lambda completes successfully.

### Sample JSON structure

```json
[
  {
    "order_id": "ORD-1001",
    "order_date": "2026-10-26",
    "total_amount": 1250.0,
    "customer": {
      "customer_id": "CUST-101",
      "name": "Sample Customer",
      "email": "customer@example.com",
      "address": "Pune, India"
    },
    "products": [
      {
        "product_id": "PROD-001",
        "name": "Laptop",
        "category": "Electronics",
        "price": 1000.0,
        "quantity": 1
      },
      {
        "product_id": "PROD-002",
        "name": "Mouse",
        "category": "Accessories",
        "price": 250.0,
        "quantity": 1
      }
    ]
  }
]
```

This example produces two flattened rows because the order contains two products.

---

## Step 8: Verify CloudWatch Logs

Open:

```text
Lambda -> json-to-parquet-etl -> Monitor -> View CloudWatch logs
```

Expected messages include:

```text
Lambda execution started
Received file: s3://namaste-etl-project-2026/orders_json_incoming/orders_etl.json
Total flattened records: 23
Parquet file created: s3://namaste-etl-project-2026/orders_parquet_datalake/orders_etl.parquet
```

If the Glue code is enabled and the crawler is ready, you should also see:

```text
Glue Crawler started: etl_pipeline_crawler
```

If no new log stream appears, verify:

1. The JSON was uploaded under `orders_json_incoming/`.
2. The object key ends with `.json`.
3. The S3 Event Notification points to the correct Lambda.
4. S3 is allowed to invoke the Lambda.
5. The latest Lambda code was deployed.

---

## Step 9: Create an AWS Glue Database

Open:

```text
AWS Glue -> Data Catalog -> Databases -> Add database
```

Example database name:

```text
orders_etl_db
```

You can choose another name.

---

## Step 10: Create and Run the Glue Crawler

Open:

```text
AWS Glue -> Crawlers -> Create crawler
```

Example crawler configuration:

```text
Crawler name: etl_pipeline_crawler
Data source: Amazon S3
S3 path: s3://namaste-etl-project-2026/orders_parquet_datalake/
Target database: orders_etl_db
```

Create or select an IAM role that allows the crawler to read the Parquet prefix and update the Glue Data Catalog.

The crawler name must match this Lambda constant:

```python
CRAWLER_NAME = "etl_pipeline_crawler"
```

Run the crawler manually for the first test.

Expected crawler result:

```text
Parquet files scanned
        |
        v
Schema discovered
        |
        v
Glue table created or updated
```

Open:

```text
AWS Glue -> Data Catalog -> Tables
```

Confirm that a table was created in `orders_etl_db`.

---

## Step 11: Query the Data in Athena

From the Glue table page, choose the option to view or query table data in Athena.

Before the first query, Athena may require a query result location. Create or select an S3 prefix such as:

```text
s3://namaste-etl-project-2026/athena-query-results/
```

Replace the example database and table names below with the names created in your environment.

### Preview rows

```sql
SELECT *
FROM orders_etl_db.orders_parquet_datalake
LIMIT 10;
```

### Count all physical rows

```sql
SELECT COUNT(*) AS total_rows
FROM orders_etl_db.orders_parquet_datalake;
```

### Check the number of rows coming from each Parquet file

```sql
SELECT
    "$path" AS parquet_file,
    COUNT(*) AS row_count
FROM orders_etl_db.orders_parquet_datalake
GROUP BY "$path"
ORDER BY parquet_file;
```

This query is useful for finding duplicate Parquet files.

### Count unique orders

```sql
SELECT COUNT(DISTINCT order_id) AS unique_orders
FROM orders_etl_db.orders_parquet_datalake;
```

### Deduplicate at order-product level

Because one order can have multiple products, use both `order_id` and `product_id` when removing duplicate products:

```sql
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id
            ORDER BY order_date DESC
        ) AS row_number
    FROM orders_etl_db.orders_parquet_datalake
)
WHERE row_number = 1;
```

---

## Duplicate-Prevention Behavior

The Lambda creates the output filename from the input filename.

```text
orders_json_incoming/orders_etl.json
                   |
                   v
orders_parquet_datalake/orders_etl.parquet
```

If the same S3 object key is uploaded again, Lambda writes to the same output key:

```text
orders_parquet_datalake/orders_etl.parquet
```

S3 replaces the current output object rather than creating another timestamp-based key.

Therefore, testing the same source key multiple times does not create files such as:

```text
orders_ETL_20261026_170000.parquet
orders_ETL_20261026_171000.parquet
orders_ETL_20261026_172000.parquet
```

### Different source filenames

These input files:

```text
orders_json_incoming/orders_001.json
orders_json_incoming/orders_002.json
orders_json_incoming/orders_003.json
```

create these outputs:

```text
orders_parquet_datalake/orders_001.parquet
orders_parquet_datalake/orders_002.parquet
orders_parquet_datalake/orders_003.parquet
```

Athena reads all Parquet files under the Glue table location.

---

## Important Limitations

This implementation provides **one deterministic Parquet output per source filename**. It does not perform record-level incremental merging.

Important behaviors:

- Uploading the same S3 key again replaces its Parquet output.
- Uploading identical content with a different filename creates another Parquet file.
- Different source files can contain duplicate `order_id` and `product_id` values.
- S3 automatically replaces an object written to an existing key unless bucket versioning retains older versions.
- This pipeline does not compare file content hashes.
- This pipeline does not merge only new rows into an existing Parquet file.
- Starting a crawler for every small file may be inefficient at larger scale.

For stronger production deduplication, consider:

- A DynamoDB processing tracker using bucket, key, object version, or content hash
- S3 Versioning for source recovery
- An SQS queue between S3 and Lambda for buffering
- A dead-letter queue or failure destination
- AWS Glue ETL for larger transformations
- Apache Iceberg for record-level merge, update, and delete operations
- A curated deduplication layer using `order_id` and `product_id`

---

## Troubleshooting

### Lambda does not run after uploading JSON

Verify the S3 object key is:

```text
orders_json_incoming/orders_etl.json
```

Confirm the Event Notification has:

```text
Prefix: orders_json_incoming/
Suffix: .json
```

Also confirm the S3 bucket is connected to the correct Lambda function.

---

### JSON appears in the Parquet prefix

The Lambda code in this repository writes only a Parquet object.

The intended layout is:

```text
orders_json_incoming/orders_etl.json
orders_parquet_datalake/orders_etl.parquet
```

If this exists:

```text
orders_parquet_datalake/orders_etl.json
```

check whether the JSON file was manually uploaded into the output prefix.

---

### Athena returns more rows than expected

Run:

```sql
SELECT
    "$path",
    COUNT(*)
FROM orders_etl_db.orders_parquet_datalake
GROUP BY "$path";
```

If four identical Parquet files contain 23 rows each, Athena returns 92 physical rows because it reads every file included in the table location.

---

### `No module named pandas`

Add a compatible AWS SDK for pandas layer or provide Pandas in a Lambda deployment package or container image.

---

### `No module named pyarrow`

Use a compatible layer or package containing PyArrow. The code requires PyArrow for:

```python
dataframe.to_parquet(engine="pyarrow")
```

---

### Lambda import error after adding the layer

Check that all three are compatible:

- Lambda Python runtime
- Lambda architecture
- AWS SDK for pandas layer version and architecture

---

### Glue Crawler not found

Confirm that the crawler name exactly matches:

```python
CRAWLER_NAME = "etl_pipeline_crawler"
```

Crawler names are case-sensitive.

---

### Glue Crawler is already running

The provided Lambda checks the crawler state and starts it only when the state is `READY`.

If it is already running, Lambda logs the current state and does not attempt another start.

---

### Access denied when reading or writing S3

Verify the Lambda execution role allows:

```text
s3:GetObject
s3:PutObject
```

Check that the policy resources include the correct bucket and prefixes.

---

### Access denied when starting the crawler

Verify the Lambda execution role allows:

```text
glue:GetCrawler
glue:StartCrawler
```

---

### Lambda times out for large files

This project loads the complete JSON object and DataFrame into Lambda memory. For larger datasets, consider:

- Increasing Lambda memory and timeout after testing
- Splitting source files
- Sending S3 events through SQS
- Using AWS Glue ETL for larger transformations
- Using Step Functions or distributed processing where appropriate

---

## Security Improvements

For a portfolio or production-ready version:

- Keep the S3 bucket private.
- Enable S3 default encryption.
- Enable S3 Versioning if source recovery is required.
- Replace broad managed policies with least-privilege IAM permissions.
- Restrict Lambda to the required input and output prefixes.
- Use synthetic data in the GitHub repository.
- Never commit access keys, secret keys, account IDs, or private customer data.
- Configure CloudWatch alarms for Lambda errors.
- Validate the JSON schema before processing.
- Use separate development and production resources.

---

## Cost Controls and Cleanup

This project uses serverless AWS services, but usage may still create charges.

### Cost-control recommendations

- Use Parquet to reduce Athena data scanned.
- Avoid repeatedly starting the Glue Crawler for every small object at higher volume.
- Store Athena results in a dedicated prefix.
- Add lifecycle policies for temporary test data and Athena results.
- Monitor Lambda invocation counts to detect recursion.
- Delete unused crawlers, tables, logs, and test files.
- Review AWS Cost Explorer and billing alerts.

### Cleanup steps

1. Delete objects under `orders_json_incoming/`.
2. Delete objects under `orders_parquet_datalake/`.
3. Delete Athena query-result objects.
4. Delete the Glue table.
5. Delete the Glue Crawler.
6. Delete the Glue database if it is no longer required.
7. Delete the Lambda function.
8. Delete the Lambda execution role and custom policies.
9. Empty and delete the S3 bucket if no data must be retained.
10. Review CloudWatch log groups and remove test logs if appropriate.

---

## Future Enhancements

- Add JSON schema validation.
- Add date-based Parquet partitioning.
- Add SQS between S3 and Lambda.
- Add retry and dead-letter handling.
- Track source object versions in DynamoDB.
- Add content-hash-based duplicate detection.
- Add automated tests for `flatten()`.
- Deploy the infrastructure using Terraform.
- Add GitHub Actions for syntax checks and tests.
- Use environment variables for the output prefix and crawler name.
- Update Glue partitions programmatically instead of crawling every file.
- Use Apache Iceberg for merge and update requirements.

### Example partitioned output

```text
orders_parquet_datalake/
`-- year=2026/
    `-- month=10/
        `-- day=26/
            `-- orders_etl.parquet
```

Partitioning can reduce Athena data scanning when queries filter by partition columns.

---

## Interview Explanation

> I designed an event-driven, serverless ETL pipeline using Amazon S3, AWS Lambda, Pandas, PyArrow, AWS Glue, and Amazon Athena. When a JSON file is uploaded to the S3 incoming prefix, S3 invokes Lambda. Lambda reads and flattens the nested order, customer, and product data, converts it into a Pandas DataFrame, and writes it as Parquet to a separate S3 data-lake prefix. The output filename is derived from the source filename, so reprocessing the same S3 key replaces its existing Parquet output instead of creating timestamp-based duplicate files. A Glue Crawler catalogs the Parquet schema, and Athena queries the resulting table.

### Resume bullet

> Built an event-driven AWS ETL pipeline using Amazon S3, AWS Lambda, Pandas, PyArrow, AWS Glue, and Amazon Athena to flatten nested JSON order data, convert it to Parquet, catalog the dataset, and support serverless SQL analytics while preventing duplicate output files during source-key reprocessing.

---

## Disclaimer

This repository is intended for AWS learning, demonstration, and portfolio use. Review IAM permissions, dependency compatibility, schema evolution, concurrency, failure recovery, security, and cost controls before adapting the design for production workloads.
