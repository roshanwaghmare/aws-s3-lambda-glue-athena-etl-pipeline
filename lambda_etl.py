# Import Python libraries

import json          # Used to parse JSON data
import boto3         # AWS SDK for Python
import pandas as pd  # Used for data transformation
import io            # Used for in-memory file operations
import os            # Used for file name manipulation


# Function to flatten nested JSON data
def flatten(data):

    # Empty list to store transformed records
    orders_data = []

    # Loop through each order in JSON
    for order in data:

        # Loop through each product inside an order
        for product in order['products']:

            # Create a flat record by combining:
            # Order details
            # Customer details
            # Product details

            row_orders = {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "total_amount": order["total_amount"],

                "customer_id": order["customer"]["customer_id"],
                "customer_name": order["customer"]["name"],
                "email": order["customer"]["email"],
                "address": order["customer"]["address"],

                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": product["quantity"]
            }

            # Add flattened row into list
            orders_data.append(row_orders)

    # Convert list into Pandas DataFrame
    return pd.DataFrame(orders_data)


# Lambda entry point
def lambda_handler(event, context):

    # Extract bucket name from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    # Extract object key (file path)
    key = event['Records'][0]['s3']['object']['key']

    print(f"Received file: {key}")

    # Process only JSON files
    # Skip parquet/csv or any other files
    if not key.endswith(".json"):
        print("Skipping non-json file")

        return {
            "statusCode": 200,
            "body": "Skipped non-json file"
        }

    # Create S3 client
    s3 = boto3.client('s3')

    # Read uploaded JSON file from S3
    response = s3.get_object(
        Bucket=bucket_name,
        Key=key
    )

    # Read file content
    content = response['Body'].read().decode('utf-8')

    # Convert JSON string into Python object
    data = json.loads(content)

    # Transform nested JSON to flat structure
    df = flatten(data)

    print(f"Total records processed: {len(df)}")

    # Create an in-memory parquet file
    parquet_buffer = io.BytesIO()

    # Convert DataFrame to Parquet format
    df.to_parquet(
        parquet_buffer,
        index=False,
        engine='pyarrow'
    )

    # Extract source filename
    # Example:
    # orders_json_incoming/orders_etl.json
    # becomes
    # orders_etl.json

    file_name = os.path.basename(key)

    # Replace .json extension with .parquet
    parquet_name = file_name.replace(
        ".json",
        ".parquet"
    )

    # Output S3 location
    # Example:
    # orders_parquet_datalake/orders_etl.parquet

    key_staging = f"orders_parquet_datalake/{parquet_name}"

    # Upload parquet file to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key_staging,
        Body=parquet_buffer.getvalue()
    )

    print(f"Parquet file created: {key_staging}")

    # Create Glue client
    glue = boto3.client('glue')

    # Start Glue crawler
    # This updates Glue Catalog metadata
    glue.start_crawler(
        Name='etl_pipeline_crawler'
    )

    print("Glue crawler started")

    # Return Lambda response
    return {
        "statusCode": 200,
        "body": {
            "source_file": key,
            "parquet_file": key_staging,
            "records_processed": len(df)
        }
    }
