# aws-s3-lambda-glue-athena-etl-pipeline
The solution automatically processes JSON files uploaded to Amazon S3, transforms nested JSON into a tabular format using AWS Lambda and Pandas, converts the data into Parquet format, stores the output in an S3-based data lake, updates the AWS Glue Data Catalog, and enables querying through Amazon Athena.
