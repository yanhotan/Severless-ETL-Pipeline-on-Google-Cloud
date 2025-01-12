# Serverless ETL Pipeline on Google Cloud

## Overview
This project demonstrates a serverless ETL (Extract, Transform, Load) pipeline built using Google Cloud services. It processes raw data stored in Google Cloud Storage, applies transformations using Python, and stores the output back in Cloud Storage for further use. The pipeline leverages serverless architecture to ensure scalability and cost-efficiency.

## Features
- **Serverless Architecture**: Utilizes Google Cloud Functions to process data on-demand.
- **Cloud Storage Buckets**: Acts as the source and destination for raw and processed data.
- **Data Transformation**: Python scripts perform data cleaning, enrichment, and formatting.
- **Scalability**: Handles variable data loads automatically without manual intervention.
- **Cost-Efficiency**: Only incurs costs when the pipeline is triggered.

## Technologies Used
- **Google Cloud Storage**: For storing raw and processed data.
- **Google Cloud Functions**: To implement the transformation logic in a serverless manner.
- **Python**: For data processing and transformation.
- **Google Cloud SDK**: For managing and deploying resources.
- **Cloud Pub/Sub** (optional): To trigger the ETL process based on specific events.

## Project Workflow
1. **Extract**: Raw data is uploaded to a designated Cloud Storage bucket.
2. **Trigger**: A Cloud Function is triggered by the upload event.
3. **Transform**: The function processes the data (e.g., cleaning, enriching, and reformatting).
4. **Load**: The processed data is saved to another Cloud Storage bucket.
