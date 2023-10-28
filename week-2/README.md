# Dataflow Pipeline: kdrama_csv_to_bq and kdrama_bq_to_bq

This repository contains a Dataflow pipeline for processing KDrama data. The pipeline is split into two steps: converting CSV data to BigQuery (`kdrama_csv_to_bq`) and then transforming and copying data within BigQuery (`kdrama_bq_to_bq`).

## Prerequisites

Before running the pipeline, make sure you have the following:

- Google Cloud Platform (GCP) account and project.
- Python installed (for running the pipeline locally).
- Google Cloud SDK installed and configured.
- Permissions to create and run Dataflow jobs, and to read and write to BigQuery.

## Setup

1. Clone this repository:
```bash
    git clone https://github.com/Razellion/datafellowship-11.git
    cd datafellowship-11
```
2. Set up your GCP credentials:
For auth:
```bash
    gcloud auth login
```
Set project id:
```bash
    gcloud config set project <YOUR-PROJECT-ID>
```

For app auth:
```bash
    gcloud auth application-default login
```

3. Copy csv to your bucket:
```bash
    gsutil cp kdrama.csv gs://YOUR-GCS-BUCKET
```

## Running the Pipeline

### Step 1: kdrama_csv_to_bq

This step reads data from a CSV file and loads it into BigQuery.
```bash
    python -m kdrama_csv_to_bq \
        --region asia-southeast2 \
        --runner DataflowRunner \
        --project YOUR-PROJECT-ID
```

Replace placeholders with your actual project ID and Dataset Name inside the code

### Step 2: kdrama_bq_to_bq
This step performs transformations and copies data within BigQuery.
```bash
    python -m kdrama_bq_to_bq \
        --region asia-southeast2 \
        --runner DataflowRunner \
        --project YOUR-PROJECT-ID
```

Replace placeholders with your actual project ID and Dataset Name inside the code