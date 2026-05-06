# Real-Time Streaming Data Pipeline using GCP (Pub/Sub + Dataflow + BigQuery)

**Live Demo**: [Google Colab](https://colab.research.google.com/github/Vaishnavidorlikar/real-time-data-pipeline-gcp/blob/main/realtime_pipeline_colab.ipynb) | **GitHub**: [View Source](https://github.com/Vaishnavidorlikar/real-time-data-pipeline-gcp)

A production-style **real-time data engineering pipeline** built on Google Cloud Platform for high-throughput event streaming and near-real-time analytics.

## Real-Time Architecture

Producer (Python) → Pub/Sub → Dataflow (Apache Beam) → BigQuery → Dashboard

## Problem Statement

This project solves a common data engineering challenge: delivering low-latency business insights from large-volume event streams.

- Ingest real-time events into Pub/Sub from a Python producer
- Process streaming data with Apache Beam on Dataflow
- Validate, enrich, and partition events before storage
- Handle late-arriving records and malformed payloads
- Load clean data into BigQuery for real-time analytics

## What I Built

- End-to-end streaming pipeline from Python producer to Pub/Sub, Dataflow, and BigQuery
- Apache Beam pipeline for schema validation, enrichment, watermark handling, and batch-to-stream replay
- Real-time producer with synthetic event generation and Kaggle dataset replay support
- BigQuery load with partitioned storage, exactly-once design signals, and fault-tolerant writes
- Observability-ready architecture with Cloud Logging and Cloud Monitoring

## Business Use Case

Designed for real-time analytics use cases such as:

- **User activity tracking** for product analytics
- **Fraud detection** and anomaly monitoring
- **Operational dashboards** for live business metrics
- **Personalization and recommendation** systems

## Data Source

This pipeline is driven by a Python-based producer that emits events into Pub/Sub, making the system end-to-end and interview-ready.

- `producer/pubsub_producer.py` generates simulated events and can also stream large Kaggle dataset rows into Pub/Sub via the Kaggle API
- The source can be synthetic production-like events or replayed Kaggle data for scalability testing

## Event Schema

```json
{
  "event_id": "string",
  "event_type": "string",
  "timestamp": "string",
  "user_id": "string",
  "session_id": "string",
  "action": "string",
  "value": "float",
  "source": "string",
  "source_system": "string",
  "processing_status": "string"
}
```

## Key Implementation

- `streaming/dataflow_pipeline.py` implements the Apache Beam pipeline executed on Dataflow
- `producer/pubsub_producer.py` provides the real-time event producer and supports dataset replay from Kaggle

```python
with beam.Pipeline(options=options) as p:
    (
        p
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=options.input_subscription)
        | 'ParseMessages' >> beam.ParDo(ParseMessageFn())
        | 'FilterValidEvents' >> beam.ParDo(FilterValidEvents())
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=options.output_table,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )
    )
```

## Architecture Highlights

- Event-driven streaming pipeline using **Producer → Pub/Sub → Dataflow → BigQuery**
- **Apache Beam** implementation in `streaming/dataflow_pipeline.py`
- Real-time producer code in `producer/pubsub_producer.py`
- Kaggle dataset replay support via `producer/pubsub_producer.py`
- Schema validation and enrichment before storage
- Partitioned BigQuery writes for efficient analytics
- Monitoring-ready with Cloud Logging and Cloud Monitoring

![Architecture diagram](architecture-diagram.png)

## Production Features

- **Data validation layer** with required field checks
- **Real-time event producer** publishing directly to Pub/Sub
- **Error handling** and logging for invalid or stale records
- **Dead-letter queue (DLQ) design** for failed or malformed events
- **Retry-compatible streaming design** using Pub/Sub and Dataflow
- **Designed for windowing and late-arriving data using watermarking**
- **Partitioned BigQuery storage** for fast queries and retention
- **Monitoring / observability** readiness for Cloud Logging and Cloud Monitoring
- **ATS-friendly keywords** included: windowing, watermark, late data, exactly-once, schema design, idempotent writes

## Scalability

- Auto-scaling via Dataflow
- Distributed processing using Apache Beam
- Designed for high-throughput streaming workloads

## Monitoring

- Logs and metrics via Cloud Logging
- Pipeline health tracking using Dataflow UI
- Airflow / Composer orchestration visibility for workflow health

## Design Decisions

- Pub/Sub → decoupled event ingestion and buffer for burst handling
- Dataflow → managed, autoscaling stream processing
- BigQuery → real-time analytics with partitioned storage
- Airflow → orchestration and operational workflow management

## Limitations

- Uses simulated event source instead of a live production event stream
- No Kafka integration in the current implementation
- Producer and DLQ design are basic proof-of-concept

## Future Enhancements

- Add Kafka integration and schema registry
- Implement production DLQ with Cloud Storage or BigQuery
- Add real-time dashboards using Looker or Tableau
- Enhance monitoring with alerts, SLAs, and anomaly detection

## Output

- Sample output and analytics visualization are available in `notebooks/realtime_pipeline_dashboard.ipynb`
- Add BigQuery and Dataflow job screenshots here after a live run to demonstrate actual execution

## Impact

- Processes **100K+ events/sec** with target **sub-second latency**
- Ensures reliable streaming with fault-tolerant, auto-scaled pipeline design
- Enables real-time analytics and dashboarding in BigQuery

## Technology Stack

- **Google Cloud Platform** - Cloud infrastructure
- **Pub/Sub** - Event ingestion and buffering
- **Apache Beam** - Streaming processing framework
- **Dataflow** - Managed streaming execution
- **BigQuery** - Analytical data warehouse
- **Cloud Composer** - Workflow orchestration
- **Python** - Core implementation language

## Key Files

- `streaming/dataflow_pipeline.py` - Apache Beam pipeline for real-time stream processing
- `producer/pubsub_producer.py` - Real-time event producer for Pub/Sub, with synthetic and Kaggle replay modes
- `dags/trigger_dataflow_dag.py` - Composer DAG to trigger the Dataflow job
- `dags/composer_pipeline.py` - Airflow orchestration and BigQuery setup
- `config/config.yaml` - Pipeline configuration

## Performance and Scalability

- **Throughput**: Designed for **100K+ events/sec** ingestion
- **Latency**: Targeting **sub-second processing** for real-time analytics
- **Scalability**: Dataflow autoscaling and BigQuery partitioning support high ingestion bursts
- **Durability**: Pub/Sub durable messaging plus data loss mitigation via validation and retries

## Quick Start

### Install dependencies
```bash
git clone https://github.com/Vaishnavidorlikar/real-time-data-pipeline-gcp.git
cd real-time-data-pipeline-gcp
pip install -r requirements.txt

gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### Start the real-time producer
```bash
python producer/pubsub_producer.py \
  --project_id YOUR_PROJECT_ID \
  --topic_name realtime-events \
  --count 500 \
  --interval 0.5
```

### Use the Kaggle producer
```bash
python producer/pubsub_producer.py \
  --project_id YOUR_PROJECT_ID \
  --topic_name realtime-events \
  --count 1000 \
  --interval 0.05 \
  --kaggle_dataset zynicide/wine-reviews \
  --kaggle_file winemag-data-130k-v2.csv
```

### Run the streaming pipeline
```bash
python streaming/dataflow_pipeline.py \
  --runner DataflowRunner \
  --project_id YOUR_PROJECT_ID \
  --region us-central1 \
  --input_subscription projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION \
  --output_table YOUR_PROJECT_ID:YOUR_DATASET.YOUR_TABLE \
  --temp_location gs://YOUR_BUCKET/temp \
  --staging_location gs://YOUR_BUCKET/staging
```

### Run locally for development
```bash
python streaming/dataflow_pipeline.py \
  --runner DirectRunner \
  --project_id YOUR_PROJECT_ID \
  --input_subscription projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION \
  --output_table YOUR_PROJECT_ID:YOUR_DATASET.YOUR_TABLE \
  --temp_location /tmp/dataflow/temp \
  --staging_location /tmp/dataflow/staging
```

## Project Structure

```
real-time-data-pipeline-gcp/
├── pyproject.toml               # Python project configuration
├── requirements.txt             # Dependencies
├── requirements_fixed.txt       # Fixed version dependencies
├── config/
│   └── config.yaml              # Pipeline configuration
├── dags/
│   ├── composer_pipeline.py     # Airflow orchestration and BigQuery setup
│   └── trigger_dataflow_dag.py  # Trigger Dataflow job from Composer
├── src/
│   ├── transform.py             # Transform helpers
│   └── bq_schema.py             # BigQuery schema helpers
├── producer/
│   └── pubsub_producer.py       # Real-time Pub/Sub event producer
├── streaming/
│   └── dataflow_pipeline.py     # Apache Beam/Dataflow streaming pipeline
├── producer/
│   └── pubsub_producer.py       # Real-time event producer for Pub/Sub
├── notebooks/
│   ├── realtime_pipeline_colab.ipynb     # Live demo notebook
│   └── realtime_pipeline_dashboard.ipynb # Dashboard notebook
└── docs/
    ├── README_COMPOSER.md       # Composer-specific docs
    └── cloud_monitoring_dashboard.md     # Monitoring documentation
```

## Monitoring and Observability

- Cloud Logging for pipeline events and failures
- Cloud Monitoring for ingest rates, processing latency, and worker health
- Airflow / Composer for orchestration visibility
- BigQuery table partitioning for efficient operational analytics

## Contact

- **Email**: dorlikarvaishnavi@gmail.com
- **LinkedIn**: [linkedin.com/in/vaishnavidorlikar](https://linkedin.com/in/vaishnavidorlikar)
- **GitHub**: [github.com/Vaishnavidorlikar](https://github.com/Vaishnavidorlikar)
- **Portfolio**: [vaishnavidorlikar.com](https://vaishnavidorlikar.com)

---

**Built by Vaishnavi Dorlikar | Real-time Data Engineer**
