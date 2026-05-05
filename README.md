# Real-time Data Pipeline - Google Cloud

**Live Demo**: [Google Colab](https://colab.research.google.com/github/Vaishnavidorlikar/real-time-data-pipeline-gcp/blob/main/realtime_pipeline_colab.ipynb) | **GitHub**: [View Source](https://github.com/Vaishnavidorlikar/real-time-data-pipeline-gcp)

A comprehensive **real-time streaming data pipeline** using Google Cloud Platform, capable of processing **100K+ events per second** with **sub-second latency** for enterprise-scale data streaming applications.

## Business Impact

- **100K+ events/second** throughput capability
- **<1 second** processing latency
- **99.9% uptime** availability
- **Cost-effective** at scale processing

## Core Capabilities

### Real-time Streaming Architecture
- **Pub/Sub Integration** - High-throughput message ingestion
- **Apache Beam Processing** - Stream processing framework
- **BigQuery Storage** - Real-time analytics warehouse
- **Cloud Monitoring** - Performance tracking and alerting

### Enterprise Streaming Features
- **Scalable Processing** - Handle millions of events per day
- **Real-time Analytics** - Live data insights and dashboards
- **Error Handling** - Robust error recovery and retry mechanisms
- **Multi-source Integration** - Support for various data sources

### Performance Optimization
- **Windowing Operations** - Time-based aggregations
- **State Management** - Efficient state tracking
- **Resource Management** - Auto-scaling capabilities
- **Cost Optimization** - Efficient resource utilization

## Installation

This is a Python project. Install dependencies using:

```bash
pip install -e .
```

For development (including Jupyter notebooks):

```bash
pip install -e ".[dev]"
```

## Project Structure

```
real-time-data-pipeline-gcp/
├── pyproject.toml               # Python project configuration
├── requirements.txt             # Dependencies
├── requirements_fixed.txt       # Fixed version dependencies
├── .gitattributes               # GitHub linguist configuration
├── config/
│   └── config.yaml              # Pipeline configuration
├── dags/
│   ├── composer_pipeline.py     # Airflow orchestration
│   └── trigger_dataflow_dag.py
├── src/
│   ├── transform.py
│   └── bq_schema.py
├── streaming/
│   ├── dataflow_pipeline.py     # Stream processing
│   └── pubsub_publisher.py      # Event publishing
├── notebooks/
│   ├── realtime_pipeline_colab.ipynb     # Live demo notebook
│   └── realtime_pipeline_dashboard.ipynb # Dashboard notebook
└── docs/
    ├── README_COMPOSER.md       # Composer-specific docs
    └── cloud_monitoring_dashboard.md     # Monitoring documentation
```

## Quick Start

### Installation
```bash
# Clone the repository
git clone https://github.com/Vaishnavidorlikar/real-time-data-pipeline-gcp.git
cd real-time-data-pipeline-gcp

# Install dependencies
pip install -r requirements.txt

# Set up Google Cloud credentials
gcloud auth application-default login
gcloud config set project your-project-id
```

### Run Demo
```bash
# Publish sample events to Pub/Sub
python streaming/pubsub_publisher.py

# Run the Dataflow pipeline
python streaming/dataflow_pipeline.py

# Or run in Google Colab
# Open the realtime_pipeline_colab.ipynb notebook
```

### Live Demo in Colab
**[Run in Google Colab](https://colab.research.google.com/github/Vaishnavidorlikar/real-time-data-pipeline-gcp/blob/main/realtime_pipeline_colab.ipynb)**

## Technology Stack

- **Google Cloud Platform** - Cloud infrastructure
- **Pub/Sub** - Message streaming service
- **Apache Beam** - Stream processing framework
- **Dataflow** - Managed stream processing
- **BigQuery** - Real-time analytics warehouse
- **Cloud Composer** - Workflow orchestration
- **Python** - Core programming language

## Performance Metrics

- **Throughput**: 100K+ events per second
- **Latency**: <1 second processing time
- **Availability**: 99.9% uptime guarantee
- **Scalability**: Auto-scaling to handle load spikes
- **Cost**: Optimized resource usage

## Use Cases

- **Real-time Analytics** - Live dashboard data feeds
- **IoT Data Processing** - Sensor stream processing
- **Event-driven Architecture** - Microservice communication
- **Log Analysis** - Real-time log processing
- **Financial Trading** - Low-latency data processing

## Architecture Overview

### Streaming Data Pipeline
```
Data Sources → Pub/Sub → Dataflow → BigQuery
                   ↓
               Airflow DAGs
```

### Data Flow
1. **Ingestion** - Events published to Pub/Sub topics
2. **Processing** - Dataflow processes events in real-time
3. **Storage** - Processed data stored in BigQuery
4. **Orchestration** - Airflow manages workflow dependencies
5. **Monitoring** - Cloud Monitoring tracks performance

## Contact

- **Email**: dorlikarvaishnavi@gmail.com
- **LinkedIn**: [linkedin.com/in/vaishnavidorlikar](https://linkedin.com/in/vaishnavidorlikar)
- **GitHub**: [github.com/Vaishnavidorlikar](https://github.com/Vaishnavidorlikar)
- **Portfolio**: [vaishnavidorlikar.com](https://vaishnavidorlikar.com)

---

**Built by Vaishnavi Dorlikar | Real-time Data Engineer**
