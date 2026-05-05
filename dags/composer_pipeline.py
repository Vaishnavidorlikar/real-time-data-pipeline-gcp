#!/usr/bin/env python3
"""
Google Cloud Composer DAG for Real-time Data Pipeline
Replaces problematic Apache Beam Dataflow with managed Composer orchestration
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubCreateSubscriptionOperator, PubSubCreateTopicOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.pubsub_to_gcs import PubSubToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from google.cloud import pubsub_v1
from google.cloud import bigquery

# Configuration
PROJECT_ID = "your-gcp-project-id"
TOPIC_NAME = "realtime-events"
SUBSCRIPTION_NAME = "realtime-events-sub"
DATASET_ID = "realtime_events"
TABLE_NAME = "events"
GCS_BUCKET = "your-gcs-bucket"

# Default arguments for DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_pubsub_infrastructure(**context):
    """Create Pub/Sub topic and subscription"""
    try:
        # Initialize Pub/Sub client
        publisher = pubsub_v1.PublisherClient()
        admin_client = pubsub_v1.AdminClient()
        
        # Create topic
        topic_path = admin_client.topic_path(PROJECT_ID, TOPIC_NAME)
        try:
            topic = admin_client.create_topic(request={"name": TOPIC_NAME})
            logging.info(f"Created Pub/Sub topic: {topic.name}")
        except Exception as e:
            if "already exists" in str(e):
                logging.info(f"Pub/Sub topic {TOPIC_NAME} already exists")
            else:
                raise e
        
        # Create subscription
        subscription_path = admin_client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
        try:
            subscription = admin_client.create_subscription(
                request={"name": SUBSCRIPTION_NAME, "topic": topic_path}
            )
            logging.info(f"Created Pub/Sub subscription: {subscription.name}")
        except Exception as e:
            if "already exists" in str(e):
                logging.info(f"Pub/Sub subscription {SUBSCRIPTION_NAME} already exists")
            else:
                raise e
                
        return {"status": "success", "message": "Pub/Sub infrastructure created"}
        
    except Exception as e:
        logging.error(f"Failed to create Pub/Sub infrastructure: {e}")
        return {"status": "error", "message": str(e)}


def setup_bigquery_table(**context):
    """Create BigQuery table with proper schema"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Create dataset
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
        try:
            dataset = client.create_dataset(dataset_ref, exists_ok=True)
            logging.info(f"Created BigQuery dataset: {dataset.dataset_id}")
        except Exception as e:
            logging.warning(f"Dataset creation note: {e}")
        
        # Define table schema
        schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("session_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("environment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_system", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("partition_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("processing_status", "STRING", mode="NULLABLE"),
        ]
        
        # Create table
        table_ref = dataset_ref.table(TABLE_NAME)
        table = bigquery.Table(table_ref, schema=schema)
        
        # Set partitioning and clustering
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        table.clustering_fields = ["event_type", "user_id", "partition_date"]
        
        try:
            table = client.create_table(table, exists_ok=True)
            logging.info(f"Created BigQuery table: {table.table_id}")
        except Exception as e:
            logging.warning(f"Table creation note: {e}")
            
        return {"status": "success", "message": "BigQuery table created"}
        
    except Exception as e:
        logging.error(f"Failed to setup BigQuery table: {e}")
        return {"status": "error", "message": str(e)}


def process_pubsub_to_bigquery(**context):
    """Process Pub/Sub messages and write to BigQuery"""
    try:
        # This function would be called by a Cloud Function or Dataflow
        # For Composer, we'll use PubSub to GCS to BigQuery pattern
        
        logging.info("Setting up Pub/Sub to GCS to BigQuery pipeline")
        return {"status": "success", "message": "Pipeline configured"}
        
    except Exception as e:
        logging.error(f"Failed to process Pub/Sub: {e}")
        return {"status": "error", "message": str(e)}


def generate_sample_events(**context):
    """Generate sample events for testing"""
    try:
        import random
        import time
        
        # Initialize Pub/Sub publisher
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        
        # Generate sample events
        event_types = ['user_activity', 'transaction', 'system_log', 'metric']
        actions = ['login', 'purchase', 'view', 'click', 'logout', 'signup']
        
        for i in range(50):
            event = {
                'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
                'event_type': random.choice(event_types),
                'timestamp': datetime.utcnow().isoformat(),
                'user_id': f"user_{random.randint(1, 10000)}",
                'session_id': f"session_{random.randint(1, 1000)}",
                'data': {
                    'action': random.choice(actions),
                    'value': round(random.uniform(1.0, 1000.0), 2),
                    'metadata': {
                        'source': 'composer_pipeline',
                        'version': '1.0',
                        'environment': 'production'
                    }
                },
                'source_system': 'composer_pipeline',
                'processing_status': 'raw'
            }
            
            # Publish event
            message = json.dumps(event).encode('utf-8')
            future = publisher.publish(topic_path, data=message)
            message_id = future.result()
            
            if i % 10 == 0:
                logging.info(f"Published {i+1}/50 events, latest ID: {message_id}")
        
        logging.info(f"Successfully published 50 sample events to Pub/Sub")
        return {"status": "success", "message": "50 events published"}
        
    except Exception as e:
        logging.error(f"Failed to generate sample events: {e}")
        return {"status": "error", "message": str(e)}


# Create DAG
with DAG(
    dag_id='realtime_data_pipeline_composer',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['realtime', 'pubsub', 'bigquery', 'data-engineering'],
    description='Real-time data pipeline using Google Cloud Composer'
) as dag:
    
    # Task 1: Create Pub/Sub infrastructure
    create_pubsub_task = PythonOperator(
        task_id='create_pubsub_infrastructure',
        python_callable=create_pubsub_infrastructure,
        docstring='Create Pub/Sub topic and subscription'
    )
    
    # Task 2: Setup BigQuery table
    setup_bigquery_task = PythonOperator(
        task_id='setup_bigquery_table',
        python_callable=setup_bigquery_table,
        docstring='Create BigQuery table with schema'
    )
    
    # Task 3: Generate sample events
    generate_events_task = PythonOperator(
        task_id='generate_sample_events',
        python_callable=generate_sample_events,
        docstring='Generate sample events for testing'
    )
    
    # Task 4: Transfer Pub/Sub to GCS (staging)
    pubsub_to_gcs = PubSubToGCSOperator(
        task_id='pubsub_to_gcs',
        project_id=PROJECT_ID,
        bucket=GCS_BUCKET,
        archive_bucket=GCS_BUCKET,
        pubsub_subscription=SUBSCRIPTION_NAME,
        ack_deadline=60,
        messages_per_batch=10,
        batch_size=10,
        on_failure_callback=True,
    )
    
    # Task 5: Load from GCS to BigQuery
    gcs_to_bigquery = BigQueryInsertJobOperator(
        task_id='gcs_to_bigquery',
        configuration={
            'query': f'''
                SELECT 
                    event_id,
                    event_type,
                    timestamp,
                    CURRENT_TIMESTAMP() as processing_timestamp,
                    user_id,
                    session_id,
                    JSON_EXTRACT_SCALAR(data, '$.action') as action,
                    JSON_EXTRACT_SCALAR(data, '$.value') as value,
                    JSON_EXTRACT_SCALAR(data, '$.metadata.source') as source,
                    JSON_EXTRACT_SCALAR(data, '$.metadata.version') as version,
                    JSON_EXTRACT_SCALAR(data, '$.metadata.environment') as environment,
                    source_system,
                    DATE(timestamp) as partition_date,
                    'processed' as processing_status
                FROM `{PROJECT_ID}.{DATASET_ID}.events_raw`
                WHERE DATE(timestamp) = DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            ''',
            'use_legacy_sql': False,
        },
        destination_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        time_partitioning={
            'type': 'DAY',
            'field': 'timestamp'
        },
        clustering_fields=['event_type', 'user_id', 'partition_date'],
    )
    
    # Define task dependencies
    create_pubsub_task >> setup_bigquery_task >> generate_events_task
    generate_events_task >> pubsub_to_gcs >> gcs_to_bigquery
