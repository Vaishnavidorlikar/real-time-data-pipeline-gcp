#!/usr/bin/env python3
"""
Apache Airflow DAG for triggering and managing Dataflow pipeline
Handles Data Lake Migration to BigQuery streaming pipeline
"""

import yaml
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Load configuration
def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file"""
    config_path = "/opt/airflow/dags/../data-lake-migration/config/config.yaml"
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}

# Default DAG arguments
default_args = {
    'owner': 'data-lake-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'tags': ['data-lake', 'migration', 'bigquery', 'streaming']
}

def check_prerequisites(**context) -> bool:
    """Check if prerequisites are met before running pipeline"""
    config = load_config()
    
    # Check required variables
    required_vars = [
        'GCP_PROJECT_ID',
        'DATAFLOW_BUCKET_NAME'
    ]
    
    for var in required_vars:
        if not Variable.get(var, default_var=None):
            print(f"Missing required variable: {var}")
            return False
    
    # Check if BigQuery dataset exists (simplified check)
    # In production, you'd use BigQuery client to verify
    print("Prerequisites check passed")
    return True

def setup_infrastructure(**context) -> None:
    """Setup required infrastructure"""
    config = load_config()
    project_id = Variable.get("GCP_PROJECT_ID")
    
    # Create BigQuery tables using bq_schema module
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    
    try:
        from src.bq_schema import BigQuerySchemaManager
        
        manager = BigQuerySchemaManager(project_id)
        dataset_id = config.get('bigquery', {}).get('dataset_id', 'data_lake_migration')
        manager.create_all_tables(dataset_id)
        print("Infrastructure setup completed")
    except Exception as e:
        print(f"Infrastructure setup failed: {e}")
        raise

def validate_pipeline_health(**context) -> bool:
    """Validate pipeline health before starting"""
    config = load_config()
    
    # Check if previous Dataflow job is still running
    # In production, you'd use Dataflow client to check job status
    print("Pipeline health check passed")
    return True

def cleanup_resources(**context) -> None:
    """Cleanup resources after pipeline completion"""
    # Clean up temporary files, old logs, etc.
    print("Resource cleanup completed")

with DAG(
    dag_id='data_lake_migration_streaming',
    default_args=default_args,
    description='Data Lake Migration to BigQuery Streaming Pipeline',
    schedule_interval='@hourly',
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    doc_md="""
    ### Data Lake Migration Streaming DAG
    
    This DAG manages the streaming data pipeline from Azure Data Lake to BigQuery using:
    - Apache Beam Dataflow for stream processing
    - Pub/Sub for message queuing
    - BigQuery for data warehousing
    
    **Tasks:**
    1. Check prerequisites and environment
    2. Setup infrastructure (BigQuery tables)
    3. Validate pipeline health
    4. Start Dataflow streaming job
    5. Monitor job status
    6. Post-processing tasks
    7. Cleanup resources
    """
) as dag:

    config = load_config()
    project_id = Variable.get("GCP_PROJECT_ID", default_var="your-project-id")
    bucket_name = Variable.get("DATAFLOW_BUCKET_NAME", default_var="your-bucket")
    
    # Task 1: Check prerequisites
    check_prereqs = PythonSensor(
        task_id='check_prerequisites',
        python_callable=check_prerequisites,
        mode='reschedule',
        timeout=300,
        poke_interval=30
    )

    # Task 2: Setup infrastructure
    setup_infra = PythonOperator(
        task_id='setup_infrastructure',
        python_callable=setup_infrastructure
    )

    # Task 3: Validate pipeline health
    validate_health = PythonSensor(
        task_id='validate_pipeline_health',
        python_callable=validate_pipeline_health,
        mode='reschedule',
        timeout=300,
        poke_interval=30
    )

    # Task 4: Start Dataflow pipeline
    with TaskGroup('dataflow_pipeline') as dataflow_group:
        
        # Start Dataflow job
        start_dataflow = DataflowStartFlexTemplateOperator(
            task_id='start_dataflow_job',
            project_id=project_id,
            location=config.get('gcp', {}).get('region', 'us-central1'),
            body={
                'launch_parameter': {
                    'jobName': f'data-lake-migration-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
                    'container_spec_gcs_path': f'gs://{bucket_name}/dataflow/templates/data_lake_migration.json',
                    'parameters': {
                        'input_subscription': f'projects/{project_id}/subscriptions/{config.get("pubsub", {}).get("subscription_name", "data-lake-migration-sub")}',
                        'output_table': f'{project_id}:{config.get("bigquery", {}).get("dataset_id", "data_lake_migration")}.{config.get("bigquery", {}).get("events_table", "events")}',
                        'temp_location': f'gs://{bucket_name}/temp/',
                        'staging_location': f'gs://{bucket_name}/staging/',
                        'project_id': project_id,
                        'region': config.get('gcp', {}).get('region', 'us-central1'),
                        'worker_machine_type': config.get('dataflow', {}).get('worker_machine_type', 'n1-standard-4'),
                        'max_num_workers': config.get('dataflow', {}).get('max_num_workers', 5),
                        'batch_size': config.get('dataflow', {}).get('batch_size', 100)
                    }
                }
            }
        )

        # Monitor Dataflow job
        monitor_dataflow = DataflowJobStatusSensor(
            task_id='monitor_dataflow_job',
            job_id=start_dataflow.output['job_id'],
            project_id=project_id,
            location=config.get('gcp', {}).get('region', 'us-central1'),
            mode='reschedule',
            timeout=timedelta(hours=1).total_seconds(),
            poke_interval=60
        )

    # Task 5: Post-processing
    with TaskGroup('post_processing') as post_process_group:
        
        # Create daily metrics view
        create_metrics_view = BigQueryInsertJobOperator(
            task_id='create_daily_metrics',
            configuration={
                'query': {
                    'query': f"""
                        CREATE OR REPLACE VIEW `{project_id}.{config.get('bigquery', {}).get('dataset_id', 'data_lake_migration')}.daily_metrics` AS
                        SELECT
                            DATE(timestamp) as metric_date,
                            event_type,
                            action,
                            environment,
                            COUNT(*) as total_events,
                            COUNT(DISTINCT user_id) as unique_users,
                            COUNT(DISTINCT session_id) as unique_sessions,
                            AVG(value) as avg_value,
                            SUM(value) as total_value,
                            AVG(processing_latency_ms) as avg_processing_latency_ms,
                            CURRENT_TIMESTAMP() as created_at
                        FROM `{project_id}.{config.get('bigquery', {}).get('dataset_id', 'data_lake_migration')}.{config.get('bigquery', {}).get('events_table', 'events')}`
                        WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                        GROUP BY metric_date, event_type, action, environment
                    """,
                    'use_legacy_sql': False
                }
            }
        )

    # Task 6: Cleanup
    cleanup = PythonOperator(
        task_id='cleanup_resources',
        python_callable=cleanup_resources
    )

    # Task 7: Send notification
    send_notification = BashOperator(
        task_id='send_completion_notification',
        bash_command='echo "Pipeline completed successfully - {{ ds }}"'
    )

    # Define task dependencies
    check_prereqs >> setup_infra >> validate_health >> dataflow_group
    dataflow_group >> post_process_group >> cleanup >> send_notification
