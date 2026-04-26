#!/usr/bin/env python3
"""
BigQuery schema definitions for Real-time Data Pipeline
Defines table schemas and partitioning/clustering strategies
"""

from typing import List, Dict, Any
from google.cloud import bigquery


# Main events table schema
BIGQUERY_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED", description="Unique identifier for the event"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED", description="Type of event (USER_ACTIVITY, TRANSACTION, etc.)"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Event timestamp in UTC"),
    bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="REQUIRED", description="When the event was processed"),
    bigquery.SchemaField("user_id", "STRING", mode="NULLABLE", description="User identifier"),
    bigquery.SchemaField("session_id", "STRING", mode="NULLABLE", description="Session identifier"),
    bigquery.SchemaField("action", "STRING", mode="NULLABLE", description="Action performed (login, purchase, etc.)"),
    bigquery.SchemaField("value", "FLOAT", mode="NULLABLE", description="Numeric value associated with the event"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE", description="Source system"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE", description="Event version"),
    bigquery.SchemaField("environment", "STRING", mode="NULLABLE", description="Environment (production, development, etc.)"),
    bigquery.SchemaField("source_system", "STRING", mode="NULLABLE", description="Original source system"),
    bigquery.SchemaField("partition_date", "DATE", mode="NULLABLE", description="Date partition for clustering"),
    bigquery.SchemaField("processing_status", "STRING", mode="NULLABLE", description="Processing status"),
    bigquery.SchemaField("event_hash", "STRING", mode="NULLABLE", description="Hash for deduplication"),
    bigquery.SchemaField("processing_latency_ms", "INTEGER", mode="NULLABLE", description="Processing latency in milliseconds")
]

# Table configuration
TABLE_CONFIGS = {
    "events": {
        "schema": BIGQUERY_SCHEMA,
        "partition_field": "timestamp",
        "partition_type": "DAY",
        "cluster_fields": ["event_type", "user_id", "partition_date"],
        "description": "Main events table with streaming data from real-time pipeline"
    }
}


class BigQuerySchemaManager:
    """Manages BigQuery table schemas and configurations"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    def create_table(self, dataset_id: str, table_name: str, config: Dict[str, Any]) -> None:
        """Create a BigQuery table with specified configuration"""
        table_ref = f"{self.project_id}.{dataset_id}.{table_name}"
        
        table = bigquery.Table(table_ref, schema=config["schema"])
        table.description = config["description"]
        
        # Set partitioning
        if "partition_field" in config:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=config["partition_field"]
            )
        
        # Set clustering
        if "cluster_fields" in config:
            table.clustering_fields = config["cluster_fields"]
        
        try:
            self.client.create_table(table)
            print(f"Created table {table_ref}")
        except Exception as e:
            print(f"Error creating table {table_ref}: {e}")
            raise
    
    def create_all_tables(self, dataset_id: str) -> None:
        """Create all configured tables"""
        for table_name, config in TABLE_CONFIGS.items():
            try:
                self.create_table(dataset_id, table_name, config)
            except Exception as e:
                print(f"Failed to create table {table_name}: {e}")


if __name__ == "__main__":
    # Example usage
    import os
    
    project_id = os.getenv("GCP_PROJECT_ID", "your-project-id")
    dataset_id = os.getenv("BQ_DATASET", "realtime_events")
    
    manager = BigQuerySchemaManager(project_id)
    
    try:
        manager.create_all_tables(dataset_id)
        print("All tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
