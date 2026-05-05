#!/usr/bin/env python3
"""
Apache Beam Dataflow Pipeline for Real-time Data Processing
Processes streaming events from Pub/Sub and writes to BigQuery with transformations
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub


class ParseMessageFn(beam.DoFn):
    """Parse Pub/Sub messages and handle parsing errors"""
    
    def process(self, message, timestamp=beam.DoFn.TimestampParam):
        try:
            # Decode message and parse JSON
            data = message.data.decode('utf-8')
            event = json.loads(data)
            
            # Add processing timestamp
            event['processing_timestamp'] = datetime.utcnow().isoformat()
            
            # Validate required fields
            required_fields = ['event_id', 'event_type', 'timestamp']
            for field in required_fields:
                if field not in event:
                    logging.warning(f"Missing required field '{field}' in event: {event}")
                    return
            
            yield event
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}, message: {message.data}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")


class FilterValidEvents(beam.DoFn):
    """Filter out invalid events and apply business rules"""
    
    def process(self, event: Dict[str, Any]):
        try:
            # Apply validation rules
            if not event.get('event_id'):
                return
                
            if not event.get('event_type'):
                return
                
            # Filter out old events (older than 24 hours)
            event_time = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            current_time = datetime.utcnow()
            time_diff = (current_time - event_time).total_seconds()
            
            if time_diff > 86400:  # 24 hours
                logging.warning(f"Filtering out old event: {event['event_id']}")
                return
            
            # Add enrichment fields
            event['partition_date'] = event_time.strftime('%Y-%m-%d')
            event['processing_status'] = 'processed'
            
            yield event
            
        except Exception as e:
            logging.error(f"Error in filter validation: {e}")


class DataflowPipelineOptions(PipelineOptions):
    """Custom pipeline options for the Dataflow job"""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_subscription',
            required=True,
            help='Pub/Sub subscription to read from'
        )
        parser.add_argument(
            '--output_table',
            required=True,
            help='BigQuery table to write to (project:dataset.table)'
        )
        parser.add_argument(
            '--temp_location',
            required=True,
            help='GCS temp location for Dataflow'
        )
        parser.add_argument(
            '--staging_location',
            required=True,
            help='GCS staging location for Dataflow'
        )
        parser.add_argument(
            '--project_id',
            required=True,
            help='GCP Project ID'
        )
        parser.add_argument(
            '--region',
            default='us-central1',
            help='Dataflow region'
        )
        parser.add_argument(
            '--worker_machine_type',
            default='n1-standard-4',
            help='Worker machine type'
        )
        parser.add_argument(
            '--max_num_workers',
            default=5,
            type=int,
            help='Maximum number of workers'
        )
        parser.add_argument(
            '--batch_size',
            default=100,
            type=int,
            help='Batch size for BigQuery writes'
        )


def run_pipeline(options: DataflowPipelineOptions) -> None:
    """Run the Dataflow pipeline"""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    with beam.Pipeline(options=options) as pipeline:
        # Read from Pub/Sub
        events = (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(
                subscription=options.input_subscription,
                with_attributes=False
            )
            | 'ParseMessages' >> beam.ParDo(ParseMessageFn())
            | 'FilterValidEvents' >> beam.ParDo(FilterValidEvents())
        )
        
        # Write to BigQuery
        (
            events
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=options.output_table,
                schema=[
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
                ],
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                batch_size=options.batch_size,
                custom_gcs_temp_locations=options.temp_location
            )
        )
        
        # Add monitoring and metrics
        (
            events
            | 'CountEvents' >> beam.combiners.Count.Globally()
            | 'LogEventCount' >> beam.Map(lambda count: logger.info(f"Total events processed: {count}"))
        )


def main():
    """Main entry point"""
    import sys
    
    # Create pipeline options
    pipeline_options = DataflowPipelineOptions()
    
    # Set streaming option
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Set setup file for dependencies
    pipeline_options.view_as(SetupOptions).setup_file = './setup.py'
    
    # Save main session
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    try:
        run_pipeline(pipeline_options)
        return 0
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
