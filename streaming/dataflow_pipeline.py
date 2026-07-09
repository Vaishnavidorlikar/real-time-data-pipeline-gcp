#!/usr/bin/env python3
"""
Apache Beam Dataflow Pipeline for Real-time Data Processing
Processes streaming events from Pub/Sub and writes to BigQuery with transformations

"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub

# Make src/ importable so we can reuse the shared transform + schema logic
# instead of re-implementing (and drifting from) it here.
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from transform import EventTransformer  # noqa: E402
from bq_schema import BIGQUERY_SCHEMA  # noqa: E402


class ParseMessageFn(beam.DoFn):
    """Parse Pub/Sub messages and handle parsing errors"""

    def process(self, message, timestamp=beam.DoFn.TimestampParam):
        try:
            data = message.data.decode('utf-8')
            event = json.loads(data)

            # Validate required fields are present before transforming
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


class TransformEventFn(beam.DoFn):
    """
    Apply the shared EventTransformer (src/transform.py) to each event.
    This replaces the old inline FilterValidEvents logic so that transform
    rules live in exactly one place.
    """

    def setup(self):
        # instantiated once per worker, not per element
        self.transformer = EventTransformer()

    def process(self, event: Dict[str, Any]):
        transformed = self.transformer.transform_event(event)
        if transformed is not None:
            yield transformed
        else:
            logging.warning(f"Event failed transformation/validation: {event.get('event_id')}")


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
        parser.add_argument(
            '--requirements_file',
            default='requirements.txt',
            help='Requirements file for Dataflow worker dependencies'
        )


def run_pipeline(options: DataflowPipelineOptions) -> None:
    """Run the Dataflow pipeline"""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    with beam.Pipeline(options=options) as pipeline:
        # Read + parse + transform, using the SAME transform logic as
        # anything else in the repo that touches this data (src/transform.py).
        events = (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(
                subscription=options.input_subscription,
                with_attributes=False
            )
            | 'ParseMessages' >> beam.ParDo(ParseMessageFn())
            | 'TransformEvents' >> beam.ParDo(TransformEventFn())
        )

        # Write to BigQuery using the single shared schema definition
        # (src/bq_schema.py), so the table schema can never silently drift
        # from what this pipeline actually writes.
        (
            events
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=options.output_table,
                schema={"fields": [field.to_api_repr() for field in BIGQUERY_SCHEMA]},
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                batch_size=options.batch_size,
                custom_gcs_temp_location=options.temp_location,  # FIXED: was custom_gcs_temp_locations
            )
        )

        # Monitoring
        (
            events
            | 'CountEvents' >> beam.combiners.Count.Globally()
            | 'LogEventCount' >> beam.Map(lambda count: logger.info(f"Total events processed: {count}"))
        )


def main():
    """Main entry point"""

    pipeline_options = DataflowPipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).requirements_file = 'requirements.txt'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    try:
        run_pipeline(pipeline_options)
        return 0
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())