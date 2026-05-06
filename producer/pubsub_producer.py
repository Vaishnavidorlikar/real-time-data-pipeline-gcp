#!/usr/bin/env python3
"""Producer for real-time Pub/Sub event generation."""

import argparse
import csv
import json
import random
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from google.cloud import pubsub_v1


class PubSubProducer:
    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)

    def publish_event(self, event_data: Dict[str, Any]) -> str:
        message = json.dumps(event_data).encode("utf-8")
        future = self.publisher.publish(self.topic_path, data=message)
        message_id = future.result()
        print(f"Published event, message_id={message_id}")
        return message_id

    def generate_sample_event(self, index: int) -> Dict[str, Any]:
        event_types = ["click", "purchase", "view", "login", "search"]
        return {
            "event_id": f"event-{int(time.time() * 1000)}-{index}",
            "event_type": random.choice(event_types),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": f"user_{random.randint(1, 10000)}",
            "session_id": f"session_{random.randint(1, 1000)}",
            "action": random.choice(["login", "purchase", "view", "click"]),
            "value": round(random.uniform(1.0, 1000.0), 2),
            "source": "producer",
            "source_system": "pubsub_producer",
            "processing_status": "raw"
        }

    def generate_event_from_row(self, row: Dict[str, str], index: int) -> Dict[str, Any]:
        return {
            "event_id": row.get("event_id", f"event-{int(time.time() * 1000)}-{index}"),
            "event_type": row.get("event_type", "click"),
            "timestamp": row.get("timestamp", datetime.utcnow().isoformat() + "Z"),
            "user_id": row.get("user_id", f"user_{random.randint(1, 10000)}"),
            "session_id": row.get("session_id", f"session_{random.randint(1, 1000)}"),
            "action": row.get("action", "click"),
            "value": float(row.get("value", 0.0)) if row.get("value") else round(random.uniform(1.0, 1000.0), 2),
            "source": "kaggle_dataset",
            "source_system": "kaggle_stream",
            "processing_status": "raw"
        }

    def download_kaggle_file(self, dataset: str, file_name: str, output_dir: Path) -> Path:
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
        except ImportError as e:
            raise ImportError(
                "The Kaggle Python package is required to download data from Kaggle. "
                "Install it with `pip install kaggle` and configure ~/.kaggle/kaggle.json."
            ) from e

        output_dir.mkdir(parents=True, exist_ok=True)
        api = KaggleApi()
        api.authenticate()

        downloaded_path = api.dataset_download_file(
            dataset=dataset,
            file_name=file_name,
            path=str(output_dir),
            force=False,
            unzip=False,
        )
        downloaded_path = Path(downloaded_path)

        if downloaded_path.suffix == ".zip":
            with zipfile.ZipFile(downloaded_path, "r") as zf:
                zf.extractall(path=output_dir)
            extracted_path = output_dir / file_name
            if not extracted_path.exists():
                raise FileNotFoundError(f"Expected extracted file not found: {extracted_path}")
            return extracted_path

        return downloaded_path

    def publish_events(
        self,
        count: int = 100,
        interval: float = 1.0,
        kaggle_dataset: Optional[str] = None,
        kaggle_file: Optional[str] = None,
    ) -> None:
        if kaggle_dataset and kaggle_file:
            output_dir = Path("/tmp/kaggle_data")
            csv_path = self.download_kaggle_file(kaggle_dataset, kaggle_file, output_dir)
            with csv_path.open("r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for i, row in enumerate(reader):
                    if i >= count:
                        break
                    event = self.generate_event_from_row(row, i)
                    self.publish_event(event)
                    if interval > 0 and i < count - 1:
                        time.sleep(interval)
        else:
            for i in range(count):
                event = self.generate_sample_event(i)
                self.publish_event(event)
                if interval > 0 and i < count - 1:
                    time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Run Pub/Sub event producer")
    parser.add_argument("--project_id", required=True, help="GCP project id")
    parser.add_argument("--topic_name", required=True, help="Pub/Sub topic name")
    parser.add_argument("--count", type=int, default=100, help="Number of events to publish")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    parser.add_argument("--kaggle_dataset", default=None, help="Kaggle dataset slug, e.g. zynicide/wine-reviews")
    parser.add_argument("--kaggle_file", default=None, help="File name within the Kaggle dataset to stream, e.g. winemag-data-130k-v2.csv")
    args = parser.parse_args()

    producer = PubSubProducer(args.project_id, args.topic_name)
    producer.publish_events(
        count=args.count,
        interval=args.interval,
        kaggle_dataset=args.kaggle_dataset,
        kaggle_file=args.kaggle_file,
    )


if __name__ == "__main__":
    main()
