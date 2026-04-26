#!/usr/bin/env python3
"""
Pub/Sub Publisher for Real-time Data Pipeline
Sends events to Google Cloud Pub/Sub topic for processing by Dataflow pipeline
"""

import json
import time
import random
from datetime import datetime
from typing import Dict, Any
from google.cloud import pubsub_v1
from google.auth import default


class PubSubPublisher:
    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
    def publish_event(self, event_data: Dict[str, Any]) -> str:
        """Publish a single event to Pub/Sub topic"""
        try:
            message = json.dumps(event_data).encode('utf-8')
            future = self.publisher.publish(self.topic_path, data=message)
            message_id = future.result()
            print(f"Published message ID: {message_id}")
            return message_id
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise
    
    def generate_sample_event(self) -> Dict[str, Any]:
        """Generate sample event data for testing"""
        event_types = ['user_activity', 'transaction', 'system_log', 'metric']
        
        return {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'event_type': random.choice(event_types),
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': f"user_{random.randint(1, 10000)}",
            'session_id': f"session_{random.randint(1, 1000)}",
            'data': {
                'action': random.choice(['login', 'purchase', 'view', 'click']),
                'value': random.uniform(1.0, 1000.0),
                'metadata': {
                    'source': 'realtime_pipeline',
                    'version': '1.0',
                    'environment': 'production'
                }
            },
            'source_system': 'realtime_pipeline',
            'processing_status': 'raw'
        }
    
    def publish_batch_events(self, count: int = 10, delay: float = 0.1) -> None:
        """Publish multiple events with optional delay between messages"""
        print(f"Publishing {count} events...")
        
        for i in range(count):
            event = self.generate_sample_event()
            try:
                self.publish_event(event)
                if delay > 0 and i < count - 1:
                    time.sleep(delay)
            except Exception as e:
                print(f"Failed to publish event {i+1}: {e}")
                continue
        
        print(f"Successfully published batch of {count} events")


def main():
    # Configuration - these should be moved to config file for production
    PROJECT_ID = "your-gcp-project-id"
    TOPIC_NAME = "realtime-events"
    
    try:
        # Initialize publisher
        publisher = PubSubPublisher(PROJECT_ID, TOPIC_NAME)
        
        # Publish sample events
        publisher.publish_batch_events(count=50, delay=0.05)
        
    except Exception as e:
        print(f"Publisher error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
