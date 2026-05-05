#!/usr/bin/env python3
"""
Data transformation module for Real-time Data Pipeline
Handles event data transformation, enrichment, and validation
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import hashlib


class EventTransformer:
    """Transform events from Pub/Sub format to BigQuery schema"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Mapping for event type normalization
        self.event_type_mapping = {
            'user_activity': 'USER_ACTIVITY',
            'transaction': 'TRANSACTION',
            'system_log': 'SYSTEM_LOG',
            'metric': 'METRIC',
            'useractivity': 'USER_ACTIVITY',
            'TRANSACTION': 'TRANSACTION',
            'systemlog': 'SYSTEM_LOG'
        }
        
        # Valid actions for validation
        self.valid_actions = {
            'login', 'logout', 'purchase', 'view', 'click', 
            'search', 'add_to_cart', 'remove_from_cart', 'checkout'
        }
    
    def transform_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform a single event to BigQuery format
        
        Args:
            event: Raw event data from Pub/Sub
            
        Returns:
            Transformed event ready for BigQuery or None if invalid
        """
        try:
            transformed = {}
            
            # Basic fields
            transformed['event_id'] = self._clean_string(event.get('event_id'))
            transformed['event_type'] = self._normalize_event_type(event.get('event_type'))
            transformed['timestamp'] = self._parse_timestamp(event.get('timestamp'))
            transformed['processing_timestamp'] = self._parse_timestamp(event.get('processing_timestamp'))
            
            # User and session fields
            transformed['user_id'] = self._clean_string(event.get('user_id'))
            transformed['session_id'] = self._clean_string(event.get('session_id'))
            
            # Transform nested data
            data = event.get('data', {})
            transformed['action'] = self._normalize_action(data.get('action'))
            transformed['value'] = self._clean_numeric(data.get('value'))
            
            # Extract metadata
            metadata = data.get('metadata', {})
            transformed['source'] = self._clean_string(metadata.get('source', 'realtime_pipeline'))
            transformed['version'] = self._clean_string(metadata.get('version', '1.0'))
            transformed['environment'] = self._normalize_environment(metadata.get('environment'))
            
            # Additional fields
            transformed['source_system'] = self._clean_string(event.get('source_system'))
            transformed['partition_date'] = self._extract_partition_date(transformed['timestamp'])
            transformed['processing_status'] = self._clean_string(event.get('processing_status', 'processed'))
            
            # Add computed fields
            transformed['event_hash'] = self._generate_event_hash(transformed)
            transformed['processing_latency_ms'] = self._calculate_latency(
                transformed['timestamp'], 
                transformed['processing_timestamp']
            )
            
            # Validate transformed event
            if self._validate_transformed_event(transformed):
                return transformed
            else:
                self.logger.warning(f"Invalid transformed event: {transformed['event_id']}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error transforming event: {e}")
            return None
    
    def _clean_string(self, value: Any) -> Optional[str]:
        """Clean and validate string values"""
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip() if value.strip() else None
        return str(value) if value is not None else None
    
    def _clean_numeric(self, value: Any) -> Optional[float]:
        """Clean and validate numeric values"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _normalize_event_type(self, event_type: str) -> str:
        """Normalize event type to standard format"""
        if not event_type:
            return 'UNKNOWN'
        
        normalized = event_type.lower().replace(' ', '_')
        return self.event_type_mapping.get(normalized, normalized.upper())
    
    def _normalize_action(self, action: str) -> str:
        """Normalize action field"""
        if not action:
            return 'unknown'
        
        normalized = action.lower().replace(' ', '_')
        return normalized if normalized in self.valid_actions else 'other'
    
    def _normalize_environment(self, environment: str) -> str:
        """Normalize environment field"""
        if not environment:
            return 'unknown'
        
        env_mapping = {
            'prod': 'production',
            'production': 'production',
            'dev': 'development',
            'development': 'development',
            'test': 'testing',
            'staging': 'staging'
        }
        
        return env_mapping.get(environment.lower(), environment.lower())
    
    def _parse_timestamp(self, timestamp: str) -> Optional[str]:
        """Parse and normalize timestamp to ISO format"""
        if not timestamp:
            return None
        
        try:
            # Handle various timestamp formats
            if timestamp.endswith('Z'):
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(timestamp)
            
            # Ensure timezone is UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            return dt.isoformat()
            
        except ValueError:
            self.logger.warning(f"Invalid timestamp format: {timestamp}")
            return None
    
    def _extract_partition_date(self, timestamp: str) -> Optional[str]:
        """Extract date partition from timestamp"""
        if not timestamp:
            return None
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None
    
    def _generate_event_hash(self, event: Dict[str, Any]) -> str:
        """Generate unique hash for deduplication"""
        # Create a deterministic string from key fields
        hash_fields = [
            event.get('event_id', ''),
            event.get('timestamp', ''),
            event.get('user_id', ''),
            event.get('action', '')
        ]
        
        hash_string = '|'.join(str(field) for field in hash_fields)
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def _calculate_latency(self, event_timestamp: str, processing_timestamp: str) -> Optional[int]:
        """Calculate processing latency in milliseconds"""
        if not event_timestamp or not processing_timestamp:
            return None
        
        try:
            event_dt = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
            processing_dt = datetime.fromisoformat(processing_timestamp.replace('Z', '+00:00'))
            
            latency_ms = int((processing_dt - event_dt).total_seconds() * 1000)
            return max(0, latency_ms)  # Ensure non-negative
            
        except ValueError:
            return None
    
    def _validate_transformed_event(self, event: Dict[str, Any]) -> bool:
        """Validate transformed event meets requirements"""
        required_fields = ['event_id', 'event_type', 'timestamp']
        
        for field in required_fields:
            if not event.get(field):
                self.logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate event_type is not unknown
        if event.get('event_type') == 'UNKNOWN':
            self.logger.warning("Event type could not be normalized")
            return False
        
        # Validate timestamp is recent (within last 7 days)
        try:
            event_dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            current_dt = datetime.now(timezone.utc)
            days_diff = (current_dt - event_dt).days
            
            if days_diff > 7:
                self.logger.warning(f"Event too old: {days_diff} days")
                return False
                
        except ValueError:
            self.logger.warning("Invalid timestamp in transformed event")
            return False
        
        return True


class BatchTransformer:
    """Handle batch transformations for multiple events"""
    
    def __init__(self):
        self.transformer = EventTransformer()
    
    def transform_batch(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform a batch of events"""
        transformed_events = []
        
        for event in events:
            transformed = self.transformer.transform_event(event)
            if transformed:
                transformed_events.append(transformed)
        
        return transformed_events
    
    def get_transformation_stats(self, original_count: int, transformed_count: int) -> Dict[str, Any]:
        """Generate transformation statistics"""
        success_rate = (transformed_count / original_count * 100) if original_count > 0 else 0
        failure_count = original_count - transformed_count
        
        return {
            'original_events': original_count,
            'transformed_events': transformed_count,
            'failed_events': failure_count,
            'success_rate_percent': round(success_rate, 2)
        }
