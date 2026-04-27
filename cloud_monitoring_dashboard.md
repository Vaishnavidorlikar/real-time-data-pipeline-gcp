# Google Cloud Monitoring Dashboard Setup

This guide helps you create a comprehensive Cloud Monitoring dashboard for your real-time data pipeline using Google Cloud Monitoring.

## Quick Access
- **Cloud Monitoring**: Access via Google Cloud Console
- **Your Project**: `leafy-tractor-277020`
- **Dashboard URL**: Requires authentication - access via Cloud Console
- **Account**: dorlikarvaishnavi@gmail.com

## Step-by-Step Setup

### 1. Access Cloud Monitoring Dashboard

1. **Open Cloud Console**: Navigate to Google Cloud Console
2. **Select Project**: `leafy-tractor-277020`
3. **Navigate to Monitoring**: Go to Monitoring section in Cloud Console
4. **Go to Dashboards**: Navigate to Dashboards in Monitoring
5. **Create New Dashboard**: Click "Create Dashboard"

### 2. Dashboard Configuration

```yaml
Dashboard Settings:
  Name: "Real-time Data Pipeline Monitoring"
  Description: "Comprehensive monitoring for real-time streaming pipeline"
  Project: leafy-tractor-277020
  Refresh Interval: 1 minute
  Time Range: Last 1 hour (default)
```

## Widget Creation Guide

### **Quick Start - Create These Widgets:**

#### **Widget 1: Event Processing Rate (Line Chart)**
1. **Click "Add Widget"** → "Line chart"
2. **Metric**: `dataflow.googleapis.com/job/element_count`
3. **Filters**: 
   - `metric.labels.job_name` = `realtime-pipeline-*`
4. **Aggregation**: 
   - Alignment period: `1m`
   - Per-series aligner: `ALIGN_RATE`
5. **Title**: "Event Processing Rate"
6. **Y-axis**: "Events per minute"

#### **Widget 2: Processing Latency (Line Chart)**
1. **Click "Add Widget"** → "Line chart"
2. **Metric**: `custom.googleapis.com/pipeline/processing_latency_ms`
3. **Aggregation**: 
   - Alignment period: `1m`
   - Per-series aligner: `ALIGN_PERCENTILE_95`
4. **Title**: "Processing Latency (95th percentile)"
5. **Y-axis**: "Latency (ms)"
6. **Add threshold lines**: 1000ms (yellow), 5000ms (red)

#### **Widget 3: Pub/Sub Queue Depth (Single Stat)**
1. **Click "Add Widget"** → "Stat"
2. **Metric**: `pubsub.googleapis.com/subscription/num_undelivered_messages`
3. **Filters**: 
   - `resource.label.subscription_id` = `realtime-events-sub`
4. **Title**: "Pub/Sub Queue Depth"
5. **Aggregation**: `ALIGN_LATEST`

#### **Widget 4: Error Rate (Stacked Area Chart)**
1. **Click "Add Widget"** → "Stacked area chart"
2. **Metric**: `dataflow.googleapis.com/job/message_count`
3. **Filters**: 
   - `metric.labels.status` = `ERROR`
4. **Aggregation**: 
   - Alignment period: `1m`
   - Per-series aligner: `ALIGN_RATE`
5. **Title**: "Error Rate"
6. **Y-axis**: "Errors per minute"

#### **Widget 5: Dataflow Workers (Line Chart)**
1. **Click "Add Widget"** → "Line chart"
2. **Metric**: `dataflow.googleapis.com/job/worker_count`
3. **Filters**: 
   - `metric.labels.job_name` = `realtime-pipeline-*`
4. **Title**: "Active Dataflow Workers"
5. **Y-axis**: "Worker Count"

#### **Widget 6: BigQuery Storage (Single Stat)**
1. **Click "Add Widget"** → "Stat"
2. **Metric**: `bigquery.googleapis.com/storage/stored_bytes`
3. **Filters**: 
   - `resource.label.dataset_id` = `realtime_events`
4. **Title**: "BigQuery Storage"
5. **Display**: Auto format (GB, MB, etc.)

#### **Widget 7: CPU Utilization (Heatmap)**
1. **Click "Add Widget"** → "Heatmap"
2. **Metric**: `dataflow.googleapis.com/job/cpu_utilization`
3. **Filters**: 
   - `metric.labels.job_name` = `realtime-pipeline-*`
4. **Title**: "Dataflow CPU Utilization"
5. **Color scale**: Blue (0%) to Red (100%)

#### **Widget 8: Memory Utilization (Gauge)**
1. **Click "Add Widget"** → "Gauge"
2. **Metric**: `dataflow.googleapis.com/job/memory_utilization`
3. **Filters**: 
   - `metric.labels.job_name` = `realtime-pipeline-*`
4. **Title**: "Memory Utilization"
5. **Range**: 0-100%

### **Layout Configuration:**

**Row 1 (KPI Cards - 4 columns):**
- Widget 3: Pub/Sub Queue Depth
- Widget 6: BigQuery Storage  
- Widget 5: Active Workers
- Custom: Error Rate %

**Row 2 (Main Charts - Full width):**
- Widget 1: Event Processing Rate

**Row 3 (Performance - 2 columns):**
- Widget 2: Processing Latency
- Widget 4: Error Rate

**Row 4 (Resources - 2 columns):**
- Widget 7: CPU Utilization
- Widget 8: Memory Utilization

### **Advanced Widget Configurations:**

#### **Custom Metrics Widget (if needed):**
```bash
# Create custom metric first
gcloud monitoring metrics descriptor create \
  --project=leafy-tractor-277020 \
  --type="custom.googleapis.com/pipeline/processing_latency_ms" \
  --display-name="Pipeline Processing Latency" \
  --metric-kind="GAUGE" \
  --value-type="DOUBLE"
```

#### **Log-based Metrics Widget:**
1. **Create log-based metric**:
   ```bash
   gcloud logging metrics create pipeline_errors \
     --project=leafy-tractor-277020 \
     --log-filter='resource.type="dataflow_step" AND severity="ERROR"'
   ```
2. **Add widget** → "Line chart"
3. **Metric**: `logging.googleapis.com/user/pipeline_errors`
4. **Title**: "Pipeline Errors from Logs"

### **Quick Widget Setup Commands:**

```bash
# Enable required APIs
gcloud services enable monitoring.googleapis.com --project=leafy-tractor-277020

# Create notification channel for alerts
gcloud monitoring notification-channels create \
  --project=leafy-tractor-277020 \
  --type="email" \
  --display-name="Pipeline Alerts" \
  --channel-labels="email_address=dorlikarvaishnavi@gmail.com"
```

### **Widget Styling Tips:**

1. **Color Scheme**: Use Google Cloud colors (Blue #4285F4, Green #34A853, Red #EA4335)
2. **Font Size**: Keep titles readable (12-14px minimum)
3. **Chart Types**: Match data type to visualization
4. **Time Range**: Use consistent time ranges across widgets
5. **Refresh Rate**: Set to 1 minute for real-time monitoring

### **Dashboard Access:**
**Access your dashboard via Google Cloud Console:**
1. Navigate to Cloud Console
2. Select project: `leafy-tractor-277020`
3. Go to Monitoring → Dashboards
4. Find your dashboard in the list

**Steps:**
1. Follow the navigation steps above
2. Click "Add Widget" for each widget
3. Follow the configurations listed
4. Save the dashboard
5. Test with live pipeline data

## Dashboard Layout Design

### Header Section
- **Title**: Real-time Data Pipeline Monitoring
- **Project**: leafy-tractor-277020
- **Last Updated**: Dynamic timestamp
- **Time Range Selector**: 1h, 6h, 24h, 7d options

### KPI Cards Row (Top Row)
1. **Event Processing Rate**: Events per minute
2. **Average Latency**: Processing time in milliseconds
3. **Error Rate**: Percentage of failed events
4. **Active Workers**: Dataflow worker count

### Main Visualizations

#### 1. Event Processing Timeline
- **Type**: Line chart
- **Metric**: `dataflow.googleapis.com/job/element_count`
- **Filters**: Job name contains "realtime-pipeline"
- **Aggregation**: Rate per minute
- **Group By**: Job name

#### 2. Processing Latency Chart
- **Type**: Line chart or heatmap
- **Metric**: `custom.googleapis.com/pipeline/processing_latency_ms`
- **Aggregation**: Average, 95th percentile
- **Time Series**: Last 1 hour

#### 3. Error Rate Monitoring
- **Type**: Stack area chart
- **Metric**: `dataflow.googleapis.com/job/message_count`
- **Filters**: Error status codes
- **Aggregation**: Rate per minute

#### 4. Pub/Sub Queue Depth
- **Type**: Single line chart
- **Metric**: `pubsub.googleapis.com/subscription/num_undelivered_messages`
- **Resource**: Pub/Sub subscription "realtime-events-sub"
- **Aggregation**: Latest value

#### 5. Dataflow Worker Utilization
- **Type**: Multi-line chart
- **Metric**: `dataflow.googleapis.com/job/cpu_utilization`
- **Group By**: Worker ID
- **Aggregation**: Average percentage

#### 6. BigQuery Storage Usage
- **Type**: Single stat card
- **Metric**: `bigquery.googleapis.com/storage/stored_bytes`
- **Resource**: BigQuery dataset "realtime_events"
- **Aggregation**: Latest value

## Custom Metrics Setup

### Create Custom Metrics for Pipeline

```bash
# 1. Enable Cloud Monitoring API
gcloud services enable monitoring.googleapis.com --project=leafy-tractor-277020

# 2. Create custom metric descriptor
gcloud monitoring metrics descriptor create \
  --project=leafy-tractor-277020 \
  --type="custom.googleapis.com/pipeline/processing_latency_ms" \
  --display-name="Pipeline Processing Latency" \
  --description="Processing latency in milliseconds for pipeline events" \
  --metric-kind="GAUGE" \
  --value-type="DOUBLE"

# 3. Create error rate metric
gcloud monitoring metrics descriptor create \
  --project=leafy-tractor-277020 \
  --type="custom.googleapis.com/pipeline/error_rate" \
  --display-name="Pipeline Error Rate" \
  --description="Error rate percentage for pipeline" \
  --metric-kind="GAUGE" \
  --value-type="DOUBLE"
```

### Python Code to Send Custom Metrics

```python
from google.cloud import monitoring_v3
import time

def send_custom_metric(project_id, metric_type, value):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"
    
    series = monitoring_v3.TimeSeries()
    series.metric.type = metric_type
    series.resource.type = "global"
    
    point = series.points.add()
    point.interval.end_time.seconds = int(time.time())
    point.value.double_value = value
    
    client.create_time_series(name=project_name, time_series=[series])

# Usage example
send_custom_metric("leafy-tractor-277020", 
                  "custom.googleapis.com/pipeline/processing_latency_ms", 
                  150.5)
```

## Predefined Cloud Monitoring Metrics

### Dataflow Metrics
```yaml
Dataflow Job Metrics:
  - dataflow.googleapis.com/job/element_count
  - dataflow.googleapis.com/job/system_lag
  - dataflow.googleapis.com/job/cpu_utilization
  - dataflow.googleapis.com/job/memory_utilization
  - dataflow.googleapis.com/job/disk_utilization
  - dataflow.googleapis.com/job/message_count
  - dataflow.googleapis.com/job/worker_count
```

### Pub/Sub Metrics
```yaml
Pub/Sub Metrics:
  - pubsub.googleapis.com/subscription/num_undelivered_messages
  - pubsub.googleapis.com/subscription/oldest_unacked_message_age
  - pubsub.googleapis.com/topic/message_sizes
  - pubsub.googleapis.com/topic/num_published_messages
  - pubsub.googleapis.com/subscription/num_acknowledged_messages
```

### BigQuery Metrics
```yaml
BigQuery Metrics:
  - bigquery.googleapis.com/storage/stored_bytes
  - bigquery.googleapis.com/storage/tables
  - bigquery.googleapis/storage/rows
  - bigquery.googleapis/query/execution_times
  - bigquery.googleapis/query/scanned_bytes
```

### Compute Metrics
```yaml
Compute Engine Metrics:
  - compute.googleapis.com/instance/cpu/utilization
  - compute.googleapis.com/instance/disk/read_bytes_count
  - compute.googleapis.com/instance/disk/write_bytes_count
  - compute.googleapis.com/instance/network/received_bytes_count
  - compute.googleapis.com/instance/network/sent_bytes_count
```

## Alert Configuration

### Create Alert Policies

```bash
# 1. High Processing Latency Alert
gcloud monitoring alerts-policies create \
  --project=leafy-tractor-277020 \
  --display-name="High Processing Latency" \
  --condition-display-name="Latency > 5000ms" \
  --condition-filter='metric.type="custom.googleapis.com/pipeline/processing_latency_ms"' \
  --condition-aggregations="alignmentPeriod=60s,perSeriesAligner=ALIGN_PERCENTILE_95" \
  --condition-trigger-threshold-value=5000 \
  --condition-trigger-threshold-comparison=COMPARISON_GT \
  --notification-channels="projects/leafy-tractor-277020/notificationChannels/[CHANNEL_ID]"

# 2. Pub/Sub Backlog Alert
gcloud monitoring alerts-policies create \
  --project=leafy-tractor-277020 \
  --display-name="Pub/Sub Message Backlog" \
  --condition-display-name="Queue depth > 1000" \
  --condition-filter='metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"' \
  --condition-trigger-threshold-value=1000 \
  --condition-trigger-threshold-comparison=COMPARISON_GT \
  --notification-channels="projects/leafy-tractor-277020/notificationChannels/[CHANNEL_ID]"

# 3. Dataflow Job Failure Alert
gcloud monitoring alerts-policies create \
  --project=leafy-tractor-277020 \
  --display-name="Dataflow Job Failure" \
  --condition-display-name="Job status = FAILED" \
  --condition-filter='metric.type="dataflow.googleapis.com/job/job_status"' \
  --condition-trigger-threshold-value=1 \
  --condition-trigger-threshold-comparison=COMPARISON_GT \
  --notification-channels="projects/leafy-tractor-277020/notificationChannels/[CHANNEL_ID]"
```

### Notification Channels Setup

```bash
# Create Email Notification Channel
gcloud monitoring notification-channels create \
  --project=leafy-tractor-277020 \
  --type="email" \
  --display-name="Pipeline Alerts" \
  --channel-labels="email_address=dorlikarvaishnavi@gmail.com"

# List notification channels to get ID
gcloud monitoring notification-channels list --project=leafy-tractor-277020
```

## Dashboard Styling and Configuration

### Chart Configuration Examples

#### Event Processing Rate Chart
```yaml
Chart Settings:
  Title: "Event Processing Rate"
  Type: Line chart
  Metric: dataflow.googleapis.com/job/element_count
  Filters:
    - metric.labels.job_name: "realtime-pipeline-*"
  Aggregation:
    - Alignment: 60s
    - Per Series Aligner: ALIGN_RATE
  Y-Axis: "Events per minute"
  Color Scheme: Blue gradient
```

#### Processing Latency Chart
```yaml
Chart Settings:
  Title: "Processing Latency (95th percentile)"
  Type: Line chart
  Metric: custom.googleapis.com/pipeline/processing_latency_ms
  Aggregation:
    - Alignment: 60s
    - Per Series Aligner: ALIGN_PERCENTILE_95
  Y-Axis: "Latency (ms)"
  Threshold Lines:
    - Value: 1000 (Warning)
    - Value: 5000 (Critical)
```

#### Error Rate Chart
```yaml
Chart Settings:
  Title: "Error Rate"
  Type: Stack area chart
  Metric: dataflow.googleapis.com/job/message_count
  Filters:
    - metric.labels.status: "ERROR"
  Aggregation:
    - Alignment: 60s
    - Per Series Aligner: ALIGN_RATE
  Y-Axis: "Errors per minute"
  Color: Red
```

## Mobile and Sharing Configuration

### Dashboard Sharing
```yaml
Sharing Settings:
  - Visibility: Private (project members only)
  - Public Access: Disabled
  - Embedding: Enabled for authorized users
  - Export: PDF and PNG options enabled
```

### Mobile Optimization
- Responsive layout for mobile devices
- Touch-friendly chart interactions
- Simplified view for small screens
- Essential metrics prioritized

## Advanced Features

### Log-Based Metrics
```bash
# Create log-based metric for errors
gcloud logging metrics create pipeline_errors \
  --description="Count of pipeline processing errors" \
  --project=leafy-tractor-277020 \
  --log-filter='resource.type="dataflow_step" AND severity="ERROR"'

# Create log-based metric for successful processing
gcloud logging metrics create pipeline_success \
  --description="Count of successful pipeline operations" \
  --project=leafy-tractor-277020 \
  --log-filter='resource.type="dataflow_step" AND severity="INFO"'
```

### Service Level Objectives (SLOs)
```yaml
SLO Configuration:
  - Latency SLO: 95% of events processed within 1000ms
  - Availability SLO: 99.9% uptime for pipeline
  - Throughput SLO: Process 1000 events/minute minimum
  - Error Budget: Allow 0.1% error rate
```

### Uptime Checks
```bash
# Create uptime check for pipeline health endpoint
gcloud monitoring uptime-checks create \
  --project=leafy-tractor-277020 \
  --display-name="Pipeline Health Check" \
  --host="your-pipeline-endpoint.com" \
  --path="/health" \
  --port=80 \
  --check-interval=60s \
  --timeout=10s \
  --selected-regions="us-central1"
```

## Dashboard JSON Configuration

### Export/Import Dashboard Configuration

```json
{
  "displayName": "Real-time Data Pipeline Monitoring",
  "gridLayout": {
    "columns": "12",
    "widgets": [
      {
        "title": "Event Processing Rate",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "prometheusQuerySource": {
                  "query": "rate(dataflow_job_element_count[1m])"
                }
              }
            }
          ]
        }
      },
      {
        "title": "Processing Latency",
        "scorecard": {
          "dataView": {
            "timeSeriesQuery": {
              "prometheusQuerySource": {
                "query": "histogram_quantile(0.95, rate(custom_pipeline_processing_latency_ms_bucket[1m]))"
              }
            }
          }
        }
      }
    ]
  }
}
```

## Deployment Steps

### 1. Create Dashboard
1. **Navigate**: Go to Cloud Console → Monitoring → Dashboards
2. **Create New**: Click "Create Dashboard"
3. **Name**: "Real-time Data Pipeline Monitoring"
4. **Add Widgets**: Follow the layout design above
5. **Configure Charts**: Use the metric configurations provided
6. **Save Dashboard**: Save and test functionality

### 2. Set Up Alerts
1. **Alerting**: Navigate to Monitoring → Alerting
2. **Create Policies**: Set up the alert policies defined above
3. **Notification Channels**: Configure email notifications to dorlikarvaishnavi@gmail.com
4. **Test Alerts**: Verify alert delivery and functionality

### 3. Validate Dashboard
1. **Generate Test Data**: Run the pipeline with sample events
2. **Check Metrics**: Verify all charts display data correctly
3. **Test Alerts**: Trigger test alerts to ensure notifications work
4. **Review Performance**: Ensure dashboard loads quickly and updates properly

## Integration with Pipeline Code

### Add Monitoring to Python Pipeline

```python
import google.cloud.monitoring_v3 as monitoring
import time

class PipelineMonitor:
    def __init__(self, project_id):
        self.client = monitoring.MetricServiceClient()
        self.project_name = f"projects/{project_id}"
    
    def record_latency(self, latency_ms):
        """Record processing latency"""
        series = monitoring.TimeSeries()
        series.metric.type = "custom.googleapis.com/pipeline/processing_latency_ms"
        series.resource.type = "global"
        
        point = series.points.add()
        point.interval.end_time.seconds = int(time.time())
        point.value.double_value = latency_ms
        
        self.client.create_time_series(name=self.project_name, time_series=[series])
    
    def record_event_processed(self, event_type):
        """Record successful event processing"""
        series = monitoring.TimeSeries()
        series.metric.type = "custom.googleapis.com/pipeline/events_processed"
        series.resource.type = "global"
        series.metric.labels["event_type"] = event_type
        
        point = series.points.add()
        point.interval.end_time.seconds = int(time.time())
        point.value.int_value = 1
        
        self.client.create_time_series(name=self.project_name, time_series=[series])

# Usage in pipeline
monitor = PipelineMonitor("leafy-tractor-277020")

# Record metrics
start_time = time.time()
# ... process event ...
latency = (time.time() - start_time) * 1000
monitor.record_latency(latency)
monitor.record_event_processed("user_activity")
```

## Support and Resources

### Documentation Links
- **Cloud Monitoring**: https://cloud.google.com/monitoring/docs
- **Alerting**: https://cloud.google.com/monitoring/alerts
- **Custom Metrics**: https://cloud.google.com/monitoring/custom-metrics
- **Dashboard Builder**: https://cloud.google.com/monitoring/dashboards

### Project Information
- **Project ID**: leafy-tractor-277020
- **Account**: dorlikarvaishnavi@gmail.com
- **Region**: us-central1 (recommended)
- **Dashboard URL**: Access via Cloud Console → Monitoring → Dashboards

---

## Next Steps

1. **Access Dashboard**: Use the provided URL to access your monitoring dashboard
2. **Configure Metrics**: Set up the custom metrics and charts as described
3. **Create Alerts**: Configure alert policies for your pipeline
4. **Test Integration**: Verify all metrics and alerts work correctly
5. **Monitor Regularly**: Use the dashboard to track pipeline performance

**Your Google Cloud Monitoring dashboard will provide comprehensive real-time insights into your data pipeline performance, system health, and operational metrics!**
