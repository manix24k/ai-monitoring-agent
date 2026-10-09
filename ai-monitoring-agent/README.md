# AI Monitoring Agent

An intelligent monitoring agent that integrates with Prometheus and Elasticsearch to provide AI-powered anomaly detection, root cause analysis, and automated incident response for multiple microservices.

## Features

- Continuous monitoring of multiple microservices
- Machine learning-based anomaly detection
- Automated root cause analysis using logs and traces
- Slack notifications with detailed incident reports
- Self-learning capabilities that improve over time
- Web dashboard for real-time monitoring and configuration
- Rate limiting and sampling to prevent system overload
- Support for service prioritization (high/medium)

## Prerequisites

- Kubernetes cluster (EKS, GKE, AKS, or local)
- Prometheus deployed in the `monitoring` namespace
- Elasticsearch deployed in the `earth` namespace
- kubectl configured for your cluster

## Quick Start

### 1. Build the Docker Image

```bash
cd ai-monitoring-agent
docker build -t ai-monitoring-agent:latest .
```

### 2. Deploy to Kubernetes

```bash
# Create the namespace
kubectl create namespace mercury

# Deploy all components
kubectl apply -f k8s/
```

### 3. Configure External Access (Optional)

To access the web dashboard externally, you can enable the ingress or use port forwarding:

```bash
# Port forward for local access
kubectl port-forward svc/ai-monitoring-agent 5000:80 -n mercury

# Or enable ingress in values.yaml and update hosts
```

## Monitored Services

The agent is configured to monitor the following services:

1. **cacheservice** (High Priority)
2. **bus-aggregation** (High Priority)
3. **channel-manager-su** (High Priority)
4. **otaconsumer** (High Priority)
5. **travelport** (Medium Priority)

Each service is monitored according to its priority level with appropriate sampling rates to balance monitoring coverage with system load.

## Configuration

### Update Slack Webhook

Edit the ConfigMap to use your Slack webhook URL:

```bash
kubectl edit configmap ai-monitoring-agent-config -n mercury
```

### Add/Remove Services

Modify the services section in the ConfigMap to monitor different services:

```json
"services": [
  {
    "name": "cacheservice",
    "priority": "high",
    "sampling": 1.0
  },
  {
    "name": "travelport",
    "priority": "medium",
    "sampling": 0.5
  }
]
```

### Safety Measures

The agent implements several safety measures to prevent system overload:

1. **Rate Limiting**: Limits Prometheus queries to 30 per minute
2. **Sampling Rates**: Configurable per service (0.0 to 1.0)
3. **Priority-Based Monitoring**: High vs medium priority services
4. **Adaptive Thresholds**: Reduces false positives over time

## Architecture

The AI Monitoring Agent consists of several key components:

1. **Prometheus Client** - Collects metrics from Prometheus
2. **Elasticsearch Client** - Retrieves logs and traces for root cause analysis
3. **Anomaly Detector** - Identifies unusual patterns using machine learning
4. **Root Cause Analyzer** - Correlates metrics, logs, and traces to determine causes
5. **Slack Notifier** - Sends detailed alerts to your team
6. **Learning Engine** - Continuously improves detection and analysis accuracy
7. **Web Dashboard** - Provides real-time visualization and management interface

## Monitoring the Agent

Check the agent status:

```bash
# Check pod status
kubectl get pods -n mercury

# Check logs
kubectl logs -l app=ai-monitoring-agent -n mercury

# Check service
kubectl get svc ai-monitoring-agent -n mercury
```

Access the web dashboard at `http://localhost:5000` (when using port forwarding).

## Customization

### Adjust Detection Sensitivity

Modify the contamination parameter in the ConfigMap to adjust anomaly detection sensitivity:

```json
"anomaly_detection": {
  "contamination": 0.1,  // Lower values = more sensitive
  "window_size": 120,
  "check_interval": 60
}
```

### Adjust Safety Parameters

Modify rate limiting and sampling parameters:

```json
"monitoring": {
  "sampling_rate": 0.5,
  "rate_limit": {
    "prometheus_queries_per_minute": 30,
    "check_interval": 60
  }
}
```

### Add More Services

Add additional services to monitor by extending the services array in the ConfigMap with appropriate priority and sampling values.

## API Endpoints

### Dashboard API

- `GET /api/status` - Get agent status
- `GET /api/metrics` - Get metrics data for charts
- `GET /api/incidents` - Get recent incidents
- `GET /api/alerts/distribution` - Get alert distribution data
- `GET /api/configuration` - Get current configuration
- `POST /api/configuration` - Update configuration
- `POST /api/feedback` - Submit feedback
- `GET /api/learning/stats` - Get machine learning statistics
- `POST /api/incident/<id>/resolve` - Resolve an incident
- `GET /health` - Health check endpoint

## Troubleshooting

### Common Issues

1. **Cannot connect to Prometheus**: Verify the Prometheus URL in the ConfigMap
2. **Cannot connect to Elasticsearch**: Check the Elasticsearch hosts configuration
3. **No alerts being generated**: Check the anomaly detection parameters
4. **Dashboard not showing data**: Ensure the agent is running and collecting metrics

### Logs

Check the agent logs for detailed information:

```bash
kubectl logs -l app=ai-monitoring-agent -n mercury
```

Look for ERROR or WARNING messages that indicate issues with connections or processing.

## License

MIT License