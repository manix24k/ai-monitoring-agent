# Service Configuration Guide for AI Monitoring Agent

## Accessing the Dashboard

The AI Monitoring Agent dashboard will be available at:
**http://mercury.fabmailers.in/ai-agent**

## Configuring Services for Analysis

To add services for monitoring and analysis, update the ConfigMap in `k8s/configmap.yaml`.

### Services Configuration

The `services` array in the ConfigMap defines which services the AI Monitoring Agent will monitor:

```json
"services": [
  {
    "name": "service-name",
    "endpoints": ["/api/endpoint1", "/api/endpoint2"]
  }
]
```

### Example Configuration

```json
"services": [
  {
    "name": "otaconsumer",
    "endpoints": ["/api/*"]
  },
  {
    "name": "user-service",
    "endpoints": ["/users", "/profiles", "/auth"]
  },
  {
    "name": "booking-service",
    "endpoints": ["/bookings", "/reservations", "/payments"]
  }
]
```

### Adding New Services

1. Add a new object to the `services` array:
   ```json
   {
     "name": "your-service-name",
     "endpoints": ["/api/endpoint1", "/api/endpoint2"]
   }
   ```

2. The `name` should match the service name used in:
   - Prometheus job labels
   - Elasticsearch indices
   - Kubernetes service names

3. The `endpoints` array should contain the API endpoints you want to monitor

### Prometheus Integration

The agent uses generic Prometheus queries that automatically discover services:
- Request rate: `sum(rate(http_requests_total[5m])) by (job)`
- Error rate: `sum(rate(http_requests_total{code=~"5.."}[5m])) by (job)`
- Latency: `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, job))`

### Elasticsearch Integration

The agent collects logs and traces from Elasticsearch using:
- Index pattern: `logs-*`
- Service-specific filtering based on the service name

### Applying Configuration Changes

After updating the ConfigMap:
1. Commit and push changes to the `ai-modal` branch
2. Your Jenkins pipeline will automatically deploy the updated configuration
3. The AI Monitoring Agent will begin monitoring the new services

### Verification

1. Visit the dashboard at http://mercury.fabmailers.in/ai-agent
2. Check the "Recent Incidents" section for data from your services
3. Monitor the "Metrics Overview" charts for service-specific metrics
4. Verify that the AI-powered root cause analysis works for all configured services