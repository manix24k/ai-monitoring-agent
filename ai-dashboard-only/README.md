# AI Monitoring Agent - Dashboard Only

This is a simplified version of the AI Monitoring Agent dashboard that runs independently without the full monitoring agent backend.

## Features

- Real-time dashboard with metrics visualization
- Incident tracking and display
- Service monitoring status
- Configuration management interface
- Responsive design with modern UI

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the dashboard:
   ```bash
   python app.py
   ```

3. Access the dashboard at http://localhost:5000

## Docker

To run with Docker:

```bash
docker build -t ai-dashboard-only .
docker run -p 5000:5000 ai-dashboard-only
```

## Kubernetes

To deploy to Kubernetes:

```bash
kubectl apply -f k8s-deployment.yaml
```

## API Endpoints

- `/` - Main dashboard page
- `/api/status` - System status information
- `/api/metrics` - Metrics data for charts
- `/api/incidents` - Recent incidents
- `/api/alerts/distribution` - Alert distribution data
- `/api/services` - Monitored services
- `/health` - Health check endpoint

## Development

The dashboard uses:
- Flask for the web framework
- Bootstrap 5 for styling
- Chart.js for data visualization