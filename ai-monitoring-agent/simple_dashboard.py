#!/usr/bin/env python3
"""
Simple Web Dashboard for AI Monitoring Agent (minimal version for testing)
"""
from flask import Flask, render_template, jsonify, request
import json
import os
from datetime import datetime, timedelta
import sys
import os.path

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ensure templates and static directories exist
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

os.makedirs(template_dir, exist_ok=True)
os.makedirs(static_dir, exist_ok=True)

app = Flask(__name__, 
            template_folder=template_dir,
            static_folder=static_dir)

# Mock data for testing
mock_active_incidents = 3
mock_resolved_today = 12
mock_accuracy_rate = 94
mock_learning_status = "Active"
mock_uptime = "2 days, 4:32:15"

@app.route('/')
def dashboard():
    """Main dashboard page"""
    try:
        return render_template('dashboard.html')
    except Exception as e:
        return f"Error loading template: {str(e)}", 500

@app.route('/api/status')
def api_status():
    """Get current agent status"""
    try:
        return jsonify({
            'active_alerts': mock_active_incidents,
            'resolved_today': mock_resolved_today,
            'accuracy_rate': mock_accuracy_rate,
            'learning_status': mock_learning_status,
            'uptime': mock_uptime
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get status: {str(e)}'}), 500

@app.route('/api/metrics')
def api_metrics():
    """Get metrics data for charts"""
    try:
        # Return sample data
        return jsonify({
            'timestamps': ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25'],
            'request_rates': [120, 190, 130, 160, 140, 180],
            'error_rates': [2, 8, 3, 5, 4, 7],
            'latencies': [0.2, 0.8, 0.3, 0.5, 0.4, 0.7]
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get metrics: {str(e)}'}), 500

@app.route('/api/incidents')
def api_incidents():
    """Get recent incidents"""
    try:
        # Sample incidents data
        sample_incidents = [
            {
                'id': 'INC-1623489',
                'time': '10:25:32',
                'service': 'api-service',
                'issue': 'High error rate detected',
                'status': 'active',
                'confidence': 92,
                'resolution': 'Pending'
            },
            {
                'id': 'INC-1623485',
                'time': '09:45:17',
                'service': 'api-service',
                'issue': 'Database connection timeout',
                'status': 'resolved',
                'confidence': 87,
                'resolution': 'Auto-resolved'
            },
            {
                'id': 'INC-1623472',
                'time': '08:32:44',
                'service': 'auth-service',
                'issue': 'Authentication failure spike',
                'status': 'resolved',
                'confidence': 78,
                'resolution': 'Auto-resolved'
            }
        ]
        return jsonify(sample_incidents)
    except Exception as e:
        return jsonify({'error': f'Failed to get incidents: {str(e)}'}), 500

@app.route('/api/alerts/distribution')
def api_alerts_distribution():
    """Get alert distribution data"""
    try:
        return jsonify({
            'labels': ['Database', 'Network', 'Memory', 'Authentication'],
            'data': [12, 8, 5, 3]
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get alert distribution: {str(e)}'}), 500

@app.route('/api/configuration', methods=['GET', 'POST'])
def api_configuration():
    """Get or update configuration"""
    try:
        # Sample configuration
        sample_config = {
            "prometheus": {
                "url": "http://localhost:9090",
                "metrics": {
                    "request_rate_query": "sum(rate(http_requests_total[5m]))",
                    "error_rate_query": "sum(rate(http_requests_total{code=~\"5..\"}[5m]))",
                    "latency_query": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))"
                }
            },
            "signoz": {
                "url": "http://localhost:3301",
                "log_limit": 100,
                "trace_limit": 50
            },
            "anomaly_detection": {
                "contamination": 0.1,
                "window_size": 60,
                "check_interval": 60
            },
            "learning": {
                "model_dir": "./models",
                "feedback_threshold": 50,
                "history_limit": 1000,
                "adaptive_threshold_window": 100
            },
            "slack": {
                "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                "channel": "#alerts",
                "enable_feedback": True
            },
            "services": [
                {
                    "name": "api-service",
                    "endpoints": ["/users", "/orders", "/products"]
                }
            ]
        }
        
        if request.method == 'POST':
            # In a real implementation, this would update the configuration
            return jsonify({'status': 'success', 'message': 'Configuration updated'})
        
        # GET request - return current configuration
        return jsonify(sample_config)
    except Exception as e:
        return jsonify({'error': f'Failed to handle configuration: {str(e)}'}), 500

@app.route('/api/feedback', methods=['POST'])
def api_feedback():
    """Receive feedback from users"""
    try:
        # In a real implementation, this would be stored
        return jsonify({'status': 'success', 'message': 'Feedback recorded'})
    except Exception as e:
        return jsonify({'error': f'Failed to record feedback: {str(e)}'}), 500

@app.route('/api/learning/stats')
def api_learning_stats():
    """Get machine learning statistics"""
    try:
        return jsonify({
            'model_accuracy': 94,
            'feedback_count': 23,
            'incident_history': 45,
            'training_status': 'Completed',
            'last_training': '2023-06-15 14:30:00'
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get learning stats: {str(e)}'}), 500

@app.route('/api/incident/<incident_id>/resolve', methods=['POST'])
def resolve_incident(incident_id):
    """Resolve an incident"""
    try:
        # In a real implementation, this would update the incident status
        return jsonify({'status': 'success', 'message': f'Incident {incident_id} resolved'})
    except Exception as e:
        return jsonify({'error': f'Failed to resolve incident: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    print("AI Monitoring Agent Dashboard (Test Mode)")
    print("=" * 40)
    print(f"Template directory: {template_dir}")
    print(f"Static directory: {static_dir}")
    print("Starting web server on http://localhost:5001")
    print("Press CTRL+C to stop")
    
    app.run(host='0.0.0.0', port=5001, debug=True)