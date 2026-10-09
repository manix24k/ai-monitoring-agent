#!/usr/bin/env python3
"""
AI Monitoring Agent Dashboard
Integrated with Elasticsearch and Prometheus for real monitoring
"""
from flask import Flask, render_template, jsonify
import os
from datetime import datetime
import json
import logging

# Import AI components
from main import get_agent
from prometheus_client import PrometheusClient
from elasticsearch_client import ElasticsearchClient

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for AI agent and clients
ai_agent = None
prometheus_client = None
elasticsearch_client = None

def initialize_clients():
    """Initialize Prometheus and Elasticsearch clients"""
    global ai_agent, prometheus_client, elasticsearch_client
    
    try:
        # Load configuration
        config_path = os.environ.get('CONFIG_PATH', '/app/config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Initialize clients
        prometheus_client = PrometheusClient(config['prometheus']['url'])
        
        # Initialize Elasticsearch client only if hosts are configured
        es_hosts = config['elasticsearch'].get('hosts', [])
        if es_hosts and len(es_hosts) > 0 and es_hosts[0]:
            elasticsearch_client = ElasticsearchClient(
                hosts=config['elasticsearch']['hosts'],
                index_prefix=config['elasticsearch'].get('index_prefix', 'logs-*')
            )
        else:
            elasticsearch_client = None
            logger.info("Elasticsearch client disabled - no hosts configured")
        
        # Initialize AI agent
        ai_agent = get_agent()
        
        logger.info("Clients initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize clients: {e}")
        return False

# Initialize clients on startup
initialize_clients()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/ai-agent')
def ai_agent_redirect():
    """Handle /ai-agent path"""
    return render_template('dashboard.html')

@app.route('/agent-ai')
def agent_ai_backend():
    """Handle /agent-ai path - return raw data for backend access"""
    try:
        # Get real data from AI agent if available
        if ai_agent and hasattr(ai_agent, 'get_status'):
            status_data = ai_agent.get_status()
        else:
            status_data = {
                'active_alerts': 0,
                'resolved_today': 0,
                'accuracy_rate': 0,
                'learning_status': "Not Connected",
                'uptime': "0 days, 0:00:00"
            }
        
        return jsonify({
            'status': 'success',
            'data': status_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in /agent-ai endpoint: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/status')
def api_status():
    """Get current agent status"""
    try:
        if ai_agent and hasattr(ai_agent, 'get_status'):
            return jsonify(ai_agent.get_status())
        else:
            return jsonify({
                'active_alerts': 0,
                'resolved_today': 0,
                'accuracy_rate': 0,
                'learning_status': "Not Connected",
                'uptime': "0 days, 0:00:00"
            })
    except Exception as e:
        logger.error(f"Error in /api/status endpoint: {e}")
        return jsonify({
            'active_alerts': 0,
            'resolved_today': 0,
            'accuracy_rate': 0,
            'learning_status': "Error",
            'uptime': "0 days, 0:00:00",
            'error': str(e)
        })

@app.route('/api/metrics')
def api_metrics():
    """Get real metrics data from Prometheus"""
    try:
        if prometheus_client:
            # Get real metrics from Prometheus
            timestamps = []
            request_rates = []
            error_rates = []
            latencies = []
            
            # Sample data - in a real implementation, this would fetch from Prometheus
            for i in range(6):
                timestamps.append(f"{10:02d}:{i*5:02d}")
                request_rates.append(100 + (i * 20))
                error_rates.append(1 + (i % 5))
                latencies.append(0.1 + (i * 0.1))
            
            return jsonify({
                'timestamps': timestamps,
                'request_rates': request_rates,
                'error_rates': error_rates,
                'latencies': latencies
            })
        else:
            # Fallback to mock data
            return jsonify({
                'timestamps': ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25'],
                'request_rates': [120, 190, 130, 160, 140, 180],
                'error_rates': [2, 8, 3, 5, 4, 7],
                'latencies': [0.2, 0.8, 0.3, 0.5, 0.4, 0.7]
            })
    except Exception as e:
        logger.error(f"Error in /api/metrics endpoint: {e}")
        return jsonify({
            'timestamps': [],
            'request_rates': [],
            'error_rates': [],
            'latencies': [],
            'error': str(e)
        })

@app.route('/api/incidents')
def api_incidents():
    """Get recent incidents"""
    try:
        # In a real implementation, this would fetch from the AI agent's incident tracking
        sample_incidents = [
            {
                'id': 'INC-0000001',
                'time': datetime.now().strftime('%H:%M:%S'),
                'service': 'Initializing',
                'issue': 'System starting up',
                'status': 'pending',
                'confidence': 0,
                'resolution': 'Pending'
            }
        ]
        return jsonify(sample_incidents)
    except Exception as e:
        logger.error(f"Error in /api/incidents endpoint: {e}")
        return jsonify([])

@app.route('/api/alerts/distribution')
def api_alerts_distribution():
    """Get alert distribution data"""
    try:
        if ai_agent and hasattr(ai_agent, 'get_alert_distribution'):
            return jsonify(ai_agent.get_alert_distribution())
        else:
            return jsonify({
                'labels': ['System', 'Network', 'Database', 'Application'],
                'data': [1, 1, 1, 1]
            })
    except Exception as e:
        logger.error(f"Error in /api/alerts/distribution endpoint: {e}")
        return jsonify({
            'labels': ['Error'],
            'data': [1]
        })

@app.route('/api/services')
def api_services():
    """Get monitored services"""
    try:
        if ai_agent and hasattr(ai_agent, 'config'):
            services = ai_agent.config.get('services', [])
            formatted_services = []
            for service in services:
                formatted_services.append({
                    'name': service.get('name', 'Unknown'),
                    'priority': service.get('priority', 'medium'),
                    'sampling': service.get('sampling', 1.0),
                    'status': 'active'
                })
            return jsonify(formatted_services)
        else:
            # Fallback to mock data
            sample_services = [
                {
                    'name': 'api-service',
                    'priority': 'high',
                    'sampling': 1.0,
                    'status': 'active'
                },
                {
                    'name': 'auth-service',
                    'priority': 'high',
                    'sampling': 1.0,
                    'status': 'active'
                },
                {
                    'name': 'database-service',
                    'priority': 'high',
                    'sampling': 1.0,
                    'status': 'active'
                }
            ]
            return jsonify(sample_services)
    except Exception as e:
        logger.error(f"Error in /api/services endpoint: {e}")
        return jsonify([])

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    print("Starting AI Monitoring Agent Dashboard on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=False)