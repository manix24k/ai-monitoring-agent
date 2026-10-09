#!/usr/bin/env python3
"""
Minimal Web Dashboard for AI Monitoring Agent (HTML only, no backend dependencies)
"""
from flask import Flask, render_template, jsonify
import os
from datetime import datetime

# Ensure templates and static directories exist
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

app = Flask(__name__, 
            template_folder=template_dir,
            static_folder=static_dir)

@app.route('/')
def dashboard():
    """Main dashboard page - serves static HTML only"""
    try:
        return render_template('dashboard.html')
    except Exception as e:
        return f"Error loading template: {str(e)}", 500

@app.route('/api/status')
def api_status():
    """Get current agent status - returns static mock data"""
    try:
        return jsonify({
            'active_alerts': 3,
            'resolved_today': 12,
            'accuracy_rate': 94,
            'learning_status': "Active",
            'uptime': "2 days, 4:32:15"
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get status: {str(e)}'}), 500

@app.route('/api/metrics')
def api_metrics():
    """Get metrics data for charts - returns static mock data"""
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
    """Get recent incidents - returns static mock data"""
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
    """Get alert distribution data - returns static mock data"""
    try:
        return jsonify({
            'labels': ['Database', 'Network', 'Memory', 'Authentication'],
            'data': [12, 8, 5, 3]
        })
    except Exception as e:
        return jsonify({'error': f'Failed to get alert distribution: {str(e)}'}), 500

@app.route('/api/services')
def api_services():
    """Get monitored services - returns static mock data"""
    try:
        # Sample services data
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
        return jsonify({'error': f'Failed to get services: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    print("AI Monitoring Agent Dashboard (Minimal HTML Only Version)")
    print("=" * 60)
    print(f"Template directory: {template_dir}")
    print(f"Static directory: {static_dir}")
    print("Starting web server on http://localhost:5001")
    print("Press CTRL+C to stop")
    print("NOTE: This version runs WITHOUT Ollama, Phi-3, or any backend services")
    
    app.run(host='0.0.0.0', port=5001, debug=True)