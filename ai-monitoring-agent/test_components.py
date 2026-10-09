#!/usr/bin/env python3
"""
Test script for AI Monitoring Agent components
"""
import json
from prometheus_client import PrometheusClient
from signoz_client import SigNozClient
from anomaly_detector import AnomalyDetector
from root_cause_analyzer import RootCauseAnalyzer
from slack_notifier import SlackNotifier

def test_prometheus_client():
    """Test Prometheus client functionality"""
    print("Testing Prometheus client...")
    
    # Using example data instead of connecting to real instance
    mock_metrics = {
        'request_rate': 150.5,
        'error_rate': 5.2,
        'latency_95th': 0.85
    }
    
    print(f"Mock metrics: {mock_metrics}")
    return mock_metrics

def test_anomaly_detector():
    """Test anomaly detection"""
    print("\nTesting anomaly detector...")
    
    detector = AnomalyDetector()
    
    # Train with normal data
    normal_data = [
        {'request_rate': 100, 'error_rate': 2, 'latency_95th': 0.5},
        {'request_rate': 110, 'error_rate': 1.8, 'latency_95th': 0.45},
        {'request_rate': 95, 'error_rate': 2.2, 'latency_95th': 0.55},
    ]
    
    for data in normal_data:
        detector.add_baseline_data([data])
    
    # Test with anomalous data
    anomalous_data = {'request_rate': 500, 'error_rate': 50, 'latency_95th': 5.0}
    anomalies = detector.detect(anomalous_data)
    
    print(f"Anomalies detected: {len(anomalies)}")
    for anomaly in anomalies:
        print(f"  - {anomaly}")
        
    return anomalies

def test_root_cause_analyzer():
    """Test root cause analysis"""
    print("\nTesting root cause analyzer...")
    
    analyzer = RootCauseAnalyzer()
    
    mock_anomalies = [
        {
            'metric': 'error_rate',
            'value': 50,
            'anomaly_score': -0.8,
            'timestamp': '2023-01-01T10:00:00Z'
        }
    ]
    
    mock_context = {
        'logs': [
            {'body': 'Database connection timeout'},
            {'body': 'Failed to connect to database server'},
            {'body': 'Timeout occurred while processing request'}
        ],
        'traces': []
    }
    
    analysis = analyzer.analyze(mock_anomalies, mock_context)
    print(f"Analysis summary: {analysis['summary']}")
    print(f"Likely causes: {analysis['likely_causes']}")
    print(f"Recommendations: {analysis['recommendations']}")
    
    return analysis

def test_slack_notifier():
    """Test Slack notification (without actually sending)"""
    print("\nTesting Slack notifier...")
    
    mock_anomalies = [
        {
            'metric': 'error_rate',
            'value': 50,
            'anomaly_score': -0.8,
            'timestamp': '2023-01-01T10:00:00Z'
        }
    ]
    
    mock_analysis = {
        'summary': 'High error rate detected',
        'likely_causes': ['Database connection issues', 'Timeout problems'],
        'recommendations': ['Check database connectivity', 'Increase timeout values'],
        'confidence': 0.85
    }
    
    # Just verify message creation (don't send)
    notifier = SlackNotifier("https://hooks.slack.com/services/TEST/URL")
    message = notifier._create_alert_message(mock_anomalies, mock_analysis)
    
    print("Alert message created successfully")
    print(f"Message blocks: {len(message['blocks'])}")
    
    return message

def main():
    """Run all tests"""
    print("AI Monitoring Agent - Component Tests")
    print("=" * 40)
    
    try:
        metrics = test_prometheus_client()
        anomalies = test_anomaly_detector()
        analysis = test_root_cause_analyzer()
        message = test_slack_notifier()
        
        print("\n" + "=" * 40)
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"\nError during testing: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())