#!/usr/bin/env python3
"""
Test script for Ollama Phi-3 integration with AI Monitoring Agent
"""
import ollama
from root_cause_analyzer import RootCauseAnalyzer
from learning_engine import LearningEngine
from feedback_handler import FeedbackHandler

def test_ollama_connection():
    """Test connection to Ollama and Phi-3 model"""
    print("Testing Ollama connection...")
    try:
        # List available models
        models = ollama.list()
        print("Ollama connection successful")
        print("Available models:")
        for model in models.models:
            print(f"  - {model}")
        
        # Test Phi-3 model specifically
        response = ollama.chat(
            model="phi3",
            messages=[{"role": "user", "content": "Hello, Phi-3! Please introduce yourself in one sentence."}],
            options={"temperature": 0.7}
        )
        print(f"Phi-3 response: {response.message.content}")
        return True
    except Exception as e:
        print(f"Error connecting to Ollama: {e}")
        return False

def test_root_cause_analyzer():
    """Test root cause analyzer with Ollama integration"""
    print("\nTesting Root Cause Analyzer...")
    try:
        analyzer = RootCauseAnalyzer()
        
        # Test summarization
        anomalies_text = "High latency detected in user-service API endpoint /api/users with response time of 5.2s (threshold: 1s). High error rate of 15% in payment-service API endpoint /api/payments (threshold: 2%)."
        summary = analyzer._summarize_anomalies(anomalies_text)
        print(f"Summary: {summary}")
        
        # Test context analysis
        logs_traces = {
            'logs': [
                {'body': 'ERROR: Connection timeout to database'},
                {'body': 'WARN: High memory usage detected (85%)'},
                {'body': 'ERROR: Failed to process payment request'}
            ]
        }
        context_analysis = analyzer._analyze_context(logs_traces)
        print(f"Root causes: {context_analysis['causes']}")
        print(f"Recommendations: {context_analysis['recommendations']}")
        
        return True
    except Exception as e:
        print(f"Error testing root cause analyzer: {e}")
        return False

def test_learning_engine():
    """Test learning engine with Ollama integration"""
    print("\nTesting Learning Engine...")
    try:
        learning_engine = LearningEngine()
        
        # Test error classification
        error_text = "Connection timeout occurred while trying to connect to the database server at db.example.com:5432"
        classification = learning_engine.classify_error(error_text)
        print(f"Error classification: {classification}")
        
        # Test remedial actions
        incident = {
            'logs': [
                {'body': 'ERROR: Connection timeout to database'},
                {'body': 'WARN: High memory usage detected (85%)'}
            ],
            'metrics': {
                'latency_95th': 5.2,
                'error_rate': 0.15
            }
        }
        actions = learning_engine.get_remedial_actions(incident)
        print(f"Remedial actions: {actions}")
        
        return True
    except Exception as e:
        print(f"Error testing learning engine: {e}")
        return False

def test_feedback_handler():
    """Test feedback handler with Ollama integration"""
    print("\nTesting Feedback Handler...")
    try:
        learning_engine = LearningEngine()
        feedback_handler = FeedbackHandler(learning_engine)
        
        # Add some mock feedback data for testing
        mock_feedback = {
            'incident_id': 'TEST-001',
            'feedback_type': 'accurate',
            'comments': 'The root cause analysis was correct',
            'correct_root_cause': 'Database connection timeout',
            'correct_actions': ['Increase timeout threshold', 'Check database connectivity'],
            'user': 'test_user'
        }
        learning_engine.record_feedback(mock_feedback)
        
        # Test feedback report generation
        report = feedback_handler.generate_feedback_report()
        print(f"Feedback report generated: {list(report.keys())}")
        
        return True
    except Exception as e:
        print(f"Error testing feedback handler: {e}")
        return False

def main():
    """Run all integration tests"""
    print("Running Ollama Phi-3 Integration Tests for AI Monitoring Agent")
    print("=" * 60)
    
    tests = [
        test_ollama_connection,
        test_root_cause_analyzer,
        test_learning_engine,
        test_feedback_handler
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 60)
    print(f"Tests completed: {passed}/{len(tests)} passed")
    
    if passed == len(tests):
        print("All tests passed! Ollama Phi-3 integration is working correctly.")
    else:
        print("Some tests failed. Please check the error messages above.")

if __name__ == "__main__":
    main()