#!/usr/bin/env python3
"""
Feedback Handler for AI Monitoring Agent
"""
import json
from typing import Dict, Any
from learning_engine import LearningEngine
import ollama

class FeedbackHandler:
    def __init__(self, learning_engine: LearningEngine):
        self.learning_engine = learning_engine
        # Initialize Ollama client for feedback analysis
        try:
            # Test connection to Ollama
            response = ollama.list()
            self.ollama_client = ollama
        except Exception as e:
            print(f"Warning: Could not connect to Ollama for feedback analysis: {e}")
            self.ollama_client = None
        
    def process_slack_feedback(self, feedback_data: Dict[str, Any]) -> bool:
        """
        Process feedback received from Slack
        
        Args:
            feedback_data: Dictionary containing feedback information
                {
                    'incident_id': str,
                    'feedback_type': str,  # 'accurate', 'inaccurate', 'partially_correct'
                    'comments': str,
                    'correct_root_cause': str,
                    'correct_actions': list,
                    'user': str
                }
                
        Returns:
            bool: True if feedback was processed successfully
        """
        try:
            # Record feedback in learning engine
            self.learning_engine.record_feedback({
                'incident_id': feedback_data.get('incident_id'),
                'feedback_type': feedback_data.get('feedback_type'),
                'comments': feedback_data.get('comments'),
                'correct_root_cause': feedback_data.get('correct_root_cause'),
                'correct_actions': feedback_data.get('correct_actions'),
                'user': feedback_data.get('user'),
                'source': 'slack'
            })
            
            # Retrain models if enough feedback collected
            self._maybe_retrain_models()
            
            return True
        except Exception as e:
            print(f"Error processing feedback: {e}")
            return False
            
    def _maybe_retrain_models(self):
        """Retrain models if enough feedback has been collected"""
        # Check if we have enough feedback for retraining
        if len(self.learning_engine.feedback_data) >= 50:
            print("Retraining models with new feedback...")
            self.learning_engine.train_error_classifier()
            
    def validate_feedback(self, feedback_data: Dict[str, Any]) -> bool:
        """Validate feedback data before processing"""
        required_fields = ['incident_id', 'feedback_type', 'user']
        
        for field in required_fields:
            if field not in feedback_data:
                print(f"Missing required field: {field}")
                return False
                
        valid_feedback_types = ['accurate', 'inaccurate', 'partially_correct']
        if feedback_data['feedback_type'] not in valid_feedback_types:
            print(f"Invalid feedback type: {feedback_data['feedback_type']}")
            return False
            
        return True
        
    def generate_feedback_report(self) -> Dict[str, Any]:
        """Generate a report on feedback statistics with LLM insights"""
        feedback_data = self.learning_engine.feedback_data
        
        if not feedback_data:
            return {'message': 'No feedback data available'}
            
        # Calculate statistics
        total_feedback = len(feedback_data)
        feedback_by_type = {}
        
        for feedback in feedback_data:
            feedback_type = feedback.get('feedback_type', 'unknown')
            feedback_by_type[feedback_type] = feedback_by_type.get(feedback_type, 0) + 1
            
        # Calculate accuracy rate
        accurate_count = feedback_by_type.get('accurate', 0)
        accuracy_rate = accurate_count / total_feedback if total_feedback > 0 else 0
        
        # Generate insights using Phi-3 model
        insights = []
        if self.ollama_client and feedback_data:
            try:
                # Prepare feedback summary for LLM analysis
                feedback_summary = []
                for feedback in feedback_data[-10:]:  # Last 10 feedback entries
                    summary = f"Type: {feedback.get('feedback_type', 'unknown')}, "
                    if feedback.get('comments'):
                        summary += f"Comment: {feedback['comments'][:100]}..., "
                    if feedback.get('correct_root_cause'):
                        summary += f"Correct cause: {feedback['correct_root_cause']}"
                    feedback_summary.append(summary)
                
                feedback_text = "\n".join(feedback_summary)
                
                prompt = f"""Analyze the following feedback data from a system monitoring agent and provide 2-3 insights about patterns or trends:

                Feedback data:
                {feedback_text}

                Respond with only the insights, one per line, without any additional explanation."""

                response = self.ollama_client.generate(
                    model="phi3",
                    prompt=prompt,
                    stream=False,
                    options={
                        "temperature": 0.5,
                        "top_p": 0.9,
                        "stop": ["\n\n"]
                    }
                )

                # Parse insights from response
                llm_insights = [line.strip() for line in response['response'].strip().split('\n') if line.strip()]
                insights = llm_insights[:3]  # Limit to top 3 insights

            except Exception as e:
                print(f"Error generating feedback insights with Phi-3: {e}")
        
        return {
            'total_feedback': total_feedback,
            'feedback_by_type': feedback_by_type,
            'accuracy_rate': accuracy_rate,
            'last_feedback': feedback_data[-5:] if feedback_data else [],
            'insights': insights
        }

# Example usage in a web API endpoint
def create_feedback_endpoint():
    """
    Example Flask endpoint for receiving feedback
    
    This would typically be part of a web service that handles
    feedback from Slack or other sources.
    """
    from flask import Flask, request, jsonify
    
    app = Flask(__name__)
    learning_engine = LearningEngine()
    feedback_handler = FeedbackHandler(learning_engine)
    
    @app.route('/feedback', methods=['POST'])
    def receive_feedback():
        try:
            feedback_data = request.get_json()
            
            if not feedback_handler.validate_feedback(feedback_data):
                return jsonify({'error': 'Invalid feedback data'}), 400
                
            success = feedback_handler.process_slack_feedback(feedback_data)
            
            if success:
                return jsonify({'message': 'Feedback recorded successfully'}), 200
            else:
                return jsonify({'error': 'Failed to process feedback'}), 500
                
        except Exception as e:
            return jsonify({'error': f'Internal server error: {str(e)}'}), 500
            
    @app.route('/feedback/report', methods=['GET'])
    def feedback_report():
        try:
            report = feedback_handler.generate_feedback_report()
            return jsonify(report), 200
        except Exception as e:
            return jsonify({'error': f'Internal server error: {str(e)}'}), 500
    
    return app

if __name__ == "__main__":
    # This module is meant to be imported and used by the main agent
    print("Feedback Handler module loaded. Import this module to use feedback functionality.")