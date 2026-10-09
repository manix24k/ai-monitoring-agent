#!/usr/bin/env python3
"""
Slack Notification for AI Monitoring Agent
"""
import requests
import json
from typing import Dict, List
import os

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
    def send_alert(self, anomalies: List[Dict], root_cause: Dict):
        """Send detailed alert to Slack"""
        if not anomalies:
            return
            
        # Create alert message
        message = self._create_alert_message(anomalies, root_cause)
        
        # Send to Slack
        self._send_to_slack(message)
        
    def send_enhanced_alert(self, anomalies: List[Dict], analysis: Dict, 
                           similar_incidents: List[Dict], remedial_actions: List[str]):
        """Send enhanced alert with learning insights to Slack"""
        if not anomalies:
            return
            
        # Create enhanced alert message
        message = self._create_enhanced_alert_message(anomalies, analysis, 
                                                     similar_incidents, remedial_actions)
        
        # Send to Slack
        self._send_to_slack(message)
        
    def _create_alert_message(self, anomalies: List[Dict], root_cause: Dict) -> Dict:
        """Create formatted alert message for Slack"""
        # Main alert section
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 API Monitoring Alert"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Anomaly Detected*\n{root_cause.get('summary', 'Service anomaly detected')}"
                }
            }
        ]
        
        # Anomaly details
        anomaly_details = []
        for anomaly in anomalies:
            metric = anomaly.get('metric') or anomaly.get('type') or anomaly.get('service') or 'unknown'
            value = anomaly.get('value', anomaly.get('count', anomaly.get('description', 'n/a')))
            score_val = anomaly.get('anomaly_score', anomaly.get('score', 0.0))
            try:
                score = float(score_val)
            except Exception:
                score = 0.0
            detail = f"• *{metric}*: {value} (Score: {score:.2f})"
            anomaly_details.append(detail)
            
        if anomaly_details:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Anomaly Details*\n" + "\n".join(anomaly_details)
                }
            })
        
        # Root cause analysis
        causes = root_cause.get('likely_causes', [])
        if causes:
            cause_text = "\n".join([f"• {cause}" for cause in causes])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Likely Causes* (Confidence: {root_cause.get('confidence', 0)*100:.1f}%)\n{cause_text}"
                }
            })
        
        # Recommendations
        recommendations = root_cause.get('recommendations', [])
        if recommendations:
            rec_text = "\n".join([f"• {rec}" for rec in recommendations])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Recommendations*\n{rec_text}"
                }
            })
        
        # Timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🔔 Alert generated at {anomalies[0].get('timestamp', 'Unknown time')}"
                }
            ]
        })
        
        return {
            "blocks": blocks,
            "icon_emoji": ":rotating_light:",
            "username": "AI Monitoring Agent"
        }
        
    def _create_enhanced_alert_message(self, anomalies: List[Dict], analysis: Dict,
                                      similar_incidents: List[Dict], remedial_actions: List[str]) -> Dict:
        """Create enhanced alert message with learning insights"""
        # Main alert section
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 AI-Enhanced Monitoring Alert"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Anomaly Detected*\n{analysis.get('summary', 'Service anomaly detected')}"
                }
            }
        ]
        
        # AI Insights section
        insights = []
        error_category = analysis.get('error_category', 'unknown')
        if error_category != 'unknown':
            insights.append(f"• *Error Category*: {error_category}")
            
        confidence = analysis.get('confidence', 0)
        insights.append(f"• *AI Confidence*: {confidence*100:.1f}%")
        
        if similar_incidents:
            insights.append(f"• *Similar Incidents*: {len(similar_incidents)} previous occurrences found")
            
        if insights:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*AI Insights*\n" + "\n".join(insights)
                }
            })
        
        # Anomaly details
        anomaly_details = []
        for anomaly in anomalies:
            metric = anomaly.get('metric') or anomaly.get('type') or anomaly.get('service') or 'unknown'
            value = anomaly.get('value', anomaly.get('count', anomaly.get('description', 'n/a')))
            score_val = anomaly.get('anomaly_score', anomaly.get('score', 0.0))
            try:
                score = float(score_val)
            except Exception:
                score = 0.0
            detail = f"• *{metric}*: {value} (Score: {score:.2f})"
            anomaly_details.append(detail)
            
        if anomaly_details:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Anomaly Details*\n" + "\n".join(anomaly_details)
                }
            })
        
        # Likely causes with enhanced confidence
        causes = analysis.get('likely_causes', [])
        if causes:
            cause_text = "\n".join([f"• {cause}" for cause in causes])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Likely Causes* (Enhanced Confidence: {confidence*100:.1f}%)\n{cause_text}"
                }
            })
        
        # Remedial actions from knowledge base
        if remedial_actions:
            action_text = "\n".join([f"• {action}" for action in remedial_actions[:5]])  # Limit to top 5
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Recommended Actions*\n{action_text}"
                }
            })
        
        # Similar incidents (if any)
        if similar_incidents:
            incident_summary = f"Found {len(similar_incidents)} similar incidents in history. "
            incident_summary += "This suggests a recurring pattern that has been successfully resolved before."
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Historical Context*\n{incident_summary}"
                }
            })
        
        # Feedback request
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":bulb: *Help us learn!* Was this analysis accurate? Please provide feedback to improve our AI."
            }
        })
        
        # Timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🔔 Alert generated at {anomalies[0].get('timestamp', 'Unknown time')} | 🤖 AI-Powered Analysis"
                }
            ]
        })
        
        return {
            "blocks": blocks,
            "icon_emoji": ":rotating_light:",
            "username": "AI Monitoring Agent"
        }
        
    def _send_to_slack(self, message: Dict):
        """Send message to Slack webhook"""
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(message),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            print("Alert sent to Slack successfully")
        except Exception as e:
            print(f"Error sending alert to Slack: {e}")
