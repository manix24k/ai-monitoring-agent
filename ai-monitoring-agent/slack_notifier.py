#!/usr/bin/env python3
"""
Slack Notification for AI Monitoring Agent
Supports Slack Bot Token (xoxb-...) via chat.postMessage API.
"""
import requests
import json
from typing import Dict, List, Optional
import os

SLACK_API_POST = "https://slack.com/api/chat.postMessage"


class SlackNotifier:
    def __init__(self, webhook_url: str = "", bot_token: str = "", channel: str = ""):
        self.webhook_url = webhook_url
        self.bot_token = bot_token.strip()
        self.channel = channel.strip()
        # Prefer bot token when available
        self._use_bot_api = bool(self.bot_token and self.channel)
        
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
        
    def send_codexa_service_report(self, service: str, namespace: str,
                                    issues_with_fixes: list,
                                    pr_urls: dict = None):
        """Send one consolidated code-fix report triggered by Generate PR."""
        if not issues_with_fixes:
            return

        import logging as _log
        pr_urls = pr_urls or {}
        total = len(issues_with_fixes)
        first_pr_url = next(iter(pr_urls.values()), "")

        # ── Header ──────────────────────────────────────────────────────────
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🤖 CodeXA — PR Created: {service}", "emoji": True}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Service:*\n`{service}`"},
                    {"type": "mrkdwn", "text": f"*Namespace:*\n`{namespace}`"},
                    {"type": "mrkdwn", "text": f"*Issues Fixed:*\n`{total}`"},
                    {"type": "mrkdwn", "text": f"*Status:*\n✅ PR Raised"},
                ]
            },
        ]

        if first_pr_url:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":merged: *Pull Request:* <{first_pr_url}|View PR on GitHub>"}
            })

        blocks.append({"type": "divider"})

        # ── Per-issue details ────────────────────────────────────────────────
        for idx, (issue, fix) in enumerate(issues_with_fixes, 1):
            if issue is None:
                continue
            if len(blocks) >= 45:
                remaining = total - idx + 1
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": f"_…and {remaining} more issue(s) — <{first_pr_url}|see PR for full details>._"}})
                break

            exc_type = getattr(issue, 'exception_type', '') or 'Unknown Error'
            if hasattr(exc_type, 'value'):
                exc_type = exc_type.value

            file_path = (getattr(fix, 'file_path', '') or getattr(issue, 'file_path', '')) if fix else getattr(issue, 'file_path', '')
            line_num  = (getattr(fix, 'line_number', 0) or getattr(issue, 'line_number', 0)) if fix else getattr(issue, 'line_number', 0)
            file_ref  = (f"`{file_path}`" + (f"  line *{line_num}*" if line_num else "")) if file_path else "_unknown file_"

            exc_msg   = (getattr(issue, 'exception_message', '') or '')[:400]
            reasoning = (getattr(fix, 'llm_reasoning', '') or '')[:400] if fix else ''
            fix_desc  = (getattr(fix, 'fix_description', '') or '')[:300] if fix else ''
            confidence = getattr(issue, 'confidence', 0) or 0
            occurrences = getattr(issue, 'occurrence_count', 1) or 1

            # Stack trace — first 3 user-relevant lines
            stack = (getattr(issue, 'stack_trace', '') or '')
            stack_lines = [l.strip() for l in stack.splitlines() if l.strip() and '\tat ' in l][:3]
            stack_excerpt = '\n'.join(stack_lines)

            # Issue summary block
            summary = (
                f"*#{idx}  ❌  {exc_type}*\n"
                f":file_folder: *File:* {file_ref}\n"
                f":repeat: *Occurrences:* `{occurrences}`   :bar_chart: *Confidence:* `{int(confidence * 100)}%`\n"
            )
            if exc_msg:
                summary += f":speech_balloon: *Message:* {exc_msg}\n"

            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": summary.strip()}})

            # Stack trace excerpt
            if stack_excerpt:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": f":scroll: *Stack Trace (top frames):*\n```{stack_excerpt}```"}})

            # Root cause + fix description
            if reasoning or fix_desc:
                analysis_text = ""
                if reasoning:
                    analysis_text += f":mag: *Root Cause:*\n{reasoning}\n"
                if fix_desc:
                    analysis_text += f"\n:hammer_and_wrench: *Fix Applied:*\n{fix_desc}"
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": analysis_text.strip()}})

            # Before / After code diff
            if fix:
                orig  = (getattr(fix, 'original_code', '') or '').strip()[:350]
                fixed = (getattr(fix, 'fixed_code', '') or '').strip()[:350]
                if orig and fixed and orig != fixed:
                    blocks.append({"type": "section", "text": {"type": "mrkdwn",
                        "text": f"*Before:*\n```{orig}```\n\n*After (fix):*\n```{fixed}```"}})
                elif fix_desc and not orig:
                    blocks.append({"type": "section", "text": {"type": "mrkdwn",
                        "text": f"_Code diff not available — see PR for changes_"}})

            # Per-issue PR link when there are multiple PRs
            issue_pr = pr_urls.get(getattr(issue, 'id', ''), '')
            if issue_pr and issue_pr != first_pr_url:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": f":white_check_mark: *PR for this issue:* <{issue_pr}|View PR>"}})

            blocks.append({"type": "divider"})

        # ── Footer ──────────────────────────────────────────────────────────
        agent_url = os.environ.get("CODEXA_AGENT_URL", "")
        prefix    = os.environ.get("CODEXA_AGENT_PREFIX", "/ai-agent")
        dashboard_url = f"{agent_url}{prefix}/codexa" if agent_url else ""
        footer_parts = [":robot_face: *CodeXA Auto Fix Engine* — fabhotels"]
        if dashboard_url:
            footer_parts.append(f"<{dashboard_url}|Open Dashboard>")
        blocks.append({"type": "context",
            "elements": [{"type": "mrkdwn", "text": "  |  ".join(footer_parts)}]})

        _log.info(f"[slack] sending PR report for {namespace}/{service} — {total} issue(s) | pr={first_pr_url}")
        self._send_to_slack({
            "text": f"CodeXA PR Created — {service} ({total} issue(s) fixed) {first_pr_url}",
            "blocks": blocks,
            "icon_emoji": ":robot_face:",
            "username": "CodeXA",
        })

    def _send_to_slack(self, message: Dict):
        """Send message via Bot Token (chat.postMessage) or fallback to webhook."""
        import logging as _log
        if self._use_bot_api:
            payload = dict(message)
            payload["channel"] = self.channel
            if "blocks" in payload and "text" not in payload:
                payload["text"] = "AI SRE Agent notification"
            response = requests.post(
                SLACK_API_POST,
                headers={
                    "Authorization": f"Bearer {self.bot_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=10,
            )
            result = response.json()
            if not result.get("ok"):
                raise RuntimeError(f"Slack API error: {result.get('error', result)}")
            _log.info(f"[slack] message sent via bot token to channel {self.channel}")
        else:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(message),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            _log.info("[slack] message sent via webhook")
