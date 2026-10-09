#!/usr/bin/env python3
"""
Root Cause Analysis for AI Monitoring Agent
"""
import ollama
from typing import Any, Dict, List, Tuple
import json
import os
import re
import logging

logger = logging.getLogger(__name__)

class RootCauseAnalyzer:
    def __init__(self):
        # Initialize Ollama client for Phi-3 model
        try:
            # Test connection to Ollama
            response = ollama.list()
            print("Ollama connection successful")
            self.ollama_client = ollama
        except Exception as e:
            print(f"Warning: Could not connect to Ollama: {e}")
            self.ollama_client = None
        self.model_disabled = False

    def is_llm_available(self) -> bool:
        """Check whether Ollama-backed LLM is available."""
        return self.ollama_client is not None and not self.model_disabled

    def _safe_generate(self, prompt: str, temperature: float = 0.4):
        """Generate via Ollama and disable model temporarily on memory errors."""
        if not self.ollama_client or self.model_disabled:
            raise RuntimeError("LLM unavailable")

        response = self.ollama_client.generate(
            model=os.getenv("OLLAMA_MODEL", "codellama:7b"),
            prompt=prompt,
            stream=False,
            options={
                "temperature": temperature,
                "top_p": 0.9,
                "stop": ["\n\n"]
            }
        )

        if isinstance(response, dict):
            error_text = str(response.get('error', '')).lower()
            if 'more system memory' in error_text or 'model request too large' in error_text:
                self.model_disabled = True
                raise RuntimeError("LLM disabled due to memory constraints")

        return response
            
    def _extract_exact_issues(self, anomalies: List[Dict], logs_traces: Dict) -> List[str]:
        """Extract exact issues from anomalies and logs"""
        exact_issues = []

        def _is_internal_otel_exporter_noise(message: str) -> bool:
            text = str(message or '')
            if not text:
                return False
            if not re.search(r'(?i)opentelemetry|http exporter|io\.opentelemetry\.exporter\.internal\.http\.httpexporter', text):
                return False
            return bool(re.search(r'(?i)failed\s+to\s+export\s+(logs|metrics|spans)|failed\s+to\s+connect\s+to', text))

        exception_regex_default = r'\b([a-zA-Z0-9_.$]+(?:Exception|Error))\b'
        exception_regex = str(os.getenv('AI_MONITORING_EXCEPTION_SIGNATURE_REGEX', exception_regex_default) or exception_regex_default)

        def _is_actionable(message: str, severity: str) -> bool:
            text = str(message or '')
            sev = str(severity or '').upper()
            if not text:
                return False
            if _is_internal_otel_exporter_noise(text):
                return False

            # Never miss concrete runtime exception signatures.
            try:
                if re.search(exception_regex, text, re.IGNORECASE):
                    return True
            except re.error:
                if re.search(exception_regex_default, text, re.IGNORECASE):
                    return True

            if re.search(r'\b(exception|error)\b', text, re.IGNORECASE):
                return True

            if sev in {'ERROR', 'FATAL', 'CRITICAL'}:
                return True
            return bool(re.search(
                r'(?i)(invaliddataaccessapiusageexception|illegalargumentexception|unknown\s+name\s+value\s*\[[^\]]+\]\s*for\s*enum|connection\s+refused|timeout|timed\s*out|deadline\s+exceeded|http\s*5\d\d|status\s*5\d\d|crashloopbackoff|imagepullbackoff|errimagepull|oomkilled|unsatisfieddependencyexception|beancreationexception|could\s+not\s+resolve\s+placeholder|failed\s+to\s+bind\s+properties|[a-zA-Z0-9_.$]+(?:Exception|Error))',
                text
            ))

        def _compact(text: Any, limit: int = 260) -> str:
            value = ' '.join(str(text or '').split())
            return value[:limit] + ('...' if len(value) > limit else '') if value else ''

        def _from_anomaly(anomaly: Dict) -> str:
            service = str(anomaly.get('service', 'unknown') or 'unknown')
            anomaly_type = str(anomaly.get('type', 'UNKNOWN') or 'UNKNOWN')
            parts = [f"Exact Issue: [{anomaly_type}] {service}"]

            if anomaly.get('pod'):
                parts.append(f"pod={anomaly.get('pod')}")
            if anomaly.get('reason'):
                parts.append(f"reason={_compact(anomaly.get('reason'))}")
            if anomaly.get('value') not in (None, ''):
                try:
                    raw_value = anomaly.get('value')
                    parts.append(f"value={float(str(raw_value)):.2f}")
                except Exception:
                    parts.append(f"value={anomaly.get('value')}")
            if anomaly.get('count') not in (None, ''):
                parts.append(f"count={anomaly.get('count')}")
            if anomaly.get('threshold') not in (None, ''):
                parts.append(f"threshold={anomaly.get('threshold')}")

            deps = anomaly.get('dependencies', []) or []
            if isinstance(deps, list) and deps:
                dep_text = ','.join([str(item) for item in deps[:4] if str(item).strip()])
                if dep_text:
                    parts.append(f"dependencies={dep_text}")

            pod_details = anomaly.get('pod_details', []) or []
            if isinstance(pod_details, list) and pod_details:
                detail_text = ' | '.join([_compact(item, 120) for item in pod_details[:3] if str(item).strip()])
                if detail_text:
                    parts.append(f"pod_details={detail_text}")

            top_errors = anomaly.get('top_errors', []) or []
            if isinstance(top_errors, list) and top_errors:
                top_text = ' || '.join([_compact(item, 120) for item in top_errors[:3] if str(item).strip()])
                if top_text:
                    parts.append(f"top_errors={top_text}")

            sample_error = _compact(anomaly.get('sample_error', ''), 180)
            if sample_error:
                parts.append(f"sample={sample_error}")

            description = _compact(anomaly.get('description', ''), 180)
            if description:
                parts.append(f"description={description}")

            return ' | '.join(parts)
        
        # Process each anomaly for exact issue identification
        for anomaly in anomalies:
            if not isinstance(anomaly, dict):
                continue
            anomaly_type = str(anomaly.get('type', 'UNKNOWN') or 'UNKNOWN').upper()
            if anomaly_type in {'HIGH_ERROR_RATE', 'SERVICE_DEGRADED', 'FREQUENT_ERRORS'}:
                sample_error = str(anomaly.get('sample_error', '') or '').strip()
                top_errors = anomaly.get('top_errors', []) or []
                has_top_error = any(str(item or '').strip() for item in top_errors[:3]) if isinstance(top_errors, list) else False
                if not sample_error and not has_top_error:
                    continue
            issue = _from_anomaly(anomaly)
            if issue:
                exact_issues.append(issue)
                
        # Add raw log/service-error evidence instead of hardcoded rule templates
        logs = logs_traces.get('logs', [])
        service_errors = logs_traces.get('service_errors', [])
        all_logs = logs + service_errors
        evidence_count = 0
        for log in all_logs:
            if not isinstance(log, dict):
                continue
            message = _compact(log.get('message', log.get('body', '')), 260)
            if not message:
                continue
            service = str(log.get('service', 'unknown') or 'unknown')
            if service == 'unknown' and anomalies:
                service = str((anomalies[0] or {}).get('service', 'unknown') or 'unknown')
            severity = str(log.get('severity', 'UNKNOWN') or 'UNKNOWN').upper()
            if not _is_actionable(message, severity):
                continue
            exact_issues.append(f"Exact Issue: [LOG] {service} | severity={severity} | message={message}")
            evidence_count += 1
            if evidence_count >= 4:
                break
                
        # Remove duplicates while preserving order
        seen = set()
        unique_issues = []
        for issue in exact_issues:
            if issue not in seen:
                seen.add(issue)
                unique_issues.append(issue)
                
        return unique_issues[:5]  # Limit to top 5 exact issues
            
    def analyze(self, anomalies: List[Dict], logs_traces: Dict) -> Dict:
        """Perform root cause analysis based on anomalies and contextual data"""
        analysis = {
            'summary': '',
            'likely_causes': [],
            'recommendations': [],
            'confidence': 0.0,
            'exact_issues': [],
            'dependency_chain': []
        }
        
        if not anomalies:
            analysis['summary'] = "No anomalies detected"
            return analysis
            
        # Generate summary of anomalies
        anomaly_text = self._format_anomalies(anomalies)
        analysis['summary'] = self._summarize_anomalies(anomaly_text)
        
        # Extract exact issues from anomalies
        exact_issues = self._extract_exact_issues(anomalies, logs_traces)
        analysis['exact_issues'] = exact_issues
        
        # Analyze logs and traces for patterns
        context_analysis = self._analyze_context(logs_traces)
        analysis['likely_causes'] = context_analysis.get('causes', [])
        analysis['recommendations'] = context_analysis.get('recommendations', [])
        analysis['confidence'] = context_analysis.get('confidence', 0.5)
        analysis['dependency_chain'] = self._build_dependency_chain(logs_traces)
        
        # If we have exact issues, boost confidence
        if exact_issues:
            analysis['confidence'] = min(0.95, analysis['confidence'] + 0.2)
            # Prepend exact issues to likely causes for higher priority
            analysis['likely_causes'] = exact_issues + analysis['likely_causes']

        # Ensure no placeholder-only issue is returned when sample evidence exists
        if analysis.get('exact_issues'):
            refined = []
            for issue in analysis['exact_issues']:
                if "recent errors - investigate error patterns" in issue:
                    continue
                refined.append(issue)
            if refined:
                analysis['exact_issues'] = refined
                analysis['likely_causes'] = refined + [c for c in analysis.get('likely_causes', []) if c not in refined]
        
        return analysis

    def _build_dependency_chain(self, logs_traces: Dict) -> List[str]:
        """Extract likely service dependency links from logs and traces."""
        links = set()

        for log in logs_traces.get('logs', []):
            source_service = log.get('service', 'unknown')
            dependency = log.get('dependency', '')
            if source_service and dependency:
                links.add(f"{source_service} -> {dependency}")

        for trace in logs_traces.get('traces', []):
            source_service = trace.get('service', '')
            target_service = trace.get('downstream_service', '')
            if source_service and target_service:
                links.add(f"{source_service} -> {target_service}")

        return sorted(list(links))[:10]
        
    def _format_anomalies(self, anomalies: List[Dict]) -> str:
        """Format anomalies into readable text"""
        texts = []
        for anomaly in anomalies:
            if 'metric' in anomaly:
                text = f"Anomaly detected in {anomaly.get('metric')} with value {anomaly.get('value')} "
                text += f"(score: {anomaly.get('anomaly_score', 'n/a')}) at {anomaly.get('timestamp', 'unknown time')}"
            else:
                service = anomaly.get('service', 'unknown')
                anomaly_type = anomaly.get('type', 'UNKNOWN')
                description = anomaly.get('description', '')
                text = f"Service anomaly [{anomaly_type}] in {service}: {description}"
            texts.append(text)
        return ". ".join(texts)
        
    def _extract_dependency_from_message(self, message: str) -> str:
        """Extract dependency service names from error messages"""
        import re
        
        # Common patterns for service dependencies in error messages
        dependency_patterns = [
            r'to ([\w-]+)-service',  # e.g., "to b2b-sales-service"
            r'([a-zA-Z0-9-]+)\.svc\.cluster\.local',  # Kubernetes service names
            r'connecting to ([\w-]+)',  # e.g., "connecting to database"
            r'([a-zA-Z0-9-]+):[0-9]+',  # service:port patterns
            r'"([^"]*service[^"]*)"',  # quoted service names
            r'from ([\w-]+)-service',  # e.g., "from payment-service"
            r'call.*?([\w-]+)-service',  # e.g., "call to inventory-service"
        ]
        
        for pattern in dependency_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                dependency = match.group(1)
                # Clean up the dependency name
                dependency = re.sub(r'[:\d]+$', '', dependency)  # Remove port numbers
                # Filter out common false positives
                if dependency.lower() not in ['http', 'https', 'tcp', 'udp']:
                    return dependency
                    
        return ""
    
    def _summarize_anomalies(self, text: str) -> str:
        """Generate summary of anomalies using Phi-3 model via Ollama"""
        if not self.ollama_client or len(text) < 50:
            return f"Anomalous behavior detected: {text[:200]}..."
            
        try:
            # Truncate if too long
            if len(text) > 2048:
                text = text[:2048]
                
            # Create prompt for summarization
            prompt = f"Summarize the following anomalies in a concise technical summary:\n\n{text}"
            
            # Generate summary using Phi-3 model
            response = self._safe_generate(prompt=prompt, temperature=0.3)
            
            summary = response['response'].strip()
            return summary if summary else f"Anomalous behavior detected: {text[:200]}..."
        except Exception as e:
            print(f"Error summarizing anomalies with Phi-3: {e}")
            return f"Anomalous behavior detected: {text[:200]}..."
            
    def _analyze_context(self, logs_traces: Dict) -> Dict:
        """Analyze logs and traces for root cause patterns using Phi-3 model"""
        analysis = {
            'causes': [],
            'recommendations': [],
            'confidence': 0.7  # Higher baseline confidence with LLM
        }
        
        if not logs_traces or not self.ollama_client:
            analysis['causes'].append("No contextual data available for root cause analysis")
            return analysis
            
        def _is_internal_otel_exporter_noise(message: str) -> bool:
            text = str(message or '')
            if not text:
                return False
            if not re.search(r'(?i)opentelemetry|http exporter|io\.opentelemetry\.exporter\.internal\.http\.httpexporter', text):
                return False
            return bool(re.search(r'(?i)failed\s+to\s+export\s+(logs|metrics|spans)|failed\s+to\s+connect\s+to', text))

        # Extract log messages
        log_messages = []
        combined_logs = (logs_traces.get('logs', []) or []) + (logs_traces.get('service_errors', []) or [])
        for log in combined_logs:
            if not isinstance(log, dict):
                continue
            if 'body' in log:
                candidate = str(log['body'])
            elif 'message' in log:
                candidate = str(log['message'])
            else:
                candidate = ''
            if not candidate or _is_internal_otel_exporter_noise(candidate):
                continue
            log_messages.append(candidate)
                
        # Extract trace information
        trace_info = []
        for trace in logs_traces.get('traces', []):
            if isinstance(trace, dict):
                trace_info.append(f"Trace {trace.get('trace_id', 'unknown')} in {trace.get('service', 'unknown')} - {trace.get('message', '')[:100]}")
                
        if not log_messages and not trace_info:
            analysis['causes'].append("No log messages or traces found for analysis")
            return analysis
            
        # Combine log messages and trace info for LLM analysis
        combined_context = ""
        if log_messages:
            combined_context += "Log Messages:\n" + "\n".join(log_messages[:20])  # Limit to first 20 logs
        if trace_info:
            combined_context += "\n\nTrace Information:\n" + "\n".join(trace_info[:10])  # Limit to first 10 traces
            
        # If we have trace information, mention it in the prompt
        trace_note = "Also analyze the trace information to identify cross-service call patterns and latency issues." if trace_info else ""
        
        # Use Phi-3 to analyze logs and traces for root causes
        try:
            prompt = f"""Analyze the following system monitoring data and identify the most likely root causes of the anomalies. 
            Focus on error patterns, exceptions, unusual behaviors, and cross-service communication issues. 
            {trace_note}
            Provide 2-3 specific root causes.
            
            Context Data:
            {combined_context}
            
            Respond with only the root causes, one per line, without any additional explanation."""
            
            response = self._safe_generate(prompt=prompt, temperature=0.4)
            
            # Parse root causes from response
            root_causes = [line.strip() for line in response['response'].strip().split('\n') if line.strip()]
            analysis['causes'] = root_causes[:3]  # Limit to top 3 causes
            
        except Exception as e:
            print(f"Error analyzing context with Phi-3: {e}")
            # Fallback to pattern-based analysis
            error_patterns = self._extract_error_patterns(log_messages)
            analysis['causes'].extend(error_patterns)
            
        # Generate recommendations using Phi-3
        if analysis['causes']:
            try:
                causes_text = "\n".join(analysis['causes'])
                prompt = f"""Given these root causes of system anomalies, provide 2-3 specific technical recommendations to resolve them:
                
                Root causes:
                {causes_text}
                
                Respond with only the recommendations, one per line, without any additional explanation."""
                
                response = self._safe_generate(prompt=prompt, temperature=0.5)
                
                # Parse recommendations from response
                recommendations = [line.strip() for line in response['response'].strip().split('\n') if line.strip()]
                analysis['recommendations'] = recommendations[:3]  # Limit to top 3 recommendations
                
            except Exception as e:
                print(f"Error generating recommendations with Phi-3: {e}")
                # Fallback to rule-based recommendations
                recommendations = self._generate_recommendations(analysis['causes'])
                analysis['recommendations'].extend(recommendations)
        else:
            # Fallback when no causes identified
            error_patterns = self._extract_error_patterns(log_messages)
            recommendations = self._generate_recommendations(error_patterns)
            analysis['recommendations'].extend(recommendations)
        
        return analysis
        
    def _extract_error_patterns(self, log_messages: List[str]) -> List[str]:
        """Extract common error patterns from log messages"""
        if not log_messages:
            return ["Actionable runtime errors not found in current window"]
            
        # Enhanced pattern extraction with pod-related issues
        common_errors = [
            ("timeout", "timeout"),
            ("connection refused", "connection refused"),
            ("out of memory", "out of memory"),
            ("database connection", "database connection"),
            ("null pointer", "null pointer"),
            ("unauthorized", "unauthorized"),
            ("image pull", "image pull failure"),
            ("trying and failing to pull image", "image pull failure"),
            ("ErrImagePull", "image pull failure"),
            ("ImagePullBackOff", "image pull failure"),
            ("crash loop", "pod crash loop"),
            ("oomkill", "out of memory kill"),
            ("liveness probe", "liveness probe failure"),
            ("readiness probe", "readiness probe failure")
        ]
        
        found_patterns = []
        text = " ".join(log_messages).lower()
        
        for error_pattern, description in common_errors:
            if error_pattern in text:
                found_patterns.append(f"Detected potential {description}")
                
        if not found_patterns:
            found_patterns.append("General service degradation detected")
            
        return found_patterns[:5]  # Limit to top 5 patterns
        
    def _generate_recommendations(self, error_patterns: List[str]) -> List[str]:
        """Generate recommendations based on error patterns"""
        recommendations = []
        
        for pattern in error_patterns:
            if "timeout" in pattern.lower():
                recommendations.append("Check network connectivity and increase timeout thresholds")
            elif "connection refused" in pattern.lower():
                recommendations.append("Verify service availability and port configurations")
            elif "out of memory" in pattern.lower() or "oomkill" in pattern.lower():
                recommendations.append("Increase memory allocation or optimize resource usage")
            elif "database connection" in pattern.lower():
                recommendations.append("Check database connectivity and connection pool settings")
            elif "image pull" in pattern.lower():
                recommendations.append("Check container registry access, image name/tag, and pull secrets")
            elif "crash loop" in pattern.lower():
                recommendations.append("Check pod logs for application startup errors and fix underlying issues")
            elif "liveness probe" in pattern.lower():
                recommendations.append("Review liveness probe configuration and application health checks")
            elif "readiness probe" in pattern.lower():
                recommendations.append("Review readiness probe configuration and application readiness logic")
            else:
                recommendations.append("Identify top repeated exception signature and map it to the failing dependency/component")
                
        if not recommendations:
            recommendations.append("Review service metrics and logs for detailed analysis")
            
        return recommendations

    def generate_code_fix(self, issue_description: str, repo_files: Dict[str, str], service_name: str) -> Dict[str, Any]:
        """Use Ollama to generate code fix for any issue automatically."""
        if not self.ollama_client:
            logger.warning("Ollama not available - cannot generate code fix")
            return {'ok': False, 'reason': 'Ollama not available', 'fixes': []}

        fixes = []
        
        # Prepare file context for Ollama
        file_context = ""
        for file_path, content in list(repo_files.items())[:10]:  # Limit to 10 files
            file_context += f"\n\n=== File: {file_path} ===\n{content[:3000]}"  # Limit each file

        prompt = f"""You are a senior software engineer. Analyze the issue and the provided source code files.
Generate a code fix for the following issue:

Issue: {issue_description}
Service: {service_name}

{file_context}

Instructions:
1. Analyze the issue and identify the root cause in the code
2. If this is a code-level issue (not infrastructure like connection refused, network timeout, etc), provide a fix
3. If this is an infrastructure issue (network, database, cache connection, etc), respond with: INFRASTRUCTURE_ISSUE
4. Show the exact code to add/replace in this format:
FILE: <filename>
POSITION: after line <number> or replace line <number>
CODE:
<exact code to add>

If the issue is about a missing enum value, add it to the enum class.
If the issue is about a missing config/placeholder, suggest adding it to the appropriate config file.
For other code-level issues, provide the exact code change needed.

Respond with 1-3 fixes maximum. If infrastructure issue, just write INFRASTRUCTURE_ISSUE"""

        try:
            response = self._safe_generate(prompt=prompt, temperature=0.3)
            response_text = response.get('response', '')

            # Check if AI says it's an infrastructure issue
            if 'INFRASTRUCTURE_ISSUE' in response_text.upper():
                logger.info("AI determined this is an infrastructure issue, not code-level")
                return {'ok': False, 'reason': 'AI determined infrastructure issue (not code-level)', 'fixes': [], 'is_infrastructure': True}

            # Parse fixes from response
            current_file = None
            current_position = None
            current_code = []
            in_code_block = False

            for line in response_text.split('\n'):
                if line.startswith('FILE:'):
                    if current_file and current_code:
                        fixes.append({
                            'file': current_file,
                            'position': current_position,
                            'code': '\n'.join(current_code)
                        })
                    current_file = line.replace('FILE:', '').strip()
                    current_code = []
                    in_code_block = False
                elif line.startswith('POSITION:'):
                    current_position = line.replace('POSITION:', '').strip()
                elif line.strip() == 'CODE:':
                    in_code_block = True
                elif in_code_block and line.strip():
                    current_code.append(line)

            if current_file and current_code:
                fixes.append({
                    'file': current_file,
                    'position': current_position,
                    'code': '\n'.join(current_code)
                })

            if fixes:
                logger.info("AI generated %d code fix(es) for this issue", len(fixes))
                return {'ok': True, 'fixes': fixes, 'reason': ''}
            else:
                logger.warning("AI could not generate any code fix for: %s", issue_description[:100])
                return {'ok': False, 'reason': 'AI could not parse fix from response', 'fixes': []}

        except Exception as e:
            logger.error("Ollama fix generation failed: %s", e)
            return {'ok': False, 'reason': f'Ollama fix generation failed: {e}', 'fixes': []}
