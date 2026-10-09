#!/usr/bin/env python3
"""
Machine Learning Engine for AI Monitoring Agent
"""
import json
import pickle
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import DBSCAN
import pandas as pd
from sklearn.utils.validation import check_is_fitted
from llm_client import LLMClient

class LearningEngine:
    def __init__(self, model_dir="./models", history_limit: int = 500, feedback_limit: int = 500, knowledge_limit: int = 1000):
        self.model_dir = model_dir
        self.history_limit = history_limit
        self.feedback_limit = feedback_limit
        self.knowledge_limit = knowledge_limit
        self.incident_history = []
        self.feedback_data = []
        self.error_classifier = None
        self.pattern_recognizer = None
        self.threshold_optimizer = None
        self.knowledge_base = {}
        self.feedback_rules = {}
        self.last_training = None
        self.llm_client = LLMClient()
        self._load_models()
            
    def _load_models(self):
        """Load trained models if they exist"""
        try:
            with open(f"{self.model_dir}/error_classifier.pkl", "rb") as f:
                self.error_classifier = pickle.load(f)
            # Guard against partially saved/corrupt model state
            try:
                check_is_fitted(self.error_classifier)
            except Exception:
                self.error_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
        except FileNotFoundError:
            # Initialize with default classifier
            self.error_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
            
        try:
            with open(f"{self.model_dir}/knowledge_base.json", "r") as f:
                self.knowledge_base = json.load(f)
                self.feedback_rules = self.knowledge_base.get('feedback_rules', {})
        except FileNotFoundError:
            self.knowledge_base = {}
            self.feedback_rules = {}
            
    def record_incident(self, incident_data: Dict):
        """Record incident for learning"""
        incident_data['timestamp'] = datetime.now().isoformat()
        self.incident_history.append(incident_data)
        
        # Extract and save error patterns from logs
        self._extract_and_save_error_patterns(incident_data)
        
        # Keep only configured number of incidents
        if len(self.incident_history) > self.history_limit:
            self.incident_history = self.incident_history[-self.history_limit:]
            
    def _extract_and_save_error_patterns(self, incident_data: Dict):
        """Extract error patterns from incident logs and save them for future learning"""
        logs = incident_data.get('logs', [])
        service = incident_data.get('service', 'unknown')
        
        for log in logs:
            message = log.get('body', log.get('message', ''))
            severity = log.get('severity', 'UNKNOWN')
            
            # Only process error-level logs
            if severity in ['ERROR', 'FATAL', 'CRITICAL']:
                # Extract error pattern
                pattern = self._extract_error_pattern_from_message(message)
                if pattern:
                    # Save pattern with service context
                    self._save_error_pattern(pattern, service, severity)
                    
    def _extract_error_pattern_from_message(self, message: str) -> str:
        """Extract generic error pattern from log message"""
        if not message:
            return ""
            
        # Remove timestamps, IDs, and other variable parts
        # This is a simplified approach - in production, use more sophisticated NLP
        pattern = message
        
        # Remove common variable elements
        import re
        pattern = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?', '[TIMESTAMP]', pattern)
        pattern = re.sub(r'\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b', '[UUID]', pattern)
        pattern = re.sub(r'\b\d+\.\d+\.\d+\.\d+\b', '[IP]', pattern)
        pattern = re.sub(r'\b\d+\b', '[NUMBER]', pattern)  # This might be too aggressive
        
        # Keep only the essential error pattern
        return pattern[:200]  # Limit length
        
    def _save_error_pattern(self, pattern: str, service: str, severity: str):
        """Save error pattern to knowledge base"""
        if not pattern:
            return
            
        # Create pattern key
        pattern_key = f"{service}_{severity}_{hash(pattern) % 10000}"
        
        # Update knowledge base
        if 'error_patterns' not in self.knowledge_base:
            self.knowledge_base['error_patterns'] = {}
            
        if pattern_key not in self.knowledge_base['error_patterns']:
            self.knowledge_base['error_patterns'][pattern_key] = {
                'pattern': pattern,
                'service': service,
                'severity': severity,
                'count': 1,
                'first_seen': datetime.now().isoformat(),
                'last_seen': datetime.now().isoformat()
            }
        else:
            # Increment count and update last seen
            self.knowledge_base['error_patterns'][pattern_key]['count'] += 1
            self.knowledge_base['error_patterns'][pattern_key]['last_seen'] = datetime.now().isoformat()
            
        # Save updated knowledge base
        try:
            error_patterns = self.knowledge_base.get('error_patterns', {})
            if len(error_patterns) > self.knowledge_limit:
                # Keep most frequent patterns first
                sorted_patterns = sorted(
                    error_patterns.items(),
                    key=lambda item: item[1].get('count', 0),
                    reverse=True
                )
                self.knowledge_base['error_patterns'] = dict(sorted_patterns[:self.knowledge_limit])

            with open(f"{self.model_dir}/knowledge_base.json", "w") as f:
                json.dump(self.knowledge_base, f)
        except Exception as e:
            print(f"Error saving knowledge base: {e}")
            
    def record_feedback(self, feedback: Dict):
        """Record human feedback for supervised learning"""
        predicted = self._normalize_category(
            feedback.get('predicted_category') or feedback.get('predicted_cause') or feedback.get('root_cause', '')
        )
        actual = self._normalize_category(
            feedback.get('actual_cause') or feedback.get('correct_category') or feedback.get('label', '')
        )
        service = feedback.get('service', 'global')

        compact_feedback = {
            'timestamp': datetime.now().isoformat(),
            'service': service,
            'predicted_category': predicted,
            'actual_cause': actual
        }
        self.feedback_data.append(compact_feedback)

        # Build correction memory for online learning/reranking
        if predicted and actual:
            for scope in (service, 'global'):
                key = f"{scope}:{predicted}"
                if key not in self.feedback_rules:
                    self.feedback_rules[key] = {}
                self.feedback_rules[key][actual] = self.feedback_rules[key].get(actual, 0) + 1

        self.knowledge_base['feedback_rules'] = self.feedback_rules
        try:
            with open(f"{self.model_dir}/knowledge_base.json", "w") as f:
                json.dump(self.knowledge_base, f)
        except Exception as e:
            print(f"Error saving feedback rules: {e}")
        
        # Keep only configured feedback entries
        if len(self.feedback_data) > self.feedback_limit:
            self.feedback_data = self.feedback_data[-self.feedback_limit:]

    def _normalize_category(self, value: str) -> str:
        """Normalize category labels for stable learning."""
        if not value:
            return "unknown"
        normalized = str(value).strip().lower().replace(' ', '_').replace('-', '_')
        aliases = {
            'db': 'database',
            'database_connection': 'database',
            'db_connection': 'database',
            'conn': 'connection',
            'pod': 'pod_failure',
            'imagepull': 'image_pull',
            'img_pull': 'image_pull'
        }
        return aliases.get(normalized, normalized)

    def apply_feedback_correction(self, predicted_category: str, service: str = 'global'):
        """Correct predicted category using accumulated human feedback."""
        predicted = self._normalize_category(predicted_category)
        if not predicted:
            return 'unknown', 0.0

        for scope in (service, 'global'):
            key = f"{scope}:{predicted}"
            stats = self.feedback_rules.get(key, {})
            if not stats:
                continue
            total = sum(stats.values())
            if total < 3:
                continue
            corrected, count = max(stats.items(), key=lambda item: item[1])
            ratio = count / total if total else 0.0
            if ratio >= 0.60:
                return corrected, ratio

        return predicted, 0.0
            
    def train_error_classifier(self):
        """Train error classification model"""
        if len(self.feedback_data) < 10:
            return
            
        # Prepare training data
        texts = []
        labels = []
        
        for feedback in self.feedback_data:
            root_feature = feedback.get('root_cause') or feedback.get('predicted_category')
            if root_feature and 'actual_cause' in feedback:
                texts.append(root_feature)
                labels.append(feedback['actual_cause'])
                
        if len(texts) < 5:
            return
            
        # Vectorize text
        vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        X = vectorizer.fit_transform(texts)
        
        # Train classifier
        if self.error_classifier:
            self.error_classifier.fit(X, labels)
        
        # Save model
        with open(f"{self.model_dir}/error_classifier.pkl", "wb") as f:
            pickle.dump(self.error_classifier, f)
        self.last_training = datetime.now().isoformat()
            
    def classify_error(self, error_text: str) -> str:
        """Classify error using trained model or Phi-3 as fallback"""
        # Try traditional ML approach first
        if self.error_classifier is not None and error_text:
            try:
                check_is_fitted(self.error_classifier)
                vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
                # This is simplified - in practice, you'd save the fitted vectorizer
                X = vectorizer.fit_transform([error_text])
                prediction = self.error_classifier.predict(X)[0]
                return prediction
            except Exception:
                # Fall back to LLM/classic unknown without breaking monitoring loop
                pass
        
        # Use configured LLM backend for error classification
        if error_text:
            try:
                prompt = f"""Classify the following error message into one of these categories:
                timeout, connection, memory, database, authentication, configuration, pod_failure, image_pull, unknown

                Error message:
                {error_text[:500]}  # Limit length for performance

                Respond with only the category name, nothing else."""

                classification = self.llm_client.generate(prompt=prompt, temperature=0.3).strip().lower()
                valid_categories = ['timeout', 'connection', 'memory', 'database', 'authentication', 'configuration', 'pod_failure', 'image_pull', 'unknown']
                
                # Validate classification
                for category in valid_categories:
                    if category in classification:
                        return category
                        
                return "unknown"
            except Exception as e:
                print(f"Error classifying error with LLM: {e}")
        
        return "unknown"
            
    def find_similar_incidents(self, new_incident: Dict, top_k: int = 5) -> List[Dict]:
        """Find similar past incidents using similarity matching"""
        if not self.incident_history:
            return []
            
        # Extract features for comparison
        new_features = self._extract_features(new_incident)
        similarities = []
        
        for past_incident in self.incident_history:
            past_features = self._extract_features(past_incident)
            similarity = self._calculate_similarity(new_features, past_features)
            similarities.append((similarity, past_incident))
            
        # Sort by similarity and return top K
        similarities.sort(key=lambda x: x[0], reverse=True)
        return [incident for _, incident in similarities[:top_k]]
        
    def _extract_features(self, incident: Dict) -> Dict:
        """Extract features from incident for comparison"""
        features = {
            'error_rate': incident.get('metrics', {}).get('error_rate', 0),
            'latency': incident.get('metrics', {}).get('latency_95th', 0),
            'request_rate': incident.get('metrics', {}).get('request_rate', 0),
            'error_pattern': self._extract_error_pattern(incident),
            'service': incident.get('service', 'unknown')
        }
        return features
        
    def _extract_error_pattern(self, incident: Dict) -> str:
        """Extract error pattern from logs"""
        logs = incident.get('logs', [])
        if not logs:
            return ""
            
        # Simple pattern extraction
        error_texts = [log.get('body', log.get('message', '')) for log in logs if (log.get('body') or log.get('message'))]
        return " ".join(error_texts[:10])  # Join first 10 log entries
        
    def _calculate_similarity(self, features1: Dict, features2: Dict) -> float:
        """Calculate similarity between two feature sets"""
        # Simple weighted similarity calculation
        numeric_sim = 0
        count = 0
        
        for key in ['error_rate', 'latency', 'request_rate']:
            if key in features1 and key in features2:
                val1, val2 = features1[key], features2[key]
                if val1 != 0 and val2 != 0:
                    sim = 1 - abs(val1 - val2) / max(val1, val2)
                    numeric_sim += max(0, sim)
                    count += 1
                    
        numeric_similarity = numeric_sim / count if count > 0 else 0
        
        # Text similarity for error patterns
        text1, text2 = features1.get('error_pattern', ''), features2.get('error_pattern', '')
        if text1 and text2:
            vectorizer = TfidfVectorizer()
            try:
                tfidf_matrix = vectorizer.fit_transform([text1, text2])
                text_similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            except:
                text_similarity = 0
        else:
            text_similarity = 0
            
        # Weighted combination
        return 0.7 * numeric_similarity + 0.3 * text_similarity
        
    def update_knowledge_base(self, incident: Dict, resolution: Dict):
        """Update knowledge base with resolved incident"""
        key = self._generate_incident_key(incident)
        self.knowledge_base[key] = {
            'incident': incident,
            'resolution': resolution,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save knowledge base
        with open(f"{self.model_dir}/knowledge_base.json", "w") as f:
            json.dump(self.knowledge_base, f)
            
    def _generate_incident_key(self, incident: Dict) -> str:
        """Generate unique key for incident"""
        service = incident.get('service', 'unknown')
        error_type = self._extract_error_type(incident)
        return f"{service}_{error_type}"
        
    def _extract_error_type(self, incident: Dict) -> str:
        """Extract error type from incident"""
        logs = incident.get('logs', [])
        if not logs:
            return "unknown"
            
        # Look for common error patterns
        error_text = " ".join([log.get('body', '') for log in logs[:5]]).lower()
        
        if 'timeout' in error_text:
            return 'timeout'
        elif 'connection' in error_text:
            return 'connection'
        elif 'memory' in error_text:
            return 'memory'
        elif 'database' in error_text:
            return 'database'
        else:
            return 'general'
            
    def get_remedial_actions(self, incident: Dict) -> List[str]:
        """Get recommended remedial actions based on knowledge base or Phi-3 model"""
        key = self._generate_incident_key(incident)
        
        # Check knowledge base first
        if key in self.knowledge_base:
            resolution = self.knowledge_base[key].get('resolution', {})
            actions = resolution.get('actions', [])
            if actions:
                return actions
            
        # Extract error information for LLM analysis
        error_type = self._extract_error_type(incident)
        logs = incident.get('logs', [])
        metrics = incident.get('metrics', {})
        
        # Use configured LLM backend for generating remedial actions
        if logs or metrics:
            try:
                # Prepare context for LLM
                log_samples = [log.get('body', log.get('message', '')) for log in logs[:5]]  # First 5 log entries
                log_context = "\n".join(log_samples)
                
                metrics_context = ""
                if metrics:
                    metrics_context = ", ".join([f"{k}: {v}" for k, v in list(metrics.items())[:5]])
                
                prompt = f"""Given the following system monitoring data, provide 3 specific technical remedial actions to resolve the issue:

                Error type: {error_type}
                Metrics: {metrics_context}
                Log samples:
                {log_context}

                Respond with only the actions, one per line, without any additional explanation."""

                llm_text = self.llm_client.generate(prompt=prompt, temperature=0.5)

                # Parse actions from response
                actions = [line.strip() for line in llm_text.strip().split('\n') if line.strip()]
                if actions:
                    return actions[:3]  # Limit to top 3 actions
                    
            except Exception as e:
                print(f"Error generating remedial actions with LLM: {e}")
        
        # Rule-based actions from incident evidence before generic fallback
        lower_logs = " ".join([str(log.get('body', log.get('message', ''))).lower() for log in logs[:8]])
        if lower_logs:
            if 'connection refused' in lower_logs:
                return [
                    "Downstream service is refusing connections; verify target pods/service endpoints and network policy.",
                    "Check recent deploy/config changes of the target dependency and rollback if needed.",
                    "Validate retry/backoff settings to avoid request storm while dependency recovers."
                ]
            if 'timeout' in lower_logs:
                return [
                    "Check dependency latency and saturation; increase timeout only after confirming capacity.",
                    "Inspect pod CPU/memory throttling for caller and downstream services.",
                    "Scale affected deployment replicas and re-check p95 latency/error rates."
                ]
            if 'imagepullbackoff' in lower_logs or 'errimagepull' in lower_logs or 'image pull' in lower_logs:
                return [
                    "Fix image tag/registry path and verify image exists in registry.",
                    "Validate imagePullSecrets and registry credentials in namespace.",
                    "Redeploy pod after confirming image pull works from node."
                ]
            if 'crashloopbackoff' in lower_logs or 'crash loop' in lower_logs:
                return [
                    "Inspect startup exception stack trace in pod logs and fix failing init path.",
                    "Validate config/env/secret values required at boot.",
                    "Increase startup probe grace period only if app boot is genuinely slow."
                ]

        # Default actions based on error type
        default_actions = {
            'timeout': [
                "Increase timeout thresholds",
                "Check network connectivity",
                "Scale up service instances"
            ],
            'connection': [
                "Verify service availability",
                "Check firewall rules",
                "Validate connection strings"
            ],
            'memory': [
                "Increase memory allocation",
                "Optimize memory usage",
                "Restart service instances"
            ],
            'database': [
                "Check database connectivity",
                "Verify database health",
                "Review connection pool settings"
            ],
            'pod_failure': [
                "Check pod logs for application errors",
                "Verify resource limits and requests",
                "Review pod configuration and restart policies"
            ],
            'image_pull': [
                "Verify container image name and tag",
                "Check container registry access and credentials",
                "Validate image pull secrets configuration"
            ]
        }
        
        return default_actions.get(error_type, ["Investigate service logs for detailed error information"])
        
    def adapt_thresholds(self, metrics_history: List[Dict]) -> Dict:
        """Adapt anomaly detection thresholds based on historical data"""
        if len(metrics_history) < 100:
            return {}
            
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(metrics_history)
        
        # Calculate adaptive thresholds (95th percentile + buffer)
        thresholds = {}
        for column in df.select_dtypes(include=[np.number]).columns:
            if column in ['timestamp']:
                continue
            percentile_95 = df[column].quantile(0.95)
            std_dev = df[column].std()
            thresholds[column] = percentile_95 + (2 * std_dev)
            
        return thresholds
