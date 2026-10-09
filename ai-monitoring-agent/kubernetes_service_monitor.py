#!/usr/bin/env python3
"""
Kubernetes Service Monitor for AI Monitoring Agent
Monitors services directly by reading their logs
"""
import subprocess
import json
import re
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import signal

logger = logging.getLogger(__name__)

class KubernetesServiceMonitor:
    def __init__(self, namespace: str = "jupiter"):
        self.namespace = namespace
        self.runtime_config = None
        # Structured error parsing patterns
        self._issue_patterns = {
            'CrashLoopBackOff': r'(?i)crashloopbackoff',
            'ImagePullBackOff': r'(?i)(imagepullbackoff|errimagepull)',
            'OOMKilled': r'(?i)(oomkilled|outofmemory|out of memory)',
            'FailedScheduling': r'(?i)(failedscheduling|unschedulable)',
            'CreateContainerConfigError': r'(?i)createcontainerconfigerror',
            'CreateContainerError': r'(?i)createcontainererror',
            'RunContainerError': r'(?i)runcontainererror',
            'InvalidImageName': r'(?i)invalidimagename',
            'PodPending': r'(?i)(pending|unschedulable)',
            'ConnectionRefused': r'(?i)connection\s*refused',
            'Timeout': r'(?i)(timeout|timed\s*out|deadline\s*exceeded)',
            'DNSError': r'(?i)(dns\s*resolution\s*failed|unknownhostexception)',
            'ServiceUnavailable': r'(?i)service.*unavailable',
        }
        self.discovery_namespaces = [namespace]
        self.max_selected_services = 25
        self.selected_services = []
        self.ignored_log_patterns = []
        self._namespace_pods_cache = {}
        self._namespace_pods_cache_ts = {}
        self._namespace_pods_backoff_until = {}
        self._namespace_pods_last_error = {}
        self._namespace_pods_locks = {}
        self._pod_event_cache = {}
        self._pod_event_cache_ts = {}
        self._pod_log_cache = {}
        self._pod_log_cache_ts = {}
        self._namespace_workloads_cache = {}
        self._namespace_workloads_cache_ts = {}
        self._namespace_workloads_locks = {}
        self._discover_services_cache = []
        self._discover_services_cache_ts = None
        # Load services from config file
        self.service_patterns = {}
        self._load_services_from_config()
        # Test if kubectl is working
        self.kubectl_working = self._test_kubectl()

    def _get_kubectl_timeout(self) -> int:
        cfg = self.runtime_config or {}
        monitoring_cfg = cfg.get('monitoring', {}) if isinstance(cfg, dict) else {}
        value = int(monitoring_cfg.get('kubectl_timeout_seconds', 10) or 10)
        return max(3, min(value, 30))

    def parse_structured_error(self, reason_text: str, pod_info: Optional[Dict] = None) -> Dict:
        """Parse error context into structured issue/reason/root_cause fields.

        Args:
            reason_text: Raw error/reason text from pod status or events
            pod_info: Optional pod information dict with status, restarts, etc.

        Returns:
            Dict with 'issue', 'reason', 'root_cause' keys
        """
        result = {
            'issue': '',
            'reason': '',
            'root_cause': ''
        }

        if not reason_text:
            return result

        reason_lower = str(reason_text).lower()
        pod_info = pod_info or {}

        # Detect issue type from patterns
        for issue_name, pattern in self._issue_patterns.items():
            if re.search(pattern, reason_text):
                result['issue'] = issue_name
                break

        # If no pattern matched, try to extract from structured text
        if not result['issue']:
            # Look for common issue markers in the text
            if 'error' in reason_lower:
                result['issue'] = 'Error'
            elif 'failed' in reason_lower:
                result['issue'] = 'Failed'
            elif 'exception' in reason_lower:
                result['issue'] = 'Exception'

        # Extract exit code and build reason
        exit_match = re.search(r'exit[_\s]*code[=:\s]*(\d+)', reason_text, re.IGNORECASE)
        if exit_match:
            code = exit_match.group(1)
            reason_parts = [f"Exit code {code}"]
            if code == '137':
                reason_parts.append("(SIGKILL/OOM)")
            elif code == '1':
                reason_parts.append("(Application error)")
            elif code == '139':
                reason_parts.append("(Segmentation fault)")
            elif code == '143':
                reason_parts.append("(SIGTERM)")
            elif code == '0':
                reason_parts.append("(Normal exit)")
            result['reason'] = ' '.join(reason_parts)

        # Extract restart info for reason
        restart_count = int(pod_info.get('restarts', 0) or 0)
        restart_cause = str(pod_info.get('restart_cause', '') or '').strip()
        if restart_count > 0 and not result['reason']:
            result['reason'] = f"{restart_count} restart(s)"
            if restart_cause:
                result['reason'] += f" due to {restart_cause}"

        # Build reason from error message if not set
        if not result['reason']:
            # Extract key details from the error text
            if 'back-off' in reason_lower and 'pulling' in reason_lower:
                result['reason'] = 'Image pull backoff'
            elif 'manifest unknown' in reason_lower:
                result['reason'] = 'Image tag not found'
            elif 'unauthorized' in reason_lower or 'access denied' in reason_lower:
                result['reason'] = 'Registry authentication failed'
            elif 'insufficient cpu' in reason_lower:
                result['reason'] = 'Not enough CPU resources'
            elif 'insufficient memory' in reason_lower:
                result['reason'] = 'Not enough memory resources'
            elif 'node affinity' in reason_lower:
                result['reason'] = 'Node affinity constraints not met'
            elif result['issue']:
                result['reason'] = result['issue']

        # Build actionable root cause
        if result['issue'] == 'OOMKilled':
            result['root_cause'] = 'Container exceeded memory limit and was killed by kernel OOM killer'
        elif result['issue'] == 'CrashLoopBackOff':
            if restart_cause:
                result['root_cause'] = f"Container repeatedly crashing: {restart_cause}"
            else:
                result['root_cause'] = 'Container crashes repeatedly after startup - check application logs'
        elif result['issue'] == 'ImagePullBackOff':
            if 'manifest unknown' in reason_lower:
                result['root_cause'] = 'Image tag does not exist in registry - verify image:tag'
            elif 'unauthorized' in reason_lower or 'access denied' in reason_lower:
                result['root_cause'] = 'Missing or invalid registry credentials - check imagePullSecrets'
            else:
                result['root_cause'] = 'Cannot pull container image - verify image name and registry access'
        elif result['issue'] == 'FailedScheduling':
            if 'insufficient' in reason_lower:
                result['root_cause'] = 'Cluster lacks resources to schedule pod - scale cluster or reduce requests'
            else:
                result['root_cause'] = 'Pod cannot be scheduled - check node selectors, taints, and affinity rules'
        elif result['issue'] == 'CreateContainerConfigError':
            result['root_cause'] = 'Container configuration error - check ConfigMaps, Secrets, and volume mounts'
        elif result['issue'] == 'ConnectionRefused':
            result['root_cause'] = 'Target service not accepting connections - verify service is running and port is correct'
        elif result['issue'] == 'Timeout':
            result['root_cause'] = 'Request timed out - check network connectivity and service health'
        elif result['issue'] == 'DNSError':
            result['root_cause'] = 'DNS resolution failed - verify service name and DNS configuration'
        elif not result['root_cause'] and reason_text:
            # Use cleaned up reason text as root cause fallback
            result['root_cause'] = re.sub(r'\s+', ' ', reason_text).strip()[:200]

        return result

    def _get_pods_cache_ttl(self) -> int:
        cfg = self.runtime_config or {}
        monitoring_cfg = cfg.get('monitoring', {}) if isinstance(cfg, dict) else {}
        value = int(monitoring_cfg.get('pods_cache_seconds', 15) or 15)
        return max(5, min(value, 120))

    def _get_namespace_pods(self, namespace: str) -> List[Dict]:
        """Return namespace pods with short TTL cache to avoid kubectl storm."""
        lock = self._namespace_pods_locks.setdefault(namespace, threading.Lock())
        with lock:
            return self._get_namespace_pods_locked(namespace)

    def _get_namespace_pods_locked(self, namespace: str) -> List[Dict]:
        """Locked body for namespace pod retrieval."""
        now = datetime.now()
        ttl = self._get_pods_cache_ttl()
        timeout_seconds = self._get_kubectl_timeout()
        backoff_seconds = int((self.runtime_config or {}).get('monitoring', {}).get('pods_backoff_seconds', 60) or 60)

        cached_ts = self._namespace_pods_cache_ts.get(namespace)
        cached_items = self._namespace_pods_cache.get(namespace)
        if cached_ts and isinstance(cached_items, list):
            age = (now - cached_ts).total_seconds()
            if age < ttl:
                return cached_items

        backoff_until = self._namespace_pods_backoff_until.get(namespace)
        if backoff_until and now < backoff_until:
            if isinstance(cached_items, list):
                return cached_items
            raise RuntimeError(f"namespace backoff active for {namespace}")

        cmd = ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json']
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
            if result.returncode != 0:
                err = result.stderr.strip() or "kubectl get pods failed"
                self._namespace_pods_backoff_until[namespace] = now + timedelta(seconds=backoff_seconds)
                self._namespace_pods_last_error[namespace] = err
                if not isinstance(cached_items, list):
                    self._namespace_pods_cache[namespace] = []
                    self._namespace_pods_cache_ts[namespace] = now
                raise RuntimeError(err)
        except subprocess.TimeoutExpired:
            err = f"kubectl get pods timed out after {timeout_seconds}s"
            self._namespace_pods_backoff_until[namespace] = now + timedelta(seconds=backoff_seconds)
            self._namespace_pods_last_error[namespace] = err
            if not isinstance(cached_items, list):
                self._namespace_pods_cache[namespace] = []
                self._namespace_pods_cache_ts[namespace] = now
            raise RuntimeError(err)

        data = json.loads(result.stdout)
        items = data.get('items', [])
        self._namespace_pods_cache[namespace] = items
        self._namespace_pods_cache_ts[namespace] = now
        self._namespace_pods_backoff_until.pop(namespace, None)
        self._namespace_pods_last_error.pop(namespace, None)
        return items

    def _get_namespace_workloads(self, namespace: str) -> List[Dict]:
        """Return namespace deployments/statefulsets with short TTL cache."""
        lock = self._namespace_workloads_locks.setdefault(namespace, threading.Lock())
        with lock:
            return self._get_namespace_workloads_locked(namespace)

    def _get_namespace_workloads_locked(self, namespace: str) -> List[Dict]:
        """Locked body for namespace workload retrieval."""
        now = datetime.now()
        ttl = self._get_pods_cache_ttl()
        timeout_seconds = self._get_kubectl_timeout()

        cached_ts = self._namespace_workloads_cache_ts.get(namespace)
        cached_items = self._namespace_workloads_cache.get(namespace)
        if cached_ts and isinstance(cached_items, list):
            age = (now - cached_ts).total_seconds()
            if age < ttl:
                return cached_items

        cmd = ['kubectl', 'get', 'deployments,statefulsets,daemonsets', '-n', namespace, '-o', 'json']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
        if result.returncode != 0:
            if isinstance(cached_items, list):
                return cached_items
            return []

        data = json.loads(result.stdout)
        items = data.get('items', [])
        self._namespace_workloads_cache[namespace] = items
        self._namespace_workloads_cache_ts[namespace] = now
        return items

    def _get_desired_replicas_for_service(self, service_name: str, namespace: str) -> Dict:
        """Resolve desired replicas from matched Deployment/StatefulSet."""
        out = {'desired_replicas': None, 'workload_kind': '', 'workload_name': ''}
        try:
            workloads = self._get_namespace_workloads(namespace)
            if not workloads:
                return out

            candidates = self._service_name_candidates(service_name)
            best = None
            best_score = -1

            for item in workloads:
                metadata = item.get('metadata', {}) if isinstance(item.get('metadata', {}), dict) else {}
                spec = item.get('spec', {}) if isinstance(item.get('spec', {}), dict) else {}
                name = str(metadata.get('name', '') or '').strip()
                if not name:
                    continue
                replicas = spec.get('replicas', None)
                kind = str(item.get('kind', '') or '')
                if kind.lower() == 'daemonset':
                    status_obj = item.get('status', {}) if isinstance(item.get('status', {}), dict) else {}
                    replicas = status_obj.get('desiredNumberScheduled', None)
                try:
                    replicas_int = int(replicas if replicas is not None else 1)
                except Exception:
                    replicas_int = 1

                score = 0
                for candidate in candidates:
                    if not candidate:
                        continue
                    if name == candidate:
                        score = max(score, 100)
                    elif self._pod_name_matches_candidate(name, candidate):
                        score = max(score, 70)
                    elif self._pod_name_matches_candidate(candidate, name):
                        score = max(score, 60)

                if score > best_score:
                    best = {
                        'desired_replicas': replicas_int,
                        'workload_kind': kind,
                        'workload_name': name
                    }
                    best_score = score

            if best is not None and best_score > 0:
                return best
            return out
        except Exception:
            return out

    def _service_name_candidates(self, service_name: str) -> List[str]:
        """Build candidate names for workload/pod matching."""
        base = (service_name or '').strip()
        if not base:
            return []

        candidates = [base]
        if base.endswith('-service'):
            candidates.append(base[:-8])
        if base.endswith('-svc'):
            candidates.append(base[:-4])

        # Token-level synonyms for common workload naming differences
        # (e.g., elasticsearch <-> es).
        expanded = []
        for item in list(candidates):
            tokenized = item.split('-')
            if 'elasticsearch' in tokenized:
                expanded.append('-'.join('es' if t == 'elasticsearch' else t for t in tokenized))
            if 'es' in tokenized:
                expanded.append('-'.join('elasticsearch' if t == 'es' else t for t in tokenized))
        candidates.extend(expanded)

        # Keep order, remove duplicates
        ordered = []
        seen = set()
        for item in candidates:
            if item and item not in seen:
                ordered.append(item)
                seen.add(item)
        return ordered

    def _pod_name_matches_candidate(self, pod_name: str, candidate: str) -> bool:
        """Match pod names by exact/prefix semantics; avoid loose substring collisions."""
        pod = str(pod_name or '').strip().lower()
        cand = str(candidate or '').strip().lower()
        if not pod or not cand:
            return False

        if pod == cand:
            return True

        # Standard Kubernetes pod naming: <workload>-<hash>-<suffix>
        if pod.startswith(f"{cand}-"):
            return True

        def _strip_runtime_suffixes(name: str) -> str:
            value = str(name or '').strip().lower()
            if not value:
                return ''
            # Deployment pod: <name>-<pod-template-hash>-<suffix>
            value = re.sub(r'-[a-f0-9]{8,10}-[a-z0-9]{4,}$', '', value)
            # ReplicaSet owner name: <name>-<pod-template-hash>
            value = re.sub(r'-[a-f0-9]{8,10}$', '', value)
            # StatefulSet pod: <name>-<ordinal>
            value = re.sub(r'-\d+$', '', value)
            return value

        pod_base = _strip_runtime_suffixes(pod)
        cand_base = _strip_runtime_suffixes(cand)
        if pod_base and cand_base and pod_base == cand_base:
            return True

        # Hyphen-insensitive exact workload-name match only.
        # Avoid prefix matches that can cross-link distinct services
        # (e.g., tripmanagement vs trip-management-search).
        pod_compact = pod_base.replace('-', '') if pod_base else pod.replace('-', '')
        cand_compact = cand_base.replace('-', '') if cand_base else cand.replace('-', '')
        if pod_compact == cand_compact:
            return True

        return False
        
    def _load_services_from_config(self, config_override: Optional[Dict] = None):
        """Load service patterns from config file"""
        try:
            if config_override is not None:
                config = config_override
            else:
                with open('config.json', 'r') as f:
                    config = json.load(f)

            services = {}

            monitoring_cfg = config.get('monitoring', {})
            configured_namespaces = monitoring_cfg.get('discovery_namespaces', ['jupiter', 'venus'])
            if isinstance(configured_namespaces, list) and configured_namespaces:
                self.discovery_namespaces = configured_namespaces

            self.max_selected_services = int(config.get('max_selected_services', 25))

            default_ignored_patterns = [
                r'(?i)io\.opentelemetry\.exporter\.internal\.http\.HttpExporter\s*-\s*Failed to export spans',
                r'(?i)otel\.javaagent.*failed to export spans',
                r'(?i)fabhotels-signoz-prod-otel-collector.*\.svc\.cluster\.local(?::4318)?',
                r'(?i)org\.apache\.catalina\.valves\.ErrorReportValve\.invoke\(ErrorReportValve\.java:92\)'
            ]
            configured_ignored_patterns = config.get('ignored_log_patterns', [])
            if isinstance(configured_ignored_patterns, list) and configured_ignored_patterns:
                self.ignored_log_patterns = configured_ignored_patterns
            else:
                self.ignored_log_patterns = default_ignored_patterns

            monitored_services = config.get('monitored_services', [])
            if monitored_services:
                cleaned = []
                for item in monitored_services[:self.max_selected_services]:
                    name = item.get('name') if isinstance(item, dict) else None
                    namespace = item.get('namespace', self.namespace) if isinstance(item, dict) else self.namespace
                    if name:
                        cleaned.append({'name': name, 'namespace': namespace})
                        services[f"{namespace}/{name}"] = name
                self.selected_services = cleaned
            else:
                # Backward compatible fallback to old services list
                cleaned = []
                for service in config.get('services', []):
                    name = service.get('name')
                    namespace = service.get('namespace', self.namespace)
                    if name:
                        cleaned.append({'name': name, 'namespace': namespace})
                        services[f"{namespace}/{name}"] = name
                self.selected_services = cleaned[:self.max_selected_services]

            self.service_patterns = services
            return services
        except Exception as e:
            logger.warning(f"Could not load services from config: {e}")
            # Keep dynamic mode only; avoid static service fallbacks.
            self.selected_services = []
            self.ignored_log_patterns = [
                r'(?i)io\.opentelemetry\.exporter\.internal\.http\.HttpExporter\s*-\s*Failed to export spans',
                r'(?i)otel\.javaagent.*failed to export spans',
                r'(?i)fabhotels-signoz-prod-otel-collector.*\.svc\.cluster\.local(?::4318)?',
                r'(?i)org\.apache\.catalina\.valves\.ErrorReportValve\.invoke\(ErrorReportValve\.java:92\)'
            ]
            self.service_patterns = {}
            return self.service_patterns

    def apply_runtime_config(self, config: Dict):
        """Apply config directly from running agent without relying on file read."""
        self.runtime_config = config
        self._load_services_from_config(config_override=config)
        
    def _test_kubectl(self) -> bool:
        """Test if kubectl is working properly"""
        try:
            # Try a simple kubectl command that should work
            cmd = ['kubectl', 'version', '--client=true']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                logger.info("kubectl is working properly")
                return True
            else:
                logger.warning(f"kubectl test failed: {result.stderr}")
                return False
        except Exception as e:
            logger.warning(f"kubectl test failed: {e}")
            return False
        
    def get_service_logs(self, service_name: str, minutes: int = 5, namespace: Optional[str] = None) -> List[Dict]:
        """
        Get recent logs for a specific service
        
        Args:
            service_name: Name of the service to monitor
            minutes: How many minutes of logs to retrieve
            
        Returns:
            List of log entries with timestamp and message
        """
        monitoring_cfg = (self.runtime_config or {}).get('monitoring', {}) if isinstance(self.runtime_config, dict) else {}
        if isinstance(monitoring_cfg, dict) and not bool(monitoring_cfg.get('use_kubectl', True)):
            return []

        # Check if kubectl is working
        if not self.kubectl_working:
            logger.warning("kubectl is not working, returning empty logs")
            return []

        namespace = namespace or self.namespace
            
        try:
            result = None
            candidates = self._service_name_candidates(service_name)

            # Try common workload types first (Deployment/StatefulSet)
            for candidate in candidates:
                for kind in ('deployment', 'statefulset'):
                    cmd = [
                        'kubectl', 'logs',
                        f'{kind}/{candidate}',
                        '-n', namespace,
                        f'--since={minutes}m',
                        '--tail=50'
                    ]
                    logger.info(f"Executing command: {' '.join(cmd)}")
                    current = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                    if current.returncode == 0:
                        result = current
                        break
                if result is not None:
                    break

            # Fallback: pick first matching pod and read pod logs
            if result is None:
                pods_cmd = ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'jsonpath={.items[*].metadata.name}']
                pods_result = subprocess.run(pods_cmd, capture_output=True, text=True, timeout=5)
                if pods_result.returncode == 0:
                    all_pods = pods_result.stdout.strip().split()
                    matched_pod = None
                    for pod_name in all_pods:
                        for candidate in candidates:
                            if candidate and candidate in pod_name:
                                matched_pod = pod_name
                                break
                        if matched_pod:
                            break

                    if matched_pod:
                        pod_log_cmd = [
                            'kubectl', 'logs',
                            matched_pod,
                            '-n', namespace,
                            f'--since={minutes}m',
                            '--tail=50'
                        ]
                        logger.info(f"Executing command: {' '.join(pod_log_cmd)}")
                        current = subprocess.run(pod_log_cmd, capture_output=True, text=True, timeout=5)
                        if current.returncode == 0:
                            result = current

            if result is None or result.returncode != 0:
                return []
                
            logs = []
            for line in result.stdout.split('\n'):
                if line.strip():
                    # Try to parse timestamp from log line
                    timestamp = self._extract_timestamp(line)
                    logs.append({
                        'timestamp': timestamp,
                        'message': line.strip(),
                        'service': service_name
                    })
                    
            logger.info(f"Retrieved {len(logs)} log entries for {service_name}")
            return logs
            
        except subprocess.TimeoutExpired as e:
            logger.error(f"Timeout getting logs for {service_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error getting logs for {service_name}: {e}", exc_info=True)
            return []
            
    def _extract_timestamp(self, log_line: str) -> str:
        """Extract timestamp from log line if possible"""
        # Common timestamp patterns
        patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',   # Standard format
            r'\[\d{2}/\d{2}/\d{4}:\d{2}:\d{2}:\d{2}'  # Apache format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, log_line)
            if match:
                return match.group(0)
                
        # If no timestamp found, use current time
        return datetime.now().isoformat()
        
    def detect_errors_in_logs(self, service_name: str, minutes: int = 5, namespace: Optional[str] = None) -> List[Dict]:
        """
        Detect error patterns in service logs
        
        Returns:
            List of error entries with severity and context
        """
        logs = self.get_service_logs(service_name, minutes, namespace=namespace)
        errors = []
        
        error_patterns = [
            (r'(?i)error', 'ERROR'),
            (r'(?i)exception', 'ERROR'),
            (r'(?i)fatal', 'FATAL'),
            (r'(?i)warn', 'WARNING'),
            (r'(?i)critical', 'CRITICAL'),
            (r'(?i)ImagePullBackOff', 'IMAGE_PULL'),
            (r'(?i)ErrImagePull', 'IMAGE_PULL'),
            (r'(?i)Back-?off pulling image', 'IMAGE_PULL'),
            (r'(?i)pull access denied', 'IMAGE_PULL'),
            (r'TimeoutException', 'TIMEOUT'),
            (r'NullPointerException', 'NULL_POINTER'),
            (r'OutOfMemory', 'MEMORY'),
            (r'Connection refused', 'CONNECTION'),
            (r'(?i)service.*unavailable', 'SERVICE_UNAVAILABLE'),
            (r'(?i)could not connect', 'CONNECTION'),
            (r'(?i)dns resolution failed', 'DNS_ERROR')
        ]
        
        for log_entry in logs:
            message = log_entry['message']

            # Skip known noisy log lines to avoid false incidents
            if self._is_ignored_log(message):
                continue

            matched_pattern = None
            matched_severity = 'ERROR'
            
            for pattern, severity in error_patterns:
                if re.search(pattern, message):
                    matched_pattern = pattern
                    matched_severity = severity
                    break  # Use first matching pattern
            
            # Extract dependency information if present
            dependency = self._extract_dependency_from_log(message)
            
            if matched_pattern:
                error_entry = {
                    'timestamp': log_entry['timestamp'],
                    'service': service_name,
                    'severity': matched_severity,
                    'message': message,
                    'pattern_matched': matched_pattern
                }
                
                if dependency:
                    error_entry['dependency'] = dependency
                    
                errors.append(error_entry)
                    
        return errors

    def _is_ignored_log(self, message: str) -> bool:
        """Return True when log line should be ignored for incident detection."""
        if not message:
            return False

        for pattern in self.ignored_log_patterns:
            try:
                if re.search(pattern, message):
                    return True
            except re.error:
                # Ignore invalid custom regex and continue safely
                continue

        return False
        
    def _extract_dependency_from_log(self, message: str) -> str:
        """Extract service dependency names from log messages"""
        # Common patterns for service dependencies
        dependency_patterns = [
            r'to ([\w-]+)-service',  # e.g., "to b2b-sales-service"
            r'([a-zA-Z0-9-]+)\.svc\.cluster\.local',  # Kubernetes service names
            r'connecting to ([\w-]+)',  # e.g., "connecting to database"
            r'([a-zA-Z0-9-]+):[0-9]+',  # service:port patterns
            r'"([^"]*service[^"]*)"',  # quoted service names
        ]
        
        for pattern in dependency_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                dependency = match.group(1)
                # Clean up the dependency name
                dependency = re.sub(r'[:\d]+$', '', dependency)  # Remove port numbers
                return dependency
                
        return ""
        
    def get_service_metrics_from_logs(self, service_name: str, minutes: int = 5, namespace: Optional[str] = None) -> Dict:
        """
        Extract metrics from service logs (request count, error rate, etc.)
        """
        logs = self.get_service_logs(service_name, minutes, namespace=namespace)
        errors = self.detect_errors_in_logs(service_name, minutes, namespace=namespace)
        
        # Count different types of messages
        total_logs = len(logs)
        error_count = len([e for e in errors if e['severity'] in ['ERROR', 'FATAL', 'CRITICAL']])
        warning_count = len([e for e in errors if e['severity'] == 'WARNING'])
        
        # Extract dependency failures
        dependency_failures = [e for e in errors if 'dependency' in e]
        
        metrics = {
            'service': service_name,
            'total_log_entries': total_logs,
            'error_count': error_count,
            'warning_count': warning_count,
            'error_rate': (error_count / total_logs * 100) if total_logs > 0 else 0,
            'dependency_failures': len(dependency_failures),
            'timestamp': datetime.now().isoformat()
        }
        
        # Add dependency information if present
        if dependency_failures:
            dependencies = list(set([e['dependency'] for e in dependency_failures if e.get('dependency')]))
            metrics['failed_dependencies'] = dependencies
        
        return metrics
        
    def get_pod_status(self, service_name: str, namespace: Optional[str] = None) -> Dict:
        """
        Get pod status for a specific service
        """
        if not self.kubectl_working:
            return {'status': 'unknown', 'pods': []}

        namespace = namespace or self.namespace
            
        try:
            candidates = self._service_name_candidates(service_name)
            all_items = self._get_namespace_pods(namespace)

            # Filter pods using labels and name matching from cached namespace pod list
            filtered_items = []
            for item in all_items:
                metadata = item.get('metadata', {})
                pod_name = metadata.get('name', '')
                owner_refs = metadata.get('ownerReferences', []) if isinstance(metadata.get('ownerReferences', []), list) else []
                owner_name = ''
                if owner_refs:
                    first_owner = owner_refs[0] if isinstance(owner_refs[0], dict) else {}
                    owner_name = str(first_owner.get('name', '') or '')

                matched = False
                for candidate in candidates:
                    if not candidate:
                        continue
                    pod_name_match = self._pod_name_matches_candidate(pod_name, candidate)
                    owner_name_match = self._pod_name_matches_candidate(owner_name, candidate)
                    if pod_name_match or owner_name_match:
                        matched = True
                        break
                if matched:
                    filtered_items.append(item)

            pods_data = {'items': filtered_items}
            
            pods = []
            all_ready = True
            
            for item in pods_data.get('items', []):
                # Get pod metadata
                metadata = item.get('metadata', {})
                pod_name = metadata.get('name', '')
                labels = metadata.get('labels', {})
                
                # Get pod status
                status = item.get('status', {})
                phase = status.get('phase', 'Unknown')
                container_statuses = status.get('containerStatuses', [])
                
                # Check if containers are ready
                # IMPORTANT: Pending pods often have empty containerStatuses; they
                # must not be treated as ready/healthy.
                phase_lower = str(phase or '').lower()
                ready = phase_lower == 'running'
                if container_statuses:
                    ready = all(bool(container.get('ready', False)) for container in container_statuses)
                
                # Check for image pull errors or other issues
                reason = ''
                restart_cause = ''
                restart_exit_code = ''
                restart_finished_at = ''
                restart_count = sum(cs.get('restartCount', 0) for cs in container_statuses)

                if not ready and phase_lower in {'pending', 'failed', 'unknown'}:
                    reason = str(status.get('reason', '') or '')
                    if not reason:
                        # Pull concise pod condition reason/message for scheduling failures.
                        for condition in status.get('conditions', []) or []:
                            if str(condition.get('status', '')).lower() != 'true':
                                c_reason = str(condition.get('reason', '') or '').strip()
                                c_msg = str(condition.get('message', '') or '').strip()
                                reason = ': '.join([p for p in [c_reason, c_msg] if p])
                                if reason:
                                    break

                if not ready:
                    for container in container_statuses:
                        if not container.get('ready', False):
                            waiting = container.get('state', {}).get('waiting', {})
                            if waiting:
                                waiting_reason = str(waiting.get('reason', '') or '').strip()
                                waiting_msg = str(waiting.get('message', '') or '').strip()
                                reason = ': '.join([p for p in [waiting_reason, waiting_msg] if p]) or reason
                                break

                # Enrich non-ready pods with describe-event evidence (FailedScheduling,
                # node affinity, autoscaler not-triggered, etc.) so pending root cause is explicit.
                if not ready:
                    events_summary = self._get_pod_event_summary(pod_name, namespace)
                    if events_summary:
                        reason = events_summary if not reason else f"{reason} | {events_summary}"

                generic_reason = str(reason or '').strip().lower()
                should_enrich_from_logs = (not ready) or restart_count > 0
                if should_enrich_from_logs and ((not reason) or generic_reason in {'error', 'failed', 'unknown'} or 'crashloopbackoff' in generic_reason):
                    log_summary = self._get_pod_log_summary(
                        pod_name,
                        namespace,
                        minutes=5,
                        include_previous=restart_count > 0
                    )
                    if log_summary:
                        reason = f"{reason} | {log_summary}" if reason else log_summary

                if reason and (
                    re.search(r'(?i)fabhotels-signoz-prod-otel-collector\.signoz-prod11\.svc\.cluster\.local', reason)
                    or re.search(r'(?i)signoz-prod11\.svc\.cluster\.local', reason)
                    or any(re.search(str(pattern), reason, re.IGNORECASE) for pattern in self.ignored_log_patterns)
                ):
                    reason = ''

                # Capture concrete restart cause from previous container termination.
                for container in container_statuses:
                    last_state = container.get('lastState', {}) if isinstance(container.get('lastState', {}), dict) else {}
                    terminated = last_state.get('terminated', {}) if isinstance(last_state.get('terminated', {}), dict) else {}
                    if terminated:
                        restart_cause = str(terminated.get('reason', '') or '')
                        restart_exit_code = str(terminated.get('exitCode', '') or '')
                        restart_finished_at = str(terminated.get('finishedAt', '') or '')
                        if not reason and restart_cause:
                            reason = restart_cause
                        break

                if 'crashloopbackoff' in str(reason or '').lower() and restart_cause:
                    termination_hint = f"last_termination={restart_cause}"
                    if restart_exit_code:
                        termination_hint += f" exit_code={restart_exit_code}"
                    if restart_finished_at:
                        termination_hint += f" finished_at={restart_finished_at}"
                    if termination_hint.lower() not in str(reason).lower():
                        reason = f"{reason} | {termination_hint}" if reason else termination_hint
                
                # Parse structured error fields
                structured_error = self.parse_structured_error(reason, {
                    'restarts': restart_count,
                    'restart_cause': restart_cause,
                    'restart_exit_code': restart_exit_code
                })

                pod_info = {
                    'name': pod_name,
                    'status': phase,
                    'ready': ready,
                    'restarts': restart_count,
                    'reason': reason,
                    'restart_cause': restart_cause,
                    'restart_exit_code': restart_exit_code,
                    'restart_finished_at': restart_finished_at,
                    # Structured error fields for 3-column display
                    'issue': structured_error.get('issue', ''),
                    'error_reason': structured_error.get('reason', ''),
                    'root_cause': structured_error.get('root_cause', '')
                }
                pods.append(pod_info)
                
                # Update overall readiness
                if not ready:
                    all_ready = False
            
            # Determine overall status
            if not pods:
                workload = self._get_desired_replicas_for_service(service_name, namespace)
                desired_replicas = workload.get('desired_replicas', None)
                if desired_replicas is not None and int(desired_replicas) == 0:
                    status = 'scaled_down'
                # Fallback: if service endpoints are ready, treat as healthy even if pod name match failed
                elif self._has_ready_endpoints(service_name, namespace):
                    status = 'healthy'
                else:
                    status = 'no_pods'
            elif all_ready:
                status = 'healthy'
            else:
                status = 'unhealthy'

            workload = self._get_desired_replicas_for_service(service_name, namespace)
            desired_replicas = workload.get('desired_replicas', None)
                
            actionable_reason_tokens = [
                'imagepullbackoff',
                'errimagepull',
                'crashloopbackoff',
                'failedscheduling',
                'oomkilled',
                'createcontainerconfigerror',
                'createcontainererror',
                'runcontainererror',
                'containerstatusunknown',
                'invalidimagename',
                'back-off pulling image',
                'no nodes available',
                'insufficient cpu',
                'insufficient memory'
            ]

            def _is_actionable_reason(text: str) -> bool:
                value = str(text or '').strip().lower()
                if not value:
                    return False
                return any(token in value for token in actionable_reason_tokens)

            issue_pods_count = 0
            for pod in pods:
                ready_flag = bool(pod.get('ready', False))
                reason_text = str(pod.get('reason', '') or '')
                if (not ready_flag) or _is_actionable_reason(reason_text):
                    issue_pods_count += 1
                
            return {
                'status': status,
                'pods': pods,
                'total_pods': len(pods),
                'ready_pods': sum(1 for pod in pods if bool(pod.get('ready', False))),
                'running_pods': sum(1 for pod in pods if str(pod.get('status', '')).lower() == 'running'),
                'issue_pods': issue_pods_count,
                'desired_replicas': desired_replicas,
                'workload_kind': workload.get('workload_kind', ''),
                'workload_name': workload.get('workload_name', ''),
                'namespace': namespace
            }
            
        except Exception as e:
            msg = str(e)
            if 'namespace backoff active' not in msg:
                logger.warning(f"Error getting pod status for {service_name}: {e}")
            return {'status': 'unknown', 'pods': [], 'namespace': namespace}

    def _has_ready_endpoints(self, service_name: str, namespace: str) -> bool:
        """Check if service has ready endpoints."""
        try:
            cmd = ['kubectl', 'get', 'endpoints', service_name, '-n', namespace, '-o', 'json']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                return False

            data = json.loads(result.stdout)
            for subset in data.get('subsets', []):
                addresses = subset.get('addresses', [])
                if addresses:
                    return True
            return False
        except Exception:
            return False

    def _get_pod_event_summary(self, pod_name: str, namespace: str) -> str:
        """Get concise warning/failure summary from pod describe output."""
        try:
            cache_key = f"{namespace}/{pod_name}"
            cached_ts = self._pod_event_cache_ts.get(cache_key)
            if cached_ts and (datetime.now() - cached_ts).total_seconds() < 20:
                return str(self._pod_event_cache.get(cache_key, '') or '')

            cmd = ['kubectl', 'describe', 'pod', pod_name, '-n', namespace]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            if result.returncode != 0:
                return ''

            interesting = []
            keywords = [
                'ImagePullBackOff',
                'ErrImagePull',
                'CrashLoopBackOff',
                'InvalidImageName',
                'Failed',
                'Back-off',
                'OOMKilled',
                'FailedScheduling',
                'ContainerStatusUnknown'
            ]

            for line in result.stdout.splitlines():
                text = line.strip()
                if not text:
                    continue
                if any(keyword.lower() in text.lower() for keyword in keywords):
                    interesting.append(text)
                if len(interesting) >= 4:
                    break

            summary = ' | '.join(interesting)
            self._pod_event_cache[cache_key] = summary
            self._pod_event_cache_ts[cache_key] = datetime.now()
            return summary
        except Exception:
            return ''

    def _get_pod_log_summary(self, pod_name: str, namespace: str, minutes: int = 5, include_previous: bool = False) -> str:
        """Get a concise actionable snippet from recent pod logs."""
        try:
            cache_key = f"{namespace}/{pod_name}:{minutes}:{int(include_previous)}"
            cached_ts = self._pod_log_cache_ts.get(cache_key)
            if cached_ts and (datetime.now() - cached_ts).total_seconds() < 20:
                return str(self._pod_log_cache.get(cache_key, '') or '')

            timeout_seconds = self._get_kubectl_timeout()
            commands = [[
                'kubectl', 'logs', pod_name,
                '-n', namespace,
                f'--since={max(1, int(minutes))}m',
                '--tail=120'
            ]]
            if include_previous:
                commands.append([
                    'kubectl', 'logs', pod_name,
                    '-n', namespace,
                    '--previous',
                    f'--since={max(1, int(minutes))}m',
                    '--tail=120'
                ])

            actionable_patterns = [
                r'(?i)(exception|error|fatal|oomkilled|outofmemory|imagepullbackoff|errimagepull|crashloopbackoff)',
                r'(?i)(failed\s+to\s+bind\s+properties|unsatisfieddependencyexception|beancreationexception)',
                r'(?i)(connection\s+refused|timed\s*out|deadline\s+exceeded|http\s*5\d\d)'
            ]
            hard_ignored_patterns = [
                r'(?i)fabhotels-signoz-prod-otel-collector\.signoz-prod11\.svc\.cluster\.local',
                r'(?i)signoz-prod11\.svc\.cluster\.local'
            ]

            monitoring_cfg = (self.runtime_config or {}).get('monitoring', {}) if isinstance(self.runtime_config, dict) else {}
            retries = int(monitoring_cfg.get('log_probe_retries', 3) or 3)
            retries = max(1, min(retries, 5))
            retry_sleep = float(monitoring_cfg.get('log_probe_wait_seconds', 1.2) or 1.2)
            retry_sleep = max(0.2, min(retry_sleep, 3.0))

            for attempt in range(retries):
                for cmd in commands:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
                    if result.returncode != 0 or not result.stdout:
                        continue

                    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
                    for line in reversed(lines):
                        if any(re.search(pattern, line) for pattern in hard_ignored_patterns):
                            continue
                        if any(re.search(str(pattern), line, re.IGNORECASE) for pattern in self.ignored_log_patterns):
                            continue
                        if any(re.search(pattern, line) for pattern in actionable_patterns):
                            compact = re.sub(r'\s+', ' ', line).strip()
                            summary = compact[:260] + ('...' if len(compact) > 260 else '')
                            self._pod_log_cache[cache_key] = summary
                            self._pod_log_cache_ts[cache_key] = datetime.now()
                            return summary

                if attempt < retries - 1:
                    time.sleep(retry_sleep)

            self._pod_log_cache[cache_key] = ''
            self._pod_log_cache_ts[cache_key] = datetime.now()
            return ''
        except Exception:
            return ''

    def _get_pod_resource_snapshot(self, pod_name: str, namespace: str) -> str:
        """Fetch concise CPU/memory usage for pod when metrics-server is available."""
        try:
            cache_key = f"top:{namespace}/{pod_name}"
            cached_ts = self._pod_event_cache_ts.get(cache_key)
            if cached_ts and (datetime.now() - cached_ts).total_seconds() < 20:
                return str(self._pod_event_cache.get(cache_key, '') or '')

            cmd = ['kubectl', 'top', 'pod', pod_name, '-n', namespace, '--no-headers']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                return ''
            line = str(result.stdout or '').strip().splitlines()
            if not line:
                return ''
            parts = re.split(r'\s+', line[0].strip())
            if len(parts) < 3:
                return ''
            summary = f"cpu={parts[1]} mem={parts[2]}"
            self._pod_event_cache[cache_key] = summary
            self._pod_event_cache_ts[cache_key] = datetime.now()
            return summary
        except Exception:
            return ''

    def _extract_actionable_log_line(self, lines: List[str]) -> str:
        """Pick the most actionable line from log output."""
        if not lines:
            return ''

        priority_patterns = [
            r'(?i)caused by:',
            r'(?i)(exception|fatal|traceback|panic|segmentation fault)',
            r'(?i)(outofmemory|oomkilled|kill process out of memory)',
            r'(?i)(connection refused|timed out|deadline exceeded|unknownhostexception)',
            r'(?i)(imagepullbackoff|errimagepull|crashloopbackoff|failedscheduling)',
            r'(?i)(failed to|unable to|cannot |permission denied|no such file|not found)'
        ]

        cleaned_lines = [re.sub(r'\s+', ' ', str(line or '').strip()) for line in lines if str(line or '').strip()]
        if not cleaned_lines:
            return ''

        ignore_patterns = [
            r'(?i)\binside\s+ping\b',
            r'(?i)\bping\s+request\s+received\b',
            r'(?i)\breturning\s+from\s+ping\b',
            r'(?i)\bkube-probe/\d',
            r'(?i)^\s*info\b'
        ]
        candidate_lines = [
            line for line in cleaned_lines
            if not any(re.search(pat, line) for pat in ignore_patterns)
        ]
        if not candidate_lines:
            candidate_lines = cleaned_lines

        for pattern in priority_patterns:
            for line in reversed(candidate_lines):
                if re.search(pattern, line):
                    return line[:420] + ('...' if len(line) > 420 else '')

        return ''

    def _score_log_line(self, line: str) -> int:
        """Score log lines for troubleshooting relevance."""
        text = re.sub(r'\s+', ' ', str(line or '').strip())
        if not text:
            return -1000

        lower = text.lower()
        score = 0

        if any(token in lower for token in ['inside ping', 'ping request received', 'returning from ping', 'kube-probe/']):
            score -= 80
        if 'suppressed:' in lower and 'terminated with an error' in lower:
            score -= 35

        if re.search(r'\berror\b', text, re.IGNORECASE):
            score += 35
        if re.search(r'\bexception\b', text, re.IGNORECASE):
            score += 40
        if re.search(r'\bcaused by\b', text, re.IGNORECASE):
            score += 30
        if re.search(r'(?i)application\s+run\s+failed', text):
            score += 55
        if re.search(r'(?i)unable\s+to\s+instantiate\s+factory\s+class', text):
            score += 50
        if re.search(r'(?i)illegalargumentexception', text):
            score += 35
        if re.search(r'\b(timeout|timed out|connection refused|unknownhostexception|oomkilled|outofmemory|crashloopbackoff|failedscheduling)\b', text, re.IGNORECASE):
            score += 45
        if re.search(r'\b(status|code|http)\D{0,8}(4\d\d|5\d\d)\b', text, re.IGNORECASE):
            score += 30
        if re.search(r'\b(INFO)\b', text) and not re.search(r'\b(error|exception|fatal|failed|timeout|bad_request|\b4\d\d\b|\b5\d\d\b)\b', text, re.IGNORECASE):
            score -= 25

        return score

    def _get_pod_log_evidence(self, pod_name: str, namespace: str, minutes: int = 20) -> Dict:
        """Collect deeper current/previous log evidence for troubleshooting."""
        evidence = {
            'best_line': '',
            'source': '',
            'excerpt': [],
            'error_lines': []
        }
        try:
            timeout_seconds = max(8, self._get_kubectl_timeout())
            monitoring_cfg = (self.runtime_config or {}).get('monitoring', {}) if isinstance(self.runtime_config, dict) else {}
            retries = int(monitoring_cfg.get('troubleshoot_log_probe_retries', 5) or 5)
            retries = max(1, min(retries, 8))
            retry_sleep = float(monitoring_cfg.get('troubleshoot_log_probe_wait_seconds', 2.0) or 2.0)
            retry_sleep = max(0.3, min(retry_sleep, 5.0))
            tail_lines = int(monitoring_cfg.get('troubleshoot_log_tail_lines', 2000) or 2000)
            tail_lines = max(200, min(tail_lines, 10000))
            since_minutes = int(monitoring_cfg.get('troubleshoot_log_since_minutes', max(20, int(minutes))) or max(20, int(minutes)))
            since_minutes = max(10, min(since_minutes, 360))
            full_scan_enabled = bool(monitoring_cfg.get('troubleshoot_full_scan_enabled', True))
            full_scan_since_minutes = int(monitoring_cfg.get('troubleshoot_full_scan_since_minutes', 5) or 5)
            full_scan_since_minutes = max(1, min(full_scan_since_minutes, 30))
            full_scan_timeout_seconds = int(monitoring_cfg.get('troubleshoot_full_scan_timeout_seconds', max(timeout_seconds * 4, 30)) or max(timeout_seconds * 4, 30))
            full_scan_timeout_seconds = max(15, min(full_scan_timeout_seconds, 120))
            ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
            error_patterns = [
                r'(?i)\berror\b',
                r'(?i)\bexception\b',
                r'(?i)\bfatal\b',
                r'(?i)\btraceback\b',
                r'(?i)\bpanic\b',
                r'(?i)oomkilled|outofmemory',
                r'(?i)\bfailed\b',
                r'(?i)\bdenied\b',
                r'(?i)timeout|timed out|deadline exceeded',
                r'(?i)\bbad_request\b',
                r'(?i)\b(status|code|http)\D{0,8}(4\d\d|5\d\d)\b',
                r'(?i)\b(4\d\d|5\d\d)\s+(bad request|unauthorized|forbidden|not found|internal server error|service unavailable)\b'
            ]
            hard_ignored = [
                r'(?i)fabhotels-signoz-prod-otel-collector\.signoz-prod11\.svc\.cluster\.local',
                r'(?i)signoz-prod11\.svc\.cluster\.local'
            ]

            commands = [
                (
                    'current',
                    [
                        'kubectl', 'logs', pod_name,
                        '-n', namespace,
                        '--all-containers=true',
                        f'--since={since_minutes}m',
                        f'--tail={tail_lines}'
                    ]
                ),
                (
                    'previous',
                    [
                        'kubectl', 'logs', pod_name,
                        '-n', namespace,
                        '--all-containers=true',
                        '--previous',
                        f'--since={since_minutes}m',
                        f'--tail={tail_lines}'
                    ]
                )
            ]

            candidates = []

            for attempt in range(retries):
                for source, cmd in commands:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
                    if result.returncode != 0 or not result.stdout:
                        continue
                    lines = [ansi_escape.sub('', line) for line in result.stdout.splitlines() if str(line or '').strip()]
                    if not lines:
                        continue

                    error_lines = []
                    for line in lines:
                        text = str(line or '').strip()
                        if not text:
                            continue
                        if any(re.search(pattern, text) for pattern in hard_ignored):
                            continue
                        if any(re.search(pattern, text) for pattern in self.ignored_log_patterns):
                            continue
                        if re.search(r'(?i)\binfo\b', text) and not re.search(r'(?i)\b(error|exception|fatal|failed|timeout|bad_request|4\d\d|5\d\d)\b', text):
                            continue
                        if any(re.search(pattern, text) for pattern in error_patterns):
                            compact = re.sub(r'\s+', ' ', text).strip()
                            error_lines.append(compact[:420] + ('...' if len(compact) > 420 else ''))

                    # Keep unique order
                    unique_error_lines = []
                    seen = set()
                    for item in error_lines:
                        key = item.lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        unique_error_lines.append(item)

                    best = self._extract_actionable_log_line(unique_error_lines)
                    excerpt_source = unique_error_lines if unique_error_lines else [re.sub(r'\s+', ' ', ln).strip() for ln in lines]
                    excerpt = excerpt_source[-25:]

                    excerpt = [ln[:320] + ('...' if len(ln) > 320 else '') for ln in excerpt if ln]

                    evidence['best_line'] = best
                    evidence['source'] = source
                    evidence['excerpt'] = excerpt
                    evidence['error_lines'] = unique_error_lines[:25]

                    for line in unique_error_lines[:60]:
                        candidates.append((self._score_log_line(line), source, line))

                if attempt < retries - 1:
                    time.sleep(retry_sleep)

            # Slow fallback: scan all logs from last N minutes when quick tail scan misses.
            # This is intentionally heavier and used only for troubleshooting mode.
            if full_scan_enabled:
                full_scan_commands = [
                    (
                        'current-fullscan',
                        [
                            'kubectl', 'logs', pod_name,
                            '-n', namespace,
                            '--all-containers=true',
                            f'--since={full_scan_since_minutes}m'
                        ]
                    ),
                    (
                        'previous-fullscan',
                        [
                            'kubectl', 'logs', pod_name,
                            '-n', namespace,
                            '--all-containers=true',
                            '--previous',
                            f'--since={full_scan_since_minutes}m'
                        ]
                    )
                ]

                for source, cmd in full_scan_commands:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=full_scan_timeout_seconds)
                    if result.returncode != 0 or not result.stdout:
                        continue

                    lines = [ansi_escape.sub('', line) for line in result.stdout.splitlines() if str(line or '').strip()]
                    if not lines:
                        continue

                    error_lines = []
                    for line in lines:
                        text = str(line or '').strip()
                        if not text:
                            continue
                        if any(re.search(pattern, text) for pattern in hard_ignored):
                            continue
                        if any(re.search(pattern, text) for pattern in self.ignored_log_patterns):
                            continue
                        if re.search(r'(?i)\binfo\b', text) and not re.search(r'(?i)\b(error|exception|fatal|failed|timeout|bad_request|4\d\d|5\d\d)\b', text):
                            continue
                        if any(re.search(pattern, text) for pattern in error_patterns):
                            compact = re.sub(r'\s+', ' ', text).strip()
                            error_lines.append(compact[:420] + ('...' if len(compact) > 420 else ''))

                    unique_error_lines = []
                    seen = set()
                    for item in error_lines:
                        key = item.lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        unique_error_lines.append(item)

                    best = self._extract_actionable_log_line(unique_error_lines)
                    excerpt_source = unique_error_lines if unique_error_lines else [re.sub(r'\s+', ' ', ln).strip() for ln in lines]
                    excerpt = excerpt_source[-30:]
                    excerpt = [ln[:320] + ('...' if len(ln) > 320 else '') for ln in excerpt if ln]

                    evidence['best_line'] = best
                    evidence['source'] = source
                    evidence['excerpt'] = excerpt
                    evidence['error_lines'] = unique_error_lines[:30]

                    for line in unique_error_lines[:100]:
                        candidates.append((self._score_log_line(line), source, line))

            if candidates:
                candidates.sort(key=lambda item: item[0], reverse=True)
                top_score, top_source, top_line = candidates[0]
                if top_score > -60:
                    evidence['best_line'] = top_line
                    evidence['source'] = top_source

            return evidence
        except Exception:
            return evidence

    def _run_exec_file_checks(self, pod_name: str, namespace: str) -> Dict:
        """Run lightweight in-pod checks for common runtime config/code paths."""
        checks = {
            'config_json': 'unknown',
            'application_yml': 'unknown',
            'application_properties': 'unknown',
            'app_dir': 'unknown'
        }
        findings = []

        probe_commands = {
            'app_dir': 'if [ -d /app ]; then echo present; else echo missing; fi',
            'config_json': 'if [ -f /app/config.json ]; then echo present; else echo missing; fi',
            'application_yml': 'if [ -f /app/application.yml ]; then echo present; else echo missing; fi',
            'application_properties': 'if [ -f /app/application.properties ]; then echo present; else echo missing; fi'
        }

        for key, command in probe_commands.items():
            try:
                cmd = ['kubectl', 'exec', pod_name, '-n', namespace, '--', 'sh', '-c', command]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=6)
                if result.returncode == 0:
                    value = (result.stdout or '').strip().lower()
                    checks[key] = 'present' if 'present' in value else 'missing'
                else:
                    checks[key] = 'error'
            except Exception:
                checks[key] = 'error'

        if checks.get('app_dir') == 'missing':
            findings.append('Expected /app directory is missing in container runtime.')
        if checks.get('config_json') == 'missing' and checks.get('application_yml') == 'missing' and checks.get('application_properties') == 'missing':
            findings.append('No common runtime config file found under /app (config.json/application.yml/application.properties).')

        return {
            'checks': checks,
            'findings': findings
        }

    def deep_inspect_service(self, service_name: str, namespace: Optional[str] = None, pod_status: Optional[Dict] = None) -> Dict:
        """Deep inspection for unhealthy services using describe/exec based on pod readiness."""
        namespace = namespace or self.namespace
        pod_status = pod_status or self.get_pod_status(service_name, namespace=namespace)
        pods = pod_status.get('pods', []) if isinstance(pod_status, dict) else []

        if not pods:
            return {
                'mode': 'skipped',
                'summary': 'No pods found for service; deep inspection skipped.'
            }

        # Prefer ready pod for exec checks
        ready_pod = None
        for pod in pods:
            if pod.get('ready'):
                ready_pod = pod
                break

        # If no ready pod, use describe-based inspection on first pod
        if ready_pod is None:
            target_pod = pods[0]
            pod_name = target_pod.get('name', 'unknown')
            reason = target_pod.get('reason', '')
            events = self._get_pod_event_summary(pod_name, namespace)
            resource = self._get_pod_resource_snapshot(pod_name, namespace)
            log_hint = self._get_pod_log_summary(pod_name, namespace, minutes=10, include_previous=True)
            summary = f"Pod {pod_name} not ready"
            if reason:
                summary += f": {reason}"
            if events:
                summary += f" | events: {events}"
            if resource:
                summary += f" | resources: {resource}"
            if log_hint:
                summary += f" | log: {log_hint}"
            return {
                'mode': 'describe',
                'pod': pod_name,
                'summary': summary
            }

        pod_name = ready_pod.get('name', 'unknown')
        file_checks = self._run_exec_file_checks(pod_name, namespace)
        findings = file_checks.get('findings', [])
        resource = self._get_pod_resource_snapshot(pod_name, namespace)
        summary = f"Exec inspection on pod {pod_name}"
        if findings:
            summary += f": {' | '.join(findings[:2])}"
        if resource:
            summary += f" | resources: {resource}"

        return {
            'mode': 'exec',
            'pod': pod_name,
            'summary': summary,
            'checks': file_checks.get('checks', {}),
            'findings': findings
        }

    def _get_pod_describe_excerpt(self, pod_name: str, namespace: str, max_lines: int = 12) -> str:
        """Return concise describe excerpt focused on failure evidence."""
        try:
            cmd = ['kubectl', 'describe', 'pod', pod_name, '-n', namespace]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            if result.returncode != 0 or not result.stdout:
                return ''

            interesting = []
            patterns = [
                r'(?i)reason:\s*',
                r'(?i)state:\s*',
                r'(?i)last state:\s*',
                r'(?i)exit code:\s*',
                r'(?i)oomkilled|crashloopbackoff|imagepullbackoff|errimagepull|failedscheduling|insufficient\s+(cpu|memory)',
                r'(?i)events:',
                r'(?i)warning\s+'
            ]
            for line in result.stdout.splitlines():
                text = line.strip()
                if not text:
                    continue
                if any(re.search(pat, text) for pat in patterns):
                    interesting.append(text)
                if len(interesting) >= max_lines:
                    break
            return ' | '.join(interesting)
        except Exception:
            return ''

    def run_troubleshooting(self, service_name: str, namespace: Optional[str] = None, progress_callback=None) -> Dict:
        """Run focused troubleshooting workflow and return structured diagnostics."""
        namespace = namespace or self.namespace

        def _progress(stage: str, percent: int, detail: str):
            if callable(progress_callback):
                try:
                    progress_callback(stage, int(percent), str(detail or ''))
                except Exception:
                    pass

        report = {
            'service': service_name,
            'namespace': namespace,
            'status': 'unknown',
            'summary': '',
            'pods': [],
            'root_causes': []
        }

        _progress('init', 5, 'Collecting pod status')
        pod_status = self.get_pod_status(service_name, namespace=namespace)
        report['status'] = str(pod_status.get('status', 'unknown') or 'unknown')
        pods = pod_status.get('pods', []) if isinstance(pod_status.get('pods', []), list) else []
        report['pod_counts'] = {
            'total': int(pod_status.get('total_pods', 0) or 0),
            'running': int(pod_status.get('running_pods', 0) or 0),
            'ready': int(pod_status.get('ready_pods', 0) or 0),
            'issue': int(pod_status.get('issue_pods', 0) or 0)
        }

        if not pods:
            report['summary'] = f'No pods found for {namespace}/{service_name}'
            report['root_causes'].append(report['summary'])
            _progress('done', 100, report['summary'])
            return report

        # Prioritize unstable pods first to improve root-cause quality.
        ordered_pods = sorted(
            pods,
            key=lambda pod: (
                0 if not bool(pod.get('ready', False)) else 1,
                -int(pod.get('restarts', 0) or 0)
            )
        )
        target_pods = ordered_pods[:3]
        per_pod_step = max(10, int(70 / max(1, len(target_pods))))
        current = 15

        for pod in target_pods:
            pod_name = str(pod.get('name', 'unknown') or 'unknown')
            _progress('pod', current, f'Analyzing pod {pod_name}')

            reason = str(pod.get('reason', '') or '').strip()
            event_summary = self._get_pod_event_summary(pod_name, namespace)
            describe_excerpt = self._get_pod_describe_excerpt(pod_name, namespace)
            resource = self._get_pod_resource_snapshot(pod_name, namespace)
            log_current = self._get_pod_log_summary(pod_name, namespace, minutes=10, include_previous=False)
            log_previous = self._get_pod_log_summary(pod_name, namespace, minutes=20, include_previous=True)
            log_evidence = self._get_pod_log_evidence(pod_name, namespace, minutes=20)

            if (not log_previous) and str(log_evidence.get('source', '')) == 'previous':
                log_previous = str(log_evidence.get('best_line', '') or '')
            if (not log_current) and str(log_evidence.get('source', '')) == 'current':
                log_current = str(log_evidence.get('best_line', '') or '')

            pod_report = {
                'name': pod_name,
                'status': str(pod.get('status', '') or ''),
                'ready': bool(pod.get('ready', False)),
                'restarts': int(pod.get('restarts', 0) or 0),
                'reason': reason,
                'event_summary': event_summary,
                'describe_excerpt': describe_excerpt,
                'resource': resource,
                'log_current': log_current,
                'log_previous': log_previous,
                'log_evidence_line': str(log_evidence.get('best_line', '') or ''),
                'log_evidence_source': str(log_evidence.get('source', '') or ''),
                'log_excerpt': log_evidence.get('excerpt', []) if isinstance(log_evidence.get('excerpt', []), list) else [],
                'error_lines': log_evidence.get('error_lines', []) if isinstance(log_evidence.get('error_lines', []), list) else [],
                'restart_cause': str(pod.get('restart_cause', '') or ''),
                'restart_exit_code': str(pod.get('restart_exit_code', '') or ''),
                'restart_finished_at': str(pod.get('restart_finished_at', '') or '')
            }
            report['pods'].append(pod_report)

            for candidate in [
                pod_report['log_evidence_line'],
                (pod_report['error_lines'][0] if pod_report['error_lines'] else ''),
                log_previous,
                log_current,
                describe_excerpt,
                event_summary,
                reason,
                pod_report['restart_cause']
            ]:
                text = str(candidate or '').strip()
                if not text:
                    continue
                if text not in report['root_causes']:
                    report['root_causes'].append(text)

            current = min(90, current + per_pod_step)

        if report['root_causes']:
            report['summary'] = report['root_causes'][0]
        else:
            report['summary'] = f'No explicit root cause extracted for {namespace}/{service_name}'

        _progress('done', 100, 'Troubleshooting completed')
        return report

    def discover_services(self, namespaces: Optional[List[str]] = None) -> List[Dict]:
        """Discover monitorable targets from cluster namespaces."""
        if not self.kubectl_working:
            return []

        target_namespaces = namespaces or self.discovery_namespaces or [self.namespace]
        now = datetime.now()
        monitoring_cfg = (self.runtime_config or {}).get('monitoring', {}) if isinstance(self.runtime_config, dict) else {}
        discovery_cache_seconds = int(monitoring_cfg.get('discovery_cache_seconds', 90) or 90)
        discovery_cache_seconds = max(15, min(discovery_cache_seconds, 600))
        if (
            self._discover_services_cache_ts is not None and
            isinstance(self._discover_services_cache, list) and
            self._discover_services_cache and
            (now - self._discover_services_cache_ts).total_seconds() < discovery_cache_seconds
        ):
            return list(self._discover_services_cache)

        discovered_map = {}
        include_k8s_services = bool(monitoring_cfg.get('include_k8s_service_objects', False))

        for namespace in target_namespaces:
            try:
                resources = [
                    ('deployments', 'deployment'),
                    ('statefulsets', 'statefulset'),
                    ('daemonsets', 'daemonset')
                ]
                if include_k8s_services:
                    resources.append(('services', 'service'))

                for resource_type, source in resources:
                    cmd = ['kubectl', 'get', resource_type, '-n', namespace, '-o', 'json']
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                    if result.returncode != 0:
                        continue

                    data = json.loads(result.stdout)
                    for item in data.get('items', []):
                        name = item.get('metadata', {}).get('name', '')
                        if not name or name in {'kubernetes'}:
                            continue

                        key = f"{namespace}/{name}"
                        if key not in discovered_map:
                            discovered_map[key] = {
                                'name': name,
                                'namespace': namespace,
                                'source': source
                            }
            except Exception as e:
                logger.error(f"Error discovering services in {namespace}: {e}")

        discovered = list(discovered_map.values())
        discovered.sort(key=lambda s: (s.get('namespace', ''), s.get('name', '')))
        if discovered:
            self._discover_services_cache = list(discovered)
            self._discover_services_cache_ts = now
            return discovered

        # Keep last known-good discovered services on transient kubectl failures.
        if isinstance(self._discover_services_cache, list) and self._discover_services_cache:
            return list(self._discover_services_cache)

        return discovered
    
    def get_all_services_status(self, minutes: int = 5) -> Dict:
        """
        Get status for all monitored services with timeout protection
        """
        try:
            # Reload from runtime config first (falls back to file if unavailable)
            if self.runtime_config is not None:
                self._load_services_from_config(config_override=self.runtime_config)
            else:
                self._load_services_from_config()
            logger.info(f"Getting status for selected services across namespaces: {self.discovery_namespaces}")
        
            # Only process selected services (max capped)
            services = self.selected_services[:self.max_selected_services]
            logger.info(f"Monitoring selected services: {services}")
            
            service_status = {}
            
            # For each configured service, get actual metrics and errors
            for item in services:
                service_name = str(item.get('name') or '')
                namespace = str(item.get('namespace', self.namespace) or self.namespace)
                if not service_name:
                    continue
                service_key = f"{namespace}/{service_name}"
                try:
                    # Get metrics from logs with individual timeout
                    metrics = self.get_service_metrics_from_logs(service_name, minutes, namespace=namespace)
                    
                    # Get recent errors
                    errors = self.detect_errors_in_logs(service_name, minutes, namespace=namespace)
                    
                    # Get pod status
                    pod_status = self.get_pod_status(service_name, namespace=namespace)

                    # If container never starts (e.g., ImagePullBackOff), deployment logs may be empty.
                    # In that case, synthesize recent errors from pod status reasons so dashboard shows exact issue.
                    if not errors and pod_status.get('pods'):
                        synthetic_errors = []
                        for pod in pod_status.get('pods', []):
                            reason = (pod.get('reason') or '').strip()
                            if reason:
                                severity = 'POD_ERROR'
                                reason_lower = reason.lower()
                                if 'imagepull' in reason_lower or 'pulling image' in reason_lower:
                                    severity = 'IMAGE_PULL'
                                elif 'crashloopbackoff' in reason_lower:
                                    severity = 'CRASH_LOOP'
                                synthetic_errors.append({
                                    'timestamp': datetime.now().isoformat(),
                                    'service': service_name,
                                    'severity': severity,
                                    'message': f"Pod {pod.get('name', 'unknown')}: {reason}",
                                    'pattern_matched': 'pod_status_reason'
                                })

                        if synthetic_errors:
                            errors = synthetic_errors
                    
                    # Determine service status based on error rate and pod status
                    error_rate = metrics.get('error_rate', 0)
                    pod_health = pod_status.get('status', 'unknown')
                    
                    # Prioritize pod status over log-based status
                    if pod_health == 'unhealthy':
                        status = 'offline'  # Critical pod issues
                    elif pod_health == 'no_pods':
                        status = 'offline'  # No pods running
                    elif error_rate > 5.0:
                        status = 'degraded'
                    elif error_rate > 0:
                        status = 'warning'
                    else:
                        status = 'healthy'
                    
                    service_status[service_key] = {
                        'name': service_name,
                        'metrics': metrics,
                        'recent_errors': errors[:5],  # Limit to last 5 errors
                        'status': status,
                        'pod_status': pod_status,
                        'namespace': namespace
                    }
                except Exception as e:
                    logger.error(f"Error processing service {service_name}: {e}")
                    # Continue with other services even if one fails
                    service_status[service_key] = {
                        'name': service_name,
                        'metrics': {
                            'total_log_entries': 0,
                            'error_count': 0,
                            'warning_count': 0,
                            'error_rate': 0.0,
                            'timestamp': datetime.now().isoformat()
                        },
                        'recent_errors': [],
                        'status': 'unknown',
                        'pod_status': {'status': 'unknown', 'pods': []},
                        'namespace': namespace
                    }
                    
            return service_status
            
        except Exception as e:
            logger.error(f"Error getting service status: {e}")
            # Return empty status to prevent hanging
            return {}
        
    def _timeout_handler(self, signum, frame):
        raise TimeoutError("Service status collection timed out")
        
    def _get_services_from_cluster(self) -> List[str]:
        """
        Get list of actual services from the Kubernetes cluster
        """
        if not self.kubectl_working:
            logger.warning("kubectl is not working, returning configured services only")
            return list(self.service_patterns.keys())
            
        try:
            # Get services from the cluster
            cmd = ['kubectl', 'get', 'services', '-n', self.namespace, '-o', 'json']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode != 0:
                logger.error(f"Failed to get services: {result.stderr}")
                return list(self.service_patterns.keys())
                
            services_data = json.loads(result.stdout)
            service_names = []
            
            for item in services_data.get('items', []):
                service_name = item.get('metadata', {}).get('name', '')
                if service_name:
                    service_names.append(service_name)
                    
            logger.info(f"Discovered services: {service_names}")
            return service_names
            
        except Exception as e:
            logger.error(f"Error getting services from cluster: {e}")
            return list(self.service_patterns.keys())

# Global instance
service_monitor = None

def get_service_monitor():
    """Get the global service monitor instance"""
    global service_monitor
    if service_monitor is None:
        service_monitor = KubernetesServiceMonitor()
    return service_monitor
