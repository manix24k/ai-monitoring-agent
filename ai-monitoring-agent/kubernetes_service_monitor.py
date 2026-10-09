#!/usr/bin/env python3
"""
Kubernetes Service Monitor for AI Monitoring Agent
Monitors services directly by reading their logs
"""
import subprocess
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import signal

logger = logging.getLogger(__name__)

class KubernetesServiceMonitor:
    def __init__(self, namespace: str = "mercury"):
        self.namespace = namespace
        self.runtime_config = None
        self.discovery_namespaces = [namespace]
        self.max_selected_services = 25
        self.selected_services = []
        self.ignored_log_patterns = []
        self._namespace_pods_cache = {}
        self._namespace_pods_cache_ts = {}
        self._namespace_pods_backoff_until = {}
        self._namespace_pods_last_error = {}
        self._pod_event_cache = {}
        self._pod_event_cache_ts = {}
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

    def _get_pods_cache_ttl(self) -> int:
        cfg = self.runtime_config or {}
        monitoring_cfg = cfg.get('monitoring', {}) if isinstance(cfg, dict) else {}
        value = int(monitoring_cfg.get('pods_cache_seconds', 15) or 15)
        return max(5, min(value, 120))

    def _get_namespace_pods(self, namespace: str) -> List[Dict]:
        """Return namespace pods with short TTL cache to avoid kubectl storm."""
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

        # Hyphen-insensitive exact workload-name match only.
        # Avoid prefix matches that can cross-link distinct services
        # (e.g., tripmanagement vs trip-management-search).
        pod_compact = pod.replace('-', '')
        cand_compact = cand.replace('-', '')
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
            configured_namespaces = monitoring_cfg.get('discovery_namespaces', ['earth', 'mercury', 'mars'])
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
            # Fallback to hardcoded services
            self.selected_services = [
                {'name': 'cacheservice', 'namespace': self.namespace},
                {'name': 'bus-aggregation', 'namespace': self.namespace},
                {'name': 'channel-manager-su', 'namespace': self.namespace},
                {'name': 'otaconsumer', 'namespace': self.namespace},
                {'name': 'travelport', 'namespace': self.namespace}
            ]
            self.ignored_log_patterns = [
                r'(?i)io\.opentelemetry\.exporter\.internal\.http\.HttpExporter\s*-\s*Failed to export spans',
                r'(?i)otel\.javaagent.*failed to export spans',
                r'(?i)fabhotels-signoz-prod-otel-collector.*\.svc\.cluster\.local(?::4318)?',
                r'(?i)org\.apache\.catalina\.valves\.ErrorReportValve\.invoke\(ErrorReportValve\.java:92\)'
            ]
            self.service_patterns = {
                'cacheservice': r'cacheservice',
                'bus-aggregation': r'bus-aggregation',
                'channel-manager-su': r'channel-manager-su',
                'otaconsumer': r'otaconsumer',
                'travelport': r'travelport'
            }
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
                
                pod_info = {
                    'name': pod_name,
                    'status': phase,
                    'ready': ready,
                    'restarts': sum(cs.get('restartCount', 0) for cs in container_statuses),
                    'reason': reason,
                    'restart_cause': restart_cause,
                    'restart_exit_code': restart_exit_code,
                    'restart_finished_at': restart_finished_at
                }
                pods.append(pod_info)
                
                # Update overall readiness
                if not ready:
                    all_ready = False
            
            # Determine overall status
            if not pods:
                # Fallback: if service endpoints are ready, treat as healthy even if pod name match failed
                if self._has_ready_endpoints(service_name, namespace):
                    status = 'healthy'
                else:
                    status = 'no_pods'
            elif all_ready:
                status = 'healthy'
            else:
                status = 'unhealthy'
                
            return {
                'status': status,
                'pods': pods,
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
            summary = f"Pod {pod_name} not ready"
            if reason:
                summary += f": {reason}"
            if events:
                summary += f" | events: {events}"
            return {
                'mode': 'describe',
                'pod': pod_name,
                'summary': summary
            }

        pod_name = ready_pod.get('name', 'unknown')
        file_checks = self._run_exec_file_checks(pod_name, namespace)
        findings = file_checks.get('findings', [])
        summary = f"Exec inspection on pod {pod_name}"
        if findings:
            summary += f": {' | '.join(findings[:2])}"

        return {
            'mode': 'exec',
            'pod': pod_name,
            'summary': summary,
            'checks': file_checks.get('checks', {}),
            'findings': findings
        }

    def discover_services(self, namespaces: Optional[List[str]] = None) -> List[Dict]:
        """Discover monitorable targets from cluster namespaces."""
        if not self.kubectl_working:
            return []

        target_namespaces = namespaces or self.discovery_namespaces or [self.namespace]
        discovered_map = {}

        for namespace in target_namespaces:
            try:
                resources = [
                    ('deployments', 'deployment'),
                    ('statefulsets', 'statefulset'),
                    ('services', 'service')
                ]

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
