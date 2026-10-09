#!/usr/bin/env python3
"""
AI Monitoring Agent for Elasticsearch and Prometheus
"""
import time
import logging
import json
import os
import re
import hashlib
import gzip
import shutil
import subprocess
from urllib.parse import urlparse
from urllib.parse import quote
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict, List, Optional, Tuple, Sequence
from prometheus_client import PrometheusClient
from elasticsearch_client import ElasticsearchClient
from anomaly_detector import AnomalyDetector
from root_cause_analyzer import RootCauseAnalyzer
from slack_notifier import SlackNotifier
from learning_engine import LearningEngine
from kubernetes_service_monitor import KubernetesServiceMonitor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIMonitoringAgent:
    def __init__(self, config_path="config.json"):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Ensure required optional sections exist at runtime.
        self._ensure_default_config_sections()
            
        # Initialize clients
        self.prometheus = PrometheusClient(self.config['prometheus']['url'])
        
        # Initialize Elasticsearch only if hosts are configured
        es_hosts = self.config['elasticsearch'].get('hosts', [])
        if es_hosts and len(es_hosts) > 0 and es_hosts[0]:  # Check if hosts are actually configured
            self.elasticsearch = ElasticsearchClient(
                hosts=self.config['elasticsearch']['hosts'],
                index_prefix=self.config['elasticsearch'].get('index_prefix', 'logs-*'),
                username=self.config['elasticsearch'].get('username'),
                password=self.config['elasticsearch'].get('password')
            )
        else:
            self.elasticsearch = None
            logger.info("Elasticsearch disabled - no hosts configured")
            
        self.service_monitor = None
        try:
            monitor_ns = self.config.get('monitoring', {}).get('discovery_namespaces', ['mercury'])
            default_ns = monitor_ns[0] if isinstance(monitor_ns, list) and monitor_ns else 'mercury'
            self.service_monitor = KubernetesServiceMonitor(namespace=default_ns)
            self.service_monitor.apply_runtime_config(self.config)
        except Exception as e:
            logger.warning(f"Kubernetes service monitor initialization failed: {e}")
            self.service_monitor = None
        self.anomaly_detector = AnomalyDetector()
        self.root_cause_analyzer = RootCauseAnalyzer()
        self.slack_notifier = SlackNotifier(self.config['slack']['webhook_url'])
        learning_cfg = self.config.get('learning', {})
        self.learning_engine = LearningEngine(
            model_dir=learning_cfg.get('model_dir', './models'),
            history_limit=learning_cfg.get('history_limit', 500),
            feedback_limit=learning_cfg.get('feedback_limit', learning_cfg.get('history_limit', 500)),
            knowledge_limit=learning_cfg.get('knowledge_limit', 1000)
        )
        
        # Metrics history for adaptive thresholds
        self.metrics_history = []
        self._selected_services_cache = []
        self._selected_services_cache_ts = None
        
        # Incident tracking for dashboard
        self.active_incidents = []
        self.resolved_incidents = []
        self.resolution_memory = {}
        self._service_rca_memory = {}
        self._service_deterministic_issue_memory = {}
        self._service_unresolved_counter = {}
        self._service_pod_presence = {}
        self._pr_attempted_signatures = set()
        self._pr_created_signatures = set()
        self._deterministic_recovery_last_fetch = {}
        self._last_service_status = {}
        self.agent_status = {
            'running': False,
            'last_check': None,
            'uptime': 0,
            'start_time': None
        }
        self.last_config_update_note = ""
        self.dependency_map = self._load_dependency_map()
        self.dynamic_dependency_graph = {}
        self._otel_trace_file_offset = 0
        self._recent_traces = []
        self._trace_retention_last_run = None
        self._trace_rotation_last_run = None

    def _ensure_default_config_sections(self):
        """Populate missing optional config blocks with safe defaults."""
        if not isinstance(self.config, dict):
            self.config = {}

        monitored = self.config.get('monitored_services')
        if not isinstance(monitored, list):
            monitored = []
        normalized_monitored = []
        seen_monitored = set()
        for item in monitored:
            if not isinstance(item, dict):
                continue
            name = str(item.get('name', '') or '').strip()
            namespace = str(item.get('namespace', 'unknown') or 'unknown').strip()
            if not name:
                continue
            key = (namespace, name)
            if key in seen_monitored:
                continue
            seen_monitored.add(key)
            normalized_monitored.append({'name': name, 'namespace': namespace})
        self.config['monitored_services'] = normalized_monitored

        service_profiles = self.config.get('services')
        if not isinstance(service_profiles, list):
            service_profiles = []

        # Single-source simplification: if monitored_services exists, auto-generate
        # services profiles so operators only need to maintain one list.
        if normalized_monitored:
            generated = []
            seen_service_names = set()
            for item in normalized_monitored:
                svc_name = str(item.get('name', '') or '').strip()
                if not svc_name:
                    continue
                if svc_name in seen_service_names:
                    continue
                seen_service_names.add(svc_name)
                generated.append({'name': svc_name, 'priority': 'high', 'sampling': 1.0})
            self.config['services'] = generated
        else:
            # Backward compatibility when only legacy `services` is configured.
            generated_monitored = []
            seen_names = set()
            for item in service_profiles:
                if not isinstance(item, dict):
                    continue
                svc_name = str(item.get('name', '') or '').strip()
                if not svc_name or svc_name in seen_names:
                    continue
                seen_names.add(svc_name)
                generated_monitored.append({'name': svc_name, 'namespace': 'unknown'})
            self.config['services'] = service_profiles
            if generated_monitored:
                self.config['monitored_services'] = generated_monitored

        pr = self.config.get('pr_automation')
        if not isinstance(pr, dict):
            pr = {}
        pr.setdefault('enabled', True)
        pr.setdefault('auto_create_pr', False)
        pr.setdefault('repo_base_url', 'https://github.com/fabhotelstech')
        pr.setdefault('target_branch', 'develop_mercury')
        pr.setdefault('default_base_branch', 'dev')
        pr.setdefault('repo_overrides', {})
        pr.setdefault('fix_mode', 'report')
        pr.setdefault('ignore_issue_patterns', [])
        pr.setdefault('eligibility_mode', 'strict')
        pr.setdefault('allow_analysis_report_pr', True)
        self.config['pr_automation'] = pr

    def _load_dependency_map(self):
        """Load static service dependency map."""
        path = os.path.join(os.path.dirname(__file__), 'service_dependencies.json')
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                return {
                    'aliases': data.get('aliases', {}),
                    'dependencies': data.get('dependencies', {})
                }
        except Exception as e:
            logger.warning(f"Could not load service dependency map: {e}")
            return {'aliases': {}, 'dependencies': {}}

    def _canonical_service(self, service_name: str) -> str:
        """Normalize service name via alias map."""
        if not service_name:
            return ''
        aliases = self.dependency_map.get('aliases', {})
        if service_name in aliases:
            return aliases[service_name]

        lowered = str(service_name).lower()
        for alias, canonical in aliases.items():
            if str(alias).lower() == lowered:
                return canonical

        return service_name

    def _normalize_service_token(self, token: str) -> str:
        """Normalize service token from traces/logs to canonical service name."""
        if not token:
            return ''

        value = str(token).strip().strip('"\'')
        if not value:
            return ''

        # URL-like values
        if '://' in value:
            try:
                parsed = urlparse(value)
                if parsed.netloc:
                    value = parsed.netloc
                elif parsed.path:
                    value = parsed.path
            except Exception:
                pass

        # remove path/query
        value = value.split('?', 1)[0]
        value = value.split('/', 1)[0]

        # remove port
        value = value.split(':', 1)[0]

        # k8s DNS names -> service name
        if '.svc.cluster.local' in value:
            value = value.split('.svc.cluster.local', 1)[0]
        elif '.svc' in value:
            value = value.split('.svc', 1)[0]

        # Keep IPv4 as-is for later aliasing/validation; otherwise collapse FQDN
        is_ipv4 = bool(re.match(r'^\d{1,3}(?:\.\d{1,3}){3}$', value))
        if '.' in value and not value.endswith('-service') and not is_ipv4:
            value = value.split('.', 1)[0]

        value = value.strip().strip('.')
        if not value:
            return ''

        ignored = {
            'http', 'https', 'tcp', 'udp', 'localhost', 'unknown', 'null', 'none',
            'true', 'false', 'grpc', 'redis', 'mysql', 'postgres', 'mongodb', 'kafka',
            'earth', 'mercury', 'mars'
        }
        if value.lower() in ignored:
            return ''

        # Drop raw IPv4 unless explicitly mapped through alias map
        if is_ipv4:
            aliased = self._canonical_service(value)
            if aliased == value:
                return ''
            return aliased

        # Drop likely class/jvm/framework tokens that are not service dependencies
        lower_value = value.lower()
        if any(marker in lower_value for marker in [
            'beanpostprocessor', 'constructorresolver', 'annotationbeanpostprocessor',
            'abstractplainsocketimpl', 'nativeio', 'applicationcontext', 'factorybean'
        ]):
            return ''
        if re.search(r'(?i)(?:exception|error|resolver|processor|configuration)\Z', value):
            return ''

        return self._canonical_service(value)

    def _extract_otel_attributes(self, trace: Dict) -> Dict[str, str]:
        """Extract OTel attributes from heterogeneous trace documents."""
        attributes = {}
        raw_attrs = trace.get('attributes', {}) if isinstance(trace, dict) else {}

        if isinstance(raw_attrs, dict):
            for key, value in raw_attrs.items():
                attributes[str(key)] = str(value)
            return attributes

        if isinstance(raw_attrs, list):
            for item in raw_attrs:
                if not isinstance(item, dict):
                    continue
                key = item.get('key')
                value_obj = item.get('value')
                if not key:
                    continue

                value = ''
                if isinstance(value_obj, dict):
                    for typed_key in ('stringValue', 'intValue', 'doubleValue', 'boolValue'):
                        if typed_key in value_obj:
                            value = str(value_obj.get(typed_key))
                            break
                elif value_obj is not None:
                    value = str(value_obj)

                attributes[str(key)] = value

        return attributes

    def _extract_otel_spans_from_record(self, record: Dict) -> List[Dict]:
        """Flatten OTel file-exporter records into span dictionaries."""
        spans = []
        if not isinstance(record, dict):
            return spans

        resource_spans = record.get('resourceSpans', [])
        if not isinstance(resource_spans, list):
            return spans

        for rspan in resource_spans:
            if not isinstance(rspan, dict):
                continue

            resource_attrs = rspan.get('resource', {}).get('attributes', [])
            service_name = ''
            if isinstance(resource_attrs, list):
                for attr in resource_attrs:
                    if not isinstance(attr, dict):
                        continue
                    if attr.get('key') == 'service.name':
                        value_obj = attr.get('value', {})
                        if isinstance(value_obj, dict):
                            service_name = str(value_obj.get('stringValue', '') or '')
                        elif value_obj is not None:
                            service_name = str(value_obj)
                        break

            scope_spans = rspan.get('scopeSpans', [])
            if not isinstance(scope_spans, list):
                continue

            service_name = self._canonical_service(service_name)

            for sspan in scope_spans:
                if not isinstance(sspan, dict):
                    continue

                raw_spans = sspan.get('spans', [])
                if not isinstance(raw_spans, list):
                    continue

                for span in raw_spans:
                    if not isinstance(span, dict):
                        continue

                    start_ns = int(span.get('startTimeUnixNano', 0) or 0)
                    end_ns = int(span.get('endTimeUnixNano', 0) or 0)
                    duration_ms = float(max(0, end_ns - start_ns)) / 1_000_000 if end_ns and start_ns else 0.0

                    spans.append({
                        'trace_id': span.get('traceId', ''),
                        'span_id': span.get('spanId', ''),
                        'parent_span_id': span.get('parentSpanId', ''),
                        'name': span.get('name', ''),
                        'kind': span.get('kind', 0),
                        'service': service_name,
                        'status_code': (span.get('status') or {}).get('code', ''),
                        'status_message': (span.get('status') or {}).get('message', ''),
                        'duration_ms': duration_ms,
                        'attributes': span.get('attributes', []),
                        'events': span.get('events', []),
                        'start_ns': start_ns
                    })

        return spans

    def _collect_sidecar_archive_traces(self, window_minutes: int, max_records: int) -> List[Dict]:
        """Read recent rotated OTel trace archives for larger lookback windows."""
        monitoring_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        if max_records <= 0:
            return []

        trace_path = str(monitoring_cfg.get('otel_trace_file_path', '/var/otel/traces.json') or '/var/otel/traces.json')
        archive_dir = str(
            monitoring_cfg.get(
                'otel_trace_archive_dir',
                os.path.join(os.path.dirname(trace_path) or '/var/otel', 'archive')
            ) or os.path.join(os.path.dirname(trace_path) or '/var/otel', 'archive')
        )
        if not os.path.isdir(archive_dir):
            return []

        lookback_minutes = max(1, int(window_minutes or 1))
        cutoff = datetime.now() - timedelta(minutes=lookback_minutes)
        max_files = max(1, int(monitoring_cfg.get('otel_trace_archive_max_files', 36) or 36))
        grace = timedelta(hours=2)

        candidates = []
        try:
            for name in os.listdir(archive_dir):
                if not name.startswith('traces-'):
                    continue
                if not (name.endswith('.json.gz') or name.endswith('.json')):
                    continue
                path = os.path.join(archive_dir, name)
                try:
                    modified = datetime.fromtimestamp(os.path.getmtime(path))
                except Exception:
                    continue
                if modified < (cutoff - grace):
                    continue
                candidates.append((modified, path))
        except Exception:
            return []

        if not candidates:
            return []

        candidates.sort(key=lambda item: item[0], reverse=True)
        traces: List[Dict] = []

        for _, path in candidates[:max_files]:
            try:
                opener = gzip.open if path.endswith('.gz') else open
                with opener(path, 'rt', encoding='utf-8', errors='ignore') as trace_file:
                    for line in trace_file:
                        raw = line.strip()
                        if not raw:
                            continue
                        try:
                            record = json.loads(raw)
                        except Exception:
                            continue

                        spans = self._extract_otel_spans_from_record(record)
                        for span in spans:
                            span_ts = None
                            try:
                                start_ns = int(span.get('start_ns', 0) or 0)
                                if start_ns > 0:
                                    span_ts = datetime.fromtimestamp(start_ns / 1_000_000_000)
                            except Exception:
                                span_ts = None

                            if span_ts is None:
                                raw_ts = str(span.get('timestamp', '') or '').strip()
                                if raw_ts:
                                    try:
                                        parsed = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
                                        if parsed.tzinfo is not None:
                                            parsed = parsed.astimezone().replace(tzinfo=None)
                                        span_ts = parsed
                                    except Exception:
                                        span_ts = None

                            if span_ts is not None and span_ts < cutoff:
                                continue

                            edge = self._extract_trace_service_edge(span, fallback_source=span.get('service', ''))
                            if edge:
                                span['service'] = edge['from']
                                span['downstream_service'] = edge['to']

                            traces.append(span)
                            if len(traces) >= max_records:
                                return traces
            except Exception:
                continue

        return traces

    def _collect_sidecar_traces(self, window_minutes: Optional[int] = None) -> List[Dict]:
        """Collect new OTel trace spans from sidecar file exporter output."""
        monitoring_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        trace_path = str(monitoring_cfg.get('otel_trace_file_path', '/var/otel/traces.json') or '/var/otel/traces.json')
        max_records = int(monitoring_cfg.get('otel_trace_max_records_per_cycle', 500) or 500)

        if not os.path.exists(trace_path):
            return []

        traces = []
        try:
            file_size = os.path.getsize(trace_path)
            if file_size < self._otel_trace_file_offset:
                self._otel_trace_file_offset = 0

            with open(trace_path, 'r', encoding='utf-8', errors='ignore') as trace_file:
                trace_file.seek(self._otel_trace_file_offset)
                new_content = trace_file.read()
                self._otel_trace_file_offset = trace_file.tell()
        except Exception as e:
            logger.warning(f"Could not read OTel trace file {trace_path}: {e}")
            return []

        if new_content.strip():
            lines = [line.strip() for line in new_content.splitlines() if line.strip()]
            for line in lines:
                try:
                    record = json.loads(line)
                except Exception:
                    continue

                spans = self._extract_otel_spans_from_record(record)
                for span in spans:
                    edge = self._extract_trace_service_edge(span, fallback_source=span.get('service', ''))
                    if edge:
                        span['service'] = edge['from']
                        span['downstream_service'] = edge['to']

                    traces.append(span)
                    if len(traces) >= max_records:
                        return traces

        lookback = max(1, int(window_minutes or 0)) if window_minutes else 0
        if lookback > 60 and len(traces) < max_records:
            remaining = max_records - len(traces)
            traces.extend(self._collect_sidecar_archive_traces(lookback, remaining))

        return traces

    def _enforce_sidecar_trace_retention(self):
        """Rotate and retain sidecar trace files for bounded disk usage."""
        monitoring_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        if not bool(monitoring_cfg.get('use_sidecar_otel_traces', False)):
            return
        if not bool(monitoring_cfg.get('enable_otel_trace_retention', True)):
            return

        trace_path = str(monitoring_cfg.get('otel_trace_file_path', '/var/otel/traces.json') or '/var/otel/traces.json')
        retention_days = max(1, int(monitoring_cfg.get('otel_trace_retention_days', 2) or 2))
        retention_check_seconds = max(30, int(monitoring_cfg.get('otel_trace_retention_check_seconds', 300) or 300))
        rotate_seconds = max(60, int(monitoring_cfg.get('otel_trace_rotate_seconds', 3600) or 3600))
        rotate_mb = max(32, int(monitoring_cfg.get('otel_trace_rotate_mb', 512) or 512))
        archive_dir = str(
            monitoring_cfg.get(
                'otel_trace_archive_dir',
                os.path.join(os.path.dirname(trace_path) or '/var/otel', 'archive')
            ) or os.path.join(os.path.dirname(trace_path) or '/var/otel', 'archive')
        )

        now = datetime.now()
        if self._trace_retention_last_run is not None:
            elapsed = (now - self._trace_retention_last_run).total_seconds()
            if elapsed < retention_check_seconds:
                return
        self._trace_retention_last_run = now

        if not os.path.exists(trace_path):
            return

        os.makedirs(archive_dir, exist_ok=True)

        try:
            file_size = os.path.getsize(trace_path)
        except Exception:
            return

        # Avoid forced rotation on every process restart. When runtime state is fresh,
        # derive age from file mtime so existing traces are not immediately truncated.
        if self._trace_rotation_last_run is None:
            try:
                last_write = datetime.fromtimestamp(os.path.getmtime(trace_path))
                rotate_due_to_time = (now - last_write).total_seconds() >= rotate_seconds
            except Exception:
                rotate_due_to_time = False
        else:
            rotate_due_to_time = (now - self._trace_rotation_last_run).total_seconds() >= rotate_seconds
        rotate_due_to_size = file_size >= rotate_mb * 1024 * 1024

        if file_size > 0 and (rotate_due_to_time or rotate_due_to_size):
            stamp = now.strftime('%Y%m%d-%H%M%S')
            archive_json = os.path.join(archive_dir, f"traces-{stamp}.json")
            archive_gz = f"{archive_json}.gz"

            try:
                with open(trace_path, 'rb') as src, open(archive_json, 'wb') as dst:
                    shutil.copyfileobj(src, dst)

                with open(trace_path, 'w', encoding='utf-8'):
                    pass

                with open(archive_json, 'rb') as src, gzip.open(archive_gz, 'wb', compresslevel=5) as dst:
                    shutil.copyfileobj(src, dst)
                os.remove(archive_json)

                self._otel_trace_file_offset = 0
                self._trace_rotation_last_run = now
                logger.info(f"Rotated OTel traces to {archive_gz}")
            except Exception as e:
                logger.warning(f"Failed rotating OTel traces: {e}")

        cutoff = now - timedelta(days=retention_days)
        deleted = 0
        try:
            for name in os.listdir(archive_dir):
                if not name.startswith('traces-'):
                    continue
                if not (name.endswith('.json.gz') or name.endswith('.json')):
                    continue
                path = os.path.join(archive_dir, name)
                try:
                    modified = datetime.fromtimestamp(os.path.getmtime(path))
                except Exception:
                    continue
                if modified < cutoff:
                    try:
                        os.remove(path)
                        deleted += 1
                    except Exception:
                        continue
        except Exception as e:
            logger.warning(f"Failed pruning OTel trace archives: {e}")

        if deleted:
            logger.info(f"Pruned {deleted} OTel trace archive files older than {retention_days} days")

    def _remember_recent_traces(self, traces: List[Dict]):
        """Keep short in-memory trace window to avoid empty-cycle RCA blind spots."""
        if not isinstance(traces, list) or not traces:
            return
        now = datetime.now().isoformat()
        for trace in traces:
            if not isinstance(trace, dict):
                continue
            self._recent_traces.append({'ts': now, 'trace': trace})

        max_items = int(self.config.get('monitoring', {}).get('recent_trace_buffer_limit', 4000) or 4000)
        if len(self._recent_traces) > max_items:
            self._recent_traces = self._recent_traces[-max_items:]

    def _recent_traces_within(self, minutes: int) -> List[Dict]:
        """Return buffered traces inside lookback window."""
        if not self._recent_traces:
            return []
        cutoff = datetime.now() - timedelta(minutes=max(1, int(minutes or 1)))
        out = []
        kept = []
        for item in self._recent_traces:
            try:
                ts = datetime.fromisoformat(str(item.get('ts', '')).replace('Z', '+00:00'))
                if ts.tzinfo is not None:
                    ts = ts.astimezone().replace(tzinfo=None)
            except Exception:
                continue
            if ts >= cutoff:
                kept.append(item)
                trace = item.get('trace')
                if isinstance(trace, dict):
                    out.append(trace)
        self._recent_traces = kept
        return out

    def _extract_trace_service_edge(self, trace: Dict, fallback_source: str = '') -> Optional[Dict[str, str]]:
        """Extract source->target service call edge from one trace document."""
        if not isinstance(trace, dict):
            return None

        attrs = self._extract_otel_attributes(trace)

        source_candidates = [
            trace.get('service'),
            trace.get('serviceName'),
            trace.get('service.name'),
            trace.get('resource.service.name'),
            attrs.get('service.name'),
            attrs.get('otel.service.name'),
            fallback_source
        ]

        target_candidates = [
            trace.get('downstream_service'),
            trace.get('peer_service'),
            trace.get('peer.service'),
            attrs.get('peer.service'),
            attrs.get('rpc.service'),
            attrs.get('db.instance'),
            attrs.get('net.peer.name'),
            attrs.get('http.host'),
            attrs.get('server.address'),
            attrs.get('http.url'),
            attrs.get('url.full')
        ]

        # Extract host from span name/message when available
        span_text = str(trace.get('name', '') or trace.get('spanName', '') or trace.get('message', '') or '')
        host_match = re.search(r'https?://([a-zA-Z0-9_.-]+)', span_text)
        if host_match:
            target_candidates.append(host_match.group(1))

        # Parse patterns like "calling inventory-service" from trace text
        svc_match = re.search(r'(?i)(?:call|calling|to|from)\s+([a-zA-Z0-9-]+(?:-service)?)', span_text)
        if svc_match:
            target_candidates.append(svc_match.group(1))

        source = ''
        for candidate in source_candidates:
            source = self._normalize_service_token(candidate)
            if source:
                break

        target = ''
        for candidate in target_candidates:
            target = self._normalize_service_token(candidate)
            if target:
                break

        if not source or not target or source == target:
            return None

        return {'from': source, 'to': target}

    def _learn_dynamic_dependencies(self, logs_traces: Dict):
        """Continuously build service dependency map from traces and logs."""
        if not isinstance(logs_traces, dict):
            return

        now_iso = datetime.now().isoformat()

        for trace in logs_traces.get('traces', []):
            edge = self._extract_trace_service_edge(trace)
            if not edge:
                continue

            source = edge['from']
            target = edge['to']
            source_graph = self.dynamic_dependency_graph.setdefault(source, {})
            edge_entry = source_graph.setdefault(target, {'count': 0, 'last_seen': now_iso})
            edge_entry['count'] += 1
            edge_entry['last_seen'] = now_iso

        for log in logs_traces.get('logs', []):
            source = self._normalize_service_token(log.get('service', ''))
            dependency = self._normalize_service_token(log.get('dependency', ''))
            if not source or not dependency or source == dependency:
                continue

            source_graph = self.dynamic_dependency_graph.setdefault(source, {})
            edge_entry = source_graph.setdefault(dependency, {'count': 0, 'last_seen': now_iso})
            edge_entry['count'] += 1
            edge_entry['last_seen'] = now_iso

    def _trace_is_error(self, trace: Dict) -> bool:
        """Return True when span indicates an error response."""
        if not isinstance(trace, dict):
            return False

        if self._is_internal_telemetry_span(trace):
            return False

        status = trace.get('status_code')
        if isinstance(status, int) and status == 2:
            return True
        status_text = str(status or '').upper()
        if status_text in {'2', 'ERROR', 'STATUS_CODE_ERROR'}:
            return True

        attrs = self._extract_otel_attributes(trace)
        for key in ('http.status_code', 'http.response.status_code', 'rpc.grpc.status_code'):
            value = attrs.get(key)
            if value is None:
                continue
            try:
                code = int(str(value))
                if code >= 500:
                    return True
            except Exception:
                continue

        return False

    def _is_internal_telemetry_span(self, trace: Dict) -> bool:
        """Ignore exporter/self-observability spans as business-failure signals."""
        if not isinstance(trace, dict):
            return False

        attrs = self._extract_otel_attributes(trace)
        blob = ' '.join([
            str(trace.get('name', '') or ''),
            str(trace.get('status_message', '') or ''),
            str(attrs.get('url.full', '') or ''),
            str(attrs.get('http.url', '') or ''),
            str(attrs.get('server.address', '') or ''),
            str(attrs.get('net.peer.name', '') or ''),
            str(attrs.get('exception.message', '') or ''),
            str(attrs.get('error.message', '') or '')
        ]).lower()

        if 'localhost:9411' in blob or '/api/v2/spans' in blob or 'zipkin' in blob:
            return True
        if 'opentelemetry' in blob and ('failed to export' in blob or 'http exporter' in blob):
            return True
        if ':4318' in blob and ('failed to connect' in blob or 'connection refused' in blob):
            return True

        return False

    def _percentile(self, values: List[float], pct: float) -> float:
        """Compute percentile with linear interpolation."""
        if not values:
            return 0.0
        ordered = sorted([float(v) for v in values if v is not None])
        if not ordered:
            return 0.0
        if len(ordered) == 1:
            return ordered[0]

        rank = (len(ordered) - 1) * max(0.0, min(1.0, pct))
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        weight = rank - low
        return ordered[low] * (1.0 - weight) + ordered[high] * weight

    def _get_dynamic_dependencies(self, service_name: str) -> List[str]:
        """Return top dynamic dependencies learned from traces/logs."""
        canonical = self._canonical_service(service_name)
        if not canonical:
            return []

        per_service_limit = int(self.config.get('monitoring', {}).get('dynamic_dependency_max_per_service', 8) or 8)
        ttl_minutes = int(self.config.get('monitoring', {}).get('dynamic_dependency_ttl_minutes', 180) or 180)
        cutoff = datetime.now() - timedelta(minutes=ttl_minutes)

        targets = self.dynamic_dependency_graph.get(canonical, {})
        scored = []
        for target, data in targets.items():
            try:
                seen = datetime.fromisoformat(str(data.get('last_seen')))
            except Exception:
                continue
            if seen < cutoff:
                continue
            scored.append((target, int(data.get('count', 0))))

        scored.sort(key=lambda item: item[1], reverse=True)
        return [name for name, _ in scored[:per_service_limit]]

    def _evaluate_static_dependency_health(self, service_status: Dict):
        """Apply static + learned dependency map to infer dependency failures and impacted_by."""
        if not service_status:
            return service_status

        deps = self.dependency_map.get('dependencies', {})

        # Build status index with namespace awareness to avoid cross-namespace bleed.
        canonical_status = {}
        canonical_status_by_ns = {}
        for _, svc in service_status.items():
            name = svc.get('name', '')
            canonical = self._canonical_service(name)
            namespace = str(svc.get('namespace', 'unknown') or 'unknown')
            if not canonical:
                continue

            svc_status = str(svc.get('status', 'unknown') or 'unknown').lower()
            pod_state = str((svc.get('pod_status') or {}).get('status', 'unknown') or 'unknown').lower()

            # Pod signal is authoritative for final liveliness.
            if pod_state == 'healthy':
                svc_status = 'healthy'
            elif pod_state == 'no_pods':
                svc_status = 'pending'

            canonical_status[(namespace, canonical)] = svc_status
            canonical_status_by_ns.setdefault(namespace, {})[canonical] = svc_status

        for key, svc in service_status.items():
            name = svc.get('name', '')
            canonical = self._canonical_service(name)
            namespace = str(svc.get('namespace', 'unknown') or 'unknown')
            service_deps = deps.get(canonical, [])
            dynamic_deps = self._get_dynamic_dependencies(canonical)

            direct_failure_present = (
                bool(svc.get('recent_errors')) or
                str(svc.get('status', 'unknown')).lower() in {'degraded', 'down', 'pending', 'offline', 'unreachable'} or
                str((svc.get('pod_status') or {}).get('status', 'unknown')).lower() in {'unhealthy', 'no_pods'}
            )

            # Avoid noisy dependency attribution when service already has direct failures.
            if direct_failure_present:
                service_status[key] = svc
                continue

            all_deps = []
            for dep in list(service_deps) + list(dynamic_deps):
                if dep not in all_deps:
                    all_deps.append(dep)

            impacted_by = []
            for dep in all_deps:
                dep_canonical = self._canonical_service(dep)
                dep_status = canonical_status.get((namespace, dep_canonical))
                if dep_status is None:
                    dep_status = canonical_status_by_ns.get(namespace, {}).get(dep_canonical, 'unknown')
                if dep_status in {'down', 'offline', 'pending', 'degraded'}:
                    impacted_by.append({
                        'service': dep,
                        'status': dep_status,
                        'confidence': 'medium'
                    })

            if impacted_by:
                metrics = svc.get('metrics', {})
                metrics['dependency_failures'] = len(impacted_by)
                metrics['failed_dependencies'] = [item['service'] for item in impacted_by]
                svc['metrics'] = metrics
                svc['impacted_by'] = impacted_by

                if svc.get('status') == 'healthy':
                    svc['status'] = 'degraded'

                if not svc.get('recent_errors'):
                    svc['recent_errors'] = [{
                        'timestamp': datetime.now().isoformat(),
                        'message': f"Likely impacted by dependencies: {', '.join(metrics['failed_dependencies'])}",
                        'severity': 'ERROR'
                    }]

            service_status[key] = svc

        return service_status

    def _selected_services(self):
        """Return configured services or auto-select bounded services per namespace."""
        monitored = self.config.get('monitored_services', [])
        if isinstance(monitored, list) and monitored:
            cleaned = []
            seen = set()
            for item in monitored:
                if not isinstance(item, dict):
                    continue
                name = item.get('name')
                namespace = item.get('namespace', 'unknown')
                if name:
                    canonical_name = self._canonical_service(str(name).strip())
                    if not canonical_name:
                        continue
                    dedupe_key = (str(namespace), canonical_name)
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    cleaned.append({'name': canonical_name, 'namespace': namespace})
            if cleaned:
                self._selected_services_cache = cleaned
                self._selected_services_cache_ts = datetime.now()
                return cleaned

        per_namespace_cap = int(self.config.get('monitoring', {}).get('auto_select_per_namespace', 5) or 5)
        overall_cap = int(self.config.get('monitoring', {}).get('auto_select_total_limit', 15) or 15)

        cache_ttl_seconds = int(self.config.get('monitoring', {}).get('auto_select_cache_seconds', 120) or 120)
        if (
            self._selected_services_cache_ts is not None and
            self._selected_services_cache and
            (datetime.now() - self._selected_services_cache_ts).total_seconds() < cache_ttl_seconds
        ):
            return self._selected_services_cache

        configured_discovery_limit = int(self.config.get('all_services_discovery_limit', overall_cap) or overall_cap)
        all_limit = min(configured_discovery_limit, overall_cap)
        if self.elasticsearch is not None:
            allowed = list(self.config.get('monitoring', {}).get('discovery_namespaces', ['earth', 'mercury', 'mars']))
            fast_names = self.elasticsearch.discover_service_names_fast(limit=all_limit * 3)
            if fast_names:
                bounded = []
                namespace_cycle = allowed if allowed else ['unknown']
                ns_len = len(namespace_cycle)
                per_ns_counts = {ns: 0 for ns in namespace_cycle}
                seen = set()

                for svc_name in fast_names:
                    if len(bounded) >= overall_cap:
                        break

                    canonical_name = self._canonical_service(str(svc_name or '').strip())
                    if not canonical_name:
                        continue

                    chosen_ns = None
                    for idx in range(ns_len):
                        ns = namespace_cycle[idx]
                        dedupe_key = (ns, canonical_name)
                        if dedupe_key in seen:
                            continue
                        if per_ns_counts.get(ns, 0) < per_namespace_cap:
                            chosen_ns = ns
                            break

                    if chosen_ns is None:
                        break

                    per_ns_counts[chosen_ns] = per_ns_counts.get(chosen_ns, 0) + 1
                    seen.add((chosen_ns, canonical_name))
                    bounded.append({'name': canonical_name, 'namespace': chosen_ns})

                if bounded:
                    self._selected_services_cache = bounded
                    self._selected_services_cache_ts = datetime.now()
                    return bounded
        fallback = [
            {
                'name': self._canonical_service(str(svc.get('name', '')).strip()),
                'namespace': svc.get('namespace', 'unknown')
            }
            for svc in self.config.get('services', []) if svc.get('name')
        ]
        fallback = [svc for svc in fallback if svc.get('name')]
        if fallback:
            self._selected_services_cache = fallback[:overall_cap]
            self._selected_services_cache_ts = datetime.now()
            return self._selected_services_cache
        return []

    def _get_pod_health_signals(self, selected_services: List[Dict]) -> Dict[str, Dict]:
        """Fetch pod-level health and reasons via kubectl for selected services only."""
        signals = {}
        use_pod_health = bool(self.config.get('monitoring', {}).get('use_kubectl_pod_health', True))
        if not use_pod_health:
            return signals

        if not self.service_monitor or not selected_services:
            return signals

        for svc in selected_services:
            name = svc.get('name') if isinstance(svc, dict) else None
            namespace = svc.get('namespace', 'unknown') if isinstance(svc, dict) else 'unknown'
            if not name:
                continue
            key = f"{namespace}/{name}"
            try:
                pod_status = self.service_monitor.get_pod_status(name, namespace=namespace)
                signals[key] = pod_status if isinstance(pod_status, dict) else {'status': 'unknown', 'pods': []}
            except Exception:
                signals[key] = {'status': 'unknown', 'pods': []}
        return signals

    def _extract_dependency_from_message(self, message: str) -> str:
        message = str(message or '')
        # Prefer endpoint-like extraction for connection failures
        conn_host_patterns = [
            r'Connection refused:\s*([a-zA-Z0-9_.-]+)/(?:\d{1,3}(?:\.\d{1,3}){3}):\d+',
            r'Failed to connect to \[\d+\]\s*host\(s\):\s*([a-zA-Z0-9_.-]+)',
            r'connecting to\s+([a-zA-Z0-9_.-]+)',
            r'http://([a-zA-Z0-9_.-]+):\d+',
            r'https://([a-zA-Z0-9_.-]+):\d+'
        ]
        for pattern in conn_host_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if not match:
                continue
            host = str(match.group(1) or '').strip().lower()
            if host in {'localhost', '127.0.0.1'}:
                return ''
            normalized = self._normalize_service_token(host)
            if normalized:
                return normalized

        patterns = [
            r'to ([\w-]+)-service',
            r'([a-zA-Z0-9-]+)\.svc\.cluster\.local',
            r'([a-zA-Z0-9-]+)\.[a-zA-Z0-9.-]+:\d+',
            r'connecting to ([\w-]+)',
            r'([a-zA-Z0-9-]+):[0-9]+'
        ]
        ignored_tokens = {
            'java', 'org', 'com', 'io', 'net', 'http', 'https', 'bean', 'error',
            'exception', 'caused', 'failed', 'connect', 'connection', 'host', 'hosts',
            'method', 'native', 'socket', 'impl', 'plain', 'abstract'
        }
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                candidate = re.sub(r'[:\d]+$', '', match.group(1) or '').strip().lower()
                if not candidate:
                    continue
                if candidate in ignored_tokens:
                    continue
                if len(candidate) <= 2:
                    continue
                normalized = self._normalize_service_token(candidate)
                return normalized or candidate

        # Generic status-style hints: "X is unavailable/down/sealed"
        status_hint = re.search(r'(?i)\b([a-zA-Z0-9_.-]{3,})\s+is\s+(sealed|unavailable|unreachable|down)\b', message)
        if status_hint:
            normalized = self._normalize_service_token(status_hint.group(1))
            if normalized:
                return normalized

        return ""

    def _extract_dependencies_from_texts(self, *messages: str) -> List[str]:
        """Extract unique dependency tokens from arbitrary error texts."""
        deps = []
        for msg in messages:
            dep = self._extract_dependency_from_message(str(msg or ''))
            if dep and dep not in deps:
                deps.append(dep)
        return deps

    def _is_internal_otel_exporter_noise(self, message: str) -> bool:
        """True when message is telemetry-exporter transport noise, not business failure."""
        text = str(message or '')
        if not text:
            return False
        if not re.search(r'(?i)opentelemetry|http exporter|io\.opentelemetry\.exporter\.internal\.http\.httpexporter', text):
            return False
        if re.search(r'(?i)failed\s+to\s+export\s+(logs|metrics|spans)|failed\s+to\s+connect\s+to', text):
            return True
        return False

    def _matches_ignored_log_pattern(self, message: str) -> bool:
        """True when message matches configured ignored/noise regexes."""
        text = str(message or '')
        if not text:
            return False

        builtin_noise_patterns = [
            r'(?i)no\s+record\s+found\s+for\s+selection\s+of\s+trigger',
            r'(?i)qrtz_(cron|simple|blob|simprop)_triggers',
            r'(?i)sendposbooking(checkin|checkout)taskjob',
            r'(?i)default\.sendposbooking(checkin|checkout)taskjob',
            r'(?i)localdatasourcejobstore.*couldn\'t\s+retrieve\s+trigger',
            r'(?i)illegalstateexception.*no\s+record\s+found\s+for\s+selection\s+of\s+trigger',
            r'(?i)springbootquartzapp.*misfirehandler',
            r'(?i)org\.quartz\.jobpersistenceexception',
        ]
        for pattern in builtin_noise_patterns:
            if re.search(pattern, text):
                return True

        ignored = self.config.get('ignored_log_patterns', [])
        for pattern in ignored:
            try:
                if re.search(str(pattern), text, re.IGNORECASE):
                    return True
            except re.error:
                continue
        return False

    def _sanitize_root_cause_message(self, message: str) -> str:
        """Strip noisy stacktrace frames and tooling noise from RCA surface."""
        text = str(message or '')
        if not text:
            return ''

        if self._matches_ignored_log_pattern(text):
            return ''

        if self._is_internal_otel_exporter_noise(text):
            return ''

        if re.search(r'(?i)\bexec\s+inspection\s+on\s+pod\b', text):
            return ''

        text = re.sub(r'(?i)\bstack\s+trace\s*:\s*[a-f0-9]{8,}\b.*$', '', text)
        text = re.sub(r'\bat\s+[a-zA-Z0-9_.$]+\([^\)]*\)', '', text)
        text = re.sub(r'\b(?:\w+\.)+\w+(?:Exception|Error)\b(?=\s+at\s+)', '', text)
        text = re.sub(r'\s+', ' ', text).strip(' |')

        # Drop pure transport tracer noise
        if re.search(r'(?i)opentelemetry.*failed\s+to\s+export', text):
            return ''

        return text[:1200]

    def _is_actionable_error_message(self, message: str) -> bool:
        """Return True only for actionable error-like messages."""
        if not message:
            return False

        if self._matches_ignored_log_pattern(message):
            return False

        if self._is_internal_otel_exporter_noise(message):
            return False

        exception_regex_default = r'\b([a-zA-Z0-9_.$]+(?:Exception|Error))\b'
        exception_regex = str(
            self.config.get('monitoring', {}).get('exception_signature_regex', exception_regex_default)
            or exception_regex_default
        )

        # Never miss concrete runtime exception signatures.
        try:
            if re.search(exception_regex, str(message), re.IGNORECASE):
                return True
        except re.error:
            if re.search(exception_regex_default, str(message), re.IGNORECASE):
                return True

        # Fallback for plain-text exception/error tokens in any case style.
        if re.search(r'\b(exception|error)\b', str(message), re.IGNORECASE):
            return True

        return bool(re.search(
            r'(?i)\b(connection\s+refused|timeout|timed\s*out|deadline\s+exceeded|http\s*5\d\d|status\s*5\d\d|imagepullbackoff|errimagepull|crashloopbackoff|oomkilled|out\s*of\s*memory|unreachable|refused\s+stream|reset\s+by\s+peer|failed\s+to\s+connect|could\s+not\s+resolve\s+placeholder|failed\s+to\s+bind\s+properties|configurationpropertiesbindexception|bindexception|unsatisfieddependencyexception|beancreationexception|context\s+initialization\s*-\s*cancelling\s+refresh\s+attempt|unable\s+to\s+start\s+reactive\s+web\s+server|invaliddataaccessapiusageexception|illegalargumentexception|unknown\s+name\s+value\s*\[[^\]]+\]\s*for\s*enum|(?:invalid|unknown|unsupported|unexpected|illegal)[^\n]{0,120}?value\s+[\'\"][^\'\"]+[\'\"][^\n]{0,120}?for\s+[a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+|[a-zA-Z0-9_.$]+(?:Exception|Error))\b',
            str(message)
        ))

    def _derive_exact_from_message(self, service: str, message: str, crashloop_present: bool = False) -> Dict[str, str]:
        """Derive exact deterministic RCA from a single log/error message."""
        out = {'issue': '', 'recommendation': ''}
        msg = str(message or '')
        if not msg:
            return out

        enum_match = re.search(r'(?i)unknown\s+name\s+value\s*\[([^\]]+)\]\s*for\s*enum\s+class\s*\[([^\]]+)\]', msg)
        if enum_match:
            enum_value = enum_match.group(1)
            enum_class = enum_match.group(2)
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = (
                f"Exact Issue: {service} request failed due to invalid enum value '{enum_value}' for {enum_class}{chain_tail}"
            )
            out['recommendation'] = (
                f"Validate incoming enum values against {enum_class} and add request validation/fallback before repository query"
            )
            return out

        if re.search(r'(?i)invaliddataaccessapiusageexception|illegalargumentexception', msg):
            enum_class_match = re.search(r'(?i)enum\s+class\s*\[([^\]]+)\]', msg)
            enum_class = enum_class_match.group(1) if enum_class_match else 'enum field'
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = (
                f"Exact Issue: {service} data access failed because request carried invalid enum for {enum_class}{chain_tail}"
            )
            out['recommendation'] = (
                "Add strict request validation and sanitize invalid enum inputs before hitting repository layer"
            )
            return out

        placeholder_match = re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', msg)
        if placeholder_match:
            placeholder = placeholder_match.group(1)
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            has_upstream_503 = bool(re.search(r'(?i)\b503\b|service\s+unavailable', msg))
            if has_upstream_503:
                out['issue'] = (
                    f"Exact Issue: {service} chain: upstream 503 -> missing config '{placeholder}' -> bean init failure{chain_tail}"
                )
                out['recommendation'] = (
                    f"Restore upstream dependency availability and ensure config key '{placeholder}' is available before startup"
                )
            else:
                out['issue'] = f"Exact Issue: {service} startup failed due to missing config '{placeholder}'{chain_tail}"
                out['recommendation'] = f"Provide config/secret key '{placeholder}' and verify environment/property mapping"
            return out

        bind_match = re.search(r'(?i)failed\s+to\s+bind\s+properties\s+under\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', msg)
        if bind_match:
            bind_key = bind_match.group(1)
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = f"Exact Issue: {service} startup failed due to invalid config binding '{bind_key}'{chain_tail}"
            out['recommendation'] = f"Fix config type/value for '{bind_key}' in configmap/secret/environment"
            return out

        if re.search(r'(?i)beandefinitionstoreexception|failed\s+to\s+read\s+candidate\s+component\s+class', msg):
            class_match = re.search(r'([A-Za-z0-9_.$-]+AutoConfiguration\.class|[A-Za-z0-9_.$-]+\.class)', msg)
            class_hint = class_match.group(1) if class_match else 'component class'
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = f"Exact Issue: {service} startup failed while loading {class_hint}{chain_tail}"
            out['recommendation'] = "Verify classpath/jar compatibility and disable/fix failing autoconfiguration"
            return out

        if re.search(r'(?i)beancreationexception|unsatisfieddependencyexception', msg):
            exception = 'BeanCreationException' if 'beancreationexception' in msg.lower() else 'UnsatisfiedDependencyException'
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = f"Exact Issue: {service} startup failed with {exception}{chain_tail}"
            out['recommendation'] = "Inspect nested 'Caused by' exception and fix failing bean/config dependency"
            return out

        if re.search(r'(?i)\b503\b|service\s+unavailable', msg):
            downstream = self._extract_dependency_from_message(msg) or 'downstream'
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            out['issue'] = f"Exact Issue: {service} -> {downstream} returned HTTP 503{chain_tail}"
            out['recommendation'] = f"Validate downstream {downstream} availability and caller configuration"
            return out

        exception_match = re.search(r'([A-Za-z0-9_.$]+(?:Exception|Error))', msg)
        if exception_match:
            exception_name = exception_match.group(1)
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            compact = re.sub(r'\s+', ' ', msg).strip()
            if len(compact) > 600:
                compact = compact[:600] + '...'
            out['issue'] = f"Exact Issue: {service} request failed with {exception_name}{chain_tail} | {compact}"
            out['recommendation'] = "Inspect the exception source and validate request/input/dependency path before repository or downstream call"
            return out

        return out

    def _classify_es_errors(self, logs):
        """Classify ES logs into actionable error entries."""
        ignored = self.config.get('ignored_log_patterns', [])
        patterns = [
            (r'(?i)ImagePullBackOff|ErrImagePull|Back-?off pulling image|pull access denied', 'IMAGE_PULL'),
            (r'(?i)crashloopbackoff', 'CRASH_LOOP'),
            (r'(?i)error|exception', 'ERROR'),
            (r'(?i)fatal', 'FATAL'),
            (r'(?i)critical', 'CRITICAL'),
            (r'(?i)warn', 'WARNING'),
            (r'TimeoutException', 'TIMEOUT'),
            (r'(?i)could not connect|Connection refused', 'CONNECTION')
        ]

        errors = []
        for entry in logs:
            message = entry.get('message', '')
            if not message:
                continue

            if self._is_internal_otel_exporter_noise(message):
                continue

            if self._matches_ignored_log_pattern(message):
                continue

            matched = None
            severity = 'INFO'
            for p, sev in patterns:
                if re.search(p, message):
                    matched = p
                    severity = sev
                    break

            if matched:
                error = {
                    'timestamp': entry.get('timestamp', datetime.now().isoformat()),
                    'service': entry.get('service', ''),
                    'severity': severity,
                    'message': message,
                    'pattern_matched': matched
                }
                dep = self._extract_dependency_from_message(message)
                if dep:
                    error['dependency'] = dep
                errors.append(error)
        return errors

    def _derive_restart_root_cause(self, service_key: str, pod_signal: Dict, metrics: Dict, window_minutes: int = 10) -> str:
        """Build deterministic restart reason from pod state + metrics hints."""
        if not isinstance(pod_signal, dict):
            return ''

        pods = pod_signal.get('pods', []) if isinstance(pod_signal.get('pods', []), list) else []
        if not pods:
            return ''

        restarts_10m = float(metrics.get('prometheus_restarts_10m', 0.0) or 0.0)
        waiting = float(metrics.get('prometheus_waiting_pods', 0.0) or 0.0)
        svc = str(service_key or 'unknown')
        lookback_minutes = max(1, int(window_minutes or 10))
        cutoff = datetime.now() - timedelta(minutes=lookback_minutes)

        def _is_recent_restart(finished_at: str) -> bool:
            raw = str(finished_at or '').strip()
            if not raw:
                return False
            try:
                parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                if parsed.tzinfo is not None:
                    parsed = parsed.astimezone().replace(tzinfo=None)
                return parsed >= cutoff
            except Exception:
                return False

        for pod in pods:
            if not isinstance(pod, dict):
                continue
            restart_count = int(pod.get('restarts', 0) or 0)
            restart_finished_at = str(pod.get('restart_finished_at', '') or '').strip()
            recent_restart = _is_recent_restart(restart_finished_at)
            if restart_count <= 0:
                continue
            if restarts_10m <= 0 and not recent_restart:
                continue

            pod_name = str(pod.get('name', 'unknown') or 'unknown')
            reason = str(pod.get('reason', '') or '').strip()
            cause = str(pod.get('restart_cause', '') or '').strip()
            exit_code = str(pod.get('restart_exit_code', '') or '').strip()

            if cause.lower() == 'oomkilled' or exit_code == '137':
                return f"Exact Issue: {svc} pod {pod_name} restarted due to OOMKilled (exitCode={exit_code or '137'})"
            if cause:
                return f"Exact Issue: {svc} pod {pod_name} restarted due to {cause}{f' (exitCode={exit_code})' if exit_code else ''}"
            if reason:
                return f"Exact Issue: {svc} pod {pod_name} restarted; reason={reason}"

            if waiting > 0:
                return f"Exact Issue: {svc} pod {pod_name} restart observed with waiting containers (possible probe/startup failure)"
            return f"Exact Issue: {svc} pod {pod_name} restarted {restart_count} time(s)"

        return ''

    def get_service_status(self, minutes: int = 5, services_subset: Optional[List[Dict]] = None):
        """Get service status using Elasticsearch logs as primary source."""
        if self.elasticsearch is None:
            return {}

        selected = services_subset if services_subset is not None else self._selected_services()
        if not selected:
            return {}

        selected_pairs = set()
        selected_namespaces = set()
        for svc in selected:
            if not isinstance(svc, dict):
                continue
            svc_name = self._canonical_service(str(svc.get('name', '') or '').strip())
            namespace = str(svc.get('namespace', 'unknown') or 'unknown').strip()
            if not svc_name:
                continue
            selected_pairs.add((namespace, svc_name))
            selected_namespaces.add(namespace)

        allowed_namespaces = sorted(ns for ns in selected_namespaces if ns) or self.config.get('monitoring', {}).get('discovery_namespaces', ['earth', 'mercury', 'mars'])

        prom_service_health_limit = int(self.config.get('prometheus_service_health_limit', 120) or 120)
        prom_services = selected[:prom_service_health_limit]
        prom_metrics = self.prometheus.get_api_metrics(monitored_services=prom_services)
        prom_request_rates = prom_metrics.get('request_rates', {}) if isinstance(prom_metrics, dict) else {}
        prom_error_rates = prom_metrics.get('error_rates', {}) if isinstance(prom_metrics, dict) else {}
        prom_service_health = prom_metrics.get('service_health', {}) if isinstance(prom_metrics, dict) else {}
        pod_signals = self._get_pod_health_signals(selected)

        # Aggregated ES query path
        selected_service_names = [str(svc.get('name')) for svc in selected if svc.get('name')]
        overview = self.elasticsearch.get_services_overview(
            minutes=minutes,
            max_docs=5000,
            namespaces=allowed_namespaces,
            service_names=selected_service_names,
            ignored_patterns=self.config.get('ignored_log_patterns', [])
        )
        # Normalize aliases (e.g. sales -> sales-service) and merge duplicate ES buckets.
        normalized_overview: Dict[str, Dict] = {}
        for _, agg in overview.items():
            namespace = str(agg.get('namespace', 'unknown') or 'unknown')
            raw_name = str(agg.get('name', '') or '').strip()
            canonical_name = self._canonical_service(raw_name)
            if not canonical_name:
                continue

            if selected_pairs and (namespace, canonical_name) not in selected_pairs:
                continue

            canonical_key = f"{namespace}/{canonical_name}"
            existing = normalized_overview.get(canonical_key)
            if not existing:
                normalized_overview[canonical_key] = {
                    'name': canonical_name,
                    'namespace': namespace,
                    'total_log_entries': int(agg.get('total_log_entries', 0) or 0),
                    'error_count': int(agg.get('error_count', 0) or 0),
                    'warning_count': int(agg.get('warning_count', 0) or 0),
                    'latest_timestamp': agg.get('latest_timestamp', ''),
                    'latest_error_message': agg.get('latest_error_message', ''),
                    'latest_any_message': agg.get('latest_any_message', '')
                }
                continue

            existing['total_log_entries'] += int(agg.get('total_log_entries', 0) or 0)
            existing['error_count'] += int(agg.get('error_count', 0) or 0)
            existing['warning_count'] += int(agg.get('warning_count', 0) or 0)

            existing_ts = str(existing.get('latest_timestamp', '') or '')
            candidate_ts = str(agg.get('latest_timestamp', '') or '')
            if candidate_ts and candidate_ts >= existing_ts:
                existing['latest_timestamp'] = candidate_ts
                if agg.get('latest_error_message'):
                    existing['latest_error_message'] = agg.get('latest_error_message', '')
                if agg.get('latest_any_message'):
                    existing['latest_any_message'] = agg.get('latest_any_message', '')
            else:
                if not existing.get('latest_error_message') and agg.get('latest_error_message'):
                    existing['latest_error_message'] = agg.get('latest_error_message', '')
                if not existing.get('latest_any_message') and agg.get('latest_any_message'):
                    existing['latest_any_message'] = agg.get('latest_any_message', '')

        service_status = {}
        for key, agg in normalized_overview.items():
            total_logs = agg.get('total_log_entries', 0)
            error_count = agg.get('error_count', 0)
            warning_count = agg.get('warning_count', 0)
            error_rate = (error_count / total_logs * 100) if total_logs > 0 else 0

            service_name = self._canonical_service(str(agg.get('name', key.split('/')[-1]) or '').strip())
            if not service_name:
                continue
            prom_req = 0.0
            prom_err = 0.0
            for job, val in prom_request_rates.items():
                if service_name in str(job):
                    prom_req = float(val)
                    break
            for job, val in prom_error_rates.items():
                if service_name in str(job):
                    prom_err = float(val)
                    break

            namespace = agg.get('namespace', 'unknown')
            service_key = f"{namespace}/{service_name}"
            health = prom_service_health.get(service_key, {}) if isinstance(prom_service_health, dict) else {}
            pod_signal = pod_signals.get(service_key, {'status': 'unknown', 'pods': []})
            readiness_ratio = health.get('readiness_ratio')
            pod_count = int(health.get('pod_count', 0) or 0)
            restarts_10m = float(health.get('restarts_10m', 0.0) or 0.0)
            waiting_pods = float(health.get('waiting_pods', 0.0) or 0.0)

            if error_rate > 5.0:
                status = 'degraded'
            elif error_rate > 0:
                status = 'warning'
            else:
                status = 'healthy'

            # Prometheus signal can degrade even when ES log sample is sparse
            if prom_err >= 0.01:
                status = 'degraded' if prom_err >= 0.05 else 'warning'

            # Fallback to pod health when request/error counters are unavailable
            if readiness_ratio is not None:
                if pod_count <= 0:
                    status = 'pending'
                elif readiness_ratio < 0.5:
                    status = 'down'
                elif readiness_ratio < 1.0:
                    status = 'degraded'
                elif waiting_pods > 0 or restarts_10m >= 1:
                    status = 'warning'

            # Pending/down inference from latest message in ES window
            latest_message = str(agg.get('latest_error_message', '') or '').lower()
            if any(marker in latest_message for marker in ['pod pending', 'failedscheduling', 'no nodes available', 'insufficient cpu', 'insufficient memory']):
                status = 'pending'
            elif any(marker in latest_message for marker in ['imagepullbackoff', 'errimagepull', 'crashloopbackoff', 'invalidimagename']):
                status = 'down'

            # Kubernetes pod signal overrides for startup/runtime failures (more authoritative)
            if pod_signal.get('status') == 'unhealthy':
                status = 'degraded'
            if pod_signal.get('status') == 'no_pods':
                status = 'pending'
            if pod_signal.get('status') == 'healthy' and status == 'pending':
                # Keep pending only when Kubernetes truly has zero pods/readiness.
                if pod_count > 0 and (readiness_ratio is None or float(readiness_ratio or 0) >= 1.0):
                    status = 'healthy'

            pod_reason_msg = ''
            for pod in pod_signal.get('pods', []):
                reason = str(pod.get('reason', '') or '')
                if reason:
                    lower_reason = reason.lower()
                    if any(token in lower_reason for token in ['imagepullbackoff', 'errimagepull', 'invalidimagename']):
                        status = 'down'
                        pod_reason_msg = f"Pod {pod.get('name', 'unknown')}: {reason}"
                        break
                    if 'crashloopbackoff' in lower_reason:
                        status = 'degraded'
                        pod_reason_msg = f"Pod {pod.get('name', 'unknown')}: {reason}"
                        break
                    if 'containerstatusunknown' in lower_reason:
                        status = 'warning'
                        pod_reason_msg = f"Pod {pod.get('name', 'unknown')}: {reason}"

            restart_root_cause = self._derive_restart_root_cause(
                service_key,
                pod_signal,
                {
                    'prometheus_restarts_10m': restarts_10m,
                    'prometheus_waiting_pods': waiting_pods
                },
                window_minutes=minutes
            )

            if pod_reason_msg:
                # Do not overwrite application-level root-cause evidence with pod wrapper symptom.
                # Keep pod reason as supplemental context only.
                existing_error = str(agg.get('latest_error_message', '') or '')
                existing_any = str(agg.get('latest_any_message', '') or '')
                if not existing_error:
                    if existing_any:
                        agg['latest_any_message'] = f"{existing_any} | {pod_reason_msg}"
                    else:
                        agg['latest_any_message'] = pod_reason_msg
                else:
                    if pod_reason_msg not in existing_error:
                        agg['latest_any_message'] = f"{existing_error} | {pod_reason_msg}"

            # Suppress OTel exporter 404 noise (logs/metrics export endpoint unsupported) from RCA surface.
            for noisy_field in ('latest_error_message', 'latest_any_message'):
                text_value = str(agg.get(noisy_field, '') or '')
                if re.search(r'(?i)opentelemetry.*failed\s+to\s+export\s+(logs|metrics).*http\s+status\s+code\s+404', text_value):
                    agg[noisy_field] = ''

            # If service has no ready pods, do not surface stale app-level exceptions
            # as current root cause unless they are explicit pod/runtime startup reasons.
            if not any(bool(p.get('ready', False)) for p in pod_signal.get('pods', []) if isinstance(p, dict)):
                agg['latest_error_message'] = ''
                agg['latest_any_message'] = ''

            agg['latest_error_message'] = self._sanitize_root_cause_message(agg.get('latest_error_message', ''))
            agg['latest_any_message'] = self._sanitize_root_cause_message(agg.get('latest_any_message', ''))

            deep_inspection = {}
            should_inspect = (
                status in {'degraded', 'down', 'pending'} or
                pod_signal.get('status') in {'unhealthy', 'no_pods'} or
                error_count > 0 or
                self._is_actionable_error_message(agg.get('latest_error_message', ''))
            )

            if self.service_monitor and should_inspect:
                try:
                    deep_inspection = self.service_monitor.deep_inspect_service(
                        service_name,
                        namespace=namespace,
                        pod_status=pod_signal
                    )
                except Exception:
                    deep_inspection = {}

                deep_summary = str(deep_inspection.get('summary', '') or '')
                if 'No pods found for service; deep inspection skipped.' in deep_summary and status == 'healthy':
                    deep_summary = ''
                if deep_summary:
                    if agg.get('latest_error_message'):
                        agg['latest_error_message'] = f"{agg.get('latest_error_message')} | {deep_summary}"
                    elif agg.get('latest_any_message'):
                        agg['latest_any_message'] = f"{agg.get('latest_any_message')} | {deep_summary}"
                    else:
                        agg['latest_any_message'] = deep_summary

            # Final sanitize pass after deep-inspection concatenation
            agg['latest_error_message'] = self._sanitize_root_cause_message(agg.get('latest_error_message', ''))
            agg['latest_any_message'] = self._sanitize_root_cause_message(agg.get('latest_any_message', ''))

            observed_dependencies = self._get_dynamic_dependencies(service_name)

            # Fallback dependency learning from latest error text when traces are sparse.
            text_deps = self._extract_dependencies_from_texts(
                agg.get('latest_error_message', ''),
                agg.get('latest_any_message', ''),
                pod_reason_msg
            )
            for dep in text_deps:
                if dep and dep not in observed_dependencies:
                    observed_dependencies.append(dep)

            recent_error_message = str(agg.get('latest_error_message', '') or '')
            if not recent_error_message:
                candidate_any = str(agg.get('latest_any_message', '') or '')
                if candidate_any and self._is_actionable_error_message(candidate_any):
                    recent_error_message = candidate_any

            # Backfill concrete RCA from raw logs when overview aggregation misses
            # multiline exceptions or severity classification in the current window.
            if not recent_error_message and self.elasticsearch is not None:
                try:
                    backfill_end_ms = int(time.time() * 1000)
                    backfill_start_ms = int((time.time() - max(1, int(minutes)) * 60) * 1000)
                    backfill_limit = int(self.config.get('elasticsearch', {}).get('status_backfill_log_limit', 40) or 40)
                    backfill_logs = self.elasticsearch.get_logs(
                        service=service_name,
                        start_time=backfill_start_ms,
                        end_time=backfill_end_ms,
                        limit=backfill_limit,
                        namespace=namespace
                    )
                except Exception:
                    backfill_logs = []

                for raw in backfill_logs:
                    if not isinstance(raw, dict):
                        continue
                    candidate = str(
                        raw.get('message')
                        or raw.get('body')
                        or raw.get('log')
                        or raw.get('msg')
                        or ''
                    ).strip()
                    if not candidate:
                        continue
                    if self._is_actionable_error_message(candidate):
                        recent_error_message = candidate
                        break

            recent_error_message = self._sanitize_root_cause_message(recent_error_message)

            if (not recent_error_message) and restart_root_cause:
                recent_error_message = restart_root_cause

            # Stabilize service-table root cause text with deterministic exact parser
            if recent_error_message and self._is_actionable_error_message(recent_error_message):
                parsed = self._derive_exact_from_message(service_key, recent_error_message, crashloop_present=('crashloopbackoff' in recent_error_message.lower()))
                if parsed.get('issue'):
                    recent_error_message = parsed['issue']

            if recent_error_message and self._matches_ignored_log_pattern(recent_error_message):
                recent_error_message = ''

            service_status[key] = {
                'name': service_name,
                'metrics': {
                    'service': service_name,
                    'total_log_entries': total_logs,
                    'error_count': error_count,
                    'warning_count': warning_count,
                    'error_rate': error_rate,
                    'prometheus_request_rate': prom_req,
                    'prometheus_error_rate': prom_err,
                    'prometheus_readiness_ratio': readiness_ratio,
                    'prometheus_pod_count': pod_count,
                    'prometheus_restarts_10m': restarts_10m,
                    'prometheus_waiting_pods': waiting_pods,
                    'dependency_failures': 0,
                    'failed_dependencies': [],
                    'observed_dependencies': observed_dependencies,
                    'timestamp': datetime.now().isoformat(),
                    'latest_timestamp': agg.get('latest_timestamp', '')
                },
                'recent_errors': ([{
                    'timestamp': agg.get('latest_timestamp', datetime.now().isoformat()),
                    'message': recent_error_message,
                    'severity': 'ERROR'
                }] if recent_error_message and self._is_actionable_error_message(recent_error_message) else []),
                'status': status,
                'pod_status': {
                    'status': pod_signal.get('status', 'unknown'),
                    'pods': pod_signal.get('pods', []),
                    'namespace': namespace
                },
                'deep_inspection': deep_inspection,
                'namespace': namespace
            }

        # Ensure selected services are still represented even when ES overview is empty/sparse
        # (common during startup failures, pending pods, or no recent logs).
        for svc in selected:
            if not isinstance(svc, dict):
                continue
            service_name = self._canonical_service(str(svc.get('name', '') or '').strip())
            namespace = str(svc.get('namespace', 'unknown') or 'unknown')
            if not service_name:
                continue

            service_key = f"{namespace}/{service_name}"
            if service_key in service_status:
                continue

            health = prom_service_health.get(service_key, {}) if isinstance(prom_service_health, dict) else {}
            pod_signal = pod_signals.get(service_key, {'status': 'unknown', 'pods': []})
            readiness_ratio = health.get('readiness_ratio')
            pod_count = int(health.get('pod_count', 0) or 0)
            restarts_10m = float(health.get('restarts_10m', 0.0) or 0.0)
            waiting_pods = float(health.get('waiting_pods', 0.0) or 0.0)

            status = 'unknown'
            if pod_signal.get('status') == 'no_pods' or pod_count <= 0:
                status = 'pending'
            elif pod_signal.get('status') == 'unhealthy':
                status = 'degraded'
            elif readiness_ratio is not None:
                if readiness_ratio < 0.5:
                    status = 'down'
                elif readiness_ratio < 1.0:
                    status = 'degraded'
                elif waiting_pods > 0 or restarts_10m >= 1:
                    status = 'warning'
                else:
                    status = 'healthy'

            pod_reason_msg = ''
            for pod in pod_signal.get('pods', []):
                reason = str(pod.get('reason', '') or '').strip()
                if reason:
                    pod_reason_msg = f"Pod {pod.get('name', 'unknown')}: {reason}"
                    break

            restart_root_cause = self._derive_restart_root_cause(
                service_key,
                pod_signal,
                {
                    'prometheus_restarts_10m': restarts_10m,
                    'prometheus_waiting_pods': waiting_pods
                },
                window_minutes=minutes
            )

            observed_dependencies = self._get_dynamic_dependencies(service_name)

            sanitized_reason = self._sanitize_root_cause_message(pod_reason_msg or restart_root_cause)

            service_status[service_key] = {
                'name': service_name,
                'metrics': {
                    'service': service_name,
                    'total_log_entries': 0,
                    'error_count': 0,
                    'warning_count': 0,
                    'error_rate': 0.0,
                    'prometheus_request_rate': 0.0,
                    'prometheus_error_rate': 0.0,
                    'prometheus_readiness_ratio': readiness_ratio,
                    'prometheus_pod_count': pod_count,
                    'prometheus_restarts_10m': restarts_10m,
                    'prometheus_waiting_pods': waiting_pods,
                    'dependency_failures': 0,
                    'failed_dependencies': [],
                    'observed_dependencies': observed_dependencies,
                    'timestamp': datetime.now().isoformat(),
                    'latest_timestamp': ''
                },
                'recent_errors': ([{
                    'timestamp': datetime.now().isoformat(),
                    'message': sanitized_reason,
                    'severity': 'ERROR'
                }] if sanitized_reason else []),
                'status': status,
                'pod_status': {
                    'status': pod_signal.get('status', 'unknown'),
                    'pods': pod_signal.get('pods', []),
                    'namespace': namespace
                },
                'deep_inspection': {},
                'namespace': namespace
            }

        return self._evaluate_static_dependency_health(service_status)
        
    def start_monitoring(self):
        """Start the monitoring process in a separate thread"""
        self.agent_status['running'] = True
        self.agent_status['start_time'] = datetime.now().isoformat()
        
        monitoring_thread = Thread(target=self.monitor_apis)
        monitoring_thread.daemon = True
        monitoring_thread.start()
        
        logger.info("Monitoring agent started in background thread")
        
    def monitor_apis(self):
        """Continuously monitor API endpoints and services for anomalies"""
        # Run monitoring every 1 minute by config default
        check_interval = self.config['anomaly_detection'].get('check_interval', 60)
        
        while self.agent_status['running']:
            try:
                self.agent_status['last_check'] = datetime.now().isoformat()
                self._enforce_sidecar_trace_retention()
                
                # Collect metrics from Prometheus
                metrics = self.prometheus.get_api_metrics()
                
                # Collect service logs and metrics for incident generation.
                # Use a shorter rolling window so incidents represent current failures
                # instead of stale errors from long historical scans.
                es_window_minutes = int(
                    self.config.get('monitoring', {}).get(
                        'incident_log_window_minutes',
                        self.config.get('elasticsearch', {}).get('log_window_minutes', 720)
                    ) or 720
                )
                es_window_minutes = max(5, es_window_minutes)
                service_status = self.get_service_status(minutes=es_window_minutes)
                self._last_service_status = service_status if isinstance(service_status, dict) else {}
                self._seed_deterministic_memory_from_service_status(service_status)
                
                # Check for service errors
                service_anomalies = self.detect_service_anomalies(service_status)
                
                # Store metrics for adaptive learning
                self.metrics_history.append({
                    'timestamp': datetime.now().isoformat(),
                    **metrics,
                    'service_status': service_status
                })
                
                # Keep only configured metrics history limit
                metrics_limit = self.config.get('learning', {}).get('history_limit', 500)
                if len(self.metrics_history) > metrics_limit:
                    self.metrics_history = self.metrics_history[-metrics_limit:]
                
                # Adapt thresholds based on historical data
                adaptive_thresholds = self.learning_engine.adapt_thresholds(self.metrics_history)
                
                # Detect anomalies from Prometheus metrics
                anomalies = self.anomaly_detector.detect(metrics)
                
                # Combine Prometheus and service anomalies
                derived_error_anomalies = self._build_runtime_error_signal_anomalies(service_status)
                all_anomalies = anomalies + service_anomalies + derived_error_anomalies
                
                actionable_anomalies = self.filter_actionable_anomalies(service_status, all_anomalies)

                debug_anomaly_logs = bool(self.config.get('monitoring', {}).get('debug_anomaly_logs', False)) if isinstance(self.config, dict) else False
                if debug_anomaly_logs:
                    logger.info(f"DEBUG: all_anomalies={len(all_anomalies)}, actionable={len(actionable_anomalies)}, svcs={list(service_status.keys())}")
                    for a in all_anomalies[:3]:
                        logger.info(f"DEBUG anomaly: type={a.get('type')}, svc={a.get('service')}, sample={str(a.get('sample_error',''))[:80]}")

                if actionable_anomalies:
                    logger.info(f"Detected {len(all_anomalies)} total anomalies ({len(service_anomalies)} service anomalies)")
                    
                    # Get logs and traces from services
                    logs_traces = self.collect_service_context(service_status)

                    # Build cross-service dependency context from logs/traces
                    dependency_context = self.build_dependency_context(service_status, logs_traces)
                    self._learn_dynamic_dependencies(logs_traces)

                    trace_failure = self._has_trace_failure_signal(dependency_context)
                    actionable_by_rule = [a for a in actionable_anomalies if self._is_actionable_anomaly(a)]
                    
                    # Collect all services that have issues from service_status
                    services_with_issues = set()
                    for key, state in service_status.items():
                        if isinstance(state, dict):
                            status = str(state.get('status', '') or '').lower()
                            root_cause = str(state.get('root_cause', '') or '').strip()
                            exact_issue = str(state.get('exact_issue', '') or '').strip()
                            # Also check recent_errors which contains the actual error messages
                            recent_errors = state.get('recent_errors', []) or []
                            has_recent_error = bool(recent_errors and len(recent_errors) > 0)
                            
                            if status in ['degraded', 'down', 'pending', 'warning', 'offline'] or root_cause or exact_issue or has_recent_error:
                                services_with_issues.add(key)
                    
                    # Log for debugging
                    if services_with_issues:
                        logger.info(f"Services with issues to process: {list(services_with_issues)}")
                    
                    # Create incidents for services with issues even if no trace failure/anomaly
                    # This ensures services with root cause detected still get incidents
                    if not trace_failure and not actionable_by_rule and not services_with_issues:
                        logger.info("Skipping incident: no trace failures, actionable anomalies, or services with issues")
                        time.sleep(check_interval)
                        continue
                    
                    # Log what we're processing
                    logger.info(f"Processing incidents for {len(services_with_issues)} services with issues: {list(services_with_issues)}")
                    
                    grouped_anomalies = self._group_anomalies_by_service(actionable_anomalies)

                    # Create incidents for services with issues (not just those with anomalies)
                    for scoped_service in services_with_issues:
                        scoped_logs_traces = self._filter_logs_traces_for_service(scoped_service, logs_traces)
                        scoped_dependency_context = self._scope_dependency_context_for_service(scoped_service, dependency_context)
                        scoped_service_status = {
                            key: value for key, value in service_status.items()
                            if self._service_matches(scoped_service, key)
                        }
                        if not scoped_service_status and scoped_service in service_status:
                            scoped_service_status = {scoped_service: service_status.get(scoped_service, {})}

                        scoped_anomalies = grouped_anomalies.get(scoped_service, [])

                        unresolved_count = int(self._service_unresolved_counter.get(scoped_service, 0) or 0)
                        es_cfg = self.config.setdefault('elasticsearch', {}) if isinstance(self.config, dict) else {}
                        default_log_limit = int(es_cfg.get('log_limit', 500) or 500)
                        if unresolved_count >= 2:
                            es_cfg['log_limit'] = max(default_log_limit, 1000)
                        else:
                            es_cfg['log_limit'] = max(default_log_limit, 500)

                        # Enhance root cause analysis with learning engine
                        enhanced_analysis = self.enhance_root_cause_analysis(
                            scoped_anomalies, scoped_logs_traces
                        )

                        trace_rca = self._derive_trace_root_cause(scoped_anomalies, scoped_dependency_context, scoped_logs_traces)
                        log_rca = self._derive_log_root_cause(scoped_anomalies, scoped_logs_traces)
                        selected_rca = trace_rca if trace_rca.get('issue') else log_rca

                        def _is_generic_issue(issue_text: Optional[str]) -> bool:
                            text = str(issue_text or '').lower()
                            return (
                                not text or
                                'actionable failure detected' in text or
                                ('crashloopbackoff' in text and 'trace ' not in text and 'status_code=' not in text) or
                                '[high_error_rate]' in text or
                                'has high error rate' in text or
                                '[service_degraded]' in text or
                                ' is degraded' in text
                            )

                        if _is_generic_issue(selected_rca.get('issue')) and trace_rca.get('issue') and not _is_generic_issue(trace_rca.get('issue')):
                            selected_rca = trace_rca
                        if _is_generic_issue(selected_rca.get('issue')) and log_rca.get('issue') and not _is_generic_issue(log_rca.get('issue')):
                            selected_rca = log_rca
                        if selected_rca.get('issue'):
                            existing_exact = enhanced_analysis.get('exact_issues', [])
                            if not isinstance(existing_exact, list):
                                existing_exact = [str(existing_exact)] if existing_exact else []
                            enhanced_analysis['exact_issues'] = [selected_rca['issue']] + [item for item in existing_exact if item != selected_rca['issue']]

                            existing_likely = enhanced_analysis.get('likely_causes', [])
                            if not isinstance(existing_likely, list):
                                existing_likely = [str(existing_likely)] if existing_likely else []
                            enhanced_analysis['likely_causes'] = [selected_rca['issue']] + [item for item in existing_likely if item != selected_rca['issue']]

                            existing_reco = enhanced_analysis.get('recommendations', [])
                            if not isinstance(existing_reco, list):
                                existing_reco = [str(existing_reco)] if existing_reco else []
                            if selected_rca.get('recommendation'):
                                enhanced_analysis['recommendations'] = [selected_rca['recommendation']] + [item for item in existing_reco if item != selected_rca['recommendation']]

                            enhanced_analysis['confidence'] = min(
                                0.98,
                                float(enhanced_analysis.get('confidence', 0.5) or 0.5) + float(selected_rca.get('confidence_boost', 0.0) or 0.0)
                            )

                            # Keep top exact issue only for cleaner, service-specific RCA output
                            if isinstance(enhanced_analysis.get('exact_issues'), list) and enhanced_analysis['exact_issues']:
                                enhanced_analysis['exact_issues'] = [enhanced_analysis['exact_issues'][0]]
                            if isinstance(enhanced_analysis.get('likely_causes'), list) and enhanced_analysis['likely_causes']:
                                enhanced_analysis['likely_causes'] = [enhanced_analysis['likely_causes'][0]]

                        # Last-resort deterministic RCA from direct service anomalies (never generic pod wrapper)
                        if not selected_rca.get('issue') or 'actionable failure detected' in str(selected_rca.get('issue', '')).lower():
                            anomaly_rca = self._derive_anomaly_root_cause(scoped_anomalies, scoped_service_status)
                            if anomaly_rca.get('issue'):
                                enhanced_analysis['exact_issues'] = [anomaly_rca['issue']]
                                existing_reco = enhanced_analysis.get('recommendations', [])
                                if not isinstance(existing_reco, list):
                                    existing_reco = [str(existing_reco)] if existing_reco else []
                                if anomaly_rca.get('recommendation'):
                                    enhanced_analysis['recommendations'] = [anomaly_rca['recommendation']] + [item for item in existing_reco if item != anomaly_rca['recommendation']]
                                selected_rca = anomaly_rca

                        # Online learning rerank: apply human-feedback correction
                        primary_service = scoped_service if scoped_service else self._extract_primary_service(scoped_anomalies)

                        # Keep per-service best RCA memory to avoid minute-to-minute downgrade to generic messages
                        def _rca_quality(issue_text: Optional[str]) -> int:
                            text = str(issue_text or '').lower()
                            score = 0
                            if 'trace ' in text and 'status_code=' in text:
                                score += 120
                            if 'chain ' in text and ' failed at ' in text:
                                score += 90
                            if 'exception=' in text:
                                score += 60
                            if self._is_deterministic_fix_candidate(issue_text or ''):
                                # Strongly prefer deterministic source-fix candidates (enum/placeholder)
                                # over generic transport failures in per-service RCA memory.
                                score += 320
                            if 'file=' in text:
                                score += 40
                            if any(token in text for token in [
                                'connection refused', 'connectexception', 'timeout',
                                'service unavailable', 'deadline exceeded'
                            ]):
                                score -= 70
                            if 'actionable failure detected' in text:
                                score -= 40
                            if 'sample: pod ' in text:
                                score -= 30
                            return score

                        service_key = str(primary_service or 'unknown')
                        if selected_rca.get('issue'):
                            current_quality = _rca_quality(selected_rca.get('issue'))
                            mem = self._service_rca_memory.get(service_key, {}) if isinstance(self._service_rca_memory, dict) else {}
                            mem_issue = mem.get('issue', '')
                            mem_quality = int(mem.get('quality', -999) or -999)
                            mem_ts = str(mem.get('timestamp', '') or '')
                            mem_recent = False
                            if mem_ts:
                                try:
                                    mem_dt = datetime.fromisoformat(mem_ts.replace('Z', '+00:00'))
                                    if mem_dt.tzinfo is not None:
                                        mem_dt = mem_dt.astimezone().replace(tzinfo=None)
                                    mem_recent = (datetime.now() - mem_dt).total_seconds() <= 900
                                except Exception:
                                    mem_recent = False

                            current_is_deterministic = self._is_deterministic_fix_candidate(selected_rca.get('issue', ''))
                            mem_is_deterministic = self._is_deterministic_fix_candidate(mem_issue)

                            # Do not downgrade deterministic current RCA to stale non-deterministic memory.
                            if current_is_deterministic and not mem_is_deterministic:
                                enhanced_analysis['exact_issues'] = [selected_rca.get('issue', '')]
                                self._service_rca_memory[service_key] = {
                                    'issue': selected_rca.get('issue', ''),
                                    'quality': current_quality,
                                    'timestamp': datetime.now().isoformat()
                                }
                            elif mem_issue and mem_recent and mem_quality > current_quality:
                                enhanced_analysis['exact_issues'] = [mem_issue]
                            else:
                                self._service_rca_memory[service_key] = {
                                    'issue': selected_rca.get('issue', ''),
                                    'quality': current_quality,
                                    'timestamp': datetime.now().isoformat()
                                }

                        # Trim RCA memory to bounded size
                        if len(self._service_rca_memory) > 500:
                            keys = list(self._service_rca_memory.keys())
                            for stale in keys[:-500]:
                                self._service_rca_memory.pop(stale, None)

                        exact_issue = str((enhanced_analysis.get('exact_issues') or [''])[0] if isinstance(enhanced_analysis.get('exact_issues'), list) else '')
                        unresolved_like = (
                            (not exact_issue) or
                            ('actionable failure detected' in exact_issue.lower()) or
                            ('high error rate' in exact_issue.lower()) or
                            ('connection refused' in exact_issue.lower() and 'while calling' not in exact_issue.lower())
                        )
                        if unresolved_like:
                            self._service_unresolved_counter[scoped_service] = unresolved_count + 1
                        else:
                            self._service_unresolved_counter[scoped_service] = 0

                        corrected_category, correction_confidence = self.learning_engine.apply_feedback_correction(
                            enhanced_analysis.get('error_category', 'unknown'),
                            service=primary_service
                        )
                        enhanced_analysis['corrected_error_category'] = corrected_category
                        enhanced_analysis['correction_confidence'] = correction_confidence
                        
                        # Find similar past incidents
                        similar_incidents = self.learning_engine.find_similar_incidents({
                            'metrics': metrics,
                            'logs': scoped_logs_traces.get('logs', []),
                            'service_status': scoped_service_status
                        })
                        
                        # Get remedial actions from knowledge base
                        remedial_actions = self.learning_engine.get_remedial_actions({
                            'metrics': metrics,
                            'logs': scoped_logs_traces.get('logs', []),
                            'service_status': scoped_service_status
                        })
                        
                        # Create incident record. Prefer issues that map to deterministic
                        # source-fix classes when multiple RCA candidates are present.
                        candidate_issues: List[str] = []
                        exact_issues = enhanced_analysis.get('exact_issues', [])
                        likely_causes = enhanced_analysis.get('likely_causes', [])
                        if isinstance(exact_issues, list):
                            candidate_issues.extend([str(x or '').strip() for x in exact_issues if str(x or '').strip()])
                        elif str(exact_issues or '').strip():
                            candidate_issues.append(str(exact_issues).strip())

                        if isinstance(likely_causes, list):
                            candidate_issues.extend([str(x or '').strip() for x in likely_causes if str(x or '').strip()])
                        elif str(likely_causes or '').strip():
                            candidate_issues.append(str(likely_causes).strip())

                        for _, state in (scoped_service_status or {}).items():
                            if not isinstance(state, dict):
                                continue

                            # Include all known service-level RCA text fields, not only recent_errors.
                            for field_value in [
                                state.get('root_cause', ''),
                                state.get('exact_issue', ''),
                                (state.get('deep_inspection', {}) or {}).get('root_cause', '') if isinstance(state.get('deep_inspection', {}), dict) else '',
                                (state.get('deep_inspection', {}) or {}).get('exact_issue', '') if isinstance(state.get('deep_inspection', {}), dict) else '',
                                (state.get('deep_inspection', {}) or {}).get('reason', '') if isinstance(state.get('deep_inspection', {}), dict) else ''
                            ]:
                                val = str(field_value or '').strip()
                                if val:
                                    candidate_issues.append(val)

                            pod_state = state.get('pod_status', {}) if isinstance(state.get('pod_status', {}), dict) else {}
                            for pod in (pod_state.get('pods', []) or [])[:6]:
                                if not isinstance(pod, dict):
                                    continue
                                pod_reason = str(pod.get('reason', '') or '').strip()
                                if pod_reason:
                                    candidate_issues.append(pod_reason)

                            for err in (state.get('recent_errors', []) or [])[:12]:
                                if not isinstance(err, dict):
                                    continue
                                msg = str(err.get('message', '') or '').strip()
                                if msg:
                                    candidate_issues.append(msg)

                            service_root = str(state.get('root_cause', '') or '').strip()
                            if service_root:
                                candidate_issues.append(service_root)
                            service_exact = str(state.get('exact_issue', '') or '').strip()
                            if service_exact:
                                candidate_issues.append(service_exact)

                        deduped_issues: List[str] = []
                        seen_issue_keys = set()
                        for item in candidate_issues:
                            key = item.lower()
                            if key in seen_issue_keys:
                                continue
                            seen_issue_keys.add(key)
                            deduped_issues.append(item)

                        def _is_too_generic_issue_text(issue_text: str) -> bool:
                            text = str(issue_text or '').strip()
                            low = text.lower()
                            if not text:
                                return True
                            if low in {'error', 'exception', 'unknown', 'n/a', 'na', 'none'}:
                                return True
                            if len(text) < 8 and re.fullmatch(r'[a-zA-Z]+', text):
                                return True
                            return False

                        deduped_issues = [i for i in deduped_issues if not _is_too_generic_issue_text(i)]

                        # Recover deterministic candidates directly from scoped logs/service errors.
                        # This prevents transient network/connectivity noise from masking concrete
                        # enum/placeholder defects that are suitable for source_fix PR automation.
                        deterministic_from_context: List[str] = []
                        parse_service = str(primary_service or scoped_service or '')
                        context_streams = [
                            scoped_logs_traces.get('service_errors', []) if isinstance(scoped_logs_traces, dict) else [],
                            scoped_logs_traces.get('logs', []) if isinstance(scoped_logs_traces, dict) else []
                        ]
                        for stream in context_streams:
                            if not isinstance(stream, list):
                                continue
                            for entry in stream[:60]:
                                if not isinstance(entry, dict):
                                    continue
                                raw_msg = str(
                                    entry.get('message')
                                    or entry.get('body')
                                    or entry.get('log')
                                    or entry.get('msg')
                                    or ''
                                ).strip()
                                if not raw_msg:
                                    continue
                                parsed_issue = raw_msg
                                try:
                                    parsed = self._derive_exact_from_message(
                                        parse_service,
                                        raw_msg,
                                        crashloop_present=('crashloopbackoff' in raw_msg.lower())
                                    )
                                    if isinstance(parsed, dict) and str(parsed.get('issue', '')).strip():
                                        parsed_issue = str(parsed.get('issue', '')).strip()
                                except Exception:
                                    pass

                                if self._is_deterministic_fix_candidate(parsed_issue):
                                    deterministic_from_context.append(parsed_issue)

                        if deterministic_from_context:
                            ordered_context = []
                            seen_ctx = set()
                            for issue in deterministic_from_context:
                                key = issue.lower()
                                if key in seen_ctx:
                                    continue
                                seen_ctx.add(key)
                                ordered_context.append(issue)
                            deduped_issues = ordered_context + [
                                item for item in deduped_issues if item.lower() not in seen_ctx
                            ]
                            logger.info(
                                "Deterministic context candidate recovered for %s: %s",
                                str(parse_service or scoped_service or 'unknown'),
                                ordered_context[0][:220]
                            )

                        # Temporary hardening: suppress pure connectivity noise from winning
                        # top_issue selection when richer candidates exist in the same cycle.
                        def _is_connectivity_noise(issue_text: str) -> bool:
                            text = str(issue_text or '').lower()
                            if not text:
                                return False
                            return bool(re.search(
                                r'(?i)\b(java\.net\.)?connectexception\b|connection\s+refused|failed\s+to\s+connect|connect\s+timed\s+out|read\s+timed\s+out',
                                text
                            ))

                        non_connect_issues = [x for x in deduped_issues if not _is_connectivity_noise(x)]
                        if non_connect_issues:
                            deduped_issues = non_connect_issues

                        top_issue = ''
                        for item in deduped_issues:
                            if self._is_deterministic_fix_candidate(item):
                                top_issue = item
                                break
                        if not top_issue:
                            top_issue = deduped_issues[0] if deduped_issues else ''

                        if _is_too_generic_issue_text(top_issue):
                            top_issue = ''

                        pr_cfg_runtime = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
                        fix_mode_runtime = str(pr_cfg_runtime.get('fix_mode', 'report') or 'report').strip().lower()

                        # In source_fix mode, force one deterministic recovery pass per service
                        # before deciding to skip PR creation.
                        if fix_mode_runtime == 'source_fix' and (not self._is_deterministic_fix_candidate(top_issue)):
                            recovered_issue = self._recover_deterministic_issue_for_service(
                                str(primary_service or scoped_service or ''),
                                scoped_logs_traces,
                                scoped_service_status
                            )
                            if recovered_issue:
                                top_issue = recovered_issue
                                logger.info(
                                    "Recovered deterministic issue for %s during source_fix selection: %s",
                                    str(primary_service or scoped_service or 'unknown'),
                                    recovered_issue[:220]
                                )

                        # Keep deterministic issue sticky per service for a short window so
                        # transient pod/connectivity wrappers do not suppress source-fix PR flow.
                        svc_for_mem = str(primary_service or scoped_service or '')
                        deterministic_mem = self._service_deterministic_issue_memory.get(svc_for_mem, {}) if isinstance(self._service_deterministic_issue_memory, dict) else {}
                        mem_issue = str(deterministic_mem.get('issue', '') or '').strip()
                        mem_ts = str(deterministic_mem.get('timestamp', '') or '')
                        mem_recent = False
                        if mem_issue and mem_ts:
                            try:
                                mem_dt = datetime.fromisoformat(mem_ts.replace('Z', '+00:00'))
                                if mem_dt.tzinfo is not None:
                                    mem_dt = mem_dt.astimezone().replace(tzinfo=None)
                                mem_recent = (datetime.now() - mem_dt).total_seconds() <= 1800
                            except Exception:
                                mem_recent = False

                        if top_issue and self._is_deterministic_fix_candidate(top_issue):
                            self._service_deterministic_issue_memory[svc_for_mem] = {
                                'issue': top_issue,
                                'timestamp': datetime.now().isoformat()
                            }
                        elif (not self._is_deterministic_fix_candidate(top_issue)) and mem_issue and mem_recent and self._is_deterministic_fix_candidate(mem_issue):
                            logger.info(
                                "Using sticky deterministic issue for %s: %s",
                                str(svc_for_mem or 'unknown'),
                                mem_issue[:220]
                            )
                            top_issue = mem_issue

                        if len(self._service_deterministic_issue_memory) > 500:
                            keys = list(self._service_deterministic_issue_memory.keys())
                            for stale in keys[:-500]:
                                self._service_deterministic_issue_memory.pop(stale, None)

                        if top_issue:
                            enhanced_analysis['exact_issues'] = [top_issue]

                        # Quick pre-check for obvious infrastructure issues - skip without AI
                        issue_lower = str(top_issue or '').lower()
                        is_obvious_infra = bool(re.search(
                            r'(?i)connection\s+refused.*at\s+java\.net|connectexception.*connection\s+refused|'
                            r'failed\s+to\s+connect.*ai-monitoring-agent|imagepullbackoff.*ai-monitoring|'
                            r'crashloopbackoff.*startup|pending.*pod|ImagePullBackOff.*not found',
                            issue_lower
                        ))
                        
                        # Let AI decide - but skip obvious infrastructure issues immediately
                        should_skip_pr = is_obvious_infra

                        # Also skip incident creation for infrastructure issues
                        should_skip_incident = is_obvious_infra

                        structured_rca = self._structured_rca_from_issue(top_issue)
                        issue_scope = self._classify_issue_scope(top_issue)
                        if should_skip_pr:
                            logger.info(
                                "Skipping PR and incident for %s: infrastructure issue detected. top_issue=%s",
                                str(primary_service or scoped_service or 'unknown'),
                                (top_issue[:120] if isinstance(top_issue, str) else str(top_issue))
                            )
                            pr_metadata = {
                                'status': 'Skipped',
                                'reason': 'Infrastructure issue (connection/network) - not code-level',
                                'mode': fix_mode_runtime,
                                'scope': issue_scope
                            }
                            # Don't create incident for infra issues - continue to next service
                            continue
                        
                        logger.info(
                            "PR evaluation for %s: mode=%s scope=%s issue=%s",
                            str(primary_service or scoped_service or 'unknown'),
                            fix_mode_runtime,
                            issue_scope,
                            (top_issue[:220] if isinstance(top_issue, str) else str(top_issue))
                        )
                        try:
                            pr_metadata = self._create_code_fix_pr_stub(
                                primary_service,
                                top_issue,
                                issue_scope,
                                structured_rca,
                                enhanced_analysis
                            )
                            logger.info(
                                "PR result for %s: status=%s reason=%s",
                                str(primary_service or scoped_service or 'unknown'),
                                str(pr_metadata.get('status', 'unknown') or ''),
                                str(pr_metadata.get('reason', '')[:100] if pr_metadata.get('reason') else '')
                            )
                        except Exception as pr_err:
                            logger.error("PR creation failed with exception: %s", str(pr_err))
                            pr_metadata = {'status': 'Error', 'reason': str(pr_err)}
                        
                        incident = {
                            'id': f"INC-{int(time.time())}",
                            'timestamp': datetime.now().isoformat(),
                            'metrics': metrics,
                            'service_status': scoped_service_status,
                            'anomalies': scoped_anomalies,
                            'analysis': enhanced_analysis,
                            'structured_rca': structured_rca,
                            'issue_scope': issue_scope,
                            'pr': pr_metadata,
                            'similar_incidents': similar_incidents,
                            'remedial_actions': remedial_actions,
                            'dependency_context': scoped_dependency_context,
                            'status': 'active'
                        }

                        signature = self._incident_signature(incident)
                        existing = None
                        for active in self.active_incidents:
                            if self._incident_signature(active) == signature and active.get('status') == 'active':
                                existing = active
                                break

                        if existing:
                            # Check if existing incident is recent (within 15 min) - don't recreate
                            existing_time = existing.get('timestamp', '')
                            if existing_time:
                                try:
                                    existing_dt = datetime.fromisoformat(existing_time.replace('Z', '+00:00'))
                                    if existing_dt.tzinfo is not None:
                                        existing_dt = existing_dt.astimezone().replace(tzinfo=None)
                                    age_minutes = (datetime.now() - existing_dt).total_seconds() / 60
                                    if age_minutes < 15:
                                        # Just update, don't recreate incident
                                        existing['timestamp'] = incident['timestamp']
                                        existing['metrics'] = incident['metrics']
                                        existing['service_status'] = incident['service_status']
                                        existing['anomalies'] = incident['anomalies']
                                        existing['analysis'] = incident['analysis']
                                        existing['similar_incidents'] = incident['similar_incidents']
                                        existing['remedial_actions'] = incident['remedial_actions']
                                        existing['dependency_context'] = incident['dependency_context']
                                        # Don't mark as new incident - just update
                                        skip_new_incident = True
                                    else:
                                        skip_new_incident = False
                                except Exception:
                                    skip_new_incident = False
                            else:
                                skip_new_incident = False
                        else:
                            skip_new_incident = False

                        if not skip_new_incident:
                            # Add to active incidents
                            self.active_incidents.append(incident)

                        # Track compact resolution memory by root cause
                        issue_key = (enhanced_analysis.get('exact_issues') or [enhanced_analysis.get('summary', 'unknown')])[0]
                        self.resolution_memory[issue_key] = {
                            'timestamp': datetime.now().isoformat(),
                            'service': primary_service,
                            'root_cause': issue_key,
                            'error_category': corrected_category,
                            'confidence': enhanced_analysis.get('confidence', 0.0)
                        }
                        
                        # Send enhanced alert to Slack
                        self.send_alert_if_persistent(incident, scoped_anomalies, enhanced_analysis, similar_incidents, remedial_actions)
                        
                        # Record incident for learning
                        self.record_incident(metrics, scoped_logs_traces, enhanced_analysis)

                # Always auto-resolve/trim incidents every cycle, even when no actionable anomalies
                for active in self.active_incidents:
                    if active.get('status') != 'active':
                        continue
                    if self._is_incident_resolved(active, service_status):
                        active['status'] = 'resolved'
                        active['resolved_at'] = datetime.now().isoformat()
                        self.resolved_incidents.append(active)
                self._trim_incidents()
                    
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(check_interval)

    def _extract_primary_service(self, anomalies):
        """Extract primary impacted service from anomalies."""
        if anomalies:
            for anomaly in anomalies:
                if anomaly.get('service'):
                    return anomaly.get('service')
        services = self.config.get('services', [])
        if services:
            return services[0].get('name', 'unknown')
        return 'unknown'
    
    def collect_context(self, anomalies):
        """Collect logs and traces related to anomalies (ES-only mode)."""
        return {'logs': [], 'traces': []}

    def collect_service_context(self, service_status, window_minutes: Optional[int] = None):
        """Collect logs and context from Kubernetes services"""
        context = {
            'logs': [],
            'traces': [],
            'service_errors': []
        }

        monitoring_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        use_sidecar_otel_traces = bool(monitoring_cfg.get('use_sidecar_otel_traces', False))
        configured_trace_window = int(self.config.get('elasticsearch', {}).get('trace_window_minutes', 30) or 30)
        trace_window_minutes = int(window_minutes or configured_trace_window or 30)
        trace_window_minutes = max(1, trace_window_minutes)
        context_log_limit = int(self.config.get('elasticsearch', {}).get('context_log_limit', 40) or 40)

        if use_sidecar_otel_traces:
            sidecar_traces = self._collect_sidecar_traces(trace_window_minutes)
            context['traces'].extend(sidecar_traces)
            self._remember_recent_traces(sidecar_traces)

            if not context['traces']:
                context['traces'].extend(self._recent_traces_within(trace_window_minutes))
        
        # Collect error logs from services with issues
        for service_key, status in service_status.items():
            service_name = status.get('name', service_key.split('/')[-1])
            namespace = status.get('namespace', 'unknown')
            service_ref = f"{namespace}/{service_name}"

            if status.get('recent_errors'):
                # Keep service attribution on each error entry so scoped RCA does not leak
                # one service's stack trace into another service's incident.
                for error in status['recent_errors'][:10]:  # Limit to 10 errors per service
                    if not isinstance(error, dict):
                        continue
                    normalized_error = dict(error)
                    normalized_error['service'] = normalized_error.get('service') or service_ref
                    context['service_errors'].append(normalized_error)

                    # Add error messages to logs for analysis
                    context['logs'].append({
                        'service': service_ref,
                        'message': str(error.get('message', '') or ''),
                        'timestamp': error.get('timestamp', datetime.now().isoformat()),
                        'severity': str(error.get('severity', 'ERROR') or 'ERROR'),
                        'dependency': error.get('dependency', '')
                    })

            # Trace collection from Elasticsearch (OTel spans forwarded to storage)
            trace_limit = int(self.config.get('elasticsearch', {}).get('trace_limit', 25) or 25)
            should_collect_traces = status.get('status') in {'degraded', 'down', 'warning', 'pending'} or bool(status.get('recent_errors'))
            end_time_ms = int(time.time() * 1000)
            start_time_ms = int((time.time() - trace_window_minutes * 60) * 1000)

            if self.elasticsearch is not None and should_collect_traces:
                if not use_sidecar_otel_traces:
                    try:
                        raw_traces = self.elasticsearch.get_traces(
                            service=service_name,
                            start_time=start_time_ms,
                            end_time=end_time_ms,
                            limit=trace_limit
                        )
                    except Exception:
                        raw_traces = []

                    for trace in raw_traces:
                        if not isinstance(trace, dict):
                            continue
                        normalized = dict(trace)
                        normalized['service'] = normalized.get('service') or service_ref
                        edge = self._extract_trace_service_edge(normalized, fallback_source=service_name)
                        if edge:
                            normalized['service'] = edge['from']
                            normalized['downstream_service'] = edge['to']
                        context['traces'].append(normalized)

                # Always pull raw logs from ES for RCA, even when sidecar traces are enabled.
                try:
                    raw_logs = self.elasticsearch.get_logs(
                        service=service_name,
                        start_time=start_time_ms,
                        end_time=end_time_ms,
                        limit=context_log_limit,
                        namespace=namespace
                    )
                except Exception:
                    raw_logs = []

                for log in raw_logs:
                    if not isinstance(log, dict):
                        continue
                    msg = str(
                        log.get('message')
                        or log.get('body')
                        or log.get('log')
                        or log.get('msg')
                        or ''
                    ).strip()
                    if not msg:
                        continue

                    sev = str(
                        log.get('severity')
                        or log.get('severity_text')
                        or log.get('log.level')
                        or log.get('level')
                        or 'UNKNOWN'
                    ).upper()
                    ts = str(
                        log.get('@timestamp')
                        or log.get('timestamp')
                        or log.get('time')
                        or datetime.now().isoformat()
                    )

                    lowered = msg.lower()
                    trace_marker_present = 'trace_id' in lowered or 'traceid' in lowered
                    if not trace_marker_present and sev not in {'ERROR', 'FATAL', 'CRITICAL'} and not self._is_actionable_error_message(msg):
                        continue

                    context['logs'].append({
                        'service': service_ref,
                        'message': msg[:1200],
                        'timestamp': ts,
                        'severity': sev,
                        'dependency': self._extract_dependency_from_message(msg)
                    })
                     
        logger.info(f"Collected service context: {len(context['service_errors'])} errors, {len(context['logs'])} log entries, {len(context['traces'])} traces")
        return context

    def build_dependency_context(self, service_status, logs_traces):
        """Build cross-service dependency graph signals from observed errors/traces."""
        dependency_edges = {}
        impacted_services = set()

        for source_service, status in service_status.items():
            metrics = status.get('metrics', {})
            failed_dependencies = metrics.get('failed_dependencies', []) or []
            for dep in failed_dependencies:
                key = (source_service, dep)
                if key not in dependency_edges:
                    dependency_edges[key] = {
                        'from': source_service,
                        'to': dep,
                        'signals': set(),
                        'count': 0
                    }
                dependency_edges[key]['signals'].add('log_dependency_failure')
                dependency_edges[key]['count'] += 1
                impacted_services.update([source_service, dep])

        for trace in logs_traces.get('traces', []):
            source_service = trace.get('service')
            target_service = trace.get('downstream_service')
            if source_service and target_service:
                key = (source_service, target_service)
                if key not in dependency_edges:
                    dependency_edges[key] = {
                        'from': source_service,
                        'to': target_service,
                        'signals': set(),
                        'count': 0,
                        'error_count': 0,
                        'latencies': [],
                        'trace_ids': set(),
                        'last_seen': ''
                    }
                dependency_edges[key]['signals'].add('trace_call_signal')
                dependency_edges[key]['count'] += 1
                if self._trace_is_error(trace):
                    dependency_edges[key]['signals'].add('trace_error_signal')
                    dependency_edges[key]['error_count'] += 1

                duration_ms = float(trace.get('duration_ms', 0.0) or 0.0)
                if duration_ms > 0:
                    dependency_edges[key]['latencies'].append(duration_ms)

                trace_id = str(trace.get('trace_id', '') or '')
                if trace_id:
                    dependency_edges[key]['trace_ids'].add(trace_id)

                start_ns = int(trace.get('start_ns', 0) or 0)
                if start_ns > 0:
                    last_seen = datetime.fromtimestamp(start_ns / 1_000_000_000).isoformat()
                    if not dependency_edges[key]['last_seen'] or last_seen > dependency_edges[key]['last_seen']:
                        dependency_edges[key]['last_seen'] = last_seen
                impacted_services.update([source_service, target_service])

        edges = []
        for edge in dependency_edges.values():
            count = max(1, int(edge.get('count', 0)))
            error_count = int(edge.get('error_count', 0))
            error_rate = (error_count / count) * 100.0
            p95_latency_ms = self._percentile(edge.get('latencies', []), 0.95)
            edges.append({
                'from': edge['from'],
                'to': edge['to'],
                'signals': sorted(list(edge['signals'])),
                'count': count,
                'error_count': error_count,
                'error_rate': round(error_rate, 2),
                'p95_latency_ms': round(p95_latency_ms, 2),
                'sample_trace_ids': sorted(list(edge.get('trace_ids', set())))[:3],
                'last_seen': edge.get('last_seen', '')
            })

        edges.sort(key=lambda item: (item.get('error_count', 0), item.get('count', 0)), reverse=True)
        return {
            'edges': edges[:20],
            'impacted_services': sorted(list(impacted_services))
        }

    def _derive_trace_root_cause(self, anomalies: List[Dict], dependency_context: Dict, logs_traces: Dict) -> Dict[str, str]:
        """Derive exact root cause from trace-linked dependency failures."""
        result = {
            'issue': '',
            'recommendation': '',
            'confidence_boost': 0.0
        }

        per_trace = self._derive_per_trace_failure_root_cause(anomalies, logs_traces)
        if per_trace.get('issue'):
            return per_trace

        edges = dependency_context.get('edges', []) if isinstance(dependency_context, dict) else []
        if not edges:
            return result

        impacted = set()
        for anomaly in anomalies or []:
            service = str(anomaly.get('service', '') or '')
            if not service:
                continue
            short = service.split('/')[-1]
            impacted.add(service)
            impacted.add(short)
            impacted.add(self._canonical_service(short))

        candidate_edges = []
        for edge in edges:
            source = str(edge.get('from', '') or '')
            target = str(edge.get('to', '') or '')
            source_short = source.split('/')[-1]
            target_short = target.split('/')[-1]
            if impacted and (
                source in impacted or source_short in impacted or self._canonical_service(source_short) in impacted or
                target in impacted or target_short in impacted or self._canonical_service(target_short) in impacted
            ):
                candidate_edges.append(edge)

        if not candidate_edges:
            candidate_edges = edges

        error_edges = [edge for edge in candidate_edges if int(edge.get('error_count', 0) or 0) > 0]
        if error_edges:
            error_edges.sort(
                key=lambda edge: (
                    float(edge.get('error_rate', 0.0) or 0.0),
                    int(edge.get('error_count', 0) or 0),
                    float(edge.get('p95_latency_ms', 0.0) or 0.0)
                ),
                reverse=True
            )
            top = error_edges[0]

            source_service = str(top.get('from', '') or '')
            target_service = str(top.get('to', '') or '')
            target_canonical = self._canonical_service(target_service).lower()
            edge_evidence = self._collect_edge_failure_evidence(top, logs_traces)
            startup_hint, crashloop_present = self._derive_startup_failure_hint(source_service, anomalies, logs_traces)

            if edge_evidence.get('has_503', False):
                chain_tail = f" -> {startup_hint} -> CrashLoopBackOff" if crashloop_present else f" -> {startup_hint}"
                result['issue'] = (
                    f"Exact Issue: {source_service} -> {target_service} returned HTTP 503{chain_tail}"
                )
                result['recommendation'] = (
                    f"Check downstream {target_service} health/availability and validate caller configuration, then restart impacted pods"
                )
                result['confidence_boost'] = 0.33
                return result

            issue_hint, issue_detail = self._classify_edge_issue(top, logs_traces)
            trace_hint = top.get('sample_trace_ids', [])
            trace_text = f", trace_id={trace_hint[0]}" if trace_hint else ""
            result['issue'] = (
                f"Exact Issue: {top.get('from')} -> {top.get('to')} failing "
                f"({top.get('error_count')}/{top.get('count')} error spans, p95 {top.get('p95_latency_ms')}ms, {issue_hint}{trace_text})"
            )
            result['recommendation'] = (
                f"Inspect downstream service {top.get('to')} for {issue_detail} and failing requests from {top.get('from')}"
            )
            result['confidence_boost'] = 0.25
            return result

        latency_threshold = float(self.config.get('monitoring', {}).get('trace_latency_threshold_ms', 1500) or 1500)
        slow_edges = [edge for edge in candidate_edges if float(edge.get('p95_latency_ms', 0.0) or 0.0) >= latency_threshold]
        if slow_edges:
            slow_edges.sort(key=lambda edge: float(edge.get('p95_latency_ms', 0.0) or 0.0), reverse=True)
            top = slow_edges[0]
            result['issue'] = (
                f"Exact Issue: Trace latency spike on {top.get('from')} -> {top.get('to')} "
                f"(p95 {top.get('p95_latency_ms')}ms across {top.get('count')} spans)"
            )
            result['recommendation'] = (
                f"Check latency bottleneck in downstream service {top.get('to')} and network path from {top.get('from')}"
            )
            result['confidence_boost'] = 0.15

        return result

    def _structured_rca_from_issue(self, issue_text: str) -> Dict[str, str]:
        """Parse issue text into structured RCA fields for dashboard/API."""
        structured = {
            'root_cause': issue_text or '',
            'failing_service': '',
            'trace_id': '',
            'status_code': '',
            'exception': '',
            'symptom': ''
        }
        text = str(issue_text or '')
        lower = text.lower()

        chain_match = re.search(r'failed at\s+([a-zA-Z0-9_./-]+)', text)
        if chain_match:
            structured['failing_service'] = chain_match.group(1)

        trace_match = re.search(r'\btrace\s+([a-fA-F0-9]{16,64})\b|\btrace_id[:=]([a-fA-F0-9]{16,64})\b', text)
        if trace_match:
            structured['trace_id'] = trace_match.group(1) or trace_match.group(2) or ''

        status_match = re.search(r'status_code\s*=\s*([0-9]{3})|HTTP\s+([0-9]{3})', text, re.IGNORECASE)
        if status_match:
            structured['status_code'] = status_match.group(1) or status_match.group(2) or ''

        exc_match = re.search(r'exception\s*=\s*([A-Za-z0-9_.$-]+(?:Exception|Error))|([A-Za-z0-9_.$-]+(?:Exception|Error))', text)
        if exc_match:
            structured['exception'] = exc_match.group(1) or exc_match.group(2) or ''

        if 'crashloopbackoff' in lower:
            structured['symptom'] = 'CrashLoopBackOff'
        elif 'image pull' in lower or 'imagepullbackoff' in lower:
            structured['symptom'] = 'ImagePullBackOff'
        elif 'no running pods' in lower or 'no pods' in lower:
            structured['symptom'] = 'NoPods'
        elif structured['status_code']:
            structured['symptom'] = f"HTTP {structured['status_code']}"

        return structured

    def _is_deterministic_fix_candidate(self, issue_text: str) -> bool:
        """Return True when issue matches one of current source-fix rule classes."""
        issue = str(issue_text or '')
        if not issue:
            return False

        if re.search(r'(?i)unknown\s+name\s+value\s*\[[^\]]+\]\s*for\s*enum\s+class\s*\[[^\]]+\]', issue):
            return True
        if re.search(r"(?i)(?:invalid|unknown|unsupported|unexpected|illegal)[^\n]{0,120}?value\s+['\"][^'\"]+['\"][^\n]{0,120}?for\s+[a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+", issue):
            return True
        if re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?[a-zA-Z0-9_.-]+[\'\"]?', issue):
            return True
        if re.search(r'(?i)placeholder.*not\s+found|missing.*config', issue):
            return True
        if re.search(r'(?i)oomkilled|out\s+of\s+memory|memory\s+limit|killed\s+by\s+memory', issue):
            return True
        if re.search(r'(?i)job\s+threw\s+an\s+unhandled\s+exception', issue):
            return True
        
        # Allow ANY issue with an exception type - let Ollama try to generate fix
        has_exception = bool(re.search(r'\b([a-zA-Z0-9_.$]+(?:Exception|Error))\b', issue, re.IGNORECASE))
        if has_exception:
            return True
        
        # Allow issues with clear error messages even without exceptions
        if re.search(r'(?i)caused\s+by:|\bno\s+[a-z0-9_.\- ]{2,80}\s+found\b|\b(invalid|illegal|unsupported|unexpected|missing|required)\b', issue):
            return True
            
        return False

    def _recover_deterministic_issue_for_service(
        self,
        service_key: str,
        scoped_logs_traces: Optional[Dict],
        scoped_service_status: Optional[Dict],
        scan_limit: int = 180
    ) -> str:
        """Recover deterministic code-fix issue from scoped status/log context."""
        service = str(service_key or '').strip()
        if not service:
            return ''

        # Direct pattern check for enum issues - most reliable recovery
        issue_lower = str(service_key or '').lower()
        
        # Check scoped_service_status for enum issues in any field
        if isinstance(scoped_service_status, dict):
            for svc_key, state in scoped_service_status.items():
                if not isinstance(state, dict):
                    continue
                # Check all string fields in state for enum patterns
                for field_name, field_value in state.items():
                    if not isinstance(field_value, str):
                        continue
                    field_str = str(field_value or '').strip()
                    if not field_str:
                        continue
                    # Direct enum pattern check
                    if re.search(r'(?i)invalid\s+enum\s+value|enum\s+.*value\s+.*for\s+\w+\.\w+|unknown\s+name\s+value.*enum', field_str):
                        if self._is_deterministic_fix_candidate(field_str):
                            self._service_deterministic_issue_memory[service] = {
                                'issue': field_str,
                                'timestamp': datetime.now().isoformat()
                            }
                            logger.info("Direct enum recovery for %s: %s", service, field_str[:100])
                            return field_str

        mem = self._service_deterministic_issue_memory.get(service, {}) if isinstance(self._service_deterministic_issue_memory, dict) else {}
        mem_issue = str(mem.get('issue', '') or '').strip()
        mem_ts = str(mem.get('timestamp', '') or '')
        if mem_issue and mem_ts:
            try:
                mem_dt = datetime.fromisoformat(mem_ts.replace('Z', '+00:00'))
                if mem_dt.tzinfo is not None:
                    mem_dt = mem_dt.astimezone().replace(tzinfo=None)
                if (datetime.now() - mem_dt).total_seconds() <= 1800 and self._is_deterministic_fix_candidate(mem_issue):
                    return mem_issue
            except Exception:
                pass

        # Use last known service-table snapshot as an additional deterministic source.
        if isinstance(self._last_service_status, dict):
            snapshot_state = self._last_service_status.get(service)
            if isinstance(snapshot_state, dict):
                snapshot_candidates = []
                for field in ['root_cause', 'exact_issue']:
                    val = str(snapshot_state.get(field, '') or '').strip()
                    if val:
                        snapshot_candidates.append(val)
                deep = snapshot_state.get('deep_inspection', {}) if isinstance(snapshot_state.get('deep_inspection', {}), dict) else {}
                for field in ['root_cause', 'exact_issue', 'reason']:
                    val = str(deep.get(field, '') or '').strip()
                    if val:
                        snapshot_candidates.append(val)
                for raw_msg in snapshot_candidates:
                    parsed = self._derive_exact_from_message(service, raw_msg, crashloop_present=('crashloopbackoff' in raw_msg.lower()))
                    parsed_issue = str((parsed or {}).get('issue', '') or '').strip() if isinstance(parsed, dict) else ''
                    issue = parsed_issue or raw_msg
                    if self._is_deterministic_fix_candidate(issue):
                        self._service_deterministic_issue_memory[service] = {
                            'issue': issue,
                            'timestamp': datetime.now().isoformat()
                        }
                        return issue

        namespace = service.split('/', 1)[0] if '/' in service else 'unknown'
        short = self._canonical_service(service.split('/')[-1]) or service.split('/')[-1]

        def _extract(entry: Dict) -> str:
            if not isinstance(entry, dict):
                return ''
            return str(
                entry.get('message')
                or entry.get('body')
                or entry.get('log')
                or entry.get('msg')
                or ''
            ).strip()

        candidates: List[str] = []
        if isinstance(scoped_service_status, dict):
            for _, state in scoped_service_status.items():
                if not isinstance(state, dict):
                    continue

                for field_value in [
                    state.get('root_cause', ''),
                    state.get('exact_issue', ''),
                    (state.get('deep_inspection', {}) or {}).get('root_cause', '') if isinstance(state.get('deep_inspection', {}), dict) else '',
                    (state.get('deep_inspection', {}) or {}).get('exact_issue', '') if isinstance(state.get('deep_inspection', {}), dict) else '',
                    (state.get('deep_inspection', {}) or {}).get('reason', '') if isinstance(state.get('deep_inspection', {}), dict) else ''
                ]:
                    msg = str(field_value or '').strip()
                    if msg:
                        candidates.append(msg)

                pod_state = state.get('pod_status', {}) if isinstance(state.get('pod_status', {}), dict) else {}
                for pod in (pod_state.get('pods', []) or [])[:8]:
                    if not isinstance(pod, dict):
                        continue
                    reason = str(pod.get('reason', '') or '').strip()
                    if reason:
                        candidates.append(reason)

                for err in (state.get('recent_errors', []) or [])[:16]:
                    msg = _extract(err)
                    if msg:
                        candidates.append(msg)

        if isinstance(scoped_logs_traces, dict):
            for key, cap in (('service_errors', 80), ('logs', 120)):
                stream = scoped_logs_traces.get(key, []) or []
                if not isinstance(stream, list):
                    continue
                for entry in stream[:cap]:
                    msg = _extract(entry)
                    if msg:
                        candidates.append(msg)

        should_fetch_es = True
        last_fetch = self._deterministic_recovery_last_fetch.get(service)
        if isinstance(last_fetch, datetime):
            if (datetime.now() - last_fetch).total_seconds() < 300:
                should_fetch_es = False

        if self.elasticsearch is not None and should_fetch_es:
            try:
                self._deterministic_recovery_last_fetch[service] = datetime.now()
                end_ms = int(time.time() * 1000)
                start_ms = end_ms - int(60 * 60 * 1000)
                backfill = self.elasticsearch.get_logs(
                    service=short,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=max(80, int(scan_limit)),
                    namespace=namespace
                )
                for raw in backfill if isinstance(backfill, list) else []:
                    msg = _extract(raw)
                    if msg:
                        candidates.append(msg)
            except Exception:
                pass

        seen = set()
        for raw_msg in candidates:
            low = str(raw_msg or '').lower()
            if not low or low in seen:
                continue
            seen.add(low)
            if self._matches_ignored_log_pattern(raw_msg):
                continue
            parsed = self._derive_exact_from_message(
                service,
                raw_msg,
                crashloop_present=('crashloopbackoff' in low)
            )
            parsed_issue = str((parsed or {}).get('issue', '') or '').strip() if isinstance(parsed, dict) else ''
            if parsed_issue and self._is_deterministic_fix_candidate(parsed_issue):
                return parsed_issue
            if self._is_deterministic_fix_candidate(raw_msg):
                return raw_msg

        return ''

    def _seed_deterministic_memory_from_service_status(self, service_status: Dict) -> None:
        """Seed deterministic memory from service table exact issues."""
        if not isinstance(service_status, dict):
            return
        for svc_key, state in service_status.items():
            if not isinstance(state, dict):
                continue
            service = str(svc_key or '').strip()
            if not service:
                continue
            candidates = []
            for field in ['root_cause', 'exact_issue']:
                val = str(state.get(field, '') or '').strip()
                if val:
                    candidates.append(val)
            deep = state.get('deep_inspection', {}) if isinstance(state.get('deep_inspection', {}), dict) else {}
            for field in ['root_cause', 'exact_issue', 'reason']:
                val = str(deep.get(field, '') or '').strip()
                if val:
                    candidates.append(val)
            for err in (state.get('recent_errors', []) or [])[:6]:
                if not isinstance(err, dict):
                    continue
                msg = str(err.get('message', '') or '').strip()
                if msg:
                    candidates.append(msg)

            for msg in candidates:
                parsed = self._derive_exact_from_message(service, msg, crashloop_present=('crashloopbackoff' in msg.lower()))
                parsed_issue = str((parsed or {}).get('issue', '') or '').strip() if isinstance(parsed, dict) else ''
                issue = parsed_issue or msg
                if self._is_deterministic_fix_candidate(issue):
                    self._service_deterministic_issue_memory[service] = {
                        'issue': issue,
                        'timestamp': datetime.now().isoformat()
                    }
                    break

    def _classify_issue_scope(self, issue_text: str) -> str:
        """Classify issue into infra/config/code for PR gating."""
        text = str(issue_text or '').lower()
        if not text:
            return 'unknown'

        # Treat OOM/resource kill as config manifest issue (eligible for k8s-manifest PR).
        if any(token in text for token in [
            'oomkilled', 'oom killed', 'outofmemoryerror', 'container killed due to memory',
            'evicted', 'memory limit', 'memory cgroup out of memory'
        ]):
            return 'config'

        # Concrete runtime exceptions should be treated as code evidence unless
        # they are clearly infra/network-only failures.
        if re.search(r'\b([a-zA-Z0-9_.$]+(?:Exception|Error))\b', str(issue_text or ''), re.IGNORECASE):
            if any(token in text for token in [
                'connection refused', 'timeout', 'deadline exceeded', 'service unavailable',
                'read timed out', 'connect timed out', 'network', 'unreachable', 'dns'
            ]):
                return 'infra'
            return 'code'

        if any(token in text for token in [
            '401', '403', 'unauthorized', 'forbidden', 'invalid token', 'token expired',
            'jwt', 'permission denied', 'access denied', 'authentication', 'authorization'
        ]):
            return 'config'

        if any(token in text for token in [
            'pending', 'no pods', 'failedscheduling', 'imagepullbackoff', 'errimagepull',
            'connection refused', 'timeout', 'http 5', 'returned http 503', 'latency spike',
            'network', 'unreachable', 'crashloopbackoff', 'dns', 'service unavailable',
            'unhealthy pods', 'not ready', 'readiness probe', 'liveness probe',
            'vault', 'sealed', 'secret not found', 'kv v2', 'permission denied on secret path'
        ]):
            return 'infra'

        if any(token in text for token in [
            'missing config', 'invalid config binding', 'placeholder', 'vault', 'secret',
            'bean init failure', 'startup failed due to'
        ]):
            return 'config'

        if any(token in text for token in [
            '.java:', '.kt:', '.py:', '.go:', 'autoconfiguration.class', 'failed while loading',
            'exception=', 'nullpointerexception', 'indexoutofboundsexception', 'illegalargumentexception',
            'beancreationexception', 'unsatisfieddependencyexception', 'stacktrace', 'traceback'
        ]):
            return 'code'

        if re.search(
            r"(?i)\b(invalid|unknown|unsupported|unexpected|illegal)\b.{0,80}?\bvalue\b.{0,120}?\bfor\b\s+([a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+)",
            text
        ):
            return 'code'

        return 'unknown'

    def _pr_candidate_for_issue(self, issue_text: str) -> bool:
        """Only code-level issues are PR candidates."""
        return self._classify_issue_scope(issue_text) == 'code'

    def _derive_repo_name_for_service(self, service_name: str) -> str:
        """Map service name to expected repo name convention."""
        short = self._canonical_service(str(service_name or '').split('/')[-1])
        
        # Try config overrides first
        overrides = self.config.get('pr_automation', {}).get('repo_overrides', {}) if isinstance(self.config, dict) else {}
        if isinstance(overrides, dict):
            for key in [str(service_name or ''), short, str(service_name or '').split('/')[-1]]:
                if key not in overrides:
                    continue
                override = overrides.get(key)
                if isinstance(override, dict):
                    for repo_key in ('repo', 'repo_name', 'name'):
                        repo_value = str(override.get(repo_key, '') or '').strip()
                        if repo_value:
                            return repo_value
                elif str(override or '').strip():
                    return str(override).strip()
        
        # Hardcoded mappings for known services
        if short == 'cacheservice':
            return 'cache-service'
        
        return short

    def _derive_repo_url_for_service(self, service_name: str) -> List[str]:
        """Generate list of possible repo URLs to try cloning."""
        short = self._canonical_service(str(service_name or '').split('/')[-1])
        repo_base_url = str(self.config.get('pr_automation', {}).get('repo_base_url', 'https://github.com/fabhotelstech') or 'https://github.com/fabhotelstech').rstrip('/')
        
        # Generate possible repo names
        possible_names = [short]
        
        # Add variations based on service name patterns
        if 'tripmanagement' in short.lower() or 'trip' in short.lower():
            possible_names.extend(['trip-management-service', 'trip-management', 'tripmanagement-service'])
        if 'booking' in short.lower():
            possible_names.extend([f'{short}-service', short.replace('booking', 'booking-service')])
        if 'b2b' in short.lower():
            possible_names.extend([f'{short}-service'])
        if 'bus-booking' in short.lower():
            possible_names.extend(['bus-booking-search-service', 'bus-booking-service'])
        
        # Remove duplicates
        possible_names = list(dict.fromkeys(possible_names))
        
        # Generate URLs
        urls = []
        for name in possible_names:
            urls.append(f"{repo_base_url}/{name}.git")
        
        return urls

    def _derive_target_branch_for_service(self, service_name: str, default_branch: str) -> str:
        """Resolve per-service target branch override when configured."""
        short = self._canonical_service(str(service_name or '').split('/')[-1])
        fallback = str(default_branch or 'develop_mercury').strip() or 'develop_mercury'
        overrides = self.config.get('pr_automation', {}).get('repo_overrides', {}) if isinstance(self.config, dict) else {}
        if not isinstance(overrides, dict):
            return fallback

        for key in [str(service_name or ''), short, str(service_name or '').split('/')[-1]]:
            if key not in overrides:
                continue
            override = overrides.get(key)
            if not isinstance(override, dict):
                continue
            for branch_key in ('branch', 'target_branch', 'base_branch'):
                branch_value = str(override.get(branch_key, '') or '').strip()
                if branch_value:
                    return branch_value
        return fallback

    def _issue_search_tokens(self, issue_text: str, structured_rca: Optional[Dict[str, str]] = None) -> List[str]:
        """Extract search tokens to locate likely failing code."""
        tokens = []
        text = str(issue_text or '')
        structured = structured_rca if isinstance(structured_rca, dict) else {}

        for value in [
            structured.get('exception', ''),
            structured.get('failing_service', ''),
            structured.get('status_code', ''),
            structured.get('trace_id', '')
        ]:
            raw = str(value or '').strip()
            if raw:
                tokens.append(raw)

        file_hint_match = re.findall(r'([A-Za-z0-9_.$/-]+\.(?:java|kt|py|go):\d+|[A-Za-z0-9_.$-]+\.class)', text)
        for item in file_hint_match:
            tokens.append(str(item))

        word_matches = re.findall(r'([A-Za-z_][A-Za-z0-9_.$-]*(?:Exception|Error))', text)
        for item in word_matches:
            tokens.append(str(item))

        # Deterministic enum/value patterns for source-fix search.
        enum_patterns = [
            r'(?i)unknown\s+name\s+value\s*\[([^\]]+)\]\s*for\s*enum\s+class\s*\[([^\]]+)\]',
            r'(?i)(?:invalid|unknown|unsupported|unexpected|illegal)[^\n]{0,120}?value\s+[\'\"]([^\'\"]+)[\'\"][^\n]{0,120}?for\s+([a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+)'
        ]
        for pattern in enum_patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            enum_value = str(match.group(1) or '').strip()
            enum_fqcn = str(match.group(2) or '').strip()
            enum_class = enum_fqcn.split('.')[-1] if enum_fqcn else ''
            if enum_fqcn:
                tokens.append(enum_fqcn)
            if enum_class:
                tokens.append(enum_class)
            if enum_value:
                tokens.append(enum_value)
            break

        # Add core service identifiers as weak anchors.
        failing_service = str(structured.get('failing_service', '') or '').split('/')[-1].strip()
        if failing_service:
            tokens.append(failing_service)

        normalized = []
        seen = set()
        for token in tokens:
            cleaned = str(token).strip().strip('"\'')
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(cleaned)
        return normalized[:12]

    def _analyze_repo_for_issue(
        self,
        service_name: str,
        issue_text: str,
        structured_rca: Optional[Dict[str, str]] = None,
        keep_clone_override: Optional[bool] = None
    ) -> Dict[str, object]:
        """Clone service repo and locate likely failing files from issue evidence."""
        result: Dict[str, object] = {
            'ok': False,
            'reason': '',
            'repo_name': '',
            'repo_url': '',
            'branch': '',
            'clone_path': '',
            'candidate_files': []
        }

        cfg = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
        enabled = bool(cfg.get('enabled', False))
        if not enabled:
            result['reason'] = 'PR automation disabled in config (pr_automation.enabled=false)'
            return result

        repo_name = self._derive_repo_name_for_service(service_name)
        short = self._canonical_service(str(service_name or '').split('/')[-1])
        repo_base_url = str(cfg.get('repo_base_url', 'https://github.com/fabhotelstech') or 'https://github.com/fabhotelstech').rstrip('/')
        target_branch = self._derive_target_branch_for_service(
            service_name,
            str(cfg.get('target_branch', 'develop_mercury') or 'develop_mercury')
        )
        workspace = str(cfg.get('workspace', '/tmp/ai-agent-pr-work') or '/tmp/ai-agent-pr-work')
        max_files = int(cfg.get('max_candidate_files', 12) or 12)
        keep_clone = bool(cfg.get('keep_clone', False)) if keep_clone_override is None else bool(keep_clone_override)

        result['repo_name'] = repo_name
        result['repo_url'] = f"{repo_base_url}/{repo_name}.git"
        result['branch'] = target_branch

        if shutil.which('git') is None:
            result['reason'] = "git binary not found in ai-monitoring-agent container; install git in image"
            return result

        try:
            os.makedirs(workspace, exist_ok=True)
        except Exception as e:
            result['reason'] = f'Failed to prepare workspace: {e}'
            return result

        # Try multiple possible repo URLs
        possible_urls = self._derive_repo_url_for_service(service_name)
        logger.info("Trying to clone repo for %s: attempting %d possible URLs", service_name, len(possible_urls))
        
        clone_success = False
        cloned_url = None
        clone_path = ''
        
        gh_token = str(os.getenv('GITHUB_TOKEN') or os.getenv('GH_TOKEN') or os.getenv('gh_token') or '').strip()
        
        for repo_url in possible_urls:
            issue_hash = hashlib.sha1(f"{service_name}|{repo_url}".encode('utf-8')).hexdigest()[:10]
            clone_path = os.path.join(workspace, f"{short}-{issue_hash}")
            
            if os.path.isdir(clone_path):
                try:
                    shutil.rmtree(clone_path)
                except Exception:
                    pass
            
            # Build clone command
            clone_cmd = [
                'git', 'clone', '--depth', '1', '--branch', target_branch,
                repo_url, clone_path
            ]
            
            logger.info("Clone attempt for %s: repo=%s branch=%s", service_name, repo_url, target_branch)
            
            if gh_token:
                try:
                    parsed = urlparse(repo_url)
                    if parsed.scheme in {'http', 'https'} and parsed.netloc:
                        safe_token = quote(gh_token, safe='')
                        auth_url = f"{parsed.scheme}://x-access-token:{safe_token}@{parsed.netloc}{parsed.path}"
                        if repo_url in clone_cmd:
                            clone_cmd[clone_cmd.index(repo_url)] = auth_url
                except Exception:
                    pass
            
            clone_proc = subprocess.run(clone_cmd, capture_output=True, text=True, timeout=120)
            
            if clone_proc.returncode == 0:
                clone_success = True
                cloned_url = repo_url
                logger.info("Clone succeeded for %s: repo=%s", service_name, repo_url)
                break
            else:
                logger.info("Clone failed for %s: repo=%s, trying next...", service_name, repo_url)
        
        if not clone_success:
            result['reason'] = f"Clone failed: Tried {len(possible_urls)} possible repos but none worked"
            return result
        
        result['repo_url'] = cloned_url
        result['clone_path'] = str(clone_path) if clone_path else ''
        clone_path = str(clone_path) if clone_path else ''

        tokens = self._issue_search_tokens(issue_text, structured_rca)
        if not tokens:
            result['reason'] = 'No deterministic search tokens derived from issue evidence'
            if not keep_clone:
                shutil.rmtree(clone_path, ignore_errors=True)
            return result

        ignore_dirs = {
            '.git', 'node_modules', 'target', 'build', 'dist', '.idea', '.vscode',
            '__pycache__', '.venv', 'venv', 'vendor', '.mvn'
        }
        allow_ext = {
            '.java', '.kt', '.py', '.go', '.js', '.ts', '.tsx', '.yml', '.yaml', '.properties', '.xml'
        }

        candidates: List[Tuple[int, str, str]] = []
        scanned_files = 0
        max_scan_files = int(cfg.get('max_scan_files', 2500) or 2500)

        for root, dirs, files in os.walk(clone_path):
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            for fname in files:
                if scanned_files >= max_scan_files:
                    break
                scanned_files += 1
                fpath = os.path.join(root, fname)
                ext = os.path.splitext(fname)[1].lower()
                if ext and ext not in allow_ext:
                    continue

                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                        content = fh.read(200_000)
                except Exception:
                    continue

                low = content.lower()
                score = 0
                matched_token = ''
                for token in tokens:
                    token_low = token.lower()
                    if token_low and token_low in low:
                        score += 3
                        if not matched_token:
                            matched_token = token

                if 'exception' in low and any(t.lower().endswith(('exception', 'error')) for t in tokens):
                    score += 2
                if score <= 0:
                    continue

                rel = os.path.relpath(fpath, clone_path)
                candidates.append((score, rel, matched_token or 'issue-token'))

            if scanned_files >= max_scan_files:
                break

        candidates.sort(key=lambda item: item[0], reverse=True)
        trimmed = candidates[:max(1, max_files)]
        result['candidate_files'] = [
            {'path': item[1], 'score': item[0], 'token': item[2]} for item in trimmed
        ]

        if not trimmed:
            result['reason'] = 'Repository analyzed but no confident file candidates found'
            if not keep_clone:
                shutil.rmtree(clone_path, ignore_errors=True)
            return result

        result['ok'] = True
        result['reason'] = f"Repository analyzed with {len(trimmed)} code candidates"
        if not keep_clone:
            shutil.rmtree(clone_path, ignore_errors=True)
            result['clone_path'] = '[cleaned]'
        return result

    def _prepare_repo_analysis_pr_branch(
        self,
        service_name: str,
        issue_text: str,
        structured_rca: Optional[Dict[str, str]],
        analysis: Optional[Dict]
    ) -> Dict[str, str]:
        """Create and push a branch with RCA findings, then open a PR URL."""
        outcome = {'ok': 'false', 'url': '', 'reason': ''}
        repo_analysis = self._analyze_repo_for_issue(
            service_name,
            issue_text,
            structured_rca,
            keep_clone_override=True
        )
        if not bool(repo_analysis.get('ok', False)):
            outcome['reason'] = str(repo_analysis.get('reason', 'Repository analysis failed'))
            return outcome

        if shutil.which('git') is None:
            outcome['reason'] = "git binary not found in ai-monitoring-agent container; install git in image"
            return outcome

        if shutil.which('gh') is None:
            outcome['reason'] = "gh binary not found in ai-monitoring-agent container; install GitHub CLI in image"
            return outcome

        clone_path = str(repo_analysis.get('clone_path', '') or '')
        repo_url = str(repo_analysis.get('repo_url', '') or '')
        target_branch = str(repo_analysis.get('branch', 'develop_mercury') or 'develop_mercury')
        raw_candidates = repo_analysis.get('candidate_files', [])
        candidates: List[Dict] = raw_candidates if isinstance(raw_candidates, list) else []
        if not clone_path or not os.path.isdir(clone_path):
            outcome['reason'] = 'Repository clone path missing after analysis'
            return outcome

        short_service = self._canonical_service(str(service_name or '').split('/')[-1]) or 'service'
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        branch_name = f"ai-agent-rca/{short_service}-{stamp}"
        report_dir = os.path.join(clone_path, '.ai-agent', 'rca')
        report_file = os.path.join(report_dir, f"{stamp}.md")

        structured = structured_rca if isinstance(structured_rca, dict) else {}
        conf = float((analysis or {}).get('confidence', 0.0) or 0.0)

        lines = [
            '# AI Agent RCA Report',
            '',
            f"- service: {service_name}",
            f"- generated_at: {datetime.now().isoformat()}",
            f"- confidence: {conf:.2f}",
            '',
            '## Exact Issue',
            str(issue_text or 'N/A'),
            '',
            '## Structured RCA',
            f"- failing_service: {structured.get('failing_service', '')}",
            f"- trace_id: {structured.get('trace_id', '')}",
            f"- status_code: {structured.get('status_code', '')}",
            f"- exception: {structured.get('exception', '')}",
            f"- symptom: {structured.get('symptom', '')}",
            '',
            '## Candidate Files',
        ]
        candidate_list = list(candidates[:12]) if isinstance(candidates, list) else []
        if candidate_list:
            for item in candidate_list:
                if not isinstance(item, dict):
                    continue
                lines.append(f"- {item.get('path', '')} (score={item.get('score', 0)}, token={item.get('token', '')})")
        else:
            lines.append('- N/A')
        lines.extend([
            '',
            '## Next Action',
            '- Review candidate files and apply deterministic code fix before merge.'
        ])

        try:
            os.makedirs(report_dir, exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
        except Exception as e:
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = f'Failed to write RCA report: {e}'
            return outcome

        try:
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(['git', 'add', '.ai-agent/rca'], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(
                [
                    'git', '-c', 'user.name=ai-monitoring-agent', '-c', 'user.email=ai-monitoring-agent@local',
                    'commit', '-m', f"add RCA report for {short_service}"
                ],
                cwd=clone_path,
                capture_output=True,
                text=True,
                timeout=30,
                check=True
            )
            subprocess.run(['git', 'push', 'origin', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=120, check=True)
        except Exception as e:
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = f'Failed to publish RCA branch: {e}'
            return outcome

        owner_repo = ''
        match = re.search(r'github\.com[:/]+([^/]+/[^/.]+)(?:\.git)?$', repo_url)
        if match:
            owner_repo = match.group(1)

        pr_title = f"fix({short_service}): address monitored incident root cause"
        confidence_text = f"{conf:.2f}" if conf else "n/a"
        pr_body = "\n".join([
            "## Summary",
            f"- Service: {service_name}",
            f"- Detected issue: {issue_text}",
            f"- RCA confidence: {confidence_text}",
            "",
            "## Evidence",
            f"- Failing service: {structured.get('failing_service', '') or 'n/a'}",
            f"- Exception: {structured.get('exception', '') or 'n/a'}",
            f"- Status code: {structured.get('status_code', '') or 'n/a'}",
            f"- Trace id: {structured.get('trace_id', '') or 'n/a'}",
            "",
            "## Notes",
            "- This PR is auto-generated by AI monitoring workflow.",
            "- Please validate changes against production-safe test coverage before merge."
        ])

        pr_url = ''
        if owner_repo:
            try:
                pr_proc = subprocess.run(
                    [
                        'gh', 'pr', 'create',
                        '--repo', owner_repo,
                        '--base', target_branch,
                        '--head', branch_name,
                        '--title', pr_title,
                        '--body', pr_body
                    ],
                    cwd=clone_path,
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if pr_proc.returncode == 0:
                    pr_output = str((pr_proc.stdout or '') + '\n' + (pr_proc.stderr or ''))
                    url_match = re.search(r'https://github\.com/[^\s]+/pull/\d+', pr_output)
                    if url_match:
                        pr_url = url_match.group(0)
                else:
                    err = (pr_proc.stderr or pr_proc.stdout or '').strip()
                    outcome['reason'] = f"Branch pushed but PR creation failed: {err[:240]}"
            except Exception as e:
                outcome['reason'] = f"Branch pushed but PR creation errored: {e}"

        shutil.rmtree(clone_path, ignore_errors=True)
        if pr_url:
            outcome['ok'] = 'true'
            outcome['url'] = pr_url
            outcome['reason'] = 'PR created successfully'
            return outcome

        outcome['ok'] = 'false'
        if not outcome.get('reason'):
            outcome['reason'] = 'Branch pushed but PR URL not created; ensure gh auth/token is configured'
        return outcome

    def _apply_source_fix_patches(
        self,
        clone_path: str,
        issue_text: str,
        structured_rca: Optional[Dict[str, str]],
        candidate_files: Optional[Sequence[object]] = None
    ) -> Dict[str, object]:
        """Apply deterministic source-code fixes for known issue classes."""
        out: Dict[str, object] = {'ok': False, 'reason': '', 'modified_files': [], 'fix_summary': ''}
        issue = str(issue_text or '')
        structured = structured_rca if isinstance(structured_rca, dict) else {}
        candidates = candidate_files if isinstance(candidate_files, list) else []

        modified: List[str] = []

        def _write_if_changed(path: str, new_content: str):
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
                    old_content = fh.read()
            except Exception:
                return
            if old_content == new_content:
                return
            with open(path, 'w', encoding='utf-8') as fh:
                fh.write(new_content)
            rel = os.path.relpath(path, clone_path)
            if rel not in modified:
                modified.append(rel)

        def _append_enum_constant(content: str, enum_class: str, enum_value: str) -> str:
            """Append missing enum constant in a tolerant way."""
            if not content or not enum_class or not enum_value:
                return content

            header_match = re.search(rf'enum(?:\s+class)?\s+{re.escape(enum_class)}\b', content)
            if not header_match:
                return content

            open_idx = content.find('{', header_match.end())
            if open_idx < 0:
                return content

            # Scan top-level enum body to locate constants section end.
            i = open_idx + 1
            depth = 1
            constants_end = -1
            close_idx = -1
            while i < len(content):
                ch = content[i]
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        close_idx = i
                        break
                elif ch == ';' and depth == 1 and constants_end < 0:
                    constants_end = i
                i += 1

            if close_idx < 0:
                return content

            if constants_end < 0:
                constants_end = close_idx

            constants_block = content[open_idx + 1:constants_end]
            if re.search(rf'(?<![A-Za-z0-9_]){re.escape(enum_value)}(?![A-Za-z0-9_])', constants_block):
                return content

            trimmed = constants_block.rstrip()
            insertion = f"\n    {enum_value}"
            if trimmed and not trimmed.rstrip().endswith(','):
                insertion = ',' + insertion

            new_constants = constants_block + insertion
            return content[:open_idx + 1] + new_constants + content[constants_end:]

        def _ensure_java_enum_from_value(content: str, enum_class: str) -> str:
            """Inject a generic fromValue helper into Java enum when missing."""
            if not content or not enum_class:
                return content
            if re.search(rf'\b{re.escape(enum_class)}\s+fromValue\s*\(', content):
                return content

            header_match = re.search(rf'enum(?:\s+class)?\s+{re.escape(enum_class)}\b', content)
            if not header_match:
                return content

            open_idx = content.find('{', header_match.end())
            if open_idx < 0:
                return content

            i = open_idx + 1
            depth = 1
            close_idx = -1
            while i < len(content):
                ch = content[i]
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        close_idx = i
                        break
                i += 1

            if close_idx < 0:
                return content

            helper = (
                "\n\n"
                f"    public static {enum_class} fromValue(String raw) {{\n"
                "        if (raw == null) return null;\n"
                "        String direct = raw.trim();\n"
                "        if (direct.isEmpty()) return null;\n"
                f"        try {{ return {enum_class}.valueOf(direct.toUpperCase()); }} catch (Exception ignored) {{}}\n"
                "        String normalized = direct.replace('-', '_').replace(' ', '_').toUpperCase();\n"
                f"        try {{ return {enum_class}.valueOf(normalized); }} catch (Exception ignored) {{}}\n"
                "        return null;\n"
                "    }\n"
            )
            return content[:close_idx] + helper + content[close_idx:]

        def _replace_enum_usage_with_from_value(enum_class: str):
            """Replace unsafe enum parsing calls in candidate java files."""
            if not enum_class:
                return
            touched = 0
            for item in candidates:
                rel = str(item.get('path', '') or '') if isinstance(item, dict) else ''
                if not rel or not rel.endswith('.java'):
                    continue
                fpath = os.path.join(clone_path, rel)
                if not os.path.isfile(fpath):
                    continue
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                        content = fh.read()
                except Exception:
                    continue

                new_content = content
                new_content = re.sub(
                    rf'(?<![A-Za-z0-9_$.]){re.escape(enum_class)}\.valueOf\s*\(',
                    f'{enum_class}.fromValue(',
                    new_content
                )
                new_content = re.sub(
                    rf'Enum\.valueOf\s*\(\s*{re.escape(enum_class)}\.class\s*,',
                    f'{enum_class}.fromValue(',
                    new_content
                )
                if new_content != content:
                    _write_if_changed(fpath, new_content)
                    touched += 1
                if touched >= 30:
                    break

        # Fix class 1: Unknown enum value for enum class -> add missing enum constant.
        enum_match = re.search(r'(?i)unknown\s+name\s+value\s*\[([^\]]+)\]\s*for\s*enum\s+class\s*\[([^\]]+)\]', issue)
        if not enum_match:
            enum_match = re.search(
                r"(?i)(?:invalid|unknown|unsupported|unexpected|illegal)[^\n]{0,120}?value\s+['\"]([^'\"]+)['\"][^\n]{0,120}?for\s+([a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+)",
                issue
            )
        if not enum_match:
            enum_match = re.search(
                r"(?i)value\s+['\"]([^'\"]+)['\"][^\n]{0,120}?for\s+([a-zA-Z_][\w$]*(?:\.[A-Za-z_][\w$]*)+)",
                issue
            )
        if enum_match:
            enum_value = str(enum_match.group(1) or '').strip()
            enum_fqcn = str(enum_match.group(2) or '').strip()
            enum_class = enum_fqcn.split('.')[-1] if enum_fqcn else ''
            enum_path = ''
            if enum_fqcn:
                enum_rel = os.path.join('src', 'main', 'java', *enum_fqcn.split('.')) + '.java'
                candidate_path = os.path.join(clone_path, enum_rel)
                if os.path.isfile(candidate_path):
                    enum_path = candidate_path
            if not enum_path and enum_class:
                for item in candidates:
                    rel = str(item.get('path', '') or '') if isinstance(item, dict) else ''
                    if (
                        rel.endswith(f"/{enum_class}.java") or rel.endswith(f"{enum_class}.java") or
                        rel.endswith(f"/{enum_class}.kt") or rel.endswith(f"{enum_class}.kt")
                    ):
                        candidate_path = os.path.join(clone_path, rel)
                        if os.path.isfile(candidate_path):
                            enum_path = candidate_path
                            break

            # Fallback: repo-wide enum file lookup when candidate list misses it.
            if not enum_path and enum_class:
                try:
                    preferred = []
                    fallback = []
                    enum_decl_pattern = re.compile(rf'enum(?:\s+class)?\s+{re.escape(enum_class)}\b')
                    for root, dirs, files in os.walk(clone_path):
                        dirs[:] = [d for d in dirs if d not in {'.git', 'target', 'build', 'dist', 'node_modules', '__pycache__'}]
                        for fname in files:
                            if not (fname.endswith('.java') or fname.endswith('.kt')):
                                continue
                            candidate_path = os.path.join(root, fname)
                            rel = os.path.relpath(candidate_path, clone_path).replace('\\\\', '/')

                            # Prefer direct filename matches first.
                            direct_name_match = fname == f"{enum_class}.java" or fname == f"{enum_class}.kt"
                            if not direct_name_match:
                                try:
                                    with open(candidate_path, 'r', encoding='utf-8', errors='ignore') as fh:
                                        snippet = fh.read(200_000)
                                    if not enum_decl_pattern.search(snippet):
                                        continue
                                except Exception:
                                    continue

                            if '/src/main/java/' in f"/{rel}" and '/enum' in rel.lower():
                                preferred.append(candidate_path)
                            elif '/src/main/kotlin/' in f"/{rel}" and '/enum' in rel.lower():
                                preferred.append(candidate_path)
                            else:
                                fallback.append(candidate_path)

                    if preferred:
                        enum_path = preferred[0]
                    elif fallback:
                        enum_path = fallback[0]
                except Exception:
                    pass

            # Final fallback: inspect top candidate files for inline enum declaration.
            if not enum_path and enum_class:
                try:
                    enum_decl_pattern = re.compile(rf'enum(?:\s+class)?\s+{re.escape(enum_class)}\b')
                    for item in candidates:
                        rel = str(item.get('path', '') or '') if isinstance(item, dict) else ''
                        if not rel:
                            continue
                        candidate_path = os.path.join(clone_path, rel)
                        if not os.path.isfile(candidate_path):
                            continue
                        if not (candidate_path.endswith('.java') or candidate_path.endswith('.kt')):
                            continue
                        try:
                            with open(candidate_path, 'r', encoding='utf-8', errors='ignore') as fh:
                                content = fh.read(200_000)
                        except Exception:
                            continue
                        if enum_decl_pattern.search(content):
                            enum_path = candidate_path
                            break
                except Exception:
                    pass

            if enum_path and enum_class and enum_value:
                try:
                    with open(enum_path, 'r', encoding='utf-8', errors='ignore') as fh:
                        content = fh.read()
                    new_content = _append_enum_constant(content, enum_class, enum_value)
                    if enum_path.endswith('.java'):
                        new_content = _ensure_java_enum_from_value(new_content, enum_class)
                    if new_content != content:
                        _write_if_changed(enum_path, new_content)
                except Exception:
                    pass

            if enum_class:
                _replace_enum_usage_with_from_value(enum_class)

        # Fix class 2: Missing Spring placeholder -> add safe default in @Value expression.
        placeholder_match = re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', issue)
        if placeholder_match:
            placeholder = str(placeholder_match.group(1) or '').strip()
            if placeholder:
                for item in candidates:
                    rel = str(item.get('path', '') or '') if isinstance(item, dict) else ''
                    if not rel.endswith('.java'):
                        continue
                    fpath = os.path.join(clone_path, rel)
                    if not os.path.isfile(fpath):
                        continue
                    try:
                        with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                            content = fh.read()
                    except Exception:
                        continue
                    escaped = re.escape(placeholder)
                    pattern_plain = rf'@Value\("\$\{{{escaped}\}}"\)'
                    pattern_with_value = rf'@Value\(\s*value\s*=\s*"\$\{{{escaped}\}}"\s*\)'
                    new_content = re.sub(pattern_plain, f'@Value("${{{placeholder}:}}")', content)
                    new_content = re.sub(pattern_with_value, f'@Value(value = "${{{placeholder}:}}")', new_content)
                    if new_content != content:
                        _write_if_changed(fpath, new_content)

        if not modified:
            out['reason'] = 'No deterministic source fix could be applied from current evidence'
            return out

        out['ok'] = True
        out['modified_files'] = modified[:20]
        out['fix_summary'] = f"Applied deterministic patch in {len(modified)} file(s)"
        return out

    def _run_repo_verification(self, clone_path: str) -> Dict[str, str]:
        """Run lightweight compile/test verification for generated source fixes."""
        result = {'ok': 'false', 'command': '', 'summary': ''}
        commands: List[List[str]] = []
        if os.path.isfile(os.path.join(clone_path, 'mvnw')):
            commands.append(['./mvnw', '-q', '-DskipTests', 'compile'])
        elif os.path.isfile(os.path.join(clone_path, 'gradlew')):
            commands.append(['./gradlew', '-q', 'testClasses'])
        elif os.path.isfile(os.path.join(clone_path, 'pom.xml')):
            commands.append(['mvn', '-q', '-DskipTests', 'compile'])
        elif os.path.isfile(os.path.join(clone_path, 'build.gradle')) or os.path.isfile(os.path.join(clone_path, 'build.gradle.kts')):
            commands.append(['gradle', '-q', 'testClasses'])
        else:
            result['summary'] = 'No known build tool found; skipped verification'
            return result

        for cmd in commands:
            if shutil.which(cmd[0]) is None and cmd[0] in {'mvn', 'gradle'}:
                continue
            if cmd[0].startswith('./') and not os.path.isfile(os.path.join(clone_path, cmd[0][2:])):
                continue
            try:
                proc = subprocess.run(cmd, cwd=clone_path, capture_output=True, text=True, timeout=300)
                result['command'] = ' '.join(cmd)
                if proc.returncode == 0:
                    result['ok'] = 'true'
                    result['summary'] = f"Verification passed: {' '.join(cmd)}"
                    return result
                stderr = (proc.stderr or proc.stdout or '').strip()
                result['summary'] = f"Verification failed ({' '.join(cmd)}): {stderr[:240]}"
                return result
            except Exception as e:
                result['summary'] = f"Verification errored ({' '.join(cmd)}): {e}"
                return result

        result['summary'] = 'No runnable verification command found in container'
        return result

    def _prepare_source_fix_pr_branch(
        self,
        service_name: str,
        issue_text: str,
        structured_rca: Optional[Dict[str, str]],
        analysis: Optional[Dict]
    ) -> Dict[str, str]:
        """Create and push PR with deterministic source-code fix when safely possible."""
        outcome = {'ok': 'false', 'url': '', 'reason': ''}

        issue_lower = str(issue_text or '').lower()
        if any(token in issue_lower for token in [
            'oomkilled', 'oom killed', 'outofmemoryerror', 'memory limit', 'evicted',
            'memory cgroup out of memory'
        ]):
            return self._prepare_k8s_manifest_oom_pr_branch(service_name, issue_text, structured_rca, analysis)

        if shutil.which('git') is None:
            outcome['reason'] = "git binary not found in ai-monitoring-agent container; install git in image"
            return outcome

        if shutil.which('gh') is None:
            outcome['reason'] = "gh binary not found in ai-monitoring-agent container; install GitHub CLI in image"
            return outcome

        repo_analysis = self._analyze_repo_for_issue(
            service_name,
            issue_text,
            structured_rca,
            keep_clone_override=True
        )
        if not bool(repo_analysis.get('ok', False)):
            outcome['reason'] = str(repo_analysis.get('reason', 'Repository analysis failed'))
            return outcome

        clone_path = str(repo_analysis.get('clone_path', '') or '')
        repo_url = str(repo_analysis.get('repo_url', '') or '')
        target_branch = str(repo_analysis.get('branch', 'develop_mercury') or 'develop_mercury')
        raw_candidates = repo_analysis.get('candidate_files', [])
        candidates: List[Dict] = raw_candidates if isinstance(raw_candidates, list) else []
        if not clone_path or not os.path.isdir(clone_path):
            outcome['reason'] = 'Repository clone path missing after analysis'
            return outcome

        # Always try Ollama AI to generate code fix - let AI decide if fix is possible
        modified = []
        if hasattr(self, 'root_cause_analyzer') and self.root_cause_analyzer.is_llm_available():
            # Read relevant files for context
            repo_files = {}
            for cand in candidates[:15]:
                if isinstance(cand, dict):
                    fpath = cand.get('path') or cand.get('file_path', '')
                else:
                    fpath = str(cand)
                if fpath and os.path.isfile(fpath):
                    try:
                        with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                            repo_files[fpath] = fh.read()
                    except Exception:
                        pass

            if repo_files:
                logger.info("Using AI (Ollama) to analyze issue and generate code fix for: %s", issue_text[:150])
                fix_result = self.root_cause_analyzer.generate_code_fix(
                    issue_description=issue_text,
                    repo_files=repo_files,
                    service_name=service_name
                )
                logger.info("AI fix generation result: ok=%s, fixes_count=%d", fix_result.get('ok'), len(fix_result.get('fixes', [])))
                
                if fix_result.get('ok'):
                    for fix in fix_result.get('fixes', []):
                        fix_file = fix.get('file', '')
                        fix_code = fix.get('code', '')
                        if fix_file and fix_code:
                            # Find full path
                            full_path = None
                            for cf in candidates:
                                cf_str = str(cf.get('path') or cf.get('file_path') or cf)
                                if fix_file in cf_str or cf_str.endswith(fix_file):
                                    full_path = cf_str
                                    break
                            if not full_path:
                                # Try to find in clone_path
                                for root, dirs, files in os.walk(clone_path):
                                    for f in files:
                                        if fix_file in f:
                                            full_path = os.path.join(root, f)
                                            break
                            if full_path and os.path.isfile(full_path):
                                try:
                                    with open(full_path, 'r', encoding='utf-8', errors='ignore') as fh:
                                        old_content = fh.read()
                                    # Append fix code
                                    new_content = old_content.rstrip() + '\n' + fix_code
                                    with open(full_path, 'w', encoding='utf-8') as fh:
                                        fh.write(new_content)
                                    rel = os.path.relpath(full_path, clone_path)
                                    modified.append(rel)
                                    logger.info("AI applied fix to: %s", rel)
                                except Exception as e:
                                    logger.error("Failed to apply AI fix to %s: %s", full_path, e)

        if not modified:
            # If AI couldn't generate fix, try hardcoded patches as fallback
            patch_result = self._apply_source_fix_patches(clone_path, issue_text, structured_rca, candidates)
            if bool(patch_result.get('ok', False)):
                raw_mod = patch_result.get('modified_files')
                if isinstance(raw_mod, list):
                    modified = raw_mod

            if not modified:
                # No fix could be generated
                shutil.rmtree(clone_path, ignore_errors=True)
                outcome['reason'] = str(patch_result.get('reason', 'No deterministic source fix available'))
                return outcome
            return outcome

        patch_result = self._apply_source_fix_patches(clone_path, issue_text, structured_rca, candidates)
        if not bool(patch_result.get('ok', False)):
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = str(patch_result.get('reason', 'No deterministic source fix available'))
            return outcome

        verify = self._run_repo_verification(clone_path)
        if verify.get('ok') != 'true':
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = str(verify.get('summary', 'Verification failed'))
            return outcome

        short_service = self._canonical_service(str(service_name or '').split('/')[-1]) or 'service'
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        branch_name = f"ai-agent-fix/{short_service}-{stamp}"

        try:
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(['git', 'add', '.'], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(
                [
                    'git', '-c', 'user.name=ai-monitoring-agent', '-c', 'user.email=ai-monitoring-agent@local',
                    'commit', '-m', f"fix({short_service}): apply deterministic monitored incident fix"
                ],
                cwd=clone_path,
                capture_output=True,
                text=True,
                timeout=45,
                check=True
            )
            subprocess.run(['git', 'push', 'origin', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=120, check=True)
        except Exception as e:
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = f'Failed to publish source-fix branch: {e}'
            return outcome

        owner_repo = ''
        match = re.search(r'github\.com[:/]+([^/]+/[^/.]+)(?:\.git)?$', repo_url)
        if match:
            owner_repo = match.group(1)

        conf = float((analysis or {}).get('confidence', 0.0) or 0.0)
        pr_title = f"fix({short_service}): auto-fix monitored code incident"
        raw_modified = patch_result.get('modified_files', [])
        modified: List[str] = [str(item) for item in raw_modified] if isinstance(raw_modified, list) else []
        verify_cmd = str(verify.get('command', '') or 'N/A')
        verify_summary = str(verify.get('summary', '') or '')
        pr_body = "\n".join([
            "## Summary",
            f"- Service: {service_name}",
            f"- Detected issue: {issue_text}",
            f"- Confidence: {conf:.2f}",
            f"- Auto-fix mode: source_fix",
            "",
            "## Applied Changes",
            *[f"- {path}" for path in modified[:15]],
            "",
            "## Verification",
            f"- Command: `{verify_cmd}`",
            f"- Result: {verify_summary}",
            "",
            "## Notes",
            "- Deterministic patch generated by AI monitoring workflow.",
            "- Please review thoroughly before merge."
        ])

        pr_url = ''
        if owner_repo:
            try:
                pr_proc = subprocess.run(
                    [
                        'gh', 'pr', 'create',
                        '--repo', owner_repo,
                        '--base', target_branch,
                        '--head', branch_name,
                        '--title', pr_title,
                        '--body', pr_body
                    ],
                    cwd=clone_path,
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if pr_proc.returncode == 0:
                    pr_output = str((pr_proc.stdout or '') + '\n' + (pr_proc.stderr or ''))
                    url_match = re.search(r'https://github\.com/[^\s]+/pull/\d+', pr_output)
                    if url_match:
                        pr_url = url_match.group(0)
                else:
                    err = (pr_proc.stderr or pr_proc.stdout or '').strip()
                    outcome['reason'] = f"Branch pushed but PR creation failed: {err[:240]}"
            except Exception as e:
                outcome['reason'] = f"Branch pushed but PR creation errored: {e}"

        shutil.rmtree(clone_path, ignore_errors=True)
        if pr_url:
            outcome['ok'] = 'true'
            outcome['url'] = pr_url
            outcome['reason'] = 'Source-fix PR created successfully'
            return outcome

        outcome['ok'] = 'false'
        if not outcome.get('reason'):
            outcome['reason'] = 'Branch pushed but PR URL not created; ensure gh auth/token is configured'
        return outcome

    def _assess_pr_eligibility(
        self,
        service_name: str,
        issue_text: str,
        issue_scope: str,
        structured_rca: Optional[Dict[str, str]] = None,
        analysis: Optional[Dict] = None
    ) -> Dict[str, str]:
        """Strict guardrail so code PR suggestions are raised only with strong evidence."""
        pr_cfg = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
        eligibility_mode = str(pr_cfg.get('eligibility_mode', 'permissive') or 'permissive').strip().lower()
        issue = str(issue_text or '').strip()
        issue_lower = issue.lower()
        structured = structured_rca if isinstance(structured_rca, dict) else {}
        
        # Always allow enum-related issues - these are real code bugs
        if re.search(r'(?i)invalid\s+enum|enum.*value|unknown\s+name\s+value.*enum', issue):
            logger.info(f"Enum issue detected - allowing PR: {issue[:100]}")
            return {'eligible': 'true', 'reason': 'Enum value issue - code-level bug'}
        
        # OOM issues are eligible for k8s manifest PR
        if any(token in issue_lower for token in [
            'oomkilled', 'oom killed', 'outofmemoryerror', 'memory limit', 'evicted',
            'memory cgroup out of memory'
        ]):
            return {'eligible': 'true', 'reason': 'OOM/resource issue eligible for manifest fix PR'}
        
        # Placeholder issues are eligible for code fix PR
        if any(token in issue_lower for token in [
            'could not resolve placeholder', 'couldn\'t resolve placeholder',
            'placeholder', 'missing config'
        ]):
            return {'eligible': 'true', 'reason': 'Placeholder issue eligible for config fix PR'}
        
        # In permissive mode, allow more issues through
        if eligibility_mode == 'permissive':
            return {'eligible': 'true', 'reason': 'Permissive PR eligibility mode enabled'}
        
        if self._matches_ignored_log_pattern(issue):
            return {'eligible': 'false', 'reason': 'Matched ignored/noise log pattern'}

        pr_ignore = self.config.get('pr_automation', {}).get('ignore_issue_patterns', []) if isinstance(self.config, dict) else []
        for pattern in pr_ignore if isinstance(pr_ignore, list) else []:
            try:
                if re.search(str(pattern), issue, re.IGNORECASE):
                    return {'eligible': 'false', 'reason': 'Matched PR ignore pattern'}
            except re.error:
                continue

        # Allow code-level exceptions
        if re.search(r'\b([a-zA-Z0-9_.$]+Exception|[a-zA-Z0-9_.$]+Error)\b', issue, re.IGNORECASE):
            return {'eligible': 'true', 'reason': 'Code-level exception detected'}

        # If we have exception name or code location, allow it
        exception_name = str(structured.get('exception', '') or '')
        if exception_name:
            return {'eligible': 'true', 'reason': f'Exception {exception_name} detected'}
        
        return {'eligible': 'true', 'reason': 'Sufficient evidence for PR'}

    def _create_code_fix_pr_stub(
        self,
        service_name: str,
        issue_text: str,
        issue_scope: str,
        structured_rca: Optional[Dict[str, str]] = None,
        analysis: Optional[Dict] = None
        ) -> Dict[str, str]:
        """Create metadata for PR action. Real PR creation requires token/env outside code."""
        # For OOM issues, skip service repo analysis and go directly to k8s manifest fix
        issue_lower = str(issue_text or '').lower()
        if any(token in issue_lower for token in [
            'oomkilled', 'oom killed', 'outofmemoryerror', 'memory limit', 'evicted',
            'memory cgroup out of memory'
        ]):
            return self._prepare_k8s_manifest_oom_pr_branch(service_name, issue_text, structured_rca, analysis)

        eligibility = self._assess_pr_eligibility(service_name, issue_text, issue_scope, structured_rca, analysis)
        if eligibility.get('eligible') != 'true':
            logger.info(
                "PR not eligible for %s: scope=%s reason=%s",
                str(service_name or 'unknown'),
                str(issue_scope or 'unknown'),
                str(eligibility.get('reason', ''))
            )
            return {'status': 'Not Eligible', 'url': '', 'reason': eligibility.get('reason', 'Issue scope is not code')}
        
        logger.info(f"PR eligible for {service_name}: {eligibility.get('reason', '')}")

        signature = f"{service_name}|{issue_text[:160]}"
        if signature in self._pr_created_signatures:
            logger.info(
                "PR skipped for %s: already created for signature",
                str(service_name or 'unknown')
            )
            return {
                'status': 'Skipped',
                'url': '',
                'reason': 'PR already created earlier for this issue signature'
            }

        if signature in self._pr_attempted_signatures:
            logger.info(
                "PR skipped for %s: already attempted for signature",
                str(service_name or 'unknown')
            )
            return {'status': 'Skipped', 'url': '', 'reason': 'PR already attempted for this signature'}
        repo_analysis = self._analyze_repo_for_issue(service_name, issue_text, structured_rca)
        if not bool(repo_analysis.get('ok', False)):
            logger.info(
                "PR not eligible for %s: repository analysis failed: %s",
                str(service_name or 'unknown'),
                str(repo_analysis.get('reason', 'Repository analysis failed'))
            )
            return {
                'status': 'Not Eligible',
                'url': '',
                'reason': str(repo_analysis.get('reason', 'Repository analysis failed')),
                'repo': str(repo_analysis.get('repo_url', ''))
            }

        cfg = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
        auto_create = bool(cfg.get('auto_create_pr', False))
        if not auto_create:
            candidates = repo_analysis.get('candidate_files', []) or []
            candidate_subset = list(candidates[:3]) if isinstance(candidates, list) else []
            candidate_hint = ', '.join([str(item.get('path', '')) for item in candidate_subset if isinstance(item, dict)])
            return {
                'status': 'Analysis Ready',
                'url': '',
                'reason': (
                    f"Code repo analyzed. Candidate files: {candidate_hint or 'N/A'}. "
                    f"Enable pr_automation.auto_create_pr to allow automatic PR attempts."
                ),
                'repo': str(repo_analysis.get('repo_url', ''))
            }

        fix_mode = str(cfg.get('fix_mode', 'report') or 'report').strip().lower()
        allow_report_pr = bool(cfg.get('allow_analysis_report_pr', True))
        logger.info(f"PR creation attempt: fix_mode={fix_mode}, allow_report_pr={allow_report_pr}, auto_create={auto_create}")
        
        if fix_mode == 'source_fix':
            self._pr_attempted_signatures.add(signature)
            prepared = self._prepare_source_fix_pr_branch(service_name, issue_text, structured_rca, analysis)
            logger.info(f"Source fix PR result: ok={prepared.get('ok')}, url={prepared.get('url', '')[:50] if prepared.get('url') else 'N/A'}")
            if prepared.get('ok') != 'true' and allow_report_pr:
                logger.info("Falling back to repo analysis PR branch")
                prepared = self._prepare_repo_analysis_pr_branch(service_name, issue_text, structured_rca, analysis)
        else:
            self._pr_attempted_signatures.add(signature)
            logger.info("Using report mode - creating repo analysis PR")
            prepared = self._prepare_repo_analysis_pr_branch(service_name, issue_text, structured_rca, analysis)
            logger.info(f"Analysis PR result: ok={prepared.get('ok')}, url={prepared.get('url', '')[:50] if prepared.get('url') else 'N/A'}, reason={prepared.get('reason', '')[:100]}")
        if prepared.get('ok') == 'true':
            self._pr_created_signatures.add(signature)
            logger.info("PR created for %s: %s", str(service_name or 'unknown'), str(prepared.get('url', '') or ''))
            return {
                'status': 'PR Created',
                'url': str(prepared.get('url', '') or ''),
                'reason': str(prepared.get('reason', 'PR created')),
                'repo': str(repo_analysis.get('repo_url', ''))
            }

        logger.warning(
            "PR creation failed for %s: %s",
            str(service_name or 'unknown'),
            str(prepared.get('reason', 'Automatic PR creation failed'))
        )
        # Allow automatic retry in subsequent cycles when branch/PR creation fails.
        self._pr_attempted_signatures.discard(signature)
        return {
            'status': 'PR Failed',
            'url': '',
            'reason': str(prepared.get('reason', 'Automatic PR creation failed')),
            'repo': str(repo_analysis.get('repo_url', ''))
        }

    def _derive_anomaly_root_cause(self, anomalies: List[Dict], service_status: Dict) -> Dict[str, str]:
        """Deterministic fallback from anomaly/service signals when logs-traces are weak."""
        result = {'issue': '', 'recommendation': '', 'confidence_boost': 0.0}
        if not anomalies:
            return result

        def _best_dependency_status(dep_name: str) -> str:
            dep_canonical = self._canonical_service(str(dep_name or '').strip())
            if not dep_canonical:
                return 'unknown'
            for key, state in (service_status or {}).items():
                if not isinstance(state, dict):
                    continue
                name = self._canonical_service(str(state.get('name', key.split('/')[-1]) or '').strip())
                if name == dep_canonical:
                    return str(state.get('status', 'unknown') or 'unknown')
            return 'unknown'

        def _best_dependency_error(dep_name: str) -> str:
            dep_canonical = self._canonical_service(str(dep_name or '').strip())
            if not dep_canonical:
                return ''
            for key, state in (service_status or {}).items():
                if not isinstance(state, dict):
                    continue
                name = self._canonical_service(str(state.get('name', key.split('/')[-1]) or '').strip())
                if name != dep_canonical:
                    continue
                errs = state.get('recent_errors', []) or []
                if isinstance(errs, list) and errs:
                    msg = str(errs[0].get('message', '') or '').strip()
                    if msg:
                        return msg[:140]
            return ''

        for anomaly in anomalies:
            if not isinstance(anomaly, dict):
                continue
            service = str(anomaly.get('service', '') or 'unknown')
            anomaly_type = str(anomaly.get('type', '') or '')
            svc_state = service_status.get(service, {}) if isinstance(service_status, dict) else {}

            if anomaly_type == 'IMAGE_PULL_ERROR':
                reason = str(anomaly.get('reason', '') or '')
                result['issue'] = f"Exact Issue: {service} cannot pull container image ({reason or 'image pull failure'})"
                result['recommendation'] = "Fix image reference/registry auth and verify image exists"
                result['confidence_boost'] = 0.28
                return result

            if anomaly_type == 'NO_PODS':
                result['issue'] = f"Exact Issue: {service} has no running pods (scheduling/startup failure)"
                result['recommendation'] = "Check scheduling constraints, node capacity, and deployment events"
                result['confidence_boost'] = 0.24
                return result

            if anomaly_type in {'CRASH_LOOP', 'POD_STARTUP_FAILURE'}:
                recent_errors = svc_state.get('recent_errors', []) if isinstance(svc_state, dict) else []
                message = ''
                if isinstance(recent_errors, list) and recent_errors:
                    message = str(recent_errors[0].get('message', '') or '')

                placeholder_match = re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', message)
                if placeholder_match:
                    placeholder = placeholder_match.group(1)
                    result['issue'] = f"Exact Issue: {service} startup failed due to missing config '{placeholder}' -> CrashLoopBackOff"
                    result['recommendation'] = f"Provide required config/secret '{placeholder}'"
                    result['confidence_boost'] = 0.34
                    return result

                if re.search(r'(?i)failed\s+to\s+bind\s+properties|configurationpropertiesbindexception|bindexception', message):
                    result['issue'] = f"Exact Issue: {service} startup failed due to invalid configuration binding -> CrashLoopBackOff"
                    result['recommendation'] = "Fix invalid config value/type in configmap/secret/environment"
                    result['confidence_boost'] = 0.31
                    return result

                if re.search(r'(?i)beancreationexception|unsatisfieddependencyexception', message):
                    result['issue'] = f"Exact Issue: {service} startup bean initialization failed -> CrashLoopBackOff"
                    result['recommendation'] = "Inspect nested dependency/config exception and fix bean wiring"
                    result['confidence_boost'] = 0.3
                    return result

                result['issue'] = f"Exact Issue: {service} process exits during startup -> CrashLoopBackOff"
                result['recommendation'] = "Inspect previous container logs for first fatal exception and fix startup path"
                result['confidence_boost'] = 0.2
                return result

            if anomaly_type == 'DEPENDENCY_FAILURE':
                deps = anomaly.get('dependencies', []) or []
                dep_names = [self._canonical_service(str(d).strip()) for d in deps[:3] if str(d).strip()]
                dep_names = [d for d in dep_names if d]
                if dep_names:
                    detail_parts = []
                    for dep in dep_names:
                        dep_status = _best_dependency_status(dep)
                        dep_error = _best_dependency_error(dep)
                        if dep_error:
                            detail_parts.append(f"{dep}(status={dep_status}, error={dep_error})")
                        else:
                            detail_parts.append(f"{dep}(status={dep_status})")
                    dep_text = '; '.join(detail_parts)
                else:
                    dep_text = 'downstream dependency(status=unknown)'
                result['issue'] = f"Exact Issue: {service} dependency call failures to {dep_text}"
                result['recommendation'] = f"Validate dependency health/connectivity for {dep_text}"
                result['confidence_boost'] = 0.2
                return result

            if anomaly_type in {'HIGH_ERROR_RATE', 'FREQUENT_ERRORS', 'SERVICE_DEGRADED'}:
                candidate_messages = []
                sample_error = str(anomaly.get('sample_error', '') or '').strip()
                if sample_error:
                    candidate_messages.append(sample_error)

                top_errors = anomaly.get('top_errors', [])
                if isinstance(top_errors, list):
                    for item in top_errors[:5]:
                        text = str(item or '').strip()
                        if text and text not in candidate_messages:
                            candidate_messages.append(text)

                recent_errors = svc_state.get('recent_errors', []) if isinstance(svc_state, dict) else []
                if isinstance(recent_errors, list):
                    for item in recent_errors[:5]:
                        if not isinstance(item, dict):
                            continue
                        text = str(item.get('message', '') or '').strip()
                        if text and text not in candidate_messages:
                            candidate_messages.append(text)

                for candidate in candidate_messages:
                    if not candidate:
                        continue
                    parsed = self._derive_exact_from_message(
                        service,
                        candidate,
                        crashloop_present=('crashloopbackoff' in candidate.lower())
                    )
                    if parsed.get('issue'):
                        result['issue'] = parsed['issue']
                        result['recommendation'] = parsed.get('recommendation', 'Inspect application/runtime error and dependency path')
                        result['confidence_boost'] = 0.25
                        return result

                for candidate in candidate_messages:
                    if re.search(r'([A-Za-z0-9_.$]+(?:Exception|Error))', str(candidate or '')):
                        short = str(candidate).strip()
                        if len(short) > 220:
                            short = short[:220] + '...'
                        result['issue'] = f"Exact Issue: {service} runtime exception observed | {short}"
                        result['recommendation'] = "Inspect latest exception stack and validate request inputs/dependencies on the failing path"
                        result['confidence_boost'] = 0.2
                        return result

        return result

    def _extract_log_exception_context(self, service_name: str, logs_traces: Dict) -> Dict[str, str]:
        """Extract likely exception/file context from logs for one service."""
        context = {
            'exception': '',
            'file_hint': '',
            'message_hint': '',
            'actionable_line': ''
        }
        service_short = str(service_name or '').split('/')[-1].lower()
        if not isinstance(logs_traces, dict):
            return context

        candidates = []
        for entry in (logs_traces.get('logs', []) or []) + (logs_traces.get('service_errors', []) or []):
            if not isinstance(entry, dict):
                continue
            msg = str(entry.get('message', entry.get('body', '')) or '')
            if not msg:
                continue
            entry_service = str(entry.get('service', '') or '').split('/')[-1].lower()
            if entry_service and service_short and entry_service != service_short:
                continue
            candidates.append(msg)
            if len(candidates) >= 20:
                break

        if not candidates:
            return context

        blob = '\n'.join(candidates)
        exc_match = re.search(r'([a-zA-Z0-9_.]+(?:Exception|Error))', blob)
        if exc_match:
            context['exception'] = exc_match.group(1)

        file_match = re.search(r'([A-Za-z0-9_.$-]+\.(?:java|kt|py|go):\d+|[A-Za-z0-9_.$-]+\.class)', blob)
        if file_match:
            context['file_hint'] = file_match.group(1)

        meaningful_line = ''
        actionable_line = ''
        for line in candidates:
            lower = line.lower()
            if any(token in lower for token in [
                'could not resolve placeholder', 'failed to bind properties',
                'beancreationexception', 'unsatisfieddependencyexception', 'unable to start',
                'context initialization - cancelling refresh attempt'
            ]):
                meaningful_line = line
                break

        for line in candidates:
            lower = line.lower()
            if any(token in lower for token in [
                'exception', 'error', 'failed', 'refused', 'timeout', 'timed out',
                'deadline exceeded', 'unauthorized', 'forbidden', 'not found',
                'connection reset', 'broken pipe', 'crashloopbackoff', 'imagepullbackoff'
            ]):
                actionable_line = line
                break

        if meaningful_line:
            context['message_hint'] = meaningful_line[:240]
        if actionable_line:
            context['actionable_line'] = actionable_line[:240]

        return context

    def _extract_trace_specific_log_context(self, trace_id: str, failing_service: str, source_service: str, logs_traces: Dict) -> Dict[str, str]:
        """Extract trace-correlated reason text from logs when spans have weak status_message."""
        out = {
            'reason': '',
            'exception': '',
            'file_hint': '',
            'matched_service': ''
        }
        trace_value = str(trace_id or '').strip().lower()
        if not trace_value or not isinstance(logs_traces, dict):
            return out

        failing_short = self._canonical_service(str(failing_service or '').split('/')[-1])
        source_short = self._canonical_service(str(source_service or '').split('/')[-1])

        matches = []
        for entry in (logs_traces.get('logs', []) or []) + (logs_traces.get('service_errors', []) or []):
            if not isinstance(entry, dict):
                continue
            message = str(entry.get('message', entry.get('body', '')) or '').strip()
            if not message:
                continue
            if trace_value not in message.lower():
                continue

            entry_service = str(entry.get('service', '') or '')
            entry_service_short = self._canonical_service(entry_service.split('/')[-1])
            priority = 0
            if entry_service_short and failing_short and entry_service_short == failing_short:
                priority += 3
            elif entry_service_short and source_short and entry_service_short == source_short:
                priority += 2
            elif entry_service_short:
                priority += 1

            severity = str(entry.get('severity', '') or '').upper()
            if severity in {'ERROR', 'FATAL', 'CRITICAL'}:
                priority += 2
            if self._is_actionable_error_message(message):
                priority += 2

            matches.append((priority, message, entry_service))

        if not matches:
            return out

        matches.sort(key=lambda item: item[0], reverse=True)
        best_message = str(matches[0][1] or '')
        out['matched_service'] = str(matches[0][2] or '')

        exc_match = re.search(r'([a-zA-Z0-9_.]+(?:Exception|Error))', best_message)
        if exc_match:
            out['exception'] = exc_match.group(1)

        file_match = re.search(r'([A-Za-z0-9_.$/-]+\.(?:java|kt|py|go):\d+|[A-Za-z0-9_.$-]+\.class)', best_message)
        if file_match:
            out['file_hint'] = file_match.group(1)

        out['reason'] = best_message[:260]
        return out

    def _prepare_k8s_manifest_oom_pr_branch(
        self,
        service_name: str,
        issue_text: str,
        structured_rca: Optional[Dict[str, str]],
        analysis: Optional[Dict]
    ) -> Dict[str, str]:
        """Create PR by raising memory requests/limits in k8s deployment manifest."""
        outcome = {'ok': 'false', 'url': '', 'reason': ''}

        if shutil.which('git') is None:
            outcome['reason'] = "git binary not found in ai-monitoring-agent container; install git in image"
            return outcome
        if shutil.which('gh') is None:
            outcome['reason'] = "gh binary not found in ai-monitoring-agent container; install GitHub CLI in image"
            return outcome

        # For OOM issues, clone k8s-manifest repo, not the service repo
        cfg = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
        repo_base_url = str(cfg.get('repo_base_url', 'https://github.com/fabhotelstech') or 'https://github.com/fabhotelstech').rstrip('/')
        k8s_repo_url = f"{repo_base_url}/k8s-manifest.git"
        workspace = str(cfg.get('workspace', '/tmp/ai-agent-pr-work') or '/tmp/ai-agent-pr-work')
        short_service = self._canonical_service(str(service_name or '').split('/')[-1]) or 'service'
        
        try:
            os.makedirs(workspace, exist_ok=True)
        except Exception as e:
            outcome['reason'] = f'Failed to prepare workspace: {e}'
            return outcome

        issue_hash = hashlib.sha1(f"oom-{service_name}|{issue_text[:80]}".encode('utf-8')).hexdigest()[:10]
        clone_path = os.path.join(workspace, f"k8s-manifest-{issue_hash}")

        gh_token = str(os.getenv('GITHUB_TOKEN') or os.getenv('GH_TOKEN') or os.getenv('gh_token') or '').strip()
        
        # Debug log
        if gh_token:
            logger.info("OOM PR: GitHub token found, will clone %s", k8s_repo_url)
        else:
            logger.warning("OOM PR: No GitHub token, will try anonymous clone")
        
        # Try multiple common branch names
        branch_names = ['main', 'master', 'develop', 'develop_mercury']
        clone_success = False
        clone_proc = None
        target_branch = 'main'
        
        for branch_name in branch_names:
            clone_cmd = ['git', 'clone', '--depth', '1', '--branch', branch_name, k8s_repo_url, clone_path]
            if gh_token:
                try:
                    parsed = urlparse(k8s_repo_url)
                    if parsed.scheme in {'http', 'https'} and parsed.netloc:
                        safe_token = quote(gh_token, safe='')
                        auth_url = f"{parsed.scheme}://x-access-token:{safe_token}@{parsed.netloc}{parsed.path}"
                        clone_cmd[5] = auth_url
                except Exception:
                    pass
            
            clone_proc = subprocess.run(clone_cmd, capture_output=True, text=True, timeout=120)
            if clone_proc.returncode == 0:
                clone_success = True
                target_branch = branch_name
                break
            # Clean up failed clone attempt
            if os.path.isdir(clone_path):
                shutil.rmtree(clone_path, ignore_errors=True)
        
        if not clone_success:
            err_text = (clone_proc.stderr or clone_proc.stdout or '').strip()[:200] if clone_proc else 'unknown'
            outcome['reason'] = f"Clone failed: {k8s_repo_url} - {err_text}"
            return outcome

        repo_analysis = {
            'ok': True,
            'clone_path': clone_path,
            'repo_url': k8s_repo_url,
            'branch': 'main',
            'repo_name': 'k8s-manifest'
        }

        clone_path = str(repo_analysis.get('clone_path', '') or '')
        repo_url = str(repo_analysis.get('repo_url', '') or '')
        target_branch = str(repo_analysis.get('branch', 'dev') or 'dev')
        if not clone_path or not os.path.isdir(clone_path):
            outcome['reason'] = 'Repository clone path missing after analysis'
            return outcome

        pr_cfg = self.config.get('pr_automation', {}) if isinstance(self.config, dict) else {}
        path_hints = pr_cfg.get('manifest_path_hints', []) if isinstance(pr_cfg.get('manifest_path_hints', []), list) else []

        service_short = self._canonical_service(str(service_name or '').split('/')[-1])
        candidates: List[str] = []

        for hint in path_hints:
            rel = str(hint or '').strip().lstrip('/')
            if rel:
                fpath = os.path.join(clone_path, rel)
                if os.path.isfile(fpath):
                    candidates.append(fpath)

        for root, dirs, files in os.walk(clone_path):
            dirs[:] = [d for d in dirs if d not in {'.git', 'target', 'build', 'dist', 'node_modules', '__pycache__'}]
            for fname in files:
                if not (fname.endswith('.yaml') or fname.endswith('.yml')):
                    continue
                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, clone_path).replace('\\\\', '/').lower()
                if 'deployment' not in rel:
                    continue
                if service_short and service_short in rel:
                    candidates.append(fpath)

        # De-duplicate preserving order
        uniq = []
        seen = set()
        for p in candidates:
            if p in seen:
                continue
            seen.add(p)
            uniq.append(p)
        candidates = uniq[:40]

        modified_files: List[str] = []

        def _bump_mem(mi: str) -> str:
            m = re.match(r'(?i)^\s*(\d+(?:\.\d+)?)\s*(mi|mib)\s*$', str(mi or ''))
            if m:
                val = float(m.group(1))
                return f"{int(max(256, round(val * 1.5)))}Mi"
            g = re.match(r'(?i)^\s*(\d+(?:\.\d+)?)\s*(gi|gib)\s*$', str(mi or ''))
            if g:
                val = float(g.group(1))
                return f"{max(1, round(val * 1.5, 1))}Gi"
            return ''

        for fpath in candidates:
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                    content = fh.read()
            except Exception:
                continue

            new_content = content
            # requests.memory
            for pat in [r'(?im)^(\s*memory\s*:\s*)([0-9]+(?:\.[0-9]+)?\s*(?:Mi|Gi|Mib|Gib))\s*$',]:
                def req_repl(m):
                    bumped = _bump_mem(m.group(2))
                    return f"{m.group(1)}{bumped or m.group(2)}"
                new_content = re.sub(pat, req_repl, new_content)

            if new_content != content:
                with open(fpath, 'w', encoding='utf-8') as fh:
                    fh.write(new_content)
                rel = os.path.relpath(fpath, clone_path)
                if rel not in modified_files:
                    modified_files.append(rel)

            if modified_files:
                break

        if not modified_files:
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = 'No deployment manifest with editable memory fields found for OOM fix'
            return outcome

        short_service = self._canonical_service(str(service_name or '').split('/')[-1]) or 'service'
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        branch_name = f"ai-agent-fix/{short_service}-oom-{stamp}"

        try:
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(['git', 'add', '.'], cwd=clone_path, capture_output=True, text=True, timeout=30, check=True)
            subprocess.run(
                [
                    'git', '-c', 'user.name=ai-monitoring-agent', '-c', 'user.email=ai-monitoring-agent@local',
                    'commit', '-m', f"fix({short_service}): increase memory resources for OOM stability"
                ],
                cwd=clone_path, capture_output=True, text=True, timeout=60, check=True
            )
            subprocess.run(['git', 'push', 'origin', branch_name], cwd=clone_path, capture_output=True, text=True, timeout=120, check=True)
        except Exception as e:
            shutil.rmtree(clone_path, ignore_errors=True)
            outcome['reason'] = f'Failed to publish source-fix branch: {e}'
            return outcome

        owner_repo = ''
        m = re.search(r'github\.com[:/]+([^/]+/[^/.]+)(?:\.git)?$', repo_url)
        if m:
            owner_repo = m.group(1)

        pr_url = ''
        if owner_repo:
            try:
                pr_proc = subprocess.run(
                    [
                        'gh', 'pr', 'create', '--repo', owner_repo,
                        '--base', target_branch, '--head', branch_name,
                        '--title', f"fix({short_service}): bump memory resources to mitigate OOM",
                        '--body', "## Summary\n- Auto-adjust memory requests/limits after OOM signal\n- Generated by AI monitoring source_fix workflow"
                    ],
                    cwd=clone_path, capture_output=True, text=True, timeout=120
                )
                if pr_proc.returncode == 0:
                    out = str((pr_proc.stdout or '') + '\n' + (pr_proc.stderr or ''))
                    u = re.search(r'https://github\.com/[^\s]+/pull/\d+', out)
                    if u:
                        pr_url = u.group(0)
                else:
                    err = (pr_proc.stderr or pr_proc.stdout or '').strip()
                    outcome['reason'] = f"Branch pushed but PR creation failed: {err[:240]}"
            except Exception as e:
                outcome['reason'] = f"Branch pushed but PR creation errored: {e}"

        shutil.rmtree(clone_path, ignore_errors=True)
        if pr_url:
            outcome['ok'] = 'true'
            outcome['url'] = pr_url
            outcome['reason'] = 'Source-fix PR created successfully'
            return outcome

        outcome['ok'] = 'false'
        if not outcome.get('reason'):
            outcome['reason'] = 'Branch pushed but PR URL not created; ensure gh auth/token is configured'
        return outcome

    def _needs_trace_log_backfill(self, reason_text: str, status_code: str, exception_text: str) -> bool:
        """Return True when span evidence is too weak and we should pull more logs."""
        reason = str(reason_text or '').strip().lower()
        status = str(status_code or '').strip()
        exc = str(exception_text or '').strip()

        if exc:
            return False
        if reason and reason not in {
            'no explicit error message in span',
            'span failed without explicit error payload; check correlated errors for failing service'
        }:
            return False
        if status and status not in {'0', '2', 'unknown'}:
            return False
        return True

    def _search_trace_logs_in_elasticsearch(self, trace_id: str, service_candidates: List[str], minutes: int, limit: int) -> List[str]:
        """Fetch extra trace-linked logs directly from Elasticsearch for stronger RCA."""
        if self.elasticsearch is None:
            return []

        trace_value = str(trace_id or '').strip()
        if not trace_value:
            return []

        end_time_ms = int(time.time() * 1000)
        start_time_ms = int((time.time() - max(1, minutes) * 60) * 1000)

        terms = []
        for svc in service_candidates or []:
            short = str(svc or '').split('/')[-1].strip()
            if short:
                terms.append(short)

        # Broad trace-id centered query; service terms help ranking but trace-id is mandatory.
        query = f'"{trace_value}"'
        if terms:
            service_q = ' OR '.join([f'"{item}"' for item in terms[:4]])
            query = f'({query}) AND ({service_q})'

        try:
            rows = self.elasticsearch.search_logs(
                query_string=query,
                start_time=start_time_ms,
                end_time=end_time_ms,
                limit=max(10, min(limit, 400))
            )
        except Exception:
            rows = []

        messages = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            msg = str(row.get('message') or row.get('body') or row.get('log') or row.get('msg') or '').strip()
            if not msg:
                continue
            if trace_value.lower() not in msg.lower():
                continue
            messages.append(msg)
            if len(messages) >= limit:
                break

        return messages

    def _derive_per_trace_failure_root_cause(self, anomalies: List[Dict], logs_traces: Dict) -> Dict[str, str]:
        """Derive RCA from a single failing trace chain (A->B->C... failure point)."""
        result = {'issue': '', 'recommendation': '', 'confidence_boost': 0.0}
        traces = logs_traces.get('traces', []) if isinstance(logs_traces, dict) else []
        if not traces:
            return result

        impacted = set()
        for anomaly in anomalies or []:
            svc = str(anomaly.get('service', '') or '')
            if svc:
                short = svc.split('/')[-1]
                impacted.update({svc, short, self._canonical_service(short)})

        by_trace = {}
        for span in traces:
            if not isinstance(span, dict):
                continue
            trace_id = str(span.get('trace_id', '') or '')
            if not trace_id:
                continue

            svc = str(span.get('service', '') or '')
            svc_short = svc.split('/')[-1]
            if impacted and svc and not (
                svc in impacted or svc_short in impacted or self._canonical_service(svc_short) in impacted
            ):
                continue

            by_trace.setdefault(trace_id, []).append(span)

        if not by_trace:
            return result

        best = None
        for trace_id, spans in by_trace.items():
            error_spans = [s for s in spans if self._trace_is_error(s)]
            if not error_spans:
                continue

            span_index = {str(s.get('span_id', '') or ''): s for s in spans}

            def _depth(span_obj: Dict) -> int:
                depth = 0
                current = span_obj
                guard = 0
                while guard < 64:
                    parent_id = str(current.get('parent_span_id', '') or '')
                    if not parent_id or parent_id not in span_index:
                        break
                    depth += 1
                    current = span_index[parent_id]
                    guard += 1
                return depth

            failing = sorted(
                error_spans,
                key=lambda s: (
                    _depth(s),
                    float(s.get('duration_ms', 0.0) or 0.0)
                ),
                reverse=True
            )[0]

            chain = []
            current = failing
            guard = 0
            while current and guard < 64:
                chain.append(current)
                parent_id = str(current.get('parent_span_id', '') or '')
                if not parent_id or parent_id not in span_index:
                    break
                current = span_index[parent_id]
                guard += 1
            chain.reverse()

            service_hops = []
            for span in chain:
                src = self._normalize_service_token(span.get('service', '')) or str(span.get('service', '') or '')
                edge = self._extract_trace_service_edge(span, fallback_source=src)
                if edge:
                    if not service_hops or service_hops[-1] != edge['from']:
                        service_hops.append(edge['from'])
                    if service_hops[-1] != edge['to']:
                        service_hops.append(edge['to'])
                elif src and (not service_hops or service_hops[-1] != src):
                    service_hops.append(src)

            attrs = self._extract_otel_attributes(failing)
            status_code = ''
            for key in ('http.response.status_code', 'http.status_code', 'status_code'):
                value = attrs.get(key) if key in attrs else failing.get(key)
                if value not in (None, ''):
                    status_code = str(value)
                    break

            reason_parts = []
            for key in ('exception.message', 'error.message'):
                if attrs.get(key):
                    reason_parts.append(str(attrs.get(key)))
            if failing.get('status_message'):
                reason_parts.append(str(failing.get('status_message')))

            target = self._normalize_service_token(
                attrs.get('server.address') or attrs.get('peer.service') or attrs.get('net.peer.name') or attrs.get('url.full') or ''
            )
            source_service = self._normalize_service_token(failing.get('service', '')) or str(failing.get('service', '') or 'unknown')
            failing_service = target or source_service

            service_context = self._extract_log_exception_context(source_service, logs_traces)
            trace_log_context = self._extract_trace_specific_log_context(
                trace_id=trace_id,
                failing_service=failing_service,
                source_service=source_service,
                logs_traces=logs_traces
            )
            best = {
                'trace_id': trace_id,
                'hops': service_hops,
                'failing_service': failing_service,
                'source_service': source_service,
                'status_code': status_code,
                'reason': next((part for part in reason_parts if part), ''),
                'span_name': str(failing.get('name', '') or ''),
                'exception': trace_log_context.get('exception', '') or service_context.get('exception', ''),
                'file_hint': trace_log_context.get('file_hint', '') or service_context.get('file_hint', ''),
                'message_hint': service_context.get('message_hint', ''),
                'actionable_line': service_context.get('actionable_line', ''),
                'trace_log_reason': trace_log_context.get('reason', ''),
                'score': (
                    len(service_hops) * 10 +
                    (30 if status_code else 0) +
                    (20 if service_context.get('exception') else 0) +
                    (18 if trace_log_context.get('reason') else 0)
                )
            }
            if best:
                break

        if not best:
            return result

        if self._needs_trace_log_backfill(best.get('reason', ''), best.get('status_code', ''), best.get('exception', '')):
            trace_window_minutes = int(self.config.get('elasticsearch', {}).get('trace_window_minutes', 30) or 30)
            backfill_limit = int(self.config.get('elasticsearch', {}).get('trace_log_backfill_limit', 80) or 80)
            extra_logs = self._search_trace_logs_in_elasticsearch(
                trace_id=best.get('trace_id', ''),
                service_candidates=[best.get('failing_service', ''), best.get('source_service', '')],
                minutes=trace_window_minutes,
                limit=backfill_limit
            )
            if extra_logs:
                best['trace_log_reason'] = extra_logs[0][:260]
                if not best.get('exception'):
                    ex_match = re.search(r'([a-zA-Z0-9_.]+(?:Exception|Error))', extra_logs[0])
                    if ex_match:
                        best['exception'] = ex_match.group(1)
                if not best.get('file_hint'):
                    file_match = re.search(r'([A-Za-z0-9_.$/-]+\.(?:java|kt|py|go):\d+|[A-Za-z0-9_.$-]+\.class)', extra_logs[0])
                    if file_match:
                        best['file_hint'] = file_match.group(1)

        chain_text = ' -> '.join(best['hops'][:8]) if best['hops'] else best['source_service']
        status_text = f"status_code={best['status_code']}" if best['status_code'] else 'status_code=unknown'
        if str(best.get('status_code', '')).strip() == '2':
            status_text = 'status=error'
        reason_text = (
            best['reason'] or
            best.get('trace_log_reason', '') or
            best.get('message_hint', '') or
            best.get('actionable_line', '') or
            best['exception'] or
            'span failed without explicit error payload; check correlated errors for failing service'
        )
        file_text = f", file={best['file_hint']}" if best['file_hint'] else ''
        exception_text = f", exception={best['exception']}" if best['exception'] else ''

        result['issue'] = (
            f"Exact Issue: Trace {best['trace_id']} chain {chain_text} failed at {best['failing_service']} "
            f"({status_text}, span={best['span_name'] or 'unknown'}{exception_text}{file_text}) | reason: {reason_text[:500]}"
        )

        # If correlated logs expose a concrete code exception, prefer that as
        # exact issue over generic dependency-edge symptom text.
        concrete_reason = str(reason_text or '')
        concrete_lower = concrete_reason.lower()
        has_concrete_exception = bool(re.search(r'\b([a-zA-Z0-9_.$]+(?:Exception|Error))\b', concrete_reason, re.IGNORECASE))
        only_network_symptom = any(token in concrete_lower for token in ['connection refused', 'timed out', 'timeout'])
        if has_concrete_exception and not only_network_symptom:
            direct_issue = self._derive_exact_from_message(best.get('failing_service', ''), concrete_reason)
            if direct_issue.get('issue'):
                result['issue'] = direct_issue['issue']
                if direct_issue.get('recommendation'):
                    result['recommendation'] = direct_issue['recommendation']

        if not result.get('recommendation'):
            result['recommendation'] = (
                f"Inspect failing service {best['failing_service']} and upstream caller {best['source_service']} for the above span/status; "
                f"use trace_id {best['trace_id']} to verify first failing hop"
            )
        result['confidence_boost'] = 0.34
        return result

    def _collect_edge_failure_evidence(self, top_edge: Dict, logs_traces: Dict) -> Dict[str, bool]:
        """Collect coarse failure hints for one dependency edge from trace evidence."""
        evidence = {
            'has_503': False,
            'has_5xx': False,
            'has_timeout': False,
            'has_connection_refused': False
        }
        if not isinstance(top_edge, dict):
            return evidence

        source = str(top_edge.get('from', '') or '')
        target = str(top_edge.get('to', '') or '')
        source_short = source.split('/')[-1]
        target_short = target.split('/')[-1]

        for trace in logs_traces.get('traces', []) if isinstance(logs_traces, dict) else []:
            if not isinstance(trace, dict):
                continue

            t_source = str(trace.get('service', '') or '')
            t_target = str(trace.get('downstream_service', '') or '')
            t_source_short = t_source.split('/')[-1]
            t_target_short = t_target.split('/')[-1]

            if not (
                t_source == source or t_source_short == source_short or self._canonical_service(t_source_short) == self._canonical_service(source_short)
            ):
                continue
            if not (
                t_target == target or t_target_short == target_short or self._canonical_service(t_target_short) == self._canonical_service(target_short)
            ):
                continue

            attrs = self._extract_otel_attributes(trace)
            if self._is_internal_telemetry_span(trace):
                continue
            status_values = []
            for key in ('http.status_code', 'http.response.status_code', 'status_code'):
                value = attrs.get(key) if key in attrs else trace.get(key)
                if value is not None and str(value) != '':
                    status_values.append(str(value))

            message_blob = ' '.join([
                str(trace.get('status_message', '') or ''),
                str(trace.get('name', '') or ''),
                str(attrs.get('error.message', '') or ''),
                str(attrs.get('exception.message', '') or ''),
                str(attrs.get('url.full', '') or ''),
                str(attrs.get('http.url', '') or ''),
                str(attrs.get('server.address', '') or '')
            ]).lower()

            for status_text in status_values:
                try:
                    code = int(str(status_text))
                except Exception:
                    continue
                if code == 503:
                    evidence['has_503'] = True
                if code >= 500:
                    evidence['has_5xx'] = True

            if re.search(r'(?i)timeout|timed\s*out|deadline\s+exceeded', message_blob):
                evidence['has_timeout'] = True
            if re.search(r'(?i)connection\s+refused|failed\s+to\s+connect|refused\s+stream|reset\s+by\s+peer', message_blob):
                evidence['has_connection_refused'] = True

        return evidence

    def _derive_startup_failure_hint(self, source_service: str, anomalies: List[Dict], logs_traces: Dict) -> Tuple[str, bool]:
        """Derive startup-failure phrase and crash-loop signal for impacted service."""
        source_short = str(source_service).split('/')[-1]
        source_canonical = self._canonical_service(source_short)

        crashloop_present = False
        for anomaly in anomalies or []:
            if not isinstance(anomaly, dict):
                continue
            a_service = str(anomaly.get('service', '') or '')
            a_short = a_service.split('/')[-1]
            if not (
                a_service == source_service or a_short == source_short or self._canonical_service(a_short) == source_canonical
            ):
                continue
            a_type = str(anomaly.get('type', '') or '')
            if a_type in {'CRASH_LOOP', 'POD_ISSUES', 'POD_STARTUP_FAILURE'}:
                crashloop_present = True

        candidate_logs = []
        if isinstance(logs_traces, dict):
            candidate_logs.extend(logs_traces.get('logs', []) or [])
            candidate_logs.extend(logs_traces.get('service_errors', []) or [])

        merged = []
        for log in candidate_logs:
            if not isinstance(log, dict):
                continue
            l_service = str(log.get('service', '') or '')
            l_short = l_service.split('/')[-1]
            if l_service and not (
                l_service == source_service or l_short == source_short or self._canonical_service(l_short) == source_canonical
            ):
                continue
            merged.append(str(log.get('message', log.get('body', '')) or '').lower())

        text = ' '.join(merged)
        if re.search(r'(?i)could\s+not\s+resolve\s+placeholder', text):
            return ('missing required configuration placeholder', crashloop_present)
        if re.search(r'(?i)failed\s+to\s+bind\s+properties|configurationpropertiesbindexception|bindexception', text):
            return ('invalid configuration value during startup binding', crashloop_present)
        if re.search(r'(?i)unsatisfieddependencyexception|beancreationexception', text):
            return ('bean initialization failure during startup', crashloop_present)
        if re.search(r'(?i)unable\s+to\s+start|context\s+initialization\s+-\s+cancelling\s+refresh', text):
            return ('application startup failure', crashloop_present)

        return ('startup/config bootstrap failure', crashloop_present)

    def _derive_log_root_cause(self, anomalies: List[Dict], logs_traces: Dict) -> Dict[str, str]:
        """Derive deterministic exact issue from actionable logs when traces are inconclusive."""
        result = {'issue': '', 'recommendation': '', 'confidence_boost': 0.0}
        logs = logs_traces.get('logs', []) if isinstance(logs_traces, dict) else []

        for entry in logs:
            if not isinstance(entry, dict):
                continue
            message = str(entry.get('message', '') or '')
            if not message:
                continue

            service = str(entry.get('service', '') or 'unknown')
            startup_hint, crashloop_present = self._derive_startup_failure_hint(service, anomalies, logs_traces)

            parsed = self._derive_exact_from_message(service, message, crashloop_present=crashloop_present)
            if parsed.get('issue'):
                result['issue'] = parsed['issue']
                result['recommendation'] = parsed.get('recommendation', 'Inspect startup exception stack trace in pod logs and fix failing init path')
                result['confidence_boost'] = 0.4
                return result

        traces = logs_traces.get('traces', []) if isinstance(logs_traces, dict) else []
        for trace in traces:
            if not isinstance(trace, dict):
                continue
            attrs = self._extract_otel_attributes(trace)
            host = str(attrs.get('server.address', '') or '').lower()
            status_503 = False
            for code_key in ('http.status_code', 'http.response.status_code'):
                try:
                    if int(str(attrs.get(code_key, '0') or '0')) == 503:
                        status_503 = True
                        break
                except Exception:
                    continue

            if status_503:
                source_service = str(trace.get('service', '') or 'unknown')
                target_service = self._normalize_service_token(
                    attrs.get('peer.service') or attrs.get('server.address') or attrs.get('net.peer.name') or attrs.get('url.full') or host
                ) or 'downstream'
                startup_hint, crashloop_present = self._derive_startup_failure_hint(source_service, anomalies, logs_traces)
                chain_tail = f" -> {startup_hint} -> CrashLoopBackOff" if crashloop_present else f" -> {startup_hint}"
                result['issue'] = f"Exact Issue: {source_service} -> {target_service} returned HTTP 503{chain_tail}"
                result['recommendation'] = f"Validate downstream {target_service} availability and caller configuration, then restart impacted pods"
                result['confidence_boost'] = 0.3
                return result

        impacted = set()
        for anomaly in anomalies or []:
            svc = str(anomaly.get('service', '') or '')
            if svc:
                short = svc.split('/')[-1]
                impacted.add(svc)
                impacted.add(short)
                impacted.add(self._canonical_service(short))

        actionable_logs = []
        for log in logs:
            message = str(log.get('message', '') or '')
            if self._is_actionable_error_message(message):
                svc = str(log.get('service', '') or '')
                short = svc.split('/')[-1]
                if not impacted or svc in impacted or short in impacted or self._canonical_service(short) in impacted:
                    actionable_logs.append(log)

        if not actionable_logs:
            for log in logs:
                message = str(log.get('message', '') or '')
                if self._is_actionable_error_message(message):
                    actionable_logs.append(log)

        if not actionable_logs:
            return result

        # Prefer startup/bootstrap fatal errors over generic crash-loop wrappers.
        def _score(log_entry: Dict) -> int:
            msg = str(log_entry.get('message', '') or '').lower()
            score = 0
            if 'could not resolve placeholder' in msg:
                score += 110
            if 'failed to bind properties' in msg or 'configurationpropertiesbindexception' in msg or 'bindexception' in msg:
                score += 100
            if 'unsatisfieddependencyexception' in msg or 'beancreationexception' in msg:
                score += 90
            if 'unable to start' in msg or 'context initialization - cancelling refresh attempt' in msg:
                score += 80
            if '503' in msg or 'service unavailable' in msg:
                score += 40
            if 'crashloopbackoff' in msg:
                score += 10
            return score

        actionable_logs.sort(key=_score, reverse=True)
        top = actionable_logs[0]
        service = str(top.get('service', '') or 'unknown')
        message = str(top.get('message', '') or '')
        dependency = self._extract_dependency_from_message(message)

        startup_hint, crashloop_present = self._derive_startup_failure_hint(service, anomalies, logs_traces)

        if re.search(r'(?i)\b503\b|service\s+unavailable', message):
            downstream = self._extract_dependency_from_message(message) or 'downstream'
            chain_tail = f" -> {startup_hint} -> CrashLoopBackOff" if crashloop_present else f" -> {startup_hint}"
            result['issue'] = f"Exact Issue: {service} -> {downstream} returned HTTP 503{chain_tail}"
            result['recommendation'] = f"Validate downstream {downstream} health and caller configuration, then retry startup"
            result['confidence_boost'] = 0.28
            return result

        if re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', message):
            missing = re.search(r'(?i)could\s+not\s+resolve\s+placeholder\s+[\'\"]?([a-zA-Z0-9_.-]+)[\'\"]?', message)
            placeholder = missing.group(1) if missing else 'unknown_placeholder'
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            result['issue'] = f"Exact Issue: {service} missing config placeholder '{placeholder}' during startup{chain_tail}"
            result['recommendation'] = f"Provide required config/secret '{placeholder}' and verify environment/property source mapping"
            result['confidence_boost'] = 0.33
            return result

        if re.search(r'(?i)failed\s+to\s+bind\s+properties|configurationpropertiesbindexception|bindexception', message):
            chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
            result['issue'] = f"Exact Issue: {service} invalid configuration binding during startup{chain_tail}"
            result['recommendation'] = "Fix invalid/mismatched config types in environment, configmap, or secret values"
            result['confidence_boost'] = 0.3
            return result

        if re.search(r'(?i)connection\s+refused|failed\s+to\s+connect', message):
            if re.search(r'(?i)localhost/127\.0\.0\.1:6379|redis', message):
                result['issue'] = f"Exact Issue: {service} redis connection refused (localhost:6379)"
                result['recommendation'] = "Verify Redis service endpoint, credentials, and network access from the service pod"
                result['confidence_boost'] = 0.2
                return result

            if re.search(r'(?i)localhost/127\.0\.0\.1:9411|zipkin|tracing', message):
                result['issue'] = f"Exact Issue: {service} tracing endpoint connection refused (localhost:9411)"
                result['recommendation'] = "Disable local tracing exporter in pod or configure reachable tracing backend endpoint"
                result['confidence_boost'] = 0.16
                return result

            # Prefer startup-fatal signatures over plain network wrapper when both are present.
            if re.search(r'(?i)beandefinitionstoreexception|failed\s+to\s+read\s+candidate\s+component\s+class', message):
                class_match = re.search(r'([A-Za-z0-9_.$-]+AutoConfiguration\.class|[A-Za-z0-9_.$-]+\.class)', message)
                class_hint = class_match.group(1) if class_match else 'component class'
                chain_tail = " -> CrashLoopBackOff" if crashloop_present else ""
                result['issue'] = f"Exact Issue: {service} startup failed while loading {class_hint}{chain_tail}"
                result['recommendation'] = "Verify classpath/jar compatibility and failing autoconfiguration dependency"
                result['confidence_boost'] = 0.32
                return result

            if re.search(r'(?i)unsatisfieddependencyexception|beancreationexception|could\s+not\s+resolve\s+placeholder|failed\s+to\s+bind\s+properties', message):
                parsed = self._derive_exact_from_message(service, message, crashloop_present=crashloop_present)
                if parsed.get('issue'):
                    result['issue'] = parsed['issue']
                    result['recommendation'] = parsed.get('recommendation', "Inspect nested startup exception and fix bean/config path")
                    result['confidence_boost'] = 0.3
                    return result

            # Ignore synthetic dependency tokens from stacktrace/class internals
            if dependency and not self._normalize_service_token(dependency):
                dependency = ''
            to_text = f" while calling {dependency}" if dependency else ''
            result['issue'] = f"Exact Issue: {service} connection refused{to_text}"
            result['recommendation'] = (
                f"Check downstream endpoint{' ' + dependency if dependency else ''} service/pods and network policy"
            )
            result['confidence_boost'] = 0.18
            return result

        if re.search(r'(?i)timeout|timed\s*out|deadline\s+exceeded', message):
            to_text = f" while calling {dependency}" if dependency else ''
            result['issue'] = f"Exact Issue: {service} timeout{to_text}"
            result['recommendation'] = (
                f"Check latency and timeout configuration for{' ' + dependency if dependency else ' downstream dependency'}"
            )
            result['confidence_boost'] = 0.16
            return result

        if re.search(r'(?i)http\s*5\d\d|status\s*5\d\d', message):
            to_text = f" from {dependency}" if dependency else ''
            result['issue'] = f"Exact Issue: {service} received downstream 5xx{to_text}"
            result['recommendation'] = (
                f"Inspect downstream service{' ' + dependency if dependency else ''} for recent 5xx failures"
            )
            result['confidence_boost'] = 0.16
            return result

        # Generic actionable fallback with short message
        short = message[:220] + ('...' if len(message) > 220 else '')
        result['issue'] = f"Exact Issue: {service} actionable failure detected | sample: {short}"
        result['recommendation'] = "Inspect application logs and dependency calls around the sampled failure"
        result['confidence_boost'] = 0.1
        return result

    def _classify_edge_issue(self, top_edge: Dict, logs_traces: Dict) -> Tuple[str, str]:
        """Infer probable issue type for an edge from recent traces."""
        if not isinstance(top_edge, dict):
            return ('error-pattern', 'recent error patterns')

        source = str(top_edge.get('from', '') or '')
        target = str(top_edge.get('to', '') or '')
        traces = logs_traces.get('traces', []) if isinstance(logs_traces, dict) else []
        source_short = source.split('/')[-1]
        target_short = target.split('/')[-1]

        evidence_texts = []
        for trace in traces:
            if not isinstance(trace, dict):
                continue
            t_source = str(trace.get('service', '') or '')
            t_target = str(trace.get('downstream_service', '') or '')
            t_source_short = t_source.split('/')[-1]
            t_target_short = t_target.split('/')[-1]
            if not (
                t_source == source or t_source_short == source_short or self._canonical_service(t_source_short) == self._canonical_service(source_short)
            ):
                continue
            if not (
                t_target == target or t_target_short == target_short or self._canonical_service(t_target_short) == self._canonical_service(target_short)
            ):
                continue

            status_message = str(trace.get('status_message', '') or '')
            span_name = str(trace.get('name', '') or '')
            attrs = self._extract_otel_attributes(trace)
            attr_chunks = []
            for key in (
                'exception.message', 'error.message', 'http.status_code', 'http.response.status_code',
                'rpc.grpc.status_code', 'db.system', 'db.statement', 'db.name', 'net.peer.name',
                'server.address', 'http.url'
            ):
                if key in attrs and attrs.get(key) not in (None, ''):
                    attr_chunks.append(f"{key}={attrs.get(key)}")

            event_chunks = []
            for event in trace.get('events', []) or []:
                if not isinstance(event, dict):
                    continue
                event_name = str(event.get('name', '') or '')
                event_attrs = event.get('attributes', [])
                event_text_parts = [event_name]
                if isinstance(event_attrs, list):
                    for item in event_attrs:
                        if not isinstance(item, dict):
                            continue
                        key = str(item.get('key', '') or '')
                        value_obj = item.get('value', {})
                        value = ''
                        if isinstance(value_obj, dict):
                            value = str(
                                value_obj.get('stringValue')
                                or value_obj.get('intValue')
                                or value_obj.get('doubleValue')
                                or value_obj.get('boolValue')
                                or ''
                            )
                        elif value_obj is not None:
                            value = str(value_obj)
                        if key and value:
                            event_text_parts.append(f"{key}={value}")
                event_chunks.append(' '.join(event_text_parts))

            evidence_texts.append(' | '.join([status_message, span_name] + attr_chunks + event_chunks))

        text = ' '.join(evidence_texts).lower()

        if float(top_edge.get('p95_latency_ms', 0.0) or 0.0) >= float(self.config.get('monitoring', {}).get('trace_latency_threshold_ms', 1500) or 1500):
            return ('latency-timeout-pattern', 'timeout or latency bottlenecks')

        if float(top_edge.get('error_rate', 0.0) or 0.0) >= 50.0:
            return ('high-error-rate-pattern', 'downstream 5xx or exception failures')

        if 'timeout' in text or 'deadline' in text:
            return ('timeout-pattern', 'timeouts or deadline exceeded errors')
        if 'refused' in text or 'reset' in text or 'unavailable' in text:
            return ('network-pattern', 'network connectivity issues')
        if 'sql' in text or 'db' in text or 'postgres' in text or 'mysql' in text:
            return ('database-pattern', 'database query or connection issues')
        if '401' in text or '403' in text or 'unauthorized' in text:
            return ('auth-pattern', 'authentication or authorization errors')

        return ('error-pattern', 'recent error patterns')
        
    def detect_service_anomalies(self, service_status):
        """Detect anomalies from service status"""
        anomalies = []
        monitor_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        alert_on_healthy_actionable = bool(monitor_cfg.get('alert_on_actionable_errors_even_if_healthy', True))
        
        for service_name, status in service_status.items():
            metrics = status.get('metrics', {})
            recent_errors = status.get('recent_errors', [])
            
            # Check for high error rates only if service is not healthy
            error_rate = metrics.get('error_rate', 0)
            if error_rate > 5.0 and status.get('status') != 'healthy':
                sample_error = ''
                top_errors = []
                for err in (recent_errors or [])[:8]:
                    if not isinstance(err, dict):
                        continue
                    msg = str(err.get('message', '') or '').strip()
                    if not msg:
                        continue
                    compact = msg[:180]
                    if compact not in top_errors:
                        top_errors.append(compact)
                    if not sample_error:
                        sample_error = compact
                    if len(top_errors) >= 3:
                        break
                anomalies.append({
                    'type': 'HIGH_ERROR_RATE',
                    'service': service_name,
                    'value': error_rate,
                    'threshold': 5.0,
                    'description': f"Service {service_name} has high error rate: {error_rate:.2f}%",
                    'sample_error': sample_error,
                    'top_errors': top_errors
                })
                
            # Check for service degradation
            if status.get('status') == 'degraded':
                anomalies.append({
                    'type': 'SERVICE_DEGRADED',
                    'service': service_name,
                    'description': f"Service {service_name} is degraded"
                })
                
            # Check for offline/unreachable services
            if status.get('status') in ['unreachable', 'offline']:
                anomalies.append({
                    'type': 'SERVICE_UNREACHABLE',
                    'service': service_name,
                    'description': f"Service {service_name} is {status.get('status')}"
                })
                
            # Check frequent actionable errors. By default, allow alerting even when
            # service is currently healthy so short-lived code exceptions still create incidents.
            has_actionable_recent_error = False
            for err in (recent_errors or [])[:8]:
                if not isinstance(err, dict):
                    continue
                msg = str(err.get('message', '') or '').strip()
                if msg and self._is_actionable_error_message(msg):
                    has_actionable_recent_error = True
                    break

            if len(recent_errors) > 0 and (
                status.get('status') != 'healthy' or
                (alert_on_healthy_actionable and has_actionable_recent_error)
            ):
                sample_error = str(recent_errors[0].get('message', '') or '')
                anomalies.append({
                    'type': 'FREQUENT_ERRORS',
                    'service': service_name,
                    'count': len(recent_errors),
                    'description': f"Service {service_name} has {len(recent_errors)} recent errors",
                    'sample_error': sample_error
                })
                
            # Check for dependency failures
            dependency_failures = metrics.get('dependency_failures', 0)
            if dependency_failures > 0:
                failed_deps = metrics.get('failed_dependencies', [])
                anomalies.append({
                    'type': 'DEPENDENCY_FAILURE',
                    'service': service_name,
                    'count': dependency_failures,
                    'dependencies': failed_deps,
                    'description': f"Service {service_name} has {dependency_failures} dependency failures"
                })
                
            # Check for pod issues
            pod_status = status.get('pod_status', {})
            pod_entries = pod_status.get('pods', [])

            # Look for specific pod startup/runtime failures first
            for pod in pod_entries:
                reason_text = (pod.get('reason') or '').strip()
                if not reason_text:
                    continue

                reason_lower = reason_text.lower()
                if ('imagepullbackoff' in reason_lower or
                    'errimagepull' in reason_lower or
                    'image pull' in reason_lower):
                    anomalies.append({
                        'type': 'IMAGE_PULL_ERROR',
                        'service': service_name,
                        'pod': pod.get('name', 'unknown'),
                        'reason': reason_text,
                        'description': f"Service {service_name} pod {pod.get('name', 'unknown')} has image pull error: {reason_text}"
                    })
                elif 'crashloopbackoff' in reason_lower:
                    anomalies.append({
                        'type': 'CRASH_LOOP',
                        'service': service_name,
                        'pod': pod.get('name', 'unknown'),
                        'reason': reason_text,
                        'description': f"Service {service_name} pod {pod.get('name', 'unknown')} is in crash loop: {reason_text}"
                    })
                elif any(keyword in reason_lower for keyword in ['createcontainerconfigerror', 'runcontainererror', 'invalidimagename']):
                    anomalies.append({
                        'type': 'POD_STARTUP_FAILURE',
                        'service': service_name,
                        'pod': pod.get('name', 'unknown'),
                        'reason': reason_text,
                        'description': f"Service {service_name} pod {pod.get('name', 'unknown')} failed to start: {reason_text}"
                    })

            if pod_status.get('status') == 'unhealthy':
                reason_parts = []
                for pod in pod_entries[:3]:
                    if not isinstance(pod, dict):
                        continue
                    pod_name = str(pod.get('name', 'unknown') or 'unknown')
                    pod_reason = str(pod.get('reason', '') or '').strip()
                    pod_state = str(pod.get('status', '') or '').strip()
                    pod_ready = str(pod.get('ready', '') or '').strip()
                    if pod_reason:
                        reason_parts.append(f"{pod_name}: {pod_reason}")
                    elif pod_state and pod_ready and pod_ready != '1/1':
                        reason_parts.append(f"{pod_name}: status={pod_state}, ready={pod_ready}")
                    elif pod_state:
                        reason_parts.append(f"{pod_name}: status={pod_state}")

                anomalies.append({
                    'type': 'POD_ISSUES',
                    'service': service_name,
                    'description': f"Service {service_name} has unhealthy pods",
                    'pod_details': reason_parts,
                    'pod_status_summary': str(pod_status.get('reason', '') or '').strip()
                })
            elif pod_status.get('status') == 'no_pods':
                pod_presence = self._service_pod_presence.get(service_name, {'seen': False, 'ts': ''})
                deleted_hint = ''
                if pod_presence.get('seen'):
                    deleted_hint = " (pods disappeared/deleted recently)"
                anomalies.append({
                    'type': 'NO_PODS',
                    'service': service_name,
                    'description': f"Service {service_name} has no running pods{deleted_hint}"
                })

            # Track if service pods were seen previously to flag delete/disappear transitions.
            self._service_pod_presence[service_name] = {
                'seen': bool(pod_entries),
                'ts': datetime.now().isoformat()
            }
                
        return anomalies

    def _build_runtime_error_signal_anomalies(self, service_status: Dict) -> List[Dict]:
        """Fallback anomaly signals from actionable recent errors in service status."""
        derived: List[Dict] = []
        if not isinstance(service_status, dict):
            return derived

        for service_name, status in service_status.items():
            if not isinstance(status, dict):
                continue
            recent_errors = status.get('recent_errors', []) or []
            if not isinstance(recent_errors, list) or not recent_errors:
                continue

            sample_error = ''
            for item in recent_errors[:8]:
                if not isinstance(item, dict):
                    continue
                msg = str(item.get('message', '') or '').strip()
                if msg and self._is_actionable_error_message(msg):
                    sample_error = msg
                    break

            if not sample_error:
                continue

            derived.append({
                'type': 'FREQUENT_ERRORS',
                'service': service_name,
                'count': len(recent_errors),
                'description': f"Service {service_name} has actionable runtime errors",
                'sample_error': sample_error
            })

        return derived

    def filter_actionable_anomalies(self, service_status, anomalies):
        """Keep only actionable anomalies to reduce noisy incidents."""
        actionable = []
        monitor_cfg = self.config.get('monitoring', {}) if isinstance(self.config, dict) else {}
        alert_on_healthy_actionable = bool(monitor_cfg.get('alert_on_actionable_errors_even_if_healthy', True))
        critical_types = {
            'IMAGE_PULL_ERROR',
            'CRASH_LOOP',
            'POD_STARTUP_FAILURE',
            'NO_PODS',
            'SERVICE_UNREACHABLE',
            'POD_ISSUES',
            'DEPENDENCY_FAILURE'
        }

        for anomaly in anomalies:
            service = anomaly.get('service', '')
            status = service_status.get(service, {}).get('status', 'unknown')
            anomaly_type = anomaly.get('type', '')

            if anomaly_type in critical_types:
                actionable.append(anomaly)
                continue

            # Suppress non-critical anomalies on healthy services, except explicit
            # actionable runtime errors when permissive healthy-alert mode is enabled.
            if status == 'healthy':
                if anomaly_type == 'FREQUENT_ERRORS' and alert_on_healthy_actionable:
                    sample_error = str(anomaly.get('sample_error', '') or '').strip()
                    if sample_error and self._is_actionable_error_message(sample_error):
                        actionable.append(anomaly)
                continue

            # Keep high error rate/warning only on non-healthy services
            if anomaly_type in {'HIGH_ERROR_RATE', 'FREQUENT_ERRORS', 'SERVICE_DEGRADED'}:
                actionable.append(anomaly)

        return actionable

    def _service_matches(self, scoped_service: str, candidate_service: str) -> bool:
        """Return True when two service identifiers refer to same service."""
        a = str(scoped_service or '')
        b = str(candidate_service or '')
        if not a or not b:
            return False
        if a == b:
            return True
        a_short = a.split('/')[-1]
        b_short = b.split('/')[-1]
        if a_short == b_short:
            return True
        return self._canonical_service(a_short) == self._canonical_service(b_short)

    def _group_anomalies_by_service(self, anomalies: List[Dict]) -> Dict[str, List[Dict]]:
        """Group anomalies by impacted service key for strict service-scoped incidents."""
        grouped: Dict[str, List[Dict]] = {}
        for anomaly in anomalies or []:
            if not isinstance(anomaly, dict):
                continue
            service = str(anomaly.get('service', '') or '')
            if not service:
                continue
            service_key = str(service)
            if '/' in service_key:
                ns, short = service_key.split('/', 1)
                short = self._canonical_service(short)
                service_key = f"{ns}/{short}" if short else service_key
            else:
                service_key = self._canonical_service(service_key) or service_key

            normalized_anomaly = dict(anomaly)
            normalized_anomaly['service'] = service_key
            grouped.setdefault(service_key, []).append(normalized_anomaly)
        return grouped

    def _filter_logs_traces_for_service(self, scoped_service: str, logs_traces: Dict) -> Dict:
        """Filter logs/traces context to one service (plus directly connected trace hops)."""
        filtered = {
            'logs': [],
            'traces': [],
            'service_errors': []
        }
        if not isinstance(logs_traces, dict):
            return filtered

        service_short = str(scoped_service or '').split('/')[-1]
        service_canonical = self._canonical_service(service_short)

        def _belongs(service_value: str) -> bool:
            val = str(service_value or '')
            if not val:
                return False
            short = val.split('/')[-1]
            return val == scoped_service or short == service_short or self._canonical_service(short) == service_canonical

        for entry in logs_traces.get('logs', []) or []:
            if not isinstance(entry, dict):
                continue
            if _belongs(str(entry.get('service', '') or '')):
                filtered['logs'].append(entry)

        for entry in logs_traces.get('service_errors', []) or []:
            if not isinstance(entry, dict):
                continue
            entry_service = str(entry.get('service', '') or '')
            if entry_service and _belongs(entry_service):
                filtered['service_errors'].append(entry)

        for trace in logs_traces.get('traces', []) or []:
            if not isinstance(trace, dict):
                continue
            src = str(trace.get('service', '') or '')
            dst = str(trace.get('downstream_service', '') or '')
            src_short = src.split('/')[-1]
            dst_short = dst.split('/')[-1]
            if (
                _belongs(src) or _belongs(dst) or
                self._canonical_service(src_short) == service_canonical or
                self._canonical_service(dst_short) == service_canonical
            ):
                filtered['traces'].append(trace)

        return filtered

    def _scope_dependency_context_for_service(self, scoped_service: str, dependency_context: Dict) -> Dict:
        """Keep only dependency edges touching the scoped service."""
        if not isinstance(dependency_context, dict):
            return {'edges': [], 'impacted_services': []}

        service_short = str(scoped_service or '').split('/')[-1]
        service_canonical = self._canonical_service(service_short)

        scoped_edges = []
        for edge in dependency_context.get('edges', []) or []:
            if not isinstance(edge, dict):
                continue
            source = str(edge.get('from', '') or '')
            target = str(edge.get('to', '') or '')
            source_short = source.split('/')[-1]
            target_short = target.split('/')[-1]
            if (
                source == scoped_service or target == scoped_service or
                source_short == service_short or target_short == service_short or
                self._canonical_service(source_short) == service_canonical or
                self._canonical_service(target_short) == service_canonical
            ):
                scoped_edges.append(edge)

        scoped_impacted = sorted(list({
            str(edge.get('from', '') or '') for edge in scoped_edges
        } | {
            str(edge.get('to', '') or '') for edge in scoped_edges
        }))

        return {
            'edges': scoped_edges,
            'impacted_services': scoped_impacted
        }

    def _trim_incidents(self):
        """Trim incident memory to keep ephemeral storage usage low."""
        incident_limit = self.config.get('learning', {}).get('history_limit', 500)
        retention_hours = int(self.config.get('monitoring', {}).get('incident_retention_hours', 24) or 24)
        cutoff = datetime.now() - timedelta(hours=retention_hours)

        def _within_retention(item):
            try:
                ts = datetime.fromisoformat(str(item.get('timestamp', '')).replace('Z', '+00:00'))
                if ts.tzinfo is not None:
                    ts = ts.astimezone().replace(tzinfo=None)
                return ts >= cutoff
            except Exception:
                return True

        self.active_incidents = [inc for inc in self.active_incidents if _within_retention(inc)]
        self.resolved_incidents = [inc for inc in self.resolved_incidents if _within_retention(inc)]

        if len(self.active_incidents) > incident_limit:
            self.active_incidents = self.active_incidents[-incident_limit:]
        if len(self.resolved_incidents) > incident_limit:
            self.resolved_incidents = self.resolved_incidents[-incident_limit:]

        # Keep compact per-service root cause memory
        if len(self.resolution_memory) > incident_limit:
            keys = list(self.resolution_memory.keys())
            for key in keys[:-incident_limit]:
                self.resolution_memory.pop(key, None)
    
    def enhance_root_cause_analysis(self, anomalies, logs_traces):
        """Enhance root cause analysis with learning engine"""
        # Perform initial analysis
        initial_analysis = self.root_cause_analyzer.analyze(anomalies, logs_traces)
        
        # Extract error patterns
        log_messages = [log.get('body', log.get('message', '')) for log in logs_traces.get('logs', [])]
        error_text = " ".join(log_messages[:10])
        
        # Classify error using learning engine without breaking monitoring loop
        try:
            error_category = self.learning_engine.classify_error(error_text)
        except Exception as e:
            logger.warning(f"Learning classification skipped: {e}")
            error_category = 'unknown'
        
        # Enhance analysis with classification
        enhanced_analysis = {
            **initial_analysis,
            'error_category': error_category,
            'confidence': min(0.99, initial_analysis.get('confidence', 0.5) * 1.2)  # Boost confidence with cap
        }
        
        return enhanced_analysis
    
    def send_enhanced_alert(self, anomalies, analysis, similar_incidents, remedial_actions):
        """Send enhanced alert with learning insights"""
        # Send to Slack with enhanced information
        self.slack_notifier.send_enhanced_alert(anomalies, analysis, similar_incidents, remedial_actions)

    def send_alert_if_persistent(self, incident, anomalies, analysis, similar_incidents, remedial_actions):
        """Send Slack alert only when incident persists for configured delay."""
        delay_minutes = int(self.config.get('slack', {}).get('alert_delay_minutes', 5))
        now = datetime.now()

        signature = self._incident_signature(incident)
        first_seen_map = getattr(self, '_incident_first_seen', {})
        notified_map = getattr(self, '_incident_notified', {})

        if signature not in first_seen_map:
            first_seen_map[signature] = now.isoformat()
        self._incident_first_seen = first_seen_map

        if notified_map.get(signature):
            return

        first_seen = datetime.fromisoformat(first_seen_map[signature])
        age_minutes = (now - first_seen).total_seconds() / 60.0
        if age_minutes >= delay_minutes:
            self.send_enhanced_alert(anomalies, analysis, similar_incidents, remedial_actions)
            notified_map[signature] = now.isoformat()
            self._incident_notified = notified_map

    def _incident_signature(self, incident):
        analysis = incident.get('analysis', {})
        exact = analysis.get('exact_issues', [])
        service = 'unknown'
        anomalies = incident.get('anomalies', [])
        if anomalies:
            service = anomalies[0].get('service', 'unknown')
        
        # Smart dedup: group by service + issue category, not exact text
        issue = exact[0] if exact else analysis.get('summary', 'unknown')
        
        # Normalize issue to category for better dedup
        issue_lower = str(issue).lower()
        if 'enum' in issue_lower or 'invalid' in issue_lower:
            issue_category = 'enum_error'
        elif 'connectexception' in issue_lower or 'connection refused' in issue_lower:
            issue_category = 'connection_issue'
        elif 'timeout' in issue_lower:
            issue_category = 'timeout_issue'
        elif 'placeholder' in issue_lower or 'missing' in issue_lower:
            issue_category = 'config_issue'
        elif 'exception' in issue_lower:
            issue_category = 'code_exception'
        else:
            issue_category = 'general'
        
        # Remove trace IDs and noise for dedup
        issue = re.sub(r'\|\s*sample:\s*.*$', '', str(issue), flags=re.IGNORECASE)
        issue = re.sub(r'trace_id=[a-zA-Z0-9]+', 'trace_id=*', str(issue))
        issue = re.sub(r'\s+', ' ', str(issue)).strip()
        
        return f"{service}|{issue_category}"

    def _is_incident_resolved(self, incident: Dict, service_status: Dict) -> bool:
        """Check whether active incident should be auto-resolved."""
        anomalies = incident.get('anomalies', []) if isinstance(incident, dict) else []
        impacted_services = set()
        for anomaly in anomalies:
            service = str(anomaly.get('service', '') or '')
            if service:
                impacted_services.add(service)

        if not impacted_services:
            return False

        for service in impacted_services:
            svc_state = service_status.get(service, {}) if isinstance(service_status, dict) else {}
            status = str(svc_state.get('status', 'unknown') or 'unknown').lower()
            if status in {'degraded', 'down', 'pending', 'warning', 'offline', 'unreachable'}:
                return False
            if svc_state.get('recent_errors'):
                return False

        return True

    def _has_trace_failure_signal(self, dependency_context: Dict) -> bool:
        """Return True when dependency context contains erroring trace edges."""
        if not isinstance(dependency_context, dict):
            return False
        for edge in dependency_context.get('edges', []) or []:
            try:
                if int(edge.get('error_count', 0) or 0) > 0:
                    return True
            except Exception:
                continue
        return False

    def _is_actionable_anomaly(self, anomaly: Dict) -> bool:
        """Gate anomalies strictly for war-mode incident generation."""
        if not isinstance(anomaly, dict):
            return False

        # Always skip ConnectException - too noisy, not actionable
        sample_error = str(anomaly.get('sample_error', '') or '').lower()
        if 'connectexception' in sample_error or 'connection refused' in sample_error:
            return False
        sample_message = str(anomaly.get('message', '') or '').lower()
        if 'connectexception' in sample_message or 'connection refused' in sample_message:
            return False

        anomaly_type = str(anomaly.get('type', '') or '')
        if anomaly_type in {
            'IMAGE_PULL_ERROR', 'CRASH_LOOP', 'POD_STARTUP_FAILURE', 'NO_PODS',
            'SERVICE_UNREACHABLE', 'POD_ISSUES', 'DEPENDENCY_FAILURE'
        }:
            return True

        if anomaly_type == 'HIGH_ERROR_RATE':
            try:
                return float(anomaly.get('value', 0.0) or 0.0) >= 5.0
            except Exception:
                return False

        if anomaly_type == 'FREQUENT_ERRORS':
            return self._is_actionable_error_message(str(anomaly.get('sample_error', '') or ''))

        return False
    
    def record_incident(self, metrics, logs_traces, analysis):
        """Record incident for continuous learning"""
        exact_issues = analysis.get('exact_issues', []) if analysis else []
        summary = analysis.get('summary', 'unknown') if analysis else 'unknown'
        issue_key = exact_issues[0] if exact_issues else summary

        # Keep only compact error evidence (avoid storing full payloads)
        compact_logs = []
        for log in logs_traces.get('logs', []):
            severity = log.get('severity', 'UNKNOWN')
            if severity in {'ERROR', 'FATAL', 'CRITICAL', 'IMAGE_PULL', 'CRASH_LOOP', 'POD_ERROR'}:
                compact_logs.append({
                    'severity': severity,
                    'message': (log.get('message', '') or '')[:220]
                })
            if len(compact_logs) >= 20:
                break

        incident_data = {
            'service': self.config.get('services', [{}])[0].get('name', 'unknown'),
            'analysis': {
                'summary': summary,
                'exact_issues': exact_issues[:3],
                'error_category': analysis.get('error_category', 'unknown') if analysis else 'unknown'
            },
            'logs': compact_logs
        }
        
        self.learning_engine.record_incident(incident_data)
        
    def get_status(self):
        """Get agent status for dashboard"""
        uptime = 0
        if self.agent_status.get('start_time'):
            start_time = datetime.fromisoformat(self.agent_status['start_time'])
            uptime = (datetime.now() - start_time).total_seconds()
            
        return {
            'running': self.agent_status['running'],
            'last_check': self.agent_status.get('last_check'),
            'uptime': uptime,
            'active_incidents': len(self.active_incidents),
            'resolved_incidents': len(self.resolved_incidents),
            'metrics_history_count': len(self.metrics_history),
            'llm_available': self.root_cause_analyzer.is_llm_available() if hasattr(self, 'root_cause_analyzer') else False
        }
        
    def get_recent_incidents(self, limit=10):
        """Get recent incidents for dashboard"""
        all_incidents = self.active_incidents + self.resolved_incidents
        # Sort by timestamp (newest first)
        all_incidents.sort(key=lambda x: x['timestamp'], reverse=True)
        return all_incidents[:limit]
        
    def get_metrics_history(self, limit=50):
        """Get metrics history for dashboard charts"""
        # Return most recent metrics
        return self.metrics_history[-limit:] if self.metrics_history else []
        
    def resolve_incident(self, incident_id):
        """Mark an incident as resolved"""
        for i, incident in enumerate(self.active_incidents):
            if incident['id'] == incident_id:
                incident['status'] = 'resolved'
                resolved_incident = self.active_incidents.pop(i)
                self.resolved_incidents.append(resolved_incident)
                return True
        return False
        
    def update_config(self, new_config):
        """Update agent configuration"""
        try:
            self.last_config_update_note = ""
            if not isinstance(new_config, dict):
                raise ValueError("Configuration payload must be a JSON object")

            # Deep-merge config so partial dashboard updates do not erase required keys
            merged_config = dict(self.config)
            for key, value in new_config.items():
                if isinstance(value, dict) and isinstance(merged_config.get(key), dict):
                    merged_section = dict(merged_config.get(key, {}))
                    merged_section.update(value)
                    merged_config[key] = merged_section
                else:
                    merged_config[key] = value

            # Validate minimal required sections
            if 'prometheus' not in merged_config:
                raise ValueError("Missing required 'prometheus' configuration")

            # Ensure optional sections are always present after update.
            self.config = merged_config
            self._ensure_default_config_sections()

            # Keep merged_config in sync with normalized runtime config.
            merged_config = dict(self.config)

            # Update configuration
            self.config = merged_config
            
            # Save to file
            try:
                with open("config.json", 'w') as f:
                    json.dump(self.config, f, indent=2)
            except Exception as e:
                # Keep runtime config even if filesystem is read-only
                self.last_config_update_note = f"Applied in memory only (could not persist to file): {e}"
                logger.warning(self.last_config_update_note)
                
            return True
        except Exception as e:
            logger.error(f"Error updating configuration: {e}")
            self.last_config_update_note = str(e)
            return False

# Global agent instance
agent = None

def get_agent():
    """Get the global agent instance"""
    global agent
    if agent is None:
        agent = AIMonitoringAgent("config.json")
    return agent

if __name__ == "__main__":
    # Initialize with configuration
    agent = get_agent()
    
    # Start monitoring
    logger.info("Starting AI Monitoring Agent with Machine Learning...")
    agent.start_monitoring()
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down AI Monitoring Agent...")
        agent.agent_status['running'] = False
