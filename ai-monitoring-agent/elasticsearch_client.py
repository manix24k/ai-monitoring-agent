#!/usr/bin/env python3
"""
Elasticsearch Client for AI Monitoring Agent
"""
try:
    from elasticsearch import Elasticsearch
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    Elasticsearch = None
    ELASTICSEARCH_AVAILABLE = False

import json
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta

class ElasticsearchClient:
    def __init__(self, hosts: list, index_prefix: str = "logs-*", 
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize Elasticsearch client
        
        Args:
            hosts: List of Elasticsearch hosts (e.g., ['http://localhost:9200'])
            index_prefix: Prefix for log indices (default: "logs-*")
            username: Elasticsearch username (optional)
            password: Elasticsearch password (optional)
        """
        self.index_prefix = index_prefix
        self.username = username
        self.password = password
        
        # Initialize Elasticsearch client only if available
        if ELASTICSEARCH_AVAILABLE and hosts and len(hosts) > 0 and hosts[0]:
            if username and password:
                self.es = Elasticsearch(
                    hosts=hosts,
                    http_auth=(username, password),
                    verify_certs=False,
                    timeout=12,
                    max_retries=3,
                    retry_on_timeout=True
                )
            else:
                self.es = Elasticsearch(
                    hosts=hosts,
                    verify_certs=False,
                    timeout=12,
                    max_retries=3,
                    retry_on_timeout=True
                )
        else:
            self.es = None
        self.index_prefix = index_prefix
        print(f"DEBUG: Elasticsearch client initialized with index prefix: {index_prefix}")

    def _index_target(self, fallback: str = "*-*") -> str:
        """Build safe ES index target from configured index_prefix."""
        raw = str(self.index_prefix or '').strip()
        if not raw:
            return fallback

        parts = [p.strip() for p in raw.split(',') if str(p).strip()]
        if not parts:
            return fallback

        normalized = []
        for token in parts:
            if '*' in token:
                normalized.append(token)
            else:
                normalized.append(f"{token}-*")
        return ','.join(normalized)

    def is_connected(self) -> bool:
        """Check connectivity with Elasticsearch cluster."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return False
        try:
            return bool(self.es.ping())
        except Exception:
            return False

    def get_service_logs(self, service: str, namespace: str, minutes: int = 5, limit: int = 100) -> List[Dict]:
        """Fetch service logs from Elasticsearch using filebeat-style indices."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return []

        service = (service or "").strip()
        namespace = (namespace or "").strip()
        if not service:
            return []

        candidates = [service]
        if service.endswith('-service'):
            candidates.append(service[:-8])
        if service.endswith('-svc'):
            candidates.append(service[:-4])
        candidates = [c for i, c in enumerate(candidates) if c and c not in candidates[:i]]

        index_patterns = [f"{candidate}-*" for candidate in candidates]
        search_index = ",".join(index_patterns) if index_patterns else "*"

        query = {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": f"now-{int(minutes)}m"}}}
                ],
                "filter": []
            }
        }

        # Do not apply namespace filter when namespace is unknown placeholder
        if namespace and str(namespace).lower() not in {'unknown', 'all', '*'}:
            query["bool"]["filter"].append({"term": {"kubernetes.namespace": namespace}})

        try:
            response = self.es.search(
                index=search_index,
                body={
                    "query": query,
                    "size": limit,
                    "sort": [{"@timestamp": {"order": "desc"}}]
                },
                ignore_unavailable=True
            )
        except Exception as e:
            print(f"Error fetching service logs for {service}: {e}")
            return []

        logs = []
        for hit in response.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            resolved_namespace = self._extract_namespace(source) or namespace or 'unknown'
            logs.append({
                'timestamp': source.get('@timestamp', datetime.now().isoformat()),
                'message': source.get('message', ''),
                'service': service,
                'namespace': resolved_namespace,
                'severity': source.get('log', {}).get('level', 'INFO') if isinstance(source.get('log'), dict) else source.get('log.level', 'INFO')
            })
        return logs

    def _extract_namespace(self, source: Dict) -> str:
        """Extract kubernetes namespace from different event shapes."""
        if not isinstance(source, dict):
            return ''

        kubernetes = source.get('kubernetes', {})
        if isinstance(kubernetes, dict):
            namespace = kubernetes.get('namespace') or kubernetes.get('namespace_name')
            if namespace:
                return str(namespace)

        flat_candidates = [
            source.get('kubernetes.namespace'),
            source.get('kubernetes.namespace_name'),
            source.get('kubernetes_namespace')
        ]
        for namespace in flat_candidates:
            if namespace:
                return str(namespace)

        return ''

    def _extract_service_name(self, source: Dict) -> str:
        """Extract service/container name from common log document fields."""
        if not isinstance(source, dict):
            return ''

        kubernetes = source.get('kubernetes', {})
        if isinstance(kubernetes, dict):
            container = kubernetes.get('container', {})
            if isinstance(container, dict):
                name = container.get('name')
                if name:
                    return str(name)

            labels = kubernetes.get('labels', {})
            if isinstance(labels, dict):
                for key in ('app', 'app_kubernetes_io/name', 'k8s-app', 'component'):
                    value = labels.get(key)
                    if value:
                        return str(value)

            pod = kubernetes.get('pod', {})
            if isinstance(pod, dict):
                pod_name = pod.get('name')
                if pod_name:
                    # Convert pod name to service-ish prefix
                    return str(pod_name).rsplit('-', 2)[0]

        service = source.get('service', {})
        if isinstance(service, dict):
            name = service.get('name')
            if name:
                return str(name)

        flat_candidates = [
            source.get('kubernetes.container.name'),
            source.get('container_name'),
            source.get('service.name')
        ]
        for value in flat_candidates:
            if value:
                return str(value)

        return ''

    def _service_variants(self, service: str) -> List[str]:
        """Generate generic service token variants for resilient matching."""
        base = str(service or '').strip()
        if not base:
            return []

        variants: List[str] = []

        def _add(value: str):
            token = str(value or '').strip()
            if token and token not in variants:
                variants.append(token)

        _add(base)
        _add(base.lower())
        _add(base.replace('-', '_'))
        _add(base.replace('_', '-'))
        _add(base.replace('-', ''))
        _add(base.replace('_', ''))

        lowered = base.lower()
        for suffix in ('-service', '_service', '-svc', '_svc'):
            if lowered.endswith(suffix):
                trimmed = base[: -len(suffix)]
                _add(trimmed)
                _add(trimmed.replace('-', '_'))
                _add(trimmed.replace('_', '-'))

        return variants

    def _service_identity(self, name: str) -> str:
        """Normalize service names for strict identity matching."""
        value = str(name or '').strip().lower()
        if not value:
            return ''
        for suffix in ('-service', '_service', '-svc', '_svc'):
            if value.endswith(suffix):
                value = value[: -len(suffix)]
                break
        return re.sub(r'[^a-z0-9]+', '', value)

    def _same_service(self, left: str, right: str) -> bool:
        """True when two service names resolve to the same identity."""
        a = self._service_identity(left)
        b = self._service_identity(right)
        return bool(a and b and a == b)

    def _service_from_index(self, index_name: str) -> str:
        """Extract service-like prefix from index name <service>-YYYY.MM.DD."""
        raw = str(index_name or '').strip()
        if not raw:
            return ''
        match = re.match(r'^(?P<svc>.+)-\d{4}\.\d{2}\.\d{2}$', raw)
        if match:
            return str(match.group('svc') or '').strip()
        return ''

    def _service_match_in_text(self, text: str, variants: List[str]) -> bool:
        raw = str(text or '')
        if not raw or not variants:
            return False
        normalized = re.sub(r'[^a-zA-Z0-9]+', '', raw).lower()
        for token in variants:
            t = str(token or '')
            if not t:
                continue
            if t.lower() in raw.lower():
                return True
            t_normalized = re.sub(r'[^a-zA-Z0-9]+', '', t).lower()
            if t_normalized and t_normalized in normalized:
                return True
        return False

    def _belongs_to_service(self, source: Dict, variants: List[str]) -> bool:
        """Best-effort service ownership check across structured and unstructured fields."""
        if not isinstance(source, dict) or not variants:
            return False

        svc = self._extract_service_name(source)
        if self._service_match_in_text(svc, variants):
            return True

        message = str(source.get('message', '') or source.get('log', '') or '')
        if self._service_match_in_text(message, variants):
            return True

        logger_name = str(source.get('logger_name', '') or source.get('logger', '') or '')
        if self._service_match_in_text(logger_name, variants):
            return True

        return False

    def get_services_overview(
        self,
        minutes: int = 30,
        max_docs: int = 5000,
        namespaces: Optional[List[str]] = None,
        service_names: Optional[List[str]] = None,
        ignored_patterns: Optional[List[str]] = None
    ) -> Dict[str, Dict]:
        """Fetch one-window aggregated service overview using a single ES query."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return {}

        query = {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": f"now-{int(minutes)}m"}}}
                ],
                "filter": []
            }
        }
        if namespaces:
            query["bool"]["filter"].append({"terms": {"kubernetes.namespace": namespaces}})

        # Build a bounded index target instead of broad "*-*"
        search_index = self._index_target(fallback="*-*")
        if service_names:
            targets = []
            for service in service_names:
                if not service:
                    continue
                candidates = [service]
                if service.endswith('-service'):
                    candidates.append(service[:-8])
                if service.endswith('-svc'):
                    candidates.append(service[:-4])
                for c in candidates:
                    if c:
                        targets.append(f"{c}-*")
            # Keep unique and cap count for safety
            deduped = []
            for t in targets:
                if t not in deduped:
                    deduped.append(t)
            if deduped:
                search_index = ",".join(deduped[:300])

        try:
            response = self.es.search(
                index=search_index,
                body={
                    "query": query,
                    "size": max_docs,
                    "sort": [{"@timestamp": {"order": "desc"}}],
                    "_source": [
                        "@timestamp",
                        "message",
                        "log.level",
                        "log",
                        "kubernetes.namespace",
                        "kubernetes.namespace_name",
                        "kubernetes.container.name",
                        "kubernetes.labels",
                        "kubernetes.pod.name",
                        "service.name"
                    ]
                },
                ignore_unavailable=True
            )
        except Exception as e:
            print(f"Error fetching services overview from Elasticsearch: {e}")
            return {}

        overview: Dict[str, Dict] = {}
        ignore_regexes = []
        for pattern in (ignored_patterns or []):
            try:
                ignore_regexes.append(re.compile(pattern))
            except Exception:
                continue

        selected_names = [str(s).strip() for s in (service_names or []) if str(s).strip()]

        for hit in response.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            service_name = self._extract_service_name(source)

            # When service scope is provided, pin each hit to the exact selected service
            # using structured service fields first, then index prefix fallback.
            if selected_names:
                matched_service = ''
                if service_name:
                    for selected in selected_names:
                        if self._same_service(service_name, selected):
                            matched_service = selected
                            break

                if not matched_service:
                    index_service = self._service_from_index(hit.get('_index', ''))
                    if index_service:
                        for selected in selected_names:
                            if self._same_service(index_service, selected):
                                matched_service = selected
                                break

                if not matched_service:
                    continue
                service_name = matched_service

            if not service_name:
                continue

            namespace = self._extract_namespace(source) or 'unknown'
            key = f"{namespace}/{service_name}"
            timestamp = source.get('@timestamp', datetime.now().isoformat())
            message = str(source.get('message', '') or '')

            ignored = False
            for rgx in ignore_regexes:
                if rgx.search(message):
                    ignored = True
                    break
            if ignored:
                continue

            log_obj = source.get('log', {})
            if isinstance(log_obj, dict):
                severity = str(log_obj.get('level', 'INFO') or 'INFO').upper()
            else:
                severity = str(source.get('log.level', 'INFO') or 'INFO').upper()

            item = overview.get(key)
            if not item:
                item = {
                    'name': service_name,
                    'namespace': namespace,
                    'total_log_entries': 0,
                    'error_count': 0,
                    'warning_count': 0,
                    'latest_timestamp': timestamp,
                    'latest_error_message': '',
                    'latest_any_message': ''
                }
                overview[key] = item

            item['total_log_entries'] += 1
            if severity in {'ERROR', 'FATAL', 'CRITICAL'} or re.search(r'(?i)\b(exception|fatal|critical|timeout|timed\s*out|imagepullbackoff|errimagepull|crashloopbackoff|connection\s+refused|http\s*5\d\d)\b', message):
                item['error_count'] += 1
                if not item['latest_error_message']:
                    item['latest_error_message'] = message
            elif severity == 'WARNING' or re.search(r'(?i)warn', message):
                item['warning_count'] += 1

            if timestamp > item['latest_timestamp']:
                item['latest_timestamp'] = timestamp

            if not item.get('latest_any_message') and message:
                item['latest_any_message'] = message

        return overview

    def index_incident(self, incident: Dict, index_name: str = "ai-monitoring-incidents") -> bool:
        """Store compact incident document in Elasticsearch for history."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return False
        try:
            target_index = f"{index_name}-{datetime.now().strftime('%Y.%m.%d')}"
            self.es.index(index=target_index, document=incident)
            return True
        except Exception as e:
            print(f"Error indexing incident: {e}")
            return False

    def discover_services(self, limit: int = 1000) -> List[Dict]:
        """Discover service names from index names like <service>-YYYY.MM.DD."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return []
        try:
            indices = self.es.cat.indices(format='json')
            pattern = re.compile(r'^(?P<svc>.+)-\d{4}\.\d{2}\.\d{2}$')
            discovered = {}

            for row in indices:
                index_name = row.get('index', '')
                match = pattern.match(index_name)
                if not match:
                    continue
                svc = match.group('svc')
                if not svc or svc.startswith('.'):
                    continue
                if svc not in discovered:
                    discovered[svc] = {'name': svc}
                if len(discovered) >= limit:
                    break

            resolved = []
            for svc in sorted(discovered.keys()):
                namespaces = self._discover_service_namespaces(svc)
                if namespaces:
                    for ns in namespaces:
                        resolved.append({
                            'name': svc,
                            'namespace': ns,
                            'source': 'elasticsearch-index'
                        })
                else:
                    resolved.append({
                        'name': svc,
                        'namespace': 'unknown',
                        'source': 'elasticsearch-index'
                    })

            return resolved[:limit]
        except Exception as e:
            print(f"Error discovering services from Elasticsearch: {e}")
            return []

    def discover_service_names_fast(self, limit: int = 1000) -> List[str]:
        """Fast service discovery from index names only (no per-service log lookups)."""
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            return []
        try:
            indices = self.es.cat.indices(format='json')
            pattern = re.compile(r'^(?P<svc>.+)-\d{4}\.\d{2}\.\d{2}$')
            names = []
            seen = set()

            for row in indices:
                index_name = row.get('index', '')
                match = pattern.match(index_name)
                if not match:
                    continue
                svc = match.group('svc')
                if not svc or svc.startswith('.'):
                    continue
                if svc in seen:
                    continue
                seen.add(svc)
                names.append(svc)
                if len(names) >= limit:
                    break

            return names
        except Exception as e:
            print(f"Error in fast service discovery: {e}")
            return []

    def _discover_service_namespaces(self, service: str) -> List[str]:
        """Resolve namespaces for a service from recent log documents."""
        if not service:
            return []
        try:
            # Sample recent docs directly (avoids noisy 400 from terms agg on text mapping)
            response = self.es.search(
                index=f"{service}-*",
                body={
                    "size": 80,
                    "query": {
                        "bool": {
                            "must": [
                                {"range": {"@timestamp": {"gte": "now-7d"}}}
                            ]
                        }
                    },
                    "sort": [{"@timestamp": {"order": "desc"}}],
                    "_source": [
                        "kubernetes.namespace",
                        "kubernetes.namespace_name",
                        "kubernetes_namespace"
                    ]
                },
                ignore_unavailable=True
            )
            namespaces = []
            for hit in response.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                ns = self._extract_namespace(source)
                if ns and ns not in namespaces:
                    namespaces.append(ns)
            return namespaces
        except Exception:
            return []
        
    def get_logs(self, service: str, start_time: int, end_time: int,
                 limit: int = 100, namespace: str = "") -> List[Dict]:
        """Get logs for a service within a time range"""
        # Check if Elasticsearch is available
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            print("DEBUG: Elasticsearch not available, returning empty logs")
            return []
            
        try:
            # Convert timestamps to Elasticsearch format
            start_date = datetime.fromtimestamp(start_time / 1000)
            end_date = datetime.fromtimestamp(end_time / 1000)
            
            service_value = str(service or '').strip()
            namespace_value = str(namespace or '').strip()
            variants = self._service_variants(service_value)

            identity_fields = [
                "service.name",
                "kubernetes.container.name",
                "container_name",
                "kubernetes.labels.app"
            ]

            # Build query
            service_should = []
            if variants:
                for token in variants:
                    for field in identity_fields:
                        service_should.append({"term": {f"{field}.keyword": token}})
                        service_should.append({"term": {field: token}})
                    service_should.append({"query_string": {"query": f"message:*{token}* OR log:*{token}*", "lenient": True}})

            base_query = {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_date.isoformat(),
                                    "lte": end_date.isoformat()
                                }
                            }
                        }
                    ],
                    "filter": []
                }
            }

            if service_should:
                base_query["bool"]["must"].append({
                    "bool": {
                        "should": service_should,
                        "minimum_should_match": 1
                    }
                })

            if namespace_value and namespace_value.lower() not in {'unknown', 'all', '*'}:
                base_query["bool"]["filter"].append({
                    "bool": {
                        "should": [
                            {"term": {"kubernetes.namespace.keyword": namespace_value}},
                            {"term": {"kubernetes.namespace": namespace_value}},
                            {"term": {"kubernetes.namespace_name.keyword": namespace_value}},
                            {"term": {"kubernetes.namespace_name": namespace_value}}
                        ],
                        "minimum_should_match": 1
                    }
                })

            def _execute(query_body: Dict) -> List[Dict]:
                response = self.es.search(
                    index=self._index_target(fallback="*-*"),
                    body={
                        "query": query_body,
                        "size": limit,
                        "sort": [{"@timestamp": {"order": "desc"}}]
                    }
                )
                out = []
                for hit in response['hits']['hits']:
                    log_entry = hit['_source']
                    log_entry['_id'] = hit['_id']
                    out.append(log_entry)
                return out

            logs = _execute(base_query)

            # Generic fallback: find error/exception logs in the same window then
            # filter service ownership client-side (future-proof for new field shapes).
            if not logs and variants:
                fallback_query = {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": start_date.isoformat(),
                                        "lte": end_date.isoformat()
                                    }
                                }
                            },
                            {
                                "query_string": {
                                    "query": "message:*Exception* OR log:*Exception* OR message:*Error* OR log:*Error*",
                                    "lenient": True
                                }
                            }
                        ],
                        "filter": list(base_query.get("bool", {}).get("filter", []))
                    }
                }
                fallback_hits = self.es.search(
                    index=self._index_target(fallback="*-*"),
                    body={
                        "query": fallback_query,
                        "size": max(limit * 5, 200),
                        "sort": [{"@timestamp": {"order": "desc"}}]
                    }
                )
                filtered = []
                for hit in fallback_hits.get('hits', {}).get('hits', []):
                    source = hit.get('_source', {})
                    if self._belongs_to_service(source, variants):
                        source['_id'] = hit.get('_id')
                        filtered.append(source)
                    if len(filtered) >= limit:
                        break
                logs = filtered

            return logs
            
        except Exception as e:
            print(f"DEBUG: Error getting logs from Elasticsearch: {e}")
            return []
            
    def get_traces(self, service: str, start_time: int, end_time: int,
                   limit: int = 50) -> List[Dict]:
        """Get traces for a service within a time range"""
        # Check if Elasticsearch is available
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            print("DEBUG: Elasticsearch not available, returning empty traces")
            return []
            
        try:
            # For traces, we'll search in a different index pattern
            trace_index = "traces-*"
            
            # Convert timestamps
            start_date = datetime.fromtimestamp(start_time / 1000)
            end_date = datetime.fromtimestamp(end_time / 1000)
            
            # Build query
            query = {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_date.isoformat(),
                                    "lte": end_date.isoformat()
                                }
                            }
                        },
                        {
                            "term": {
                                "serviceName.keyword": service
                            }
                        }
                    ]
                }
            }
            
            # Execute search
            response = self.es.search(
                index=trace_index,
                body={
                    "query": query,
                    "size": limit,
                    "sort": [{"@timestamp": {"order": "desc"}}]
                }
            )
            
            # Process results
            traces = []
            for hit in response['hits']['hits']:
                trace_entry = hit['_source']
                trace_entry['_id'] = hit['_id']
                traces.append(trace_entry)
                
            return traces
        except Exception as e:
            print(f"Error fetching traces: {e}")
            return []
            
    def search_logs(self, query_string: str, start_time: int, end_time: int,
                    limit: int = 100) -> List[Dict]:
        """Search logs with a query string"""
        # Check if Elasticsearch is available
        if not ELASTICSEARCH_AVAILABLE or self.es is None:
            print("DEBUG: Elasticsearch not available, returning empty search results")
            return []
            
        try:
            # Convert timestamps
            start_date = datetime.fromtimestamp(start_time / 1000)
            end_date = datetime.fromtimestamp(end_time / 1000)
            
            # Build query
            query = {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_date.isoformat(),
                                    "lte": end_date.isoformat()
                                }
                            }
                        },
                        {
                            "query_string": {
                                "query": query_string
                            }
                        }
                    ]
                }
            }
            
            # Execute search
            response = self.es.search(
                index=self._index_target(fallback="*-*"),
                body={
                    "query": query,
                    "size": limit,
                    "sort": [{"@timestamp": {"order": "desc"}}]
                }
            )
            
            # Process results
            logs = []
            for hit in response['hits']['hits']:
                log_entry = hit['_source']
                log_entry['_id'] = hit['_id']
                logs.append(log_entry)
                
            return logs
        except Exception as e:
            print(f"Error searching logs: {e}")
            return []
