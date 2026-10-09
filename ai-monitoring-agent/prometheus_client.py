#!/usr/bin/env python3
"""
Prometheus Client for AI Monitoring Agent
"""
import requests
import json
import re
from typing import Dict, List, Optional

class PrometheusClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.timeout_seconds = 10
        
    def query(self, query: str) -> Dict:
        """Execute a PromQL query"""
        url = f"{self.base_url}/api/v1/query"
        response = requests.get(url, params={'query': query}, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

    def _query_result(self, query: str) -> List[Dict]:
        """Execute query and return vector result safely."""
        try:
            response = self.query(query)
            if response.get('status') != 'success':
                return []
            return response.get('data', {}).get('result', []) or []
        except Exception:
            return []

    def _first_non_empty_result(self, queries: List[str]) -> List[Dict]:
        """Run candidate queries and return the first non-empty result."""
        for query in queries:
            result = self._query_result(query)
            if result:
                return result
        return []

    def _service_pod_regex(self, service_name: str) -> str:
        """Build regex to match pod names for a service."""
        escaped = re.escape(service_name)
        return f"^{escaped}(-.*)?$"

    def _get_service_health_from_pods(self, monitored_services: List[Dict]) -> Dict:
        """Get pod readiness/restart signals for configured services."""
        service_health = {}
        seen = set()

        for service in monitored_services:
            if not isinstance(service, dict):
                continue

            service_name = service.get('name')
            namespace = service.get('namespace', 'unknown')
            if not service_name:
                continue

            service_key = f"{namespace}/{service_name}"
            if service_key in seen:
                continue
            seen.add(service_key)

            pod_regex = self._service_pod_regex(service_name)
            selector = f'namespace="{namespace}",pod=~"{pod_regex}"'

            ready_sum_result = self._query_result(
                f'sum(kube_pod_container_status_ready{{{selector}}})'
            )
            ready_count_result = self._query_result(
                f'count(kube_pod_container_status_ready{{{selector}}})'
            )
            restart_result = self._query_result(
                f'sum(increase(kube_pod_container_status_restarts_total{{{selector}}}[10m]))'
            )
            waiting_result = self._query_result(
                f'sum(kube_pod_container_status_waiting{{{selector}}})'
            )

            ready_sum = float(ready_sum_result[0]['value'][1]) if ready_sum_result else 0.0
            pod_count = int(float(ready_count_result[0]['value'][1])) if ready_count_result else 0
            restarts_10m = float(restart_result[0]['value'][1]) if restart_result else 0.0
            waiting_count = float(waiting_result[0]['value'][1]) if waiting_result else 0.0

            readiness_ratio = None
            if pod_count > 0:
                readiness_ratio = ready_sum / pod_count

            service_health[service_key] = {
                'namespace': namespace,
                'service': service_name,
                'pod_count': pod_count,
                'ready_pods': ready_sum,
                'readiness_ratio': readiness_ratio,
                'restarts_10m': restarts_10m,
                'waiting_pods': waiting_count
            }

        return service_health
        
    def get_api_metrics(self, service_name: Optional[str] = None, monitored_services: Optional[List[Dict]] = None) -> Dict:
        """Get key API metrics for anomaly detection"""
        metrics = {}
        
        # If no specific service, get overall metrics
        if service_name is None:
            # API request rate across all services with metric fallback
            request_result = self._first_non_empty_result([
                'sum(rate(http_requests_total[5m])) by (job)',
                'sum(rate(istio_requests_total[5m])) by (destination_workload)',
                'sum(rate(istio_requests_total[5m])) by (destination_service_name)'
            ])
            if request_result:
                metrics['request_rates'] = {}
                for item in request_result:
                    item_metric = item.get('metric', {})
                    name = (
                        item_metric.get('job')
                        or item_metric.get('destination_workload')
                        or item_metric.get('destination_service_name')
                        or 'unknown'
                    )
                    metrics['request_rates'][name] = float(item['value'][1])

            # API error rate across all services with metric fallback
            error_result = self._first_non_empty_result([
                'sum(rate(http_requests_total{code=~"5.."}[5m])) by (job)',
                'sum(rate(istio_requests_total{response_code=~"5.."}[5m])) by (destination_workload)',
                'sum(rate(istio_requests_total{response_code=~"5.."}[5m])) by (destination_service_name)'
            ])
            if error_result:
                metrics['error_rates'] = {}
                for item in error_result:
                    item_metric = item.get('metric', {})
                    name = (
                        item_metric.get('job')
                        or item_metric.get('destination_workload')
                        or item_metric.get('destination_service_name')
                        or 'unknown'
                    )
                    metrics['error_rates'][name] = float(item['value'][1])
                
            # API latency (95th percentile) across all services
            try:
                result = self.query('histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, job))')
                if result['data']['result']:
                    metrics['latencies'] = {}
                    for item in result['data']['result']:
                        job = item['metric'].get('job', 'unknown')
                        metrics['latencies'][job] = float(item['value'][1])
            except Exception as e:
                print(f"Error getting latencies: {e}")

            if monitored_services:
                metrics['service_health'] = self._get_service_health_from_pods(monitored_services)
        else:
            # API request rate for specific service with fallback
            request_result = self._first_non_empty_result([
                f'sum(rate(http_requests_total{{job="{service_name}"}}[5m]))',
                f'sum(rate(istio_requests_total{{destination_workload="{service_name}"}}[5m]))',
                f'sum(rate(istio_requests_total{{destination_service_name="{service_name}"}}[5m]))'
            ])
            if request_result:
                metrics['request_rate'] = float(request_result[0]['value'][1])
                
            # API error rate for specific service with fallback
            error_result = self._first_non_empty_result([
                f'sum(rate(http_requests_total{{job="{service_name}",code=~"5.."}}[5m]))',
                f'sum(rate(istio_requests_total{{destination_workload="{service_name}",response_code=~"5.."}}[5m]))',
                f'sum(rate(istio_requests_total{{destination_service_name="{service_name}",response_code=~"5.."}}[5m]))'
            ])
            if error_result:
                metrics['error_rate'] = float(error_result[0]['value'][1])
                
            # API latency (95th percentile) for specific service
            try:
                result = self.query(f'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{job="{service_name}"}}[5m])) by (le))')
                if result['data']['result']:
                    metrics['latency_95th'] = float(result['data']['result'][0]['value'][1])
            except Exception as e:
                print(f"Error getting latency for {service_name}: {e}")
            
        return metrics
        
    def get_endpoint_metrics(self, endpoint: str) -> Dict:
        """Get metrics for a specific endpoint"""
        metrics = {}
        
        # Request count for endpoint
        try:
            result = self.query(f'sum(http_requests_total{{endpoint="{endpoint}"}})')
            if result['data']['result']:
                metrics['request_count'] = float(result['data']['result'][0]['value'][1])
        except Exception as e:
            print(f"Error getting endpoint request count: {e}")
            
        return metrics
