#!/usr/bin/env python3
"""
SigNoz Client for AI Monitoring Agent
"""
import requests
import json
from typing import Dict, List, Optional

class SigNozClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        
    def get_logs(self, service: str, start_time: int, end_time: int, 
                 limit: int = 100) -> List[Dict]:
        """Get logs for a service within a time range"""
        url = f"{self.base_url}/api/v1/logs"
        params = {
            'service': service,
            'start': start_time,
            'end': end_time,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json().get('logs', [])
        except Exception as e:
            print(f"Error fetching logs: {e}")
            return []
            
    def get_traces(self, service: str, start_time: int, end_time: int,
                   limit: int = 50) -> List[Dict]:
        """Get traces for a service within a time range"""
        url = f"{self.base_url}/api/v1/traces"
        params = {
            'service': service,
            'start': start_time,
            'end': end_time,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json().get('traces', [])
        except Exception as e:
            print(f"Error fetching traces: {e}")
            return []
            
    def search_logs(self, query: str, start_time: int, end_time: int,
                    limit: int = 100) -> List[Dict]:
        """Search logs with a query string"""
        url = f"{self.base_url}/api/v1/logs/search"
        params = {
            'q': query,
            'start': start_time,
            'end': end_time,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json().get('logs', [])
        except Exception as e:
            print(f"Error searching logs: {e}")
            return []