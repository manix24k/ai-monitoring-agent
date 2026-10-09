#!/usr/bin/env python3
"""
Centralized Service Manager for AI Monitoring Agent
"""
import json
import os
from typing import List, Dict, Any

class ServiceManager:
    def __init__(self, config_path: str = "/app/config.json"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[Any, Any]:
        """Load configuration from file with multiple fallback paths"""
        config_paths = [
            self.config_path,
            '/app/config.json',
            'config.json',
            './ai-monitoring-agent/config.json'
        ]
        
        for path in config_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        config = json.load(f)
                    print(f"Successfully loaded config from {path}")
                    return config
                except Exception as e:
                    print(f"Failed to load config from {path}: {str(e)}")
                    continue
        
        print("Failed to load config from any path, using default config")
        return self._get_default_config()
    
    def _get_default_config(self) -> Dict[Any, Any]:
        """Return default configuration"""
        return {
            "services": [],
            "prometheus": {
                "url": "http://localhost:9090"
            },
            "elasticsearch": {
                "hosts": ["http://localhost:9200"]
            }
        }
    
    def get_services(self) -> List[Dict[str, Any]]:
        """Get all configured services"""
        services = self.config.get('services', [])
        if not isinstance(services, list):
            print(f"Warning: services is not a list, got {type(services)}")
            return []
        return services
    
    def add_service(self, service_name: str, priority: str = "medium", sampling: float = 1.0) -> bool:
        """Add a new service to monitoring"""
        services = self.get_services()
        
        # Check if service already exists
        for service in services:
            if service.get('name') == service_name:
                print(f"Service {service_name} already exists")
                return False
        
        # Add new service
        new_service = {
            "name": service_name,
            "priority": priority,
            "sampling": sampling
        }
        services.append(new_service)
        
        # Update config
        self.config['services'] = services
        return self._save_config()
    
    def remove_service(self, service_name: str) -> bool:
        """Remove a service from monitoring"""
        services = self.get_services()
        filtered_services = [s for s in services if s.get('name') != service_name]
        
        if len(filtered_services) == len(services):
            print(f"Service {service_name} not found")
            return False
        
        self.config['services'] = filtered_services
        return self._save_config()
    
    def update_service(self, service_name: str, priority: str = None, sampling: float = None) -> bool:
        """Update service configuration"""
        services = self.get_services()
        updated = False
        
        for service in services:
            if service.get('name') == service_name:
                if priority is not None:
                    service['priority'] = priority
                if sampling is not None:
                    service['sampling'] = sampling
                updated = True
                break
        
        if not updated:
            print(f"Service {service_name} not found")
            return False
        
        self.config['services'] = services
        return self._save_config()
    
    def _save_config(self) -> bool:
        """Save configuration to file"""
        try:
            # Try to save to the original config path
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            print(f"Successfully saved config to {self.config_path}")
            return True
        except Exception as e:
            print(f"Failed to save config to {self.config_path}: {str(e)}")
            # Try alternative paths
            for path in ['/app/config.json', 'config.json']:
                try:
                    with open(path, 'w') as f:
                        json.dump(self.config, f, indent=2)
                    print(f"Successfully saved config to {path}")
                    return True
                except Exception as e2:
                    print(f"Failed to save config to {path}: {str(e2)}")
                    continue
        
        print("Failed to save config to any path")
        return False
    
    def get_config(self) -> Dict[Any, Any]:
        """Get current configuration"""
        return self.config
    
    def update_config(self, new_config: Dict[Any, Any]) -> bool:
        """Update entire configuration"""
        self.config = new_config
        return self._save_config()

# Global service manager instance
service_manager = None

def get_service_manager() -> ServiceManager:
    """Get the global service manager instance"""
    global service_manager
    if service_manager is None:
        service_manager = ServiceManager()
    return service_manager