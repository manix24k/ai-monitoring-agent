#!/usr/bin/env python3
"""
Anomaly Detection for AI Monitoring Agent
"""
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Dict, List
import pandas as pd

class AnomalyDetector:
    def __init__(self):
        self.scaler = StandardScaler()
        self.model = IsolationForest(contamination=0.1, random_state=42)
        self.is_fitted = False
        
    def detect(self, metrics: Dict) -> List[Dict]:
        """Detect anomalies in metrics"""
        if not metrics:
            return []
            
        # Convert metrics to features for detection
        features = self._prepare_features(metrics)
        
        if not self.is_fitted:
            # First time fitting
            try:
                scaled_features = self.scaler.fit_transform(features)
                self.model.fit(scaled_features)
                self.is_fitted = True
            except Exception as e:
                print(f"Error fitting model: {e}")
                return []
        
        # Detect anomalies
        try:
            scaled_features = self.scaler.transform(features)
            predictions = self.model.predict(scaled_features)
            anomaly_scores = self.model.decision_function(scaled_features)
            
            anomalies = []
            for i, (prediction, score) in enumerate(zip(predictions, anomaly_scores)):
                if prediction == -1:  # Anomaly detected
                    anomalies.append({
                        'metric': list(metrics.keys())[i],
                        'value': list(metrics.values())[i],
                        'anomaly_score': score,
                        'timestamp': pd.Timestamp.now().isoformat()
                    })
                    
            return anomalies
        except Exception as e:
            print(f"Error detecting anomalies: {e}")
            return []
            
    def _prepare_features(self, metrics: Dict) -> np.ndarray:
        """Convert metrics dictionary to feature array"""
        # Convert values to numpy array
        values = np.array(list(metrics.values())).reshape(-1, 1)
        return values
        
    def add_baseline_data(self, historical_metrics: List[Dict]):
        """Add historical data to improve anomaly detection"""
        if not historical_metrics:
            return
            
        # Combine historical metrics
        combined_metrics = {}
        for metric_dict in historical_metrics:
            combined_metrics.update(metric_dict)
            
        features = self._prepare_features(combined_metrics)
        scaled_features = self.scaler.fit_transform(features)
        self.model.fit(scaled_features)
        self.is_fitted = True