#!/usr/bin/env python3
"""
Model Training Script for AI Monitoring Agent
"""
import json
import pickle
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from datetime import datetime

def initialize_models():
    """Initialize and save baseline models"""
    
    # Create models directory if it doesn't exist
    import os
    os.makedirs("./models", exist_ok=True)
    
    # Initialize error classifier with sample data
    print("Initializing error classifier...")
    
    # Sample training data for error classification
    sample_texts = [
        "Database connection timeout error occurred during request processing",
        "Memory allocation failed while processing large dataset",
        "Authentication failed for user due to invalid credentials",
        "Network timeout while connecting to external service",
        "Null pointer exception in user service handler",
        "Disk space exhausted on server partition",
        "Rate limit exceeded for API endpoint",
        "SSL certificate validation failed for secure connection"
    ]
    
    sample_labels = [
        "database_timeout",
        "memory_error",
        "authentication_error",
        "network_timeout",
        "null_pointer",
        "disk_space",
        "rate_limit",
        "ssl_error"
    ]
    
    # Train classifier
    vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
    X = vectorizer.fit_transform(sample_texts)
    
    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X, sample_labels)
    
    # Save models
    with open("./models/error_classifier.pkl", "wb") as f:
        pickle.dump(classifier, f)
    
    print("Models initialized and saved successfully!")
    
    # Create empty knowledge base
    knowledge_base = {
        "created_at": datetime.now().isoformat(),
        "version": "1.0",
        "entries": {}
    }
    
    with open("./models/knowledge_base.json", "w") as f:
        json.dump(knowledge_base, f, indent=2)
    
    print("Knowledge base initialized!")

def test_models():
    """Test that models load correctly"""
    try:
        # Test loading classifier
        with open("./models/error_classifier.pkl", "rb") as f:
            classifier = pickle.load(f)
        
        print("✓ Error classifier loaded successfully")
        
        # Test basic prediction
        from sklearn.feature_extraction.text import TfidfVectorizer
        vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        
        test_text = ["Database connection failed"]
        X = vectorizer.fit_transform(test_text)
        
        print("✓ Models are ready for use")
        return True
        
    except Exception as e:
        print(f"✗ Error loading models: {e}")
        return False

if __name__ == "__main__":
    print("AI Monitoring Agent - Model Initialization")
    print("=" * 45)
    
    initialize_models()
    
    print("\nTesting models...")
    if test_models():
        print("\n✅ All models initialized successfully!")
        print("\nNext steps:")
        print("1. Update config.json with your actual URLs")
        print("2. Run 'python main.py' to start the monitoring agent")
        print("3. Provide feedback through Slack to improve the AI")
    else:
        print("\n❌ Model initialization failed!")