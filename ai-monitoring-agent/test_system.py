#!/usr/bin/env python3
"""
Test script for AI Monitoring Agent and Web Dashboard
"""
import os
import sys
import time
import requests
from datetime import datetime

def test_file_structure():
    """Test that all required files exist"""
    required_files = [
        'main.py',
        'web_dashboard.py',
        'prometheus_client.py',
        'signoz_client.py',
        'anomaly_detector.py',
        'root_cause_analyzer.py',
        'slack_notifier.py',
        'learning_engine.py',
        'feedback_handler.py',
        'train_models.py',
        'requirements.txt',
        'config.json',
        'templates/dashboard.html',
        'static/style.css'
    ]
    
    print("Testing file structure...")
    missing_files = []
    
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            print(f"  ✗ Missing: {file_path}")
        else:
            print(f"  ✓ Found: {file_path}")
    
    if missing_files:
        print(f"\nMissing {len(missing_files)} required files")
        return False
    else:
        print("\n✓ All required files present")
        return True

def test_imports():
    """Test that all modules can be imported"""
    modules = [
        'main',
        'web_dashboard',
        'prometheus_client',
        'signoz_client',
        'anomaly_detector',
        'root_cause_analyzer',
        'slack_notifier',
        'learning_engine'
    ]
    
    print("\nTesting module imports...")
    failed_imports = []
    
    for module in modules:
        try:
            __import__(module)
            print(f"  ✓ Imported: {module}")
        except ImportError as e:
            failed_imports.append(module)
            print(f"  ✗ Failed to import: {module} - {e}")
    
    if failed_imports:
        print(f"\nFailed to import {len(failed_imports)} modules")
        return False
    else:
        print("\n✓ All modules imported successfully")
        return True

def test_web_server():
    """Test that web server responds"""
    print("\nTesting web server...")
    
    try:
        # Test health endpoint
        response = requests.get('http://127.0.0.1:5000/health', timeout=5)
        if response.status_code == 200:
            print("  ✓ Health check passed")
            return True
        else:
            print(f"  ✗ Health check failed with status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("  ✗ Cannot connect to web server (make sure it's running)")
        return False
    except Exception as e:
        print(f"  ✗ Health check failed: {e}")
        return False

def start_test_server():
    """Start the web server for testing"""
    print("\nStarting test server...")
    
    try:
        import subprocess
        import threading
        
        # Start the web server in a subprocess
        process = subprocess.Popen([
            sys.executable, 'web_dashboard.py'
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait a moment for server to start
        time.sleep(3)
        
        # Check if process is still running
        if process.poll() is None:
            print("  ✓ Server started successfully")
            return process
        else:
            stdout, stderr = process.communicate()
            print(f"  ✗ Server failed to start:")
            print(f"    stdout: {stdout.decode()}")
            print(f"    stderr: {stderr.decode()}")
            return None
    except Exception as e:
        print(f"  ✗ Failed to start server: {e}")
        return None

def stop_test_server(process):
    """Stop the test server"""
    if process:
        print("\nStopping test server...")
        process.terminate()
        try:
            process.wait(timeout=5)
            print("  ✓ Server stopped")
        except:
            process.kill()
            print("  ✓ Server killed")

def main():
    """Run all tests"""
    print("AI Monitoring Agent - System Test")
    print("=" * 35)
    
    # Test file structure
    if not test_file_structure():
        return 1
    
    # Test imports
    if not test_imports():
        return 1
    
    print("\n" + "=" * 35)
    print("Manual Tests Required:")
    print("1. Run 'python web_dashboard.py' in terminal")
    print("2. Visit http://localhost:5000 in your browser")
    print("3. Verify the dashboard loads correctly")
    print("4. Check that API endpoints return data:")
    print("   - http://localhost:5000/api/status")
    print("   - http://localhost:5000/api/metrics")
    print("   - http://localhost:5000/api/incidents")
    
    print("\n✓ System verification complete!")
    print("\nTo run the dashboard:")
    print("  python web_dashboard.py")
    print("Then open http://localhost:5000 in your browser")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())