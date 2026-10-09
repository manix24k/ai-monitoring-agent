#!/usr/bin/env python3
"""
Lightweight test script for AI Monitoring Agent and Web Dashboard
"""
import os
import sys
import json

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

def test_config_file():
    """Test that config file is valid JSON"""
    print("\nTesting configuration file...")
    
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        # Check required sections
        required_sections = ['prometheus', 'signoz', 'anomaly_detection']
        missing_sections = []
        
        for section in required_sections:
            if section not in config:
                missing_sections.append(section)
                
        if missing_sections:
            print(f"  ✗ Missing sections: {missing_sections}")
            return False
        else:
            print("  ✓ Configuration file is valid")
            return True
            
    except json.JSONDecodeError as e:
        print(f"  ✗ Invalid JSON in config.json: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Error reading config.json: {e}")
        return False

def test_template_files():
    """Test that template files exist and are not empty"""
    print("\nTesting template files...")
    
    template_files = [
        ('templates/dashboard.html', 5000),  # Should be at least 5KB
        ('static/style.css', 100)  # Should be at least 100 bytes
    ]
    
    issues = []
    
    for file_path, min_size in template_files:
        if not os.path.exists(file_path):
            issues.append(f"Missing: {file_path}")
            print(f"  ✗ Missing: {file_path}")
        else:
            size = os.path.getsize(file_path)
            if size < min_size:
                issues.append(f"Too small: {file_path} ({size} bytes)")
                print(f"  ✗ Too small: {file_path} ({size} bytes)")
            else:
                print(f"  ✓ Valid: {file_path} ({size} bytes)")
    
    if issues:
        print(f"\nFound {len(issues)} template issues")
        return False
    else:
        print("\n✓ All template files are valid")
        return True

def main():
    """Run all tests"""
    print("AI Monitoring Agent - Lightweight System Test")
    print("=" * 45)
    
    # Test file structure
    tests = [
        test_file_structure(),
        test_config_file(),
        test_template_files()
    ]
    
    passed_tests = sum(tests)
    total_tests = len(tests)
    
    print("\n" + "=" * 45)
    print(f"Test Results: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("\n✓ All tests passed!")
        print("\nTo run the dashboard (when dependencies are installed):")
        print("  python web_dashboard.py")
        print("Then open http://localhost:5000 in your browser")
        return 0
    else:
        print(f"\n✗ {total_tests - passed_tests} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())