#!/usr/bin/env python3
"""
Startup script for AI Monitoring Agent Dashboard
Runs the dashboard on port 5003 to avoid macOS conflicts
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the web dashboard
import web_dashboard

if __name__ == "__main__":
    print("AI Monitoring Agent Dashboard")
    print("=" * 40)
    print(f"Template directory: {web_dashboard.template_dir}")
    print(f"Static directory: {web_dashboard.static_dir}")
    print(f"Agent connection: {'Connected' if web_dashboard.agent_available else 'Not Connected'}")
    print("Starting web server on http://localhost:5003")
    print("Press CTRL+C to stop")
    
    # Run on port 5003 to avoid macOS conflicts
    web_dashboard.app.run(host='0.0.0.0', port=5003, debug=True)