#!/bin/bash
# Start the AI Monitoring Agent Dashboard on port 5003 to avoid macOS conflicts

echo "Starting AI Monitoring Agent Dashboard..."
echo "Dashboard will be available at http://localhost:5003"

# Run the web dashboard on port 5003
cd "$(dirname "$0")"
python3 -c "
import web_dashboard
import sys

# Modify the port before running
web_dashboard.app.run(host='0.0.0.0', port=5003, debug=True)
" || python3 web_dashboard.py