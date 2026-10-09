#!/usr/bin/env python3
"""
Simple HTTP Server for AI Monitoring Agent Demo
"""
import http.server
import socketserver
import os
import sys

# Change to the project directory
os.chdir('/app')

# Define the port
PORT = 5001

# Create a simple request handler
class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Serve demo_dashboard.html as the main page
        if self.path == '/' or self.path == '/index.html':
            self.path = '/demo_dashboard.html'
        return http.server.SimpleHTTPRequestHandler.do_GET(self)

# Create the server
try:
    with socketserver.TCPServer(("", PORT), MyHttpRequestHandler) as httpd:
        print(f"AI Monitoring Agent Demo Server")
        print("=" * 35)
        print(f"Serving at http://localhost:{PORT}")
        print("Press CTRL+C to stop")
        print(f"\nOpen your browser and go to http://localhost:{PORT}")
        httpd.serve_forever()
except KeyboardInterrupt:
    print("\nServer stopped.")
except Exception as e:
    print(f"Error starting server: {e}")
    sys.exit(1)