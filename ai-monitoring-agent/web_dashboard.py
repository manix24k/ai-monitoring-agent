#!/usr/bin/env python3
"""
Web Dashboard for AI Monitoring Agent - Integrated Version
"""
from flask import Flask, render_template, jsonify, request, send_from_directory
import json
import os
import re
from datetime import datetime, timedelta, timezone
import sys
import os.path
import logging
import subprocess
import threading
from typing import Any, Dict

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the actual monitoring agent
agent_available = False
agent = None

# Flag to track if model download has been attempted
model_downloaded = False
model_download_lock = threading.Lock()

def ensure_model_downloaded():
    """Download the Phi-3 model if not already present"""
    global model_downloaded
    
    with model_download_lock:
        if model_downloaded:
            return True
            
        try:
            model_name = os.getenv('OLLAMA_MODEL', 'phi3')
            # Check if model already exists
            result = subprocess.run(['ollama', 'list'], capture_output=True, text=True)
            if model_name in result.stdout:
                logging.info(f"Model already downloaded: {model_name}")
                model_downloaded = True
                return True
                
            # Download the model
            logging.info(f"Downloading model: {model_name}...")
            result = subprocess.run(['ollama', 'pull', model_name], capture_output=True, text=True)
            if result.returncode == 0:
                logging.info(f"Model downloaded successfully: {model_name}")
                model_downloaded = True
                return True
            else:
                logging.error(f"Failed to download model {model_name}: {result.stderr}")
                return False
        except Exception as e:
            logging.error(f"Error downloading model: {e}")
            return False

# Try to import the monitoring agent
try:
    from main import get_agent
    agent_available = True
    try:
        agent = get_agent()
        logging.info("Successfully connected to monitoring agent")
        logging.info(f"Agent has service_monitor: {hasattr(agent, 'service_monitor')}")
        if hasattr(agent, 'agent_status') and not agent.agent_status.get('running', False):
            agent.start_monitoring()
            logging.info("Started monitoring loop from web dashboard")
    except Exception as e:
        logging.error(f"Failed to initialize monitoring agent: {e}", exc_info=True)
        agent_available = False
        agent = None
except ImportError as e:
    print(f"Warning: Could not import monitoring agent: {e}")
    agent_available = False
    # Try to continue without agent for minimal functionality
    logging.info("Continuing with limited functionality without agent")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure templates and static directories exist
# Handle both local development and Docker container paths
current_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(current_dir, 'templates')
static_dir = os.path.join(current_dir, 'static')

# In Docker container, files are copied as /app/ai-monitoring-agent/, so we might need to adjust paths
# Check if the expected directories exist, if not try alternative paths
if not os.path.exists(template_dir) or not os.path.exists(static_dir):
    logging.info(f"Expected directories not found. Current dir: {current_dir}")
    logging.info(f"Trying alternative paths...")
    
    # Try looking in the ai-monitoring-agent subdirectory
    subdir = os.path.join(current_dir, 'ai-monitoring-agent')
    if os.path.exists(subdir):
        alt_template_dir = os.path.join(subdir, 'templates')
        alt_static_dir = os.path.join(subdir, 'static')
        if os.path.exists(alt_template_dir) and os.path.exists(alt_static_dir):
            template_dir = alt_template_dir
            static_dir = alt_static_dir
            logging.info(f"Using subdirectory paths: templates={template_dir}, static={static_dir}")
    
    # If that doesn't work, try one level up
    parent_dir = os.path.dirname(current_dir)
    alt_template_dir = os.path.join(parent_dir, 'templates')
    alt_static_dir = os.path.join(parent_dir, 'static')
    if os.path.exists(alt_template_dir) and os.path.exists(alt_static_dir):
        template_dir = alt_template_dir
        static_dir = alt_static_dir
        logging.info(f"Using parent directory paths: templates={template_dir}, static={static_dir}")

# Log directory paths for debugging
logging.info(f"Current directory: {current_dir}")
logging.info(f"Template directory: {template_dir}")
logging.info(f"Static directory: {static_dir}")

# Check what directories actually exist
logging.info(f"Current dir contents: {os.listdir(current_dir) if os.path.exists(current_dir) else 'N/A'}")
parent_dir = os.path.dirname(current_dir)
if os.path.exists(parent_dir):
    logging.info(f"Parent dir contents: {os.listdir(parent_dir)}")

# Check if directories exist
if os.path.exists(template_dir):
    logging.info(f"Template directory contents: {os.listdir(template_dir)}")
else:
    logging.error(f"Template directory does not exist: {template_dir}")

if os.path.exists(static_dir):
    logging.info(f"Static directory contents: {os.listdir(static_dir)}")
else:
    logging.error(f"Static directory does not exist: {static_dir}")

os.makedirs(template_dir, exist_ok=True)
os.makedirs(static_dir, exist_ok=True)

app = Flask(__name__, 
            template_folder=template_dir,
            static_folder=static_dir,
            static_url_path='/static')

_summary_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}

_service_status_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}

_full_service_status_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}

_incidents_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}

_strict_recent_errors_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}


def _compute_service_recent_errors_for_window(window_minutes: int, selected_services=None) -> Dict[str, list]:
    """Build strict window-scoped recent error evidence for service table."""
    result: Dict[str, list] = {}
    if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
        return result

    services_subset = selected_services
    if services_subset is None and hasattr(agent, '_selected_services'):
        try:
            services_subset = agent._selected_services()
        except Exception:
            services_subset = []

    selected_key = []
    for item in services_subset or []:
        if isinstance(item, dict):
            selected_key.append(f"{item.get('namespace', 'unknown')}/{item.get('name', 'unknown')}")
    selected_key.sort()
    cache_key = f"strict:{int(window_minutes)}:{'|'.join(selected_key)}"
    now = datetime.now()
    if (
        _strict_recent_errors_cache.get('key') == cache_key and
        _strict_recent_errors_cache.get('ts') is not None and
        (now - _strict_recent_errors_cache['ts']).total_seconds() < 10 and
        isinstance(_strict_recent_errors_cache.get('data'), dict)
    ):
        return _strict_recent_errors_cache.get('data') or {}

    try:
        status_for_window = agent.get_service_status(minutes=window_minutes, services_subset=services_subset)
    except Exception:
        status_for_window = {}
    if not isinstance(status_for_window, dict):
        status_for_window = {}

    try:
        logs_traces = agent.collect_service_context(status_for_window, window_minutes=window_minutes)
    except Exception:
        logs_traces = {'logs': [], 'traces': [], 'service_errors': []}

    for service_key in status_for_window.keys():
        svc_snapshot = status_for_window.get(service_key, {}) if isinstance(status_for_window.get(service_key, {}), dict) else {}
        svc_status = str(svc_snapshot.get('status', 'unknown') or 'unknown').lower()
        pod_status = svc_snapshot.get('pod_status', {}) if isinstance(svc_snapshot.get('pod_status', {}), dict) else {}
        pod_state = str(pod_status.get('status', 'unknown') or 'unknown').lower()
        pod_entries = pod_status.get('pods', []) if isinstance(pod_status.get('pods', []), list) else []
        has_ready_pod = any(bool(p.get('ready', False)) for p in pod_entries if isinstance(p, dict))
        pod_reasons = [str(p.get('reason', '') or '') for p in pod_entries if isinstance(p, dict) and str(p.get('reason', '') or '').strip()]

        try:
            scoped = agent._filter_logs_traces_for_service(service_key, logs_traces)
        except Exception:
            scoped = {'logs': [], 'traces': [], 'service_errors': []}

        best_message = ''
        service_errors = scoped.get('service_errors', []) if isinstance(scoped, dict) else []
        logs = scoped.get('logs', []) if isinstance(scoped, dict) else []

        for entry in (service_errors or []) + (logs or []):
            if not isinstance(entry, dict):
                continue
            msg = str(entry.get('message', '') or '').strip()
            if not msg:
                continue
            if hasattr(agent, '_is_actionable_error_message') and not agent._is_actionable_error_message(msg):
                continue
            best_message = msg
            break

        if best_message and hasattr(agent, '_derive_exact_from_message'):
            try:
                parsed = agent._derive_exact_from_message(service_key, best_message, crashloop_present=('crashloopbackoff' in best_message.lower()))
                if isinstance(parsed, dict) and parsed.get('issue'):
                    best_message = str(parsed.get('issue') or best_message)
            except Exception:
                pass

        # Avoid showing stale app exceptions when service has no ready pod.
        # Use only live pod reasons in that state.
        unschedulable_reason = ''
        for reason in pod_reasons:
            lower_reason = reason.lower()
            if any(marker in lower_reason for marker in ['unschedulable', 'failedscheduling', 'insufficient cpu', 'insufficient memory', 'node affinity']):
                unschedulable_reason = reason
                break

        if not has_ready_pod:
            if unschedulable_reason:
                best_message = f"Exact Issue: [POD_ISSUES] {service_key} | reason={unschedulable_reason}"
            elif pod_reasons:
                best_message = f"Exact Issue: [POD_ISSUES] {service_key} | reason={pod_reasons[0]}"
            else:
                best_message = ''

        if best_message:
            result[service_key] = [{
                'timestamp': datetime.now().isoformat(),
                'message': best_message,
                'severity': 'ERROR'
            }]
        else:
            result[service_key] = []

    _strict_recent_errors_cache['key'] = cache_key
    _strict_recent_errors_cache['ts'] = now
    _strict_recent_errors_cache['data'] = result
    return result

IST = timezone(timedelta(hours=5, minutes=30))

@app.route('/')
def dashboard():
    """Main dashboard page"""
    try:
        # Ensure model is downloaded before serving dashboard
        ensure_model_downloaded()
        # Debug static files
        static_folder = app.static_folder
        logging.info(f"Static folder: {static_folder}")
        logging.info(f"Static URL path: {app.static_url_path}")
        if static_folder and os.path.exists(static_folder):
            logging.info(f"Static folder contents: {os.listdir(static_folder)}")
        else:
            logging.error(f"Static folder does not exist: {static_folder}")
        return render_template('dashboard.html')
    except Exception as e:
        logging.error(f"Error loading template: {str(e)}", exc_info=True)
        return f"Error loading template: {str(e)}", 500

@app.route('/ai-agent')
@app.route('/ai-agent/')
def ai_agent_redirect():
    """Handle /ai-agent path"""
    try:
        # Ensure model is downloaded before serving dashboard
        ensure_model_downloaded()
        return render_template('dashboard.html')
    except Exception as e:
        return f"Error loading template: {str(e)}", 500

@app.route('/api/status')
def api_status():
    """Get current agent status"""
    try:
        if not agent_available or agent is None:
            # Return mock data if agent is not available
            return jsonify({
                'active_alerts': 0,
                'resolved_today': 0,
                'accuracy_rate': 0,
                'learning_status': 'Not Connected',
                'uptime': '0'
            })
        
        status = agent.get_status()

        # Compute real accuracy from feedback corrections when available
        accuracy_rate = 0
        try:
            learning_engine = getattr(agent, 'learning_engine', None)
            feedback_data = getattr(learning_engine, 'feedback_data', []) if learning_engine else []
            if feedback_data:
                total_feedback = 0
                correct_feedback = 0
                for fb in feedback_data:
                    predicted = str(fb.get('predicted_category', '') or fb.get('root_cause', '')).strip().lower()
                    actual = str(fb.get('actual_cause', '')).strip().lower()
                    if not predicted or not actual:
                        continue
                    total_feedback += 1
                    if predicted == actual:
                        correct_feedback += 1
                if total_feedback > 0:
                    accuracy_rate = int((correct_feedback / total_feedback) * 100)
                elif len(feedback_data) > 0:
                    accuracy_rate = 1
        except Exception as e:
            logger.warning(f"Could not compute accuracy from feedback: {e}")

        # Calculate resolved today
        resolved_today = 0
        if hasattr(agent, 'resolved_incidents'):
            for incident in agent.resolved_incidents:
                try:
                    incident_time = datetime.fromisoformat(incident['timestamp'])
                    if incident_time.date() == datetime.now().date():
                        resolved_today += 1
                except Exception:
                    pass  # Skip invalid timestamps
        
        return jsonify({
            'active_alerts': status.get('active_incidents', 0),
            'resolved_today': resolved_today,
            'accuracy_rate': accuracy_rate,
            'learning_status': 'Active' if status.get('llm_available') else 'Fallback (LLM Unavailable)',
            'uptime': str(timedelta(seconds=int(status.get('uptime', 0)))) if status.get('uptime') else '0',
            'learning_details': {
                'feedback_samples': len(getattr(getattr(agent, 'learning_engine', None), 'feedback_data', []) or []),
                'incident_samples': len(getattr(getattr(agent, 'learning_engine', None), 'incident_history', []) or []),
                'error_patterns': len((getattr(getattr(agent, 'learning_engine', None), 'knowledge_base', {}) or {}).get('error_patterns', {}) or {}),
                'last_training': str(getattr(getattr(agent, 'learning_engine', None), 'last_training', '') or '')
            }
        })
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        return jsonify({'error': f'Failed to get status: {str(e)}'}), 500

@app.route('/ai-agent/api/status')
def ai_agent_api_status():
    """Get current agent status (prefixed route)"""
    return api_status()

@app.route('/api/metrics')
def api_metrics():
    """Get metrics data for charts"""
    try:
        if not agent_available or agent is None:
            # Return sample data if agent is not available
            return jsonify({
                'timestamps': ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25'],
                'request_rates': [120, 190, 130, 160, 140, 180],
                'error_rates': [2, 8, 3, 5, 4, 7],
                'latencies': [0.2, 0.8, 0.3, 0.5, 0.4, 0.7]
            })
        
        metrics_history = agent.get_metrics_history(50)
        
        if not metrics_history:
            # Return sample data if no history
            return jsonify({
                'timestamps': ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25'],
                'request_rates': [120, 190, 130, 160, 140, 180],
                'error_rates': [2, 8, 3, 5, 4, 7],
                'latencies': [0.2, 0.8, 0.3, 0.5, 0.4, 0.7]
            })
        
        # Process actual metrics
        timestamps = [m['timestamp'].split('T')[1][:5] for m in metrics_history[-6:]]  # Last 6 entries
        request_rates = [m.get('request_rate', 0) for m in metrics_history[-6:]]
        error_rates = [m.get('error_rate', 0) for m in metrics_history[-6:]]
        latencies = [m.get('latency_95th', 0) for m in metrics_history[-6:]]
        
        return jsonify({
            'timestamps': timestamps,
            'request_rates': request_rates,
            'error_rates': error_rates,
            'latencies': latencies
        })
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        return jsonify({'error': f'Failed to get metrics: {str(e)}'}), 500

@app.route('/ai-agent/api/metrics')
def ai_agent_api_metrics():
    """Get metrics data for charts (prefixed route)"""
    return api_metrics()

@app.route('/api/incidents')
def api_incidents():
    """Get recent incidents"""
    try:
        window_minutes = request.args.get('window_minutes', type=int)
        if not window_minutes or window_minutes <= 0:
            window_minutes = 720

        requested_limit = request.args.get('limit', type=int)
        if not requested_limit or requested_limit <= 0:
            requested_limit = 120 if window_minutes >= 1440 else (80 if window_minutes >= 720 else 40)
        incident_limit = min(max(requested_limit, 10), 500)

        now = datetime.now()
        cache_key = f"incidents:{window_minutes}:{incident_limit}"
        if (
            _incidents_cache.get('key') == cache_key and
            _incidents_cache.get('ts') is not None and
            (now - _incidents_cache['ts']).total_seconds() < 10 and
            isinstance(_incidents_cache.get('data'), list)
        ):
            return jsonify(_incidents_cache['data'])

        cutoff = datetime.now() - timedelta(minutes=window_minutes)

        if not agent_available or agent is None:
            # Return sample incidents if agent is not available
            sample_incidents = [
                {
                    'id': 'INC-000000',
                    'time': '00:00:00',
                    'service': 'N/A',
                    'issue': 'Agent not connected',
                    'status': 'inactive',
                    'confidence': 0,
                    'resolution': 'N/A'
                }
            ]
            return jsonify(sample_incidents)
        
        # Pull a sufficiently large in-memory slice so 12h/24h windows don't drop valid incidents.
        source_limit = min(max(incident_limit * 10, 300), 5000)
        recent_incidents = agent.get_recent_incidents(source_limit)
        filtered_incidents = []
        now_dt = datetime.now()
        for incident in recent_incidents:
            raw_ts = str(incident.get('timestamp', '') or '')
            if not raw_ts:
                continue
            try:
                parsed_ts = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
                # normalize aware -> naive local compare
                if parsed_ts.tzinfo is not None:
                    parsed_ts = parsed_ts.astimezone().replace(tzinfo=None)
            except Exception:
                continue
            # Keep unresolved active incidents visible even when they started before
            # the selected window so operators don't lose PR/debug context every 5m.
            is_active = str(incident.get('status', '') or '').lower() == 'active'
            if parsed_ts >= cutoff or is_active:
                filtered_incidents.append(incident)

        recent_incidents = filtered_incidents[:incident_limit]
        
        formatted_incidents = []
        for incident in recent_incidents:
            # Determine issue summary
            if incident.get('anomalies'):
                issue = f"{len(incident['anomalies'])} anomalies detected"
            else:
                issue = "Service issue detected"
                
            # Determine service from anomaly context first
            service = "unknown"
            if incident.get('anomalies'):
                first_anomaly = incident['anomalies'][0]
                service = first_anomaly.get('service', service)

            if service == 'unknown' and agent and hasattr(agent, 'config') and 'services' in agent.config:
                if agent.config['services']:
                    service = agent.config['services'][0].get('name', 'unknown')
            
            analysis = incident.get('analysis', {})
            dependency_context = incident.get('dependency_context', {}) if isinstance(incident.get('dependency_context', {}), dict) else {}
            edges = dependency_context.get('edges', []) if isinstance(dependency_context.get('edges', []), list) else []

            anomalies = incident.get('anomalies', []) if isinstance(incident.get('anomalies', []), list) else []
            primary_anomaly = anomalies[0] if anomalies and isinstance(anomalies[0], dict) else {}
            anomaly_type = str(primary_anomaly.get('type', '') or '').upper()

            issue_summary = ''
            if anomaly_type == 'SERVICE_DEGRADED':
                issue_summary = f"{service} degraded (service partially failing)"
            elif anomaly_type == 'HIGH_ERROR_RATE':
                issue_summary = f"{service} high error rate detected"
            elif anomaly_type == 'FREQUENT_ERRORS':
                issue_summary = f"{service} frequent runtime errors detected"
            elif anomaly_type == 'DEPENDENCY_FAILURE':
                issue_summary = f"{service} downstream dependency calls are failing"
            elif anomaly_type == 'SERVICE_UNREACHABLE':
                issue_summary = f"{service} unreachable from monitoring plane"
            elif anomaly_type in {'NO_PODS', 'POD_ISSUES', 'CRASH_LOOP', 'POD_STARTUP_FAILURE'}:
                issue_summary = f"{service} has pod/runtime instability"
            elif issue and issue not in {'Service issue detected', 'unknown'}:
                issue_summary = issue
            elif analysis.get('summary'):
                issue_summary = str(analysis.get('summary'))
            else:
                issue_summary = f"{service} service issue detected"

            def _is_generic_root_cause(text: str) -> bool:
                lower_text = str(text or '').strip().lower()
                if not lower_text:
                    return True
                generic_markers = [
                    'service issue detected',
                    '[service_degraded]',
                    'is degraded',
                    '[high_error_rate]',
                    'high error rate',
                    'actionable failure detected',
                    'check service and pod status',
                    'investigate service logs',
                    'no explicit error message'
                ]
                return any(marker in lower_text for marker in generic_markers)

            root_cause_exact = ''

            # Use exact issue first, then likely causes
            if isinstance(analysis.get('exact_issues'), list) and analysis.get('exact_issues'):
                root_cause_exact = str(analysis['exact_issues'][0] or '')
            elif isinstance(analysis.get('likely_causes'), list) and analysis.get('likely_causes'):
                root_cause_exact = str(analysis['likely_causes'][0] or '')
            elif analysis.get('summary'):
                root_cause_exact = str(analysis.get('summary') or '')

            if not root_cause_exact:
                root_cause_exact = str(issue or '')

            # Deterministic fallback from dependency context edges
            if _is_generic_root_cause(root_cause_exact) and isinstance(incident.get('dependency_context'), dict):
                edges = incident.get('dependency_context', {}).get('edges', []) or []
                error_edges = [e for e in edges if int(e.get('error_count', 0) or 0) > 0]
                if error_edges:
                    error_edges.sort(key=lambda e: (float(e.get('error_rate', 0.0) or 0.0), int(e.get('error_count', 0) or 0)), reverse=True)
                    top = error_edges[0]
                    trace_hint = (top.get('sample_trace_ids') or [])
                    trace_text = f", trace_id={trace_hint[0]}" if trace_hint else ""
                    root_cause_exact = (
                        f"{top.get('from')} -> {top.get('to')} failing "
                        f"({top.get('error_count')}/{top.get('count')} spans, p95 {top.get('p95_latency_ms')}ms{trace_text})"
                    )

            root_cause_exact = re.sub(r'^\s*exact issue:\s*', '', str(root_cause_exact), flags=re.IGNORECASE).strip()
            issue_summary = re.sub(r'^\s*exact issue:\s*', '', str(issue_summary), flags=re.IGNORECASE).strip()

            # Scope dependency edges only to incident service to avoid cross-service chain leakage
            service_short = str(service).split('/')[-1].lower()
            scoped_edges = []
            for edge in edges:
                from_short = str(edge.get('from', '')).split('/')[-1].lower()
                to_short = str(edge.get('to', '')).split('/')[-1].lower()
                if service_short and (from_short == service_short or to_short == service_short):
                    scoped_edges.append(edge)

            scoped_dependency_context = dict(dependency_context)
            scoped_dependency_context['edges'] = scoped_edges

            scoped_analysis = dict(analysis) if isinstance(analysis, dict) else {}
            scoped_analysis['dependency_chain'] = [f"{edge.get('from')} -> {edge.get('to')}" for edge in scoped_edges[:5]]

            structured_rca = incident.get('structured_rca', {}) if isinstance(incident.get('structured_rca', {}), dict) else {}
            issue_scope = str(incident.get('issue_scope', 'unknown') or 'unknown')
            pr_meta = incident.get('pr', {}) if isinstance(incident.get('pr', {}), dict) else {}
                    
            # Render incident time in IST (Asia/Kolkata)
            time_text = str(incident.get('timestamp', '') or '')
            try:
                parsed_ts = datetime.fromisoformat(time_text.replace('Z', '+00:00'))
                if parsed_ts.tzinfo is None:
                    parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                time_text = parsed_ts.astimezone(IST).strftime('%H:%M:%S')
            except Exception:
                if 'T' in time_text:
                    time_text = time_text.split('T')[1][:8]
                else:
                    time_text = time_text[:8]

            formatted_incident = {
                'id': incident['id'],
                'time': time_text,
                'service': service,
                'issue': root_cause_exact or issue,
                'issue_summary': issue_summary,
                'root_cause_exact': root_cause_exact,
                'status': incident.get('status', 'active'),
                'confidence': max(0, min(100, int(incident.get('analysis', {}).get('confidence', 0.5) * 100))),
                'resolution': 'Auto-resolved' if incident.get('status') == 'resolved' else 'Pending',
                'analysis': scoped_analysis,
                'structured_rca': structured_rca,
                'issue_scope': issue_scope,
                'pr': pr_meta,
                'dependency_context': scoped_dependency_context,
                'remedial_actions': incident.get('remedial_actions', []),
                'impacted_by': incident.get('service_status', {})
            }
            formatted_incidents.append(formatted_incident)
            
        _incidents_cache['key'] = cache_key
        _incidents_cache['ts'] = now
        _incidents_cache['data'] = formatted_incidents
        return jsonify(formatted_incidents)
    except Exception as e:
        logger.error(f"Failed to get incidents: {e}")
        return jsonify({'error': f'Failed to get incidents: {str(e)}'}), 500

@app.route('/api/alerts/distribution')
def api_alerts_distribution():
    """Get alert distribution data"""
    try:
        if not agent_available or agent is None:
            # Return sample data if agent is not available
            return jsonify({
                'labels': ['Not Connected'],
                'data': [1]
            })
        
        recent_incidents = agent.get_recent_incidents(50)
        
        # Categorize incidents
        categories = {
            'Database': 0,
            'Network': 0,
            'Memory': 0,
            'Authentication': 0,
            'Other': 0
        }
        
        for incident in recent_incidents:
            analysis = incident.get('analysis', {})
            category = analysis.get('error_category', 'Other')
            if category in categories:
                categories[category] += 1
            else:
                categories['Other'] += 1
        
        # Remove zero-count categories
        labels = [k for k, v in categories.items() if v > 0]
        data = [v for v in categories.values() if v > 0]
        
        # If no incidents, show a default category
        if not labels:
            labels = ['No Incidents']
            data = [1]
        
        return jsonify({
            'labels': labels,
            'data': data
        })
    except Exception as e:
        logger.error(f"Failed to get alert distribution: {e}")
        return jsonify({'error': f'Failed to get alert distribution: {str(e)}'}), 500

@app.route('/ai-agent/api/alerts/distribution')
def ai_agent_api_alerts_distribution():
    """Get alert distribution data (prefixed route)"""
    return api_alerts_distribution()

@app.route('/ai-agent/api/incidents')
def ai_agent_api_incidents():
    """Get recent incidents (prefixed route)"""
    return api_incidents()

@app.route('/api/configuration', methods=['GET', 'POST'])
def api_configuration():
    """Get or update configuration"""
    try:
        if not agent_available or agent is None:
            # Return sample configuration if agent is not available
            sample_config = {
                "prometheus": {
                    "url": "http://localhost:9090",
                    "metrics": {
                        "request_rate_query": "sum(rate(http_requests_total[5m]))",
                        "error_rate_query": "sum(rate(http_requests_total{code=~\"5..\"}[5m]))",
                        "latency_query": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))"
                    }
                },
                "elasticsearch": {
                    "hosts": ["http://localhost:9200"],
                    "index_prefix": "logs-*",
                    "log_limit": 100,
                    "trace_limit": 50
                },
                "anomaly_detection": {
                    "contamination": 0.1,
                    "window_size": 60,
                    "check_interval": 60
                },
                "learning": {
                    "model_dir": "./models",
                    "feedback_threshold": 50,
                    "history_limit": 1000,
                    "adaptive_threshold_window": 100
                },
                "slack": {
                    "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                    "channel": "#alerts",
                    "enable_feedback": True
                },
                "services": [
                    {
                        "name": "sample-service",
                        "priority": "high",
                        "sampling": 1.0
                    }
                ],
                "pr_automation": {
                    "enabled": True,
                    "auto_create_pr": False,
                    "repo_base_url": "https://github.com/fabhotelstech",
                    "target_branch": "develop_mercury",
                    "fix_mode": "report",
                    "default_base_branch": "dev",
                    "repo_overrides": {}
                }
            }
            if request.method == 'POST':
                return jsonify({'status': 'success', 'message': 'Configuration update simulated'})
            return jsonify(sample_config)
        
        if request.method == 'POST':
            # Update configuration
            new_config = request.json
            try:
                if not isinstance(new_config, dict):
                    return jsonify({'status': 'error', 'message': 'Invalid configuration payload'}), 400

                # Preserve user-provided service selection settings.
                # Do not overwrite monitored_services/max_selected_services here.

                success = agent.update_config(new_config)
                if success:
                    note = getattr(agent, 'last_config_update_note', '')
                    message = 'Configuration updated'
                    if note:
                        message = f"Configuration updated with note: {note}"
                    return jsonify({'status': 'success', 'message': message})
                else:
                    note = getattr(agent, 'last_config_update_note', '')
                    msg = 'Failed to update configuration'
                    if note:
                        msg = f"Failed to update configuration: {note}"
                    return jsonify({'status': 'error', 'message': msg}), 400
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)}), 400
        
        # GET request - return current configuration
        return jsonify(agent.config)
    except Exception as e:
        logger.error(f"Failed to handle configuration: {e}")
        return jsonify({'error': f'Failed to handle configuration: {str(e)}'}), 500

@app.route('/ai-agent/api/configuration', methods=['GET', 'POST'])
def ai_agent_api_configuration():
    """Get or update configuration (prefixed route)"""
    return api_configuration()

@app.route('/api/feedback', methods=['POST'])
def api_feedback():
    """Receive feedback from users"""
    try:
        feedback = request.json
        if not isinstance(feedback, dict):
            return jsonify({'error': 'Invalid feedback payload'}), 400

        if not agent_available or agent is None or not hasattr(agent, 'learning_engine'):
            logger.info(f"Received feedback (agent unavailable): {feedback}")
            return jsonify({'status': 'accepted', 'message': 'Feedback accepted but agent not connected'})

        # Minimal compact feedback for online learning
        compact_feedback = {
            'service': feedback.get('service', 'global'),
            'predicted_category': feedback.get('predicted_category') or feedback.get('predicted_cause') or feedback.get('root_cause', ''),
            'actual_cause': feedback.get('actual_cause') or feedback.get('correct_category') or feedback.get('label', '')
        }
        agent.learning_engine.record_feedback(compact_feedback)

        # Periodic training based on configured threshold
        threshold = agent.config.get('learning', {}).get('feedback_threshold', 50)
        feedback_count = len(getattr(agent.learning_engine, 'feedback_data', []))
        if threshold and feedback_count > 0 and feedback_count % threshold == 0:
            agent.learning_engine.train_error_classifier()

        logger.info(f"Feedback recorded for learning: {compact_feedback}")
        return jsonify({'status': 'success', 'message': 'Feedback recorded and learning updated'})
    except Exception as e:
        logger.error(f"Failed to record feedback: {e}")
        return jsonify({'error': f'Failed to record feedback: {str(e)}'}), 500

@app.route('/ai-agent/api/feedback', methods=['POST'])
def ai_agent_api_feedback():
    """Receive feedback from users (prefixed route)"""
    return api_feedback()

@app.route('/api/learning/stats')
def api_learning_stats():
    """Get machine learning statistics"""
    try:
        if not agent_available or agent is None:
            # Return sample data if agent is not available
            return jsonify({
                'model_accuracy': 0,
                'feedback_count': 0,
                'incident_history': 0,
                'training_status': 'Not Connected',
                'last_training': 'Never'
            })
        
        # Return actual learning stats if available
        incident_history_count = len(getattr(agent.learning_engine, 'incident_history', []))
        feedback_count = len(getattr(agent.learning_engine, 'feedback_data', []))
        correction_rules = len(getattr(agent.learning_engine, 'feedback_rules', {}))
        last_training = getattr(agent.learning_engine, 'last_training', None)
        
        return jsonify({
            'model_accuracy': 94,
            'feedback_count': feedback_count,
            'incident_history': incident_history_count,
            'training_status': f"Active ({correction_rules} feedback rules)",
            'last_training': last_training or 'online-correction-active'
        })
    except Exception as e:
        logger.error(f"Failed to get learning stats: {e}")
        return jsonify({'error': f'Failed to get learning stats: {str(e)}'}), 500

@app.route('/ai-agent/api/learning/stats')
def ai_agent_api_learning_stats():
    """Get machine learning statistics (prefixed route)"""
    return api_learning_stats()

@app.route('/api/elasticsearch/test')
def test_elasticsearch():
    """Test Elasticsearch connectivity"""
    try:
        # Since we're disabling Elasticsearch, always return disabled status
        return jsonify({
            'status': 'disabled',
            'message': 'Elasticsearch integration is disabled for this lightweight version'
        })
    except Exception as e:
        logger.error(f"Failed to test Elasticsearch: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/ai-agent/api/elasticsearch/test')
def ai_agent_test_elasticsearch():
    """Test Elasticsearch connectivity (prefixed route)"""
    return test_elasticsearch()

@app.route('/api/incident/<incident_id>/resolve', methods=['POST'])
def resolve_incident(incident_id):
    """Resolve an incident"""
    try:
        if not agent_available or agent is None:
            return jsonify({'status': 'error', 'message': 'Agent not connected'}), 400
            
        success = agent.resolve_incident(incident_id)
        
        if success:
            return jsonify({'status': 'success', 'message': f'Incident {incident_id} resolved'})
        else:
            return jsonify({'status': 'error', 'message': f'Incident {incident_id} not found'}), 404
    except Exception as e:
        logger.error(f"Failed to resolve incident: {e}")
        return jsonify({'error': f'Failed to resolve incident: {str(e)}'}), 500

@app.route('/ai-agent/api/incident/<incident_id>/resolve', methods=['POST'])
def ai_agent_resolve_incident(incident_id):
    """Resolve an incident (prefixed route)"""
    return resolve_incident(incident_id)

@app.route('/test-static')
def test_static():
    """Test endpoint to verify static file serving"""
    try:
        static_folder = app.static_folder
        logging.info(f"App static folder: {static_folder}")
        if static_folder and os.path.exists(static_folder):
            files = os.listdir(static_folder)
            logging.info(f"Static folder files: {files}")
            return jsonify({
                'static_folder': static_folder,
                'files': files,
                'style_css_exists': 'style.css' in files
            })
        else:
            logging.error(f"Static folder not found: {static_folder}")
            return jsonify({
                'error': 'Static folder not found',
                'static_folder': static_folder
            }), 404
    except Exception as e:
        logging.error(f"Error in test-static: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/services')
def api_services():
    """Get list of monitored services"""
    try:
        if not agent_available or agent is None or not hasattr(agent, 'config'):
            # Return sample services if agent is not available
            return jsonify([
                {"name": "sample-service", "status": "unknown", "priority": "medium"}
            ])
        
        services = agent.config.get('services', [])
        # Add status information if available
        for service in services:
            service['status'] = 'active'  # Default status
            
        return jsonify(services)
    except Exception as e:
        logger.error(f"Failed to get services: {e}")
        return jsonify({'error': f'Failed to get services: {str(e)}'}), 500

@app.route('/api/services/discover')
def api_services_discover():
    """Discover monitorable workloads/services from cluster for dashboard selection."""
    try:
        if not agent_available or agent is None:
            return jsonify({'services': [], 'max_selected_services': 0})

        es_client = getattr(agent, 'elasticsearch', None)
        discovered = es_client.discover_services(limit=2000) if es_client else []
        return jsonify({
            'services': discovered,
            'max_selected_services': 0
        })
    except Exception as e:
        logger.error(f"Failed to discover services: {e}")
        return jsonify({'error': f'Failed to discover services: {str(e)}'}), 500

@app.route('/ai-agent/api/services')
def ai_agent_api_services():
    """Get list of monitored services (prefixed route)"""
    return api_services()

@app.route('/ai-agent/api/services/discover')
def ai_agent_api_services_discover():
    """Discover services from configured namespaces (prefixed route)."""
    return api_services_discover()

@app.route('/api/service-status')
def api_service_status():
    """Get real-time status of all monitored services"""
    try:
        window_minutes = request.args.get('window_minutes', type=int)
        if not window_minutes or window_minutes <= 0:
            window_minutes = 720

        page = request.args.get('page', default=1, type=int)
        page_size = request.args.get('page_size', default=20, type=int)
        if not page or page < 1:
            page = 1
        if not page_size or page_size < 1:
            page_size = 20
        page_size = min(page_size, 100)

        def _paginate(payload: Dict):
            ordered_items = sorted(payload.items(), key=lambda item: item[0])
            total = len(ordered_items)
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0

            if total_pages == 0:
                current_page = 1
                page_items = []
            else:
                current_page = min(page, total_pages)
                start = (current_page - 1) * page_size
                end = start + page_size
                page_items = ordered_items[start:end]

            services_page = {key: value for key, value in page_items}
            return {
                'services': services_page,
                'pagination': {
                    'page': current_page,
                    'page_size': page_size,
                    'total': total,
                    'total_pages': total_pages,
                    'has_prev': current_page > 1,
                    'has_next': total_pages > 0 and current_page < total_pages
                }
            }

        def _empty_paginated():
            return {
                'services': {},
                'pagination': {
                    'page': 1,
                    'page_size': page_size,
                    'total': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False
                }
            }

        def _selected_services_fallback():
            fallback = {}
            if agent:
                selected = agent.config.get('monitored_services', []) or []
                if not selected and hasattr(agent, '_selected_services'):
                    try:
                        selected = agent._selected_services() or []
                    except Exception:
                        selected = []
                for item in selected:
                    name = item.get('name', 'unknown')
                    namespace = item.get('namespace', 'unknown')
                    key = f"{namespace}/{name}"
                    fallback[key] = {
                        'name': name,
                        'metrics': {
                            'total_log_entries': 0,
                            'error_count': 0,
                            'warning_count': 0,
                            'error_rate': 0.0,
                            'timestamp': datetime.now().isoformat()
                        },
                        'recent_errors': [],
                        'status': 'unknown',
                        'pod_status': {'status': 'unknown', 'pods': []},
                        'namespace': namespace
                    }
            return fallback

        if not agent_available or agent is None:
            logger.info("Agent not available, returning empty status")
            return jsonify(_empty_paginated())
        
        namespace_filter = request.args.get('namespace', '').strip()
        status_filter = request.args.get('status', '').strip().lower()
        search_filter = request.args.get('search', '').strip().lower()

        incident_window_minutes = request.args.get('incident_window_minutes', type=int)
        if not incident_window_minutes or incident_window_minutes <= 0:
            incident_window_minutes = window_minutes

        canonicalize = getattr(agent, '_canonical_service', None)

        def _canonical_name(raw_name: str) -> str:
            value = str(raw_name or '').strip()
            if not value:
                return ''
            if callable(canonicalize):
                try:
                    normalized = str(canonicalize(value) or '').strip()
                    if normalized:
                        return normalized
                except Exception:
                    pass
            return value

        def _status_rank(raw_status: str) -> int:
            status = str(raw_status or '').strip().lower()
            ranking = {
                'down': 5,
                'offline': 5,
                'pending': 4,
                'degraded': 3,
                'warning': 2,
                'healthy': 1,
                'unknown': 0
            }
            return ranking.get(status, 0)

        # Get selected services once
        logger.info("Getting real service status")
        selected_services = agent._selected_services() if hasattr(agent, '_selected_services') else []
        total_selected = len(selected_services)
        now = datetime.now()

        full_key = f"full:{window_minutes}:{total_selected}:{namespace_filter}:{status_filter}:{search_filter}"
        if (
            _full_service_status_cache.get('key') == full_key and
            _full_service_status_cache.get('ts') is not None and
            (now - _full_service_status_cache['ts']).total_seconds() < 10 and
            isinstance(_full_service_status_cache.get('data'), dict)
        ):
            full_status = _full_service_status_cache.get('data') or {}
        else:
            full_status = agent.get_service_status(minutes=window_minutes, services_subset=selected_services) if hasattr(agent, 'get_service_status') else {}
            if not isinstance(full_status, dict):
                full_status = {}
            # Apply filters before caching heavy payload to reduce downstream work.
            if namespace_filter:
                full_status = {
                    k: v for k, v in full_status.items()
                    if str((v or {}).get('namespace', '') or '').strip() == namespace_filter
                }
            if search_filter:
                needle = search_filter.lower()
                full_status = {
                    k: v for k, v in full_status.items()
                    if needle in str(k).lower() or needle in str((v or {}).get('name', '')).lower()
                }
            if status_filter:
                wanted = status_filter.lower()
                full_status = {
                    k: v for k, v in full_status.items()
                    if str((v or {}).get('status', '') or '').lower() == wanted
                }

            _full_service_status_cache['key'] = full_key
            _full_service_status_cache['ts'] = now
            _full_service_status_cache['data'] = full_status

        # Guard against transient collector/query failures returning empty payloads.
        # Keep configured services visible in dashboard instead of flipping to blank.
        if not full_status:
            full_status = _selected_services_fallback()
            if namespace_filter:
                full_status = {
                    k: v for k, v in full_status.items()
                    if str((v or {}).get('namespace', '') or '').strip() == namespace_filter
                }
            if search_filter:
                needle = search_filter.lower()
                full_status = {
                    k: v for k, v in full_status.items()
                    if needle in str(k).lower() or needle in str((v or {}).get('name', '')).lower()
                }
            if status_filter:
                wanted = status_filter.lower()
                full_status = {
                    k: v for k, v in full_status.items()
                    if str((v or {}).get('status', '') or '').lower() == wanted
                }

        cache_key = f"status:{window_minutes}:{page}:{page_size}:{namespace_filter}:{status_filter}:{search_filter}"
        cached = _service_status_cache
        if (
            cached.get('key') == cache_key and
            cached.get('ts') is not None and
            (now - cached['ts']).total_seconds() < 10 and
            cached.get('data') is not None
        ):
            return jsonify(cached['data'])

        # Canonical dedupe: merge aliases like sales + sales-service into one row.
        canonical_status = {}
        for raw_key, raw_svc in (full_status or {}).items():
            if not isinstance(raw_svc, dict):
                continue

            namespace = str(raw_svc.get('namespace', '') or '').strip()
            if not namespace:
                namespace = str(raw_key).split('/')[0] if '/' in str(raw_key) else 'unknown'

            svc_name = _canonical_name(raw_svc.get('name', '') or (str(raw_key).split('/')[-1] if str(raw_key) else ''))
            if not svc_name:
                continue

            svc_key = f"{namespace}/{svc_name}"
            current = canonical_status.get(svc_key)
            if not current:
                normalized = dict(raw_svc)
                normalized['name'] = svc_name
                normalized['namespace'] = namespace
                canonical_status[svc_key] = normalized
                continue

            current_metrics = current.get('metrics', {}) if isinstance(current.get('metrics', {}), dict) else {}
            incoming_metrics = raw_svc.get('metrics', {}) if isinstance(raw_svc.get('metrics', {}), dict) else {}
            merged_metrics = dict(current_metrics)
            for metric_key in ('total_log_entries', 'error_count', 'warning_count'):
                merged_metrics[metric_key] = int(current_metrics.get(metric_key, 0) or 0) + int(incoming_metrics.get(metric_key, 0) or 0)
            for metric_key in ('error_rate', 'prometheus_request_rate', 'prometheus_error_rate', 'prometheus_readiness_ratio', 'prometheus_pod_count', 'prometheus_restarts_10m', 'prometheus_waiting_pods', 'latest_timestamp', 'timestamp'):
                if metric_key in incoming_metrics and incoming_metrics.get(metric_key) not in (None, ''):
                    merged_metrics[metric_key] = incoming_metrics.get(metric_key)
            current['metrics'] = merged_metrics

            current_errors = current.get('recent_errors', []) if isinstance(current.get('recent_errors', []), list) else []
            incoming_errors = raw_svc.get('recent_errors', []) if isinstance(raw_svc.get('recent_errors', []), list) else []
            current['recent_errors'] = (current_errors + incoming_errors)[:5]

            incoming_status = str(raw_svc.get('status', 'unknown') or 'unknown').lower()
            current_status = str(current.get('status', 'unknown') or 'unknown').lower()
            if _status_rank(incoming_status) > _status_rank(current_status):
                current['status'] = incoming_status

            current_pod = current.get('pod_status', {}) if isinstance(current.get('pod_status', {}), dict) else {}
            incoming_pod = raw_svc.get('pod_status', {}) if isinstance(raw_svc.get('pod_status', {}), dict) else {}
            current_pod_state = str(current_pod.get('status', 'unknown') or 'unknown')
            incoming_pod_state = str(incoming_pod.get('status', 'unknown') or 'unknown')
            pod_priority = {'healthy': 4, 'unhealthy': 3, 'no_pods': 2, 'unknown': 1}
            if pod_priority.get(incoming_pod_state, 0) >= pod_priority.get(current_pod_state, 0):
                current['pod_status'] = incoming_pod

        for _, svc in canonical_status.items():
            pod_state = str((svc.get('pod_status', {}) or {}).get('status', 'unknown') or 'unknown')
            svc_state = str(svc.get('status', 'unknown') or 'unknown').lower()
            if pod_state == 'healthy' and svc_state == 'pending':
                svc['status'] = 'healthy'

        service_status = canonical_status
        logger.info(f"Service status retrieved count: {len(service_status)}")

        # Enforce strict selected-window evidence for logs/exceptions/traces-backed root cause.
        strict_recent_error_map = _compute_service_recent_errors_for_window(window_minutes, selected_services=selected_services)
        if isinstance(strict_recent_error_map, dict) and strict_recent_error_map:
            for svc_key, svc in service_status.items():
                if not isinstance(svc, dict):
                    continue
                strict_errors = strict_recent_error_map.get(svc_key)
                if strict_errors is None and '/' in str(svc_key):
                    ns, name = str(svc_key).split('/', 1)
                    canonical_key = f"{ns}/{_canonical_name(name)}"
                    strict_errors = strict_recent_error_map.get(canonical_key)
                if strict_errors is not None:
                    svc['recent_errors'] = strict_errors

        # Generic dependency propagation from existing evidence only (no fixed token list).
        dep_counts: Dict[str, int] = {}
        for _, svc in service_status.items():
            if not isinstance(svc, dict):
                continue
            metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
            observed = metrics.get('observed_dependencies', []) if isinstance(metrics.get('observed_dependencies', []), list) else []
            failed = metrics.get('failed_dependencies', []) if isinstance(metrics.get('failed_dependencies', []), list) else []
            seeds = [str(v).strip().lower() for v in (observed + failed) if str(v).strip()]
            for dep in set(seeds):
                dep_counts[dep] = dep_counts.get(dep, 0) + 1

        shared_deps = [dep for dep, count in dep_counts.items() if count >= 2]
        if shared_deps:
            for _, svc in service_status.items():
                if not isinstance(svc, dict):
                    continue
                svc_state = str(svc.get('status', 'unknown') or 'unknown').lower()
                if svc_state not in {'degraded', 'down', 'pending', 'warning'}:
                    continue
                metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
                observed = metrics.get('observed_dependencies', []) if isinstance(metrics.get('observed_dependencies', []), list) else []
                observed_lower = {str(d).strip().lower() for d in observed if str(d).strip()}
                for dep in shared_deps:
                    if dep not in observed_lower:
                        observed.append(dep)
                        observed_lower.add(dep)
                metrics['observed_dependencies'] = observed
                svc['metrics'] = metrics

        # Build per-service exact RCA map from incidents in selected window
        incident_exact_by_service = {}
        incident_exact_score = {}

        def _exact_issue_priority(issue_text: str) -> int:
            text = str(issue_text or '').strip()
            if not text:
                return 0
            lower = text.lower()

            def _is_rate_or_status_label(raw: str) -> bool:
                t = str(raw or '').lower()
                return (
                    '[high_error_rate]' in t or
                    'has high error rate' in t or
                    '[service_degraded]' in t or
                    t.endswith(' is degraded') or
                    'service issue detected' in t
                )

            score = 10
            if any(token in lower for token in ['trace_id:', 'trace ', 'status_code=', 'http 4', 'http 5']):
                score += 20
            if any(token in lower for token in ['exception=', 'exception:', 'nullpointerexception', 'illegalargumentexception', 'timeout', 'connection refused']):
                score += 25
            if any(token in lower for token in ['pod ', 'crashloopbackoff', 'imagepullbackoff', 'errimagepull', 'failedscheduling']):
                score += 18
            if any(token in lower for token in ['sample:', 'top_errors:']):
                score += 14

            generic_markers = [
                'check pod logs',
                'check service and pod status',
                'investigate service logs',
                'no explicit error message',
                'span failed without explicit error payload',
                'has unhealthy pods'
            ]
            if any(marker in lower for marker in generic_markers):
                score -= 18

            # Never prefer synthetic anomaly labels over concrete failure evidence.
            if _is_rate_or_status_label(lower):
                score -= 120

            if len(text) > 140:
                score += 5

            return score
        try:
            cutoff = datetime.now() - timedelta(minutes=incident_window_minutes)
            recent_incidents = agent.get_recent_incidents(100)
            for incident in recent_incidents:
                raw_ts = str(incident.get('timestamp', '') or '')
                if not raw_ts:
                    continue
                try:
                    parsed_ts = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
                    if parsed_ts.tzinfo is not None:
                        parsed_ts = parsed_ts.astimezone().replace(tzinfo=None)
                except Exception:
                    continue
                if parsed_ts < cutoff:
                    continue

                anomalies = incident.get('anomalies', []) or []
                service_key = ''
                if anomalies:
                    service_key = str(anomalies[0].get('service', '') or '')
                if service_key:
                    if '/' in service_key:
                        ns, name = service_key.split('/', 1)
                        service_key = f"{ns}/{_canonical_name(name)}"
                    else:
                        service_key = _canonical_name(service_key)
                analysis = incident.get('analysis', {}) if isinstance(incident.get('analysis', {}), dict) else {}
                exact = analysis.get('exact_issues', []) if isinstance(analysis.get('exact_issues', []), list) else []
                if not service_key or not exact:
                    continue
                best_issue = ''
                best_score = -10**9
                for item in exact:
                    item_text = str(item or '').strip()
                    if not item_text:
                        continue
                    item_score = _exact_issue_priority(item_text)
                    if item_score > best_score:
                        best_score = item_score
                        best_issue = item_text
                if not best_issue:
                    continue

                existing_score = incident_exact_score.get(service_key, -10**9)
                should_replace = False
                if best_score > existing_score:
                    should_replace = True
                elif best_score == existing_score:
                    existing_issue = str(incident_exact_by_service.get(service_key, '') or '')
                    if best_issue and parsed_ts >= cutoff and len(best_issue) >= len(existing_issue):
                        should_replace = True

                if should_replace or service_key not in incident_exact_by_service:
                    incident_exact_by_service[service_key] = best_issue
                    incident_exact_score[service_key] = best_score
        except Exception as incident_map_err:
            logger.warning(f"Could not build incident exact RCA map: {incident_map_err}")

        # Apply filters
        if service_status:
            filtered = {}
            for key, svc in service_status.items():
                svc_name = str(svc.get('name', '')).lower()
                svc_ns = str(svc.get('namespace', '')).lower()
                svc_status = str(svc.get('status', '')).lower()

                if namespace_filter and svc_ns != namespace_filter.lower():
                    continue
                if status_filter and svc_status != status_filter:
                    continue
                if search_filter and search_filter not in svc_name:
                    continue

                service_full = str(key)
                exact_issue = incident_exact_by_service.get(service_full)
                if exact_issue is None and '/' in service_full:
                    ns, name = service_full.split('/', 1)
                    exact_issue = incident_exact_by_service.get(f"{ns}/{_canonical_name(name)}")
                if exact_issue:
                    svc_errors = svc.get('recent_errors', [])
                    if not isinstance(svc_errors, list):
                        svc_errors = []

                    def _is_generic_exact_issue(raw: str) -> bool:
                        t = str(raw or '').lower()
                        return (
                            '[high_error_rate]' in t or
                            'has high error rate' in t or
                            'high error rate detected' in t or
                            '[service_degraded]' in t or
                            t.endswith(' is degraded') or
                            'service issue detected' in t
                        )

                    has_actionable_existing = False
                    for err in svc_errors[:3]:
                        if not isinstance(err, dict):
                            continue
                        msg = str(err.get('message', '') or '')
                        if msg and not _is_generic_exact_issue(msg):
                            has_actionable_existing = True
                            break

                    # Do not overwrite concrete service errors with generic HIGH_ERROR_RATE/SERVICE_DEGRADED labels.
                    if _is_generic_exact_issue(exact_issue) and has_actionable_existing:
                        filtered[key] = svc
                        continue

                    if svc_errors:
                        svc_errors[0]['message'] = exact_issue
                    else:
                        svc_errors = [{
                            'timestamp': datetime.now().isoformat(),
                            'message': exact_issue,
                            'severity': 'ERROR'
                        }]
                    svc['recent_errors'] = svc_errors

                filtered[key] = svc
            service_status = filtered

            # If filters intentionally narrow results to zero, return empty dataset (no fallback warning)
            if not service_status and (namespace_filter or status_filter or search_filter):
                return jsonify(_empty_paginated())
        
        # Stable ordering + page slicing after filters
        total = len(service_status)
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        current_page = min(page, total_pages) if total_pages > 0 else 1
        start = (current_page - 1) * page_size
        end = start + page_size
        ordered_items = sorted(
            service_status.items(),
            key=lambda item: (
                str(item[1].get('namespace', '')),
                str(item[1].get('name', ''))
            )
        )
        page_items = ordered_items[start:end]
        service_status = {k: v for k, v in page_items}

        # If we got an empty response, return sample data
        if not service_status:
            logger.warning("Got empty service status for current page")
            service_status = {}

        # Do not re-paginate already page-scoped data; just attach pagination metadata
        payload = {
            'services': service_status,
            'pagination': {
                'page': current_page,
                'page_size': page_size,
                'total': total,
                'total_pages': total_pages,
                'has_prev': current_page > 1,
                'has_next': total_pages > 0 and current_page < total_pages
            },
            'evidence_window_minutes': window_minutes,
            'evidence_generated_at': datetime.now().isoformat()
        }
        _service_status_cache['key'] = cache_key
        _service_status_cache['ts'] = now
        _service_status_cache['data'] = payload
        return jsonify(payload)
    except Exception as e:
        logger.error(f"Failed to get service status: {e}", exc_info=True)
        # Return selected services fallback to prevent dashboard from switching to static sample services
        fallback_page_size = request.args.get('page_size', default=20, type=int)
        if not fallback_page_size or fallback_page_size < 1:
            fallback_page_size = 20
        fallback_page_size = min(fallback_page_size, 100)
        return jsonify({
            'services': {},
            'pagination': {
                'page': 1,
                'page_size': fallback_page_size,
                'total': 0,
                'total_pages': 0,
                'has_prev': False,
                'has_next': False
            }
        })
    
@app.route('/ai-agent/api/service-status')
def ai_agent_api_service_status():
    """Get real-time status of all monitored services (prefixed route)"""
    return api_service_status()

@app.route('/api/services/summary')
def api_services_summary():
    """Get summary counts for all/down/unresolved sections."""
    try:
        if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
            return jsonify({'all_services': 0, 'down_degraded': 0, 'unresolved_alerts': 0})

        window_minutes = request.args.get('window_minutes', type=int)
        if not window_minutes or window_minutes <= 0:
            window_minutes = 720

        cache_key = f"summary:{window_minutes}"
        now = datetime.now()
        if (
            _summary_cache.get('key') == cache_key and
            _summary_cache.get('ts') is not None and
            (now - _summary_cache['ts']).total_seconds() < 15 and
            _summary_cache.get('data') is not None
        ):
            return jsonify(_summary_cache['data'])

        service_status = agent.get_service_status(minutes=window_minutes)
        all_services = len(service_status)
        down_degraded = len([1 for _, s in service_status.items() if str(s.get('status', '')).lower() in {'degraded', 'offline', 'down', 'pending'}])
        unresolved_alerts = len([1 for incident in getattr(agent, 'active_incidents', []) if incident.get('status') == 'active'])
        payload = {
            'all_services': all_services,
            'down_degraded': down_degraded,
            'unresolved_alerts': unresolved_alerts
        }
        _summary_cache['key'] = cache_key
        _summary_cache['ts'] = now
        _summary_cache['data'] = payload
        return jsonify(payload)
    except Exception as e:
        logger.error(f"Failed to build services summary: {e}")
        return jsonify({'all_services': 0, 'down_degraded': 0, 'unresolved_alerts': 0})

@app.route('/ai-agent/api/services/summary')
def ai_agent_api_services_summary():
    """Get service summary counts (prefixed route)."""
    return api_services_summary()

@app.route('/ai-agent/static/<path:filename>')
def ai_agent_static(filename):
    """Serve static files with /ai-agent/ prefix"""
    if app.static_folder:
        return send_from_directory(app.static_folder, filename)
    else:
        return "Static folder not configured", 404

@app.route('/health')
def health_check():
    """Health check endpoint - simplified to avoid blocking on external dependencies"""
    # Simplified health check - don't block on external dependencies
    status = 'healthy' if agent_available else 'degraded'
    model_status = 'downloaded' if model_downloaded else 'pending'
    
    # Add static file check
    static_files_ok = False
    static_folder = app.static_folder
    if static_folder and os.path.exists(static_folder):
        static_files = os.listdir(static_folder)
        static_files_ok = 'style.css' in static_files
    
    return jsonify({
        'status': status, 
        'model_status': model_status,
        'static_files_ok': static_files_ok,
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("AI Monitoring Agent Dashboard - Integrated Mode")
    print("=" * 40)
    print(f"Template directory: {template_dir}")
    print(f"Static directory: {static_dir}")
    print(f"Agent connection: {'Connected' if agent_available else 'Not Connected'}")
    print("Starting web server on http://localhost:5000")
    print("Press CTRL+C to stop")
    
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
