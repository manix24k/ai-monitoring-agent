#!/usr/bin/env python3
"""
Web Dashboard for AI Monitoring Agent - Integrated Version
"""
from flask import Flask, Response, render_template, jsonify, request, send_from_directory, stream_with_context, session, redirect, url_for, flash
from functools import wraps
import json
import os
import re
import time
import asyncio
import hashlib
import base64
import gzip
import secrets
from datetime import datetime, timedelta, timezone
import sys
import os.path
import logging
import subprocess
import threading
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from exact_rca import get_exact_rca, get_rca_prometheus_metrics, get_rca_runtime_metrics

try:
    import infra_fix as _infra_fix
    INFRA_FIX_AVAILABLE = True
except ImportError as e:
    _infra_fix = None
    INFRA_FIX_AVAILABLE = False
    logging.warning(f"infra_fix module not available: {e}")

try:
    from slack_notifier import SlackNotifier as _SlackNotifier
    _SLACK_AVAILABLE = True
except ImportError:
    _SlackNotifier = None
    _SLACK_AVAILABLE = False

_slack_notifier = None
_slack_lock = threading.Lock()


def _init_slack() -> None:
    global _slack_notifier
    if not _SLACK_AVAILABLE:
        return
    bot_token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "")
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if bot_token and channel:
        try:
            _slack_notifier = _SlackNotifier(bot_token=bot_token, channel=channel)
            logging.info(f"[slack] SlackNotifier initialized with bot token → channel {channel}")
        except Exception as e:
            logging.warning(f"[slack] init failed: {e}")
    elif webhook:
        try:
            _slack_notifier = _SlackNotifier(webhook_url=webhook)
            logging.info("[slack] SlackNotifier initialized with webhook URL")
        except Exception as e:
            logging.warning(f"[slack] init failed: {e}")


def _slack_send_pr_report(issue, fix, pr_url: str) -> None:
    """Send ONE consolidated Slack doc when a code PR is manually created.
    Collects all issues for this service from the last 1 hour and sends
    a single message with every issue, fix diff, and the PR link.
    Only called on manual 'Generate PR' — never automatically.
    """
    if not _slack_notifier:
        return
    try:
        from datetime import datetime, timedelta, timezone as _tz
        ns = getattr(issue, 'namespace', '')
        svc = getattr(issue, 'service_name', '')
        cutoff = datetime.now(tz=_tz.utc) - timedelta(hours=1)

        # Gather all issues for this service in the last 1 hour
        all_issues = (_codexa_repository.get_issues(limit=500)
                      if _codexa_repository else [])
        svc_issues = []
        for i in all_issues:
            if i.service_name != svc or i.namespace != ns:
                continue
            ts = getattr(i, 'detected_at', None) or datetime.utcnow()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            if ts >= cutoff:
                svc_issues.append(i)

        # Build (issue, fix) pairs; current issue first
        seen_ids = set()
        items = []
        pr_urls = {}

        # Always include the current issue+fix at the top
        items.append((issue, fix))
        seen_ids.add(getattr(issue, 'id', ''))
        pr_urls[getattr(issue, 'id', '')] = pr_url

        for i in svc_issues:
            iid = getattr(i, 'id', '')
            if iid in seen_ids:
                continue
            seen_ids.add(iid)
            fixes = _codexa_repository.get_fixes_for_issue(iid) if _codexa_repository else []
            f = fixes[0] if fixes else None
            items.append((i, f))

        _slack_notifier.send_codexa_service_report(svc, ns, items, pr_urls=pr_urls)
        logging.info(f"[slack] PR report sent for {ns}/{svc} — {len(items)} issue(s) in last 1h")
    except Exception as e:
        logging.warning(f"[slack] PR report send failed: {e}")

try:
    import rollout_monitor as _rollout_monitor
    ROLLOUT_MONITOR_AVAILABLE = True
except ImportError as e:
    _rollout_monitor = None
    ROLLOUT_MONITOR_AVAILABLE = False
    logging.warning(f"rollout_monitor module not available: {e}")

try:
    import k8s_agent as _k8s_agent
    K8S_AGENT_AVAILABLE = True
except ImportError as e:
    _k8s_agent = None
    K8S_AGENT_AVAILABLE = False
    logging.warning(f"k8s_agent module not available: {e}")

# Import CodeXA for ASGI mounting
try:
    from codexa.app import codexa_app
    CODEXA_AVAILABLE = True
except ImportError as e:
    CODEXA_AVAILABLE = False
    logging.warning(f"CodeXA not available: {e}")

from learning_store import (
    enqueue_feedback_task,
    get_learning_stats,
    get_saved_rca_result,
    get_top_patterns,
    process_rca_feedback,
)

try:
    import redis as redis_lib
except Exception:
    redis_lib = None

try:
    from celery import Celery
except Exception:
    Celery = None

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
        try:
            _start_snapshot_warmer_if_needed()
        except Exception:
            pass
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

# Session configuration
app.secret_key = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(32))
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

# ============================================================================
# Authentication & User Management
# ============================================================================

USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'users.json')
_users_cache: Dict[str, Any] = {'data': None, 'ts': None}
_users_lock = threading.Lock()


def _count_admin_users(users_data: Dict[str, Any]) -> int:
    users = users_data.get('users', []) if isinstance(users_data, dict) else []
    if not isinstance(users, list):
        return 0
    return sum(1 for user in users if isinstance(user, dict) and str(user.get('role', '')).strip().lower() == 'admin')


def _normalize_users_data(users_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize users payload and enforce hardened auth defaults."""
    out = users_data if isinstance(users_data, dict) else {}
    users = out.get('users', []) if isinstance(out.get('users', []), list) else []
    settings = out.get('settings', {}) if isinstance(out.get('settings', {}), dict) else {}

    normalized_users: List[Dict[str, Any]] = []
    protected_admin_exists = False
    for user in users:
        if not isinstance(user, dict):
            continue
        normalized = dict(user)
        role = str(normalized.get('role', 'viewer') or 'viewer').strip().lower()
        if role not in {'admin', 'operator', 'viewer'}:
            role = 'viewer'
        normalized['role'] = role
        if not str(normalized.get('id', '') or '').strip():
            normalized['id'] = str(uuid4())
        if not str(normalized.get('username', '') or '').strip():
            continue
        if role == 'admin' and bool(normalized.get('protected', False)):
            protected_admin_exists = True
        normalized_users.append(normalized)

    # Ensure we always have one protected admin account.
    if not protected_admin_exists:
        for user in normalized_users:
            if str(user.get('role', '')).strip().lower() == 'admin':
                user['protected'] = True
                protected_admin_exists = True
                break

    settings['auth_enabled'] = True
    settings['session_timeout_hours'] = max(1, min(168, int(settings.get('session_timeout_hours', 24) or 24)))

    out['users'] = normalized_users
    out['settings'] = settings
    return out

def _hash_password(password: str) -> str:
    """Hash password using SHA-256 with salt."""
    salt = "ai-monitor-salt-2024"
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()

def _load_users() -> Dict[str, Any]:
    """Load users from JSON file."""
    # Use timeout to prevent deadlock - if lock not acquired in 2 seconds, return defaults
    lock_acquired = _users_lock.acquire(timeout=2)
    if not lock_acquired:
        logging.warning("Could not acquire users lock, returning cached/default")
        if _users_cache['data'] is not None:
            return _users_cache['data']
        return {'users': [], 'settings': {'auth_enabled': False}}
    try:
        # Check cache
        if _users_cache['data'] is not None and _users_cache['ts']:
            if (datetime.now() - _users_cache['ts']).total_seconds() < 30:
                return _users_cache['data']

        if not os.path.exists(USERS_FILE):
            # Create default admin user.
            default_users = {
                'users': [
                    {
                        'id': str(uuid4()),
                        'username': 'admin',
                        'password': _hash_password('admin123'),
                        'role': 'admin',
                        'protected': True,
                        'name': 'Administrator',
                        'created_at': datetime.now().isoformat(),
                        'active': True
                    }
                ],
                'settings': {
                    'auth_enabled': True,
                    'session_timeout_hours': 24
                }
            }
            default_users = _normalize_users_data(default_users)
            try:
                _save_users(default_users)
            except Exception as e:
                logging.warning(f"Could not save default users file: {e}")
            _users_cache['data'] = default_users
            _users_cache['ts'] = datetime.now()
            return default_users

        try:
            with open(USERS_FILE, 'r') as f:
                data = json.load(f)
                normalized = _normalize_users_data(data)
                if normalized != data:
                    _save_users(normalized)
                _users_cache['data'] = normalized
                _users_cache['ts'] = datetime.now()
                return normalized
        except Exception as e:
            logging.warning(f"Could not load users file: {e}")
            return {'users': [], 'settings': {'auth_enabled': True}}
    finally:
        _users_lock.release()

def _save_users(data: Dict[str, Any]) -> bool:
    """Save users to JSON file."""
    try:
        with open(USERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        # Update cache directly - don't acquire lock here (caller may already hold it)
        _users_cache['data'] = data
        _users_cache['ts'] = datetime.now()
        return True
    except Exception:
        return False

def _get_user_by_username(username: str) -> Dict[str, Any]:
    """Get user by username."""
    users_data = _load_users()
    for user in users_data.get('users', []):
        if user.get('username', '').lower() == username.lower():
            return user
    return {}

def _get_user_by_id(user_id: str) -> Dict[str, Any]:
    """Get user by ID."""
    users_data = _load_users()
    for user in users_data.get('users', []):
        if user.get('id') == user_id:
            return user
    return {}

def _authenticate_user(username: str, password: str) -> Dict[str, Any]:
    """Authenticate user and return user data if valid."""
    user = _get_user_by_username(username)
    if not user:
        return {}
    if not user.get('active', True):
        return {}
    if user.get('password') == _hash_password(password):
        return user
    return {}

def _is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    try:
        users_data = _load_users()
        return bool(users_data.get('settings', {}).get('auth_enabled', True))
    except Exception as e:
        logging.warning(f"Auth check failed, defaulting to enabled: {e}")
        return True

def login_required(f):
    """Decorator to require login for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_auth_enabled():
            return f(*args, **kwargs)
        if not session.get('user_id'):
            if request.is_json or request.path.startswith('/api/') or request.path.startswith('/ai-agent/api/'):
                return jsonify({'error': 'Authentication required', 'redirect': '/ai-agent/login'}), 401
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin role."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_auth_enabled():
            return f(*args, **kwargs)
        if not session.get('user_id'):
            if request.is_json:
                return jsonify({'error': 'Authentication required'}), 401
            return redirect(url_for('login_page'))
        user = _get_user_by_id(session.get('user_id', ''))
        if user.get('role') != 'admin':
            if request.is_json:
                return jsonify({'error': 'Admin access required'}), 403
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def operator_required(f):
    """Decorator to require operator or admin role (for troubleshooting/PR actions)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_auth_enabled():
            return f(*args, **kwargs)
        if not session.get('user_id'):
            if request.is_json:
                return jsonify({'error': 'Authentication required'}), 401
            return redirect(url_for('login_page'))
        user = _get_user_by_id(session.get('user_id', ''))
        if user.get('role') not in ['operator', 'admin']:
            if request.is_json:
                return jsonify({'error': 'Operator access required'}), 403
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================================================
# End Authentication
# ============================================================================

_summary_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None
}

_service_status_cache: Dict[str, Any] = {
    'req_key': None,
    'key': None,
    'ts': None,
    'data': None
}

_last_nonempty_service_status_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'items': None
}

_last_nonempty_page_payload_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'payload': None
}

_full_service_status_cache: Dict[str, Any] = {
    'key': None,
    'ts': None,
    'data': None,
    'refreshing': False,
    'refresh_key': None
}

_full_service_status_lock = threading.Lock()
_external_cache_lock = threading.Lock()
_external_cache_client = None
_snapshot_cache_client = None
_snapshot_warmer_started = False
_progressive_build_lock = threading.Lock()
_progressive_build_state: Dict[str, Any] = {}
_celery_app = None

_troubleshoot_jobs: Dict[str, Dict[str, Any]] = {}
_troubleshoot_jobs_lock = threading.Lock()
_jenkins_dispatch_lock = threading.Lock()
_jenkins_dispatch_inflight = 0
_jenkins_auto_rebuild_lock = threading.Lock()
_jenkins_auto_rebuild_state: Dict[str, Dict[str, Any]] = {}
_jenkins_auto_rebuild_started = False
_jenkins_auto_rebuild_scope_lock = threading.Lock()
_jenkins_auto_rebuild_active_namespace = str(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_ACTIVE_NAMESPACE', 'venus') or 'venus').strip().lower()
_jenkins_pipeline_overrides: Dict[str, str] = {}
_jenkins_pipeline_override_lock = threading.Lock()
JENKINS_PIPELINE_OVERRIDES_FILE = os.getenv(
    'JENKINS_PIPELINE_OVERRIDES_FILE',
    os.path.join(
        os.getenv('LEARNING_STORE_PATH', '/var/otel/rca-learning').strip() or '/var/otel/rca-learning',
        'jenkins_pipeline_overrides.json'
    )
)


def _load_jenkins_pipeline_overrides() -> Dict[str, str]:
    """Load persisted service->pipeline overrides from disk."""
    path = str(JENKINS_PIPELINE_OVERRIDES_FILE or '').strip()
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            raw = json.load(f)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    cleaned: Dict[str, str] = {}
    for k, v in raw.items():
        key = str(k or '').strip().lower()
        val = str(v or '').strip()
        if not key or not val or '/' not in key:
            continue
        ns, svc = key.split('/', 1)
        if ns not in {'venus', 'jupiter'} or not svc:
            continue
        cleaned[f"{ns}/{svc}"] = val
    return cleaned


def _save_jenkins_pipeline_overrides(overrides: Dict[str, str]) -> bool:
    """Persist service->pipeline overrides to disk."""
    path = str(JENKINS_PIPELINE_OVERRIDES_FILE or '').strip()
    if not path:
        return False
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(overrides or {}, f, indent=2, sort_keys=True)
        return True
    except Exception as e:
        logger.warning(f"Could not persist Jenkins pipeline overrides: {e}")
        return False


with _jenkins_pipeline_override_lock:
    _jenkins_pipeline_overrides = _load_jenkins_pipeline_overrides()

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

_SERVICE_ISSUE_RETENTION_SECONDS = 6 * 60 * 60
_SERVICE_ISSUE_MAX_ROWS_PER_SERVICE = 2000
_service_issue_store_lock = threading.Lock()
_service_issue_store: Dict[str, List[Dict[str, Any]]] = {}


def _parse_iso_datetime(raw: str) -> Optional[datetime]:
    value = str(raw or '').strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except Exception:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _service_issue_storage_key(namespace: str, service: str) -> str:
    ns = str(namespace or '').strip().lower() or 'unknown'
    name = str(service or '').strip().lower()
    return f"{ns}/{name}"


# Messages matching any of these substrings are infra telemetry noise and
# must never appear in the service-issue explorer regardless of exception type.
_DASHBOARD_NOISE_SUBSTRINGS = [
    'fabhotels-signoz-prod-otel-collector.signoz-prod11.svc.cluster.local',
    'signoz-prod11.svc.cluster.local',
]


def _is_dashboard_noise(text: str) -> bool:
    lower = text.lower()
    return any(s in lower for s in _DASHBOARD_NOISE_SUBSTRINGS)


def _event_fingerprint(namespace: str, service: str, issue: str, reason: str, root_cause: str, stacktrace: str) -> str:
    base = '|'.join([
        str(namespace or '').strip().lower(),
        str(service or '').strip().lower(),
        str(issue or '').strip().lower(),
        str(reason or '').strip().lower(),
        str(root_cause or '').strip().lower(),
        str(stacktrace or '').strip().lower(),
    ])
    return hashlib.sha1(base.encode('utf-8')).hexdigest()


def _prune_service_issue_store_locked(now_dt: Optional[datetime] = None):
    if now_dt is None:
        now_dt = datetime.utcnow()
    cutoff = now_dt - timedelta(seconds=_SERVICE_ISSUE_RETENTION_SECONDS)
    cutoff_iso = cutoff.isoformat()
    keys_to_drop = []
    for key, rows in list(_service_issue_store.items()):
        if not isinstance(rows, list) or not rows:
            keys_to_drop.append(key)
            continue
        kept = [row for row in rows if str(row.get('last_seen_at', '') or row.get('detected_at', '') or '') >= cutoff_iso]
        if kept:
            _service_issue_store[key] = kept[-_SERVICE_ISSUE_MAX_ROWS_PER_SERVICE:]
        else:
            keys_to_drop.append(key)
    for key in keys_to_drop:
        _service_issue_store.pop(key, None)


def _upsert_service_issue_event(namespace: str, service: str, event: Dict[str, Any]):
    key = _service_issue_storage_key(namespace, service)
    rows = _service_issue_store.get(key)
    if not isinstance(rows, list):
        rows = []
        _service_issue_store[key] = rows

    fp = str(event.get('fingerprint', '') or '')
    if fp:
        for existing in reversed(rows[-80:]):
            if str(existing.get('fingerprint', '') or '') != fp:
                continue
            existing['last_seen_at'] = str(event.get('detected_at', '') or existing.get('last_seen_at', '') or datetime.utcnow().isoformat())
            existing['occurrences'] = int(existing.get('occurrences', 1) or 1) + int(event.get('occurrences', 1) or 1)
            if len(str(event.get('message', '') or '')) > len(str(existing.get('message', '') or '')):
                existing['message'] = str(event.get('message', '') or '')
            if len(str(event.get('root_cause', '') or '')) > len(str(existing.get('root_cause', '') or '')):
                existing['root_cause'] = str(event.get('root_cause', '') or '')
            if len(str(event.get('stacktrace', '') or '')) > len(str(existing.get('stacktrace', '') or '')):
                existing['stacktrace'] = str(event.get('stacktrace', '') or '')
            if str(event.get('issue', '') or '').strip() and not str(existing.get('issue', '') or '').strip():
                existing['issue'] = str(event.get('issue', '') or '')
            if str(event.get('reason', '') or '').strip() and not str(existing.get('reason', '') or '').strip():
                existing['reason'] = str(event.get('reason', '') or '')
            return

    rows.append(event)
    if len(rows) > _SERVICE_ISSUE_MAX_ROWS_PER_SERVICE:
        del rows[:-_SERVICE_ISSUE_MAX_ROWS_PER_SERVICE]


def _extract_stacktrace_excerpt(raw_text: str, max_lines: int = 16) -> str:
    text = str(raw_text or '').strip()
    if not text:
        return ''
    lines = text.splitlines()
    if len(lines) == 1 and ('\\n' in lines[0] or '\\t' in lines[0]):
        lines = lines[0].replace('\\t', '    ').split('\\n')

    stack = []
    capturing = False
    for line in lines:
        candidate = str(line or '').rstrip()
        if not candidate:
            if capturing and stack:
                break
            continue
        lower = candidate.lower()
        if (
            'traceback' in lower or
            'stacktrace' in lower or
            lower.startswith('caused by:') or
            re.match(r'^\s*at\s+[\w.$]+\(', candidate) is not None or
            re.search(r'\b[A-Z][A-Za-z0-9_.$]*(Exception|Error)\b', candidate) is not None
        ):
            capturing = True
        if capturing:
            stack.append(candidate.strip())
            if len(stack) >= max_lines:
                break
    return '\n'.join(stack)


def _record_service_status_issues(service_status: Dict[str, Any]):
    if not isinstance(service_status, dict) or not service_status:
        return
    now_dt = datetime.utcnow()
    with _service_issue_store_lock:
        _prune_service_issue_store_locked(now_dt)
        for svc_key, svc in service_status.items():
            if not isinstance(svc, dict):
                continue
            service = str(svc.get('name', '') or '').strip() or str(svc_key).split('/')[-1]
            namespace = str(svc.get('namespace', '') or '').strip().lower()
            if not service or not namespace:
                continue
            pod_status = svc.get('pod_status', {}) if isinstance(svc.get('pod_status', {}), dict) else {}
            errors = svc.get('recent_errors', []) if isinstance(svc.get('recent_errors', []), list) else []
            for err in errors[:20]:
                if not isinstance(err, dict):
                    continue
                raw_message = str(err.get('message', '') or err.get('root_cause', '') or '').strip()
                if not raw_message or _is_dashboard_noise(raw_message):
                    continue
                parsed = _parse_error_to_structured(raw_message, pod_status, str(svc.get('status', '') or ''))
                issue = str(err.get('issue', '') or parsed.get('issue', '') or '').strip()
                reason = str(err.get('reason', '') or parsed.get('reason', '') or '').strip()
                root_cause = str(err.get('root_cause', '') or parsed.get('root_cause', '') or raw_message).strip()
                stacktrace = _extract_stacktrace_excerpt(raw_message)
                ts_dt = _parse_iso_datetime(str(err.get('timestamp', '') or '')) or now_dt
                if ts_dt < now_dt - timedelta(seconds=_SERVICE_ISSUE_RETENTION_SECONDS):
                    continue
                detected_at = ts_dt.isoformat()
                event = {
                    'namespace': namespace,
                    'service': service,
                    'detected_at': detected_at,
                    'last_seen_at': detected_at,
                    'message': raw_message,
                    'issue': issue,
                    'reason': reason,
                    'root_cause': root_cause,
                    'stacktrace': stacktrace,
                    'source': 'service_status',
                    'occurrences': 1,
                }
                event['fingerprint'] = _event_fingerprint(namespace, service, issue, reason, root_cause, stacktrace)
                _upsert_service_issue_event(namespace, service, event)


def _resolve_time_window_from_request(default_minutes: int = 60, max_minutes: int = 360) -> Tuple[datetime, datetime, int, bool]:
    now_dt = datetime.utcnow()
    window_minutes = request.args.get('window_minutes', default=default_minutes, type=int) or default_minutes
    window_minutes = max(1, int(window_minutes))

    start_raw = str(request.args.get('start_time', '') or '').strip()
    end_raw = str(request.args.get('end_time', '') or '').strip()
    start_dt = _parse_iso_datetime(start_raw)
    end_dt = _parse_iso_datetime(end_raw)

    if start_dt is not None and end_dt is not None and start_dt < end_dt:
        start = start_dt
        end = end_dt
    else:
        end = now_dt
        start = now_dt - timedelta(minutes=window_minutes)

    requested_minutes = max(1, int((end - start).total_seconds() / 60))
    effective_minutes = min(requested_minutes, max_minutes)
    clamped = requested_minutes > effective_minutes
    if clamped:
        start = end - timedelta(minutes=effective_minutes)

    if end > now_dt:
        end = now_dt
    if start >= end:
        start = end - timedelta(minutes=effective_minutes)

    return start, end, effective_minutes, clamped


def _collect_es_actionable_service_issues(namespace: str, service: str, start_ms: int, end_ms: int, limit: int = 500) -> List[Dict[str, Any]]:
    if not agent_available or agent is None:
        return []
    es_client = getattr(agent, 'elasticsearch', None)
    if es_client is None or not hasattr(es_client, 'get_logs'):
        return []

    try:
        rows = es_client.get_logs(service=service, start_time=start_ms, end_time=end_ms, limit=max(50, min(limit, 1000)), namespace=namespace)
    except Exception:
        rows = []
    if not isinstance(rows, list):
        rows = []

    is_actionable = getattr(agent, '_is_actionable_error_message', None)
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_message = str(
            row.get('message') or
            row.get('body') or
            row.get('log') or
            row.get('msg') or
            row.get('exception') or
            ''
        ).strip()
        if not raw_message or _is_dashboard_noise(raw_message):
            continue
        lower = raw_message.lower()
        if callable(is_actionable):
            try:
                if not bool(is_actionable(raw_message)):
                    continue
            except Exception:
                if not any(token in lower for token in ('error', 'exception', 'traceback', 'stacktrace', 'fatal')):
                    continue
        elif not any(token in lower for token in ('error', 'exception', 'traceback', 'stacktrace', 'fatal')):
            continue

        detected_at = str(row.get('@timestamp') or row.get('timestamp') or datetime.utcnow().isoformat())
        parsed = _parse_error_to_structured(raw_message, {}, '')
        stacktrace = _extract_stacktrace_excerpt(raw_message)
        issue = str(parsed.get('issue', '') or '').strip()
        reason = str(parsed.get('reason', '') or '').strip()
        root_cause = str(parsed.get('root_cause', '') or raw_message).strip()
        item = {
            'namespace': namespace,
            'service': service,
            'detected_at': detected_at,
            'last_seen_at': detected_at,
            'message': raw_message,
            'issue': issue,
            'reason': reason,
            'root_cause': root_cause,
            'stacktrace': stacktrace,
            'source': 'elasticsearch',
            'occurrences': 1,
        }
        item['fingerprint'] = _event_fingerprint(namespace, service, issue, reason, root_cause, stacktrace)
        out.append(item)
        if len(out) >= limit:
            break
    return out


# Issue detection patterns for structured error parsing (Pod-level)
_ISSUE_PATTERNS = {
    'CrashLoopBackOff': r'(?i)crashloopbackoff',
    'ImagePullBackOff': r'(?i)(imagepullbackoff|errimagepull)',
    'OOMKilled': r'(?i)(oomkilled|outofmemory|out of memory)',
    'FailedScheduling': r'(?i)(failedscheduling|unschedulable)',
    'CreateContainerConfigError': r'(?i)createcontainerconfigerror',
    'CreateContainerError': r'(?i)createcontainererror',
    'ConnectionRefused': r'(?i)connection\s*refused',
    'Timeout': r'(?i)(timeout|timed\s*out|deadline\s*exceeded)',
    'DNSError': r'(?i)(dns\s*resolution\s*failed|unknownhostexception)',
    'Exception': r'(?i)(exception|nullpointer|illegalargument)',
}

# Log-based issue detection patterns (Application-level errors from ES logs)
_LOG_ISSUE_PATTERNS = [
    # Database issues
    (r'(?i)(sql|database|db)\s*(exception|error|timeout)', 'DatabaseError', 'Database query failed'),
    (r'(?i)connection\s*(pool|limit)\s*(exhausted|exceeded)', 'ConnectionPoolExhausted', 'Connection pool limit reached'),
    (r'(?i)deadlock\s*(detected|found)', 'Deadlock', 'Database deadlock detected'),
    # HTTP/API issues
    (r'(?i)status[:\s]*(5\d{2})', 'HTTP5xx', 'Server error response'),
    (r'(?i)status[:\s]*(4\d{2})', 'HTTP4xx', 'Client error response'),
    (r'(?i)(api|service)\s*(call|request)\s*failed', 'APICallFailed', 'External API call failed'),
    # Memory/Resource issues
    (r'(?i)(heap|memory)\s*(space|limit)\s*(exceeded|exhausted)', 'MemoryExhausted', 'Memory limit exceeded'),
    (r'(?i)java\.lang\.OutOfMemoryError', 'OutOfMemory', 'JVM out of memory'),
    (r'(?i)gc\s*overhead\s*limit', 'GCOverhead', 'GC overhead limit exceeded'),
    # Connection issues
    (r'(?i)(socket|read)\s*timeout', 'SocketTimeout', 'Network socket timeout'),
    (r'(?i)connection\s*reset', 'ConnectionReset', 'Connection reset by peer'),
    (r'(?i)no\s*route\s*to\s*host', 'NoRouteToHost', 'Network routing failure'),
    (r'(?i)host\s*(not\s*found|unreachable)', 'HostUnreachable', 'Target host unreachable'),
    # Auth issues
    (r'(?i)(authentication|auth)\s*(failed|error|denied)', 'AuthFailed', 'Authentication failure'),
    (r'(?i)(unauthorized|401\s*unauthorized)', 'Unauthorized', 'Invalid credentials'),
    (r'(?i)(forbidden|403\s*forbidden)', 'Forbidden', 'Access denied'),
    # Queue/Messaging issues
    (r'(?i)(kafka|rabbitmq|queue)\s*(error|exception)', 'MessagingError', 'Message queue error'),
    # Cache issues
    (r'(?i)(redis|cache)\s*(error|exception|timeout)', 'CacheError', 'Cache service error'),
    # Common Java exceptions
    (r'NullPointerException', 'NullPointerException', 'Null pointer dereference'),
    (r'IllegalArgumentException', 'IllegalArgumentException', 'Invalid argument'),
    (r'IllegalStateException', 'IllegalStateException', 'Invalid state'),
    (r'IOException', 'IOException', 'I/O operation failed'),
    (r'RuntimeException', 'RuntimeException', 'Runtime error'),
]

# Custom patterns loaded from config (populated at runtime)
_CUSTOM_ERROR_PATTERNS_CACHE = {'loaded': False, 'patterns': []}


def _get_custom_error_patterns() -> list:
    """Load custom error patterns from config.json (cached)."""
    if _CUSTOM_ERROR_PATTERNS_CACHE['loaded']:
        return _CUSTOM_ERROR_PATTERNS_CACHE['patterns']

    try:
        if agent_available and agent is not None:
            config = getattr(agent, 'config', {}) or {}
            custom_patterns = config.get('custom_error_patterns', [])
            if isinstance(custom_patterns, list):
                _CUSTOM_ERROR_PATTERNS_CACHE['patterns'] = custom_patterns
                _CUSTOM_ERROR_PATTERNS_CACHE['loaded'] = True
                logger.info(f"Loaded {len(custom_patterns)} custom error patterns from config")
    except Exception as e:
        logger.warning(f"Could not load custom error patterns: {e}")
        _CUSTOM_ERROR_PATTERNS_CACHE['loaded'] = True

    return _CUSTOM_ERROR_PATTERNS_CACHE['patterns']


# Root cause mapping for all issue types
_ROOT_CAUSE_MAP = {
    'OOMKilled': 'Container exceeded memory limit - increase memory limits or fix memory leak',
    'CrashLoopBackOff': 'Container crashes repeatedly - check application logs for startup errors',
    'FailedScheduling': 'Pod cannot be scheduled - check cluster resources and node constraints',
    'ConnectionRefused': 'Target service not accepting connections - verify service is running',
    'Timeout': 'Request timed out - check network connectivity and service health',
    'SocketTimeout': 'Network socket timed out - check target service health and network latency',
    'DatabaseError': 'Database operation failed - check DB connectivity and query syntax',
    'ConnectionPoolExhausted': 'All DB connections in use - increase pool size or fix connection leaks',
    'Deadlock': 'Database deadlock - review transaction ordering and locking strategy',
    'HTTP5xx': 'Downstream service returned 5xx error - check downstream service logs',
    'HTTP4xx': 'Client error response - verify request parameters and auth tokens',
    'APICallFailed': 'External API call failed - check API availability and credentials',
    'MemoryExhausted': 'Application ran out of memory - increase limits or fix memory leak',
    'OutOfMemory': 'JVM heap exhausted - increase -Xmx or investigate memory leak',
    'GCOverhead': 'Too much time in GC - likely memory leak or undersized heap',
    'ConnectionReset': 'Connection dropped by peer - check network stability and timeouts',
    'NoRouteToHost': 'Cannot reach target - check network configuration and firewall rules',
    'HostUnreachable': 'Target host not reachable - verify DNS resolution and network path',
    'AuthFailed': 'Authentication failed - verify credentials and auth service health',
    'Unauthorized': 'Missing or invalid auth token - check authentication flow',
    'Forbidden': 'Access denied - verify permissions and RBAC settings',
    'MessagingError': 'Message broker error - check Kafka/RabbitMQ connectivity',
    'CacheError': 'Cache service error - check Redis connectivity and configuration',
    'NullPointerException': 'Null reference accessed - fix null check in application code',
    'IllegalArgumentException': 'Invalid argument passed - validate input parameters',
    'IllegalStateException': 'Invalid state transition - review application logic',
    'IOException': 'I/O operation failed - check file/network permissions and availability',
    'RuntimeException': 'Runtime exception - check application logs for stack trace',
}


def _parse_error_to_structured(message: str, pod_status: Dict = None, service_state: str = '') -> Dict:
    """Extract structured issue/reason/root_cause from ANY error message.

    This function handles ANY log error - not just predefined patterns.
    It intelligently extracts issue type, reason, and shows the exact
    cleaned error message as root cause.

    Args:
        message: Raw error message text (from logs, pod status, etc.)
        pod_status: Optional pod status dict
        service_state: Current service status (degraded, down, etc.)

    Returns:
        Dict with 'issue', 'reason', 'root_cause' keys - always populated
    """
    result = {
        'issue': '',
        'reason': '',
        'root_cause': ''
    }

    if not message:
        return result

    # Clean the message first - this will always be shown
    cleaned_message = re.sub(r'\s+', ' ', str(message)).strip()
    message_lower = cleaned_message.lower()
    pod_status = pod_status or {}

    # ALWAYS set root_cause to the cleaned actual error message
    # This ensures every error from logs is visible on dashboard
    result['root_cause'] = cleaned_message[:300] if len(cleaned_message) > 300 else cleaned_message

    # Step 1: Try to extract issue type dynamically from the message
    issue_extracted = ''
    reason_extracted = ''

    # 1-PRE: Handle "Exact Issue:" format first (e.g., "Exact Issue: ns/svc pod pod-name | OOMKilled (exitCode=137)")
    exact_issue_match = re.search(r'Exact Issue:[^|]+\|\s*([A-Za-z][A-Za-z0-9_-]*)', message)
    if exact_issue_match:
        issue_extracted = exact_issue_match.group(1)
        # Map common issues to cleaner names
        issue_map = {
            'oomkilled': 'OOMKilled',
            'crashloopbackoff': 'CrashLoopBackOff',
            'imagepullbackoff': 'ImagePullBackOff',
            'errimagepull': 'ImagePullError',
            'createcontainerconfigerror': 'ConfigError',
            'error': 'Error',
            'failed': 'Failed',
            'running': 'Running',
            'pending': 'Pending',
        }
        issue_extracted = issue_map.get(issue_extracted.lower(), issue_extracted)

    # Extract exit code if present
    exit_match = re.search(r'exitCode[=:\s]*(\d+)', message, re.IGNORECASE)
    if exit_match:
        code = exit_match.group(1)
        code_meanings = {
            '0': 'Success',
            '1': 'General error',
            '137': 'SIGKILL (OOM)',
            '143': 'SIGTERM (Graceful)',
            '255': 'Exit error',
            '126': 'Permission denied',
            '127': 'Command not found',
            '128': 'Invalid exit code',
            '130': 'SIGINT (Ctrl+C)',
            '134': 'SIGABRT',
            '139': 'SIGSEGV',
        }
        meaning = code_meanings.get(code, '')
        reason_extracted = f"Exit {code}" + (f" ({meaning})" if meaning else "")

    # 1a. Check for Exception class names (e.g., NullPointerException, SQLException)
    if not issue_extracted:
        exc_match = re.search(r'\b([A-Z][a-zA-Z]*(?:Exception|Error|Failure))\b', message)
        if exc_match:
            issue_extracted = exc_match.group(1)

    # 1b. Check for common error keywords and extract context
    if not issue_extracted:
        # Try to find error type from common patterns
        error_type_patterns = [
            (r'(?i)\b(timeout|timed\s*out)\b', 'Timeout'),
            (r'(?i)\b(connection\s*refused)\b', 'ConnectionRefused'),
            (r'(?i)\b(connection\s*reset)\b', 'ConnectionReset'),
            (r'(?i)\b(out\s*of\s*memory|oom)\b', 'OutOfMemory'),
            (r'(?i)\b(authentication\s*failed|auth\s*error)\b', 'AuthFailed'),
            (r'(?i)\b(permission\s*denied|access\s*denied|forbidden)\b', 'AccessDenied'),
            (r'(?i)\b(not\s*found|404)\b', 'NotFound'),
            (r'(?i)\b(service\s*unavailable|503)\b', 'ServiceUnavailable'),
            (r'(?i)\b(bad\s*gateway|502)\b', 'BadGateway'),
            (r'(?i)\b(internal\s*server\s*error|500)\b', 'ServerError'),
            (r'(?i)\b(rate\s*limit|throttl)', 'RateLimited'),
            (r'(?i)\b(circuit\s*breaker)\b', 'CircuitBreaker'),
            (r'(?i)\b(deadlock)\b', 'Deadlock'),
            (r'(?i)\b(duplicate\s*key|unique\s*constraint)\b', 'DuplicateKey'),
            (r'(?i)\b(disk\s*full|no\s*space)\b', 'DiskFull'),
            (r'(?i)\b(ssl|tls|certificate)\s*(error|failed|invalid)', 'SSLError'),
        ]
        for pattern, issue_type in error_type_patterns:
            if re.search(pattern, message):
                issue_extracted = issue_type
                break

    # 1c. Check pod-level issues (CrashLoopBackOff, ImagePullBackOff, etc.)
    if not issue_extracted:
        for issue_name, pattern in _ISSUE_PATTERNS.items():
            if re.search(pattern, message):
                issue_extracted = issue_name
                break

    # 1d. Check predefined log patterns for enhancement
    if not issue_extracted:
        for pattern, issue_name, reason in _LOG_ISSUE_PATTERNS:
            if re.search(pattern, message):
                issue_extracted = issue_name
                if not result['reason']:
                    result['reason'] = reason
                break

    # 1e. Check custom patterns from config.json
    if not issue_extracted:
        custom_patterns = _get_custom_error_patterns()
        for cp in custom_patterns:
            if not isinstance(cp, dict):
                continue
            pattern = cp.get('pattern', '')
            if pattern and re.search(pattern, message):
                issue_extracted = cp.get('issue', 'Error')
                if not result['reason']:
                    result['reason'] = cp.get('reason', '')
                # Custom pattern can override root_cause with more specific info
                custom_root = cp.get('root_cause', '')
                if custom_root:
                    result['root_cause'] = f"{custom_root} | {cleaned_message[:150]}"
                break

    # 1f. Generic fallback - still extract something useful
    if not issue_extracted:
        if 'error' in message_lower:
            issue_extracted = 'Error'
        elif 'failed' in message_lower:
            issue_extracted = 'Failed'
        elif 'exception' in message_lower:
            issue_extracted = 'Exception'
        elif 'warning' in message_lower:
            issue_extracted = 'Warning'
        elif 'critical' in message_lower:
            issue_extracted = 'Critical'
        else:
            # Extract first significant word as issue type
            words = re.findall(r'\b[A-Z][a-zA-Z]+\b', message[:100])
            if words:
                issue_extracted = words[0]
            else:
                issue_extracted = 'Issue'

    result['issue'] = issue_extracted

    # Step 2: Extract reason (short context/summary)
    # Use pre-extracted exit code reason if available
    if reason_extracted:
        result['reason'] = reason_extracted
    elif not result['reason']:
        # Try to extract HTTP status codes
        status_match = re.search(r'\b(status|code)[:\s]*(\d{3})\b', message, re.IGNORECASE)
        if status_match:
            result['reason'] = f"HTTP {status_match.group(2)}"
        # Try to extract "reason:" or "cause:" from message
        elif re.search(r'(?i)reason[:\s]+', message):
            reason_match = re.search(r'(?i)reason[:\s]+([^,.\n]+)', message)
            if reason_match:
                result['reason'] = reason_match.group(1).strip()[:50]
        elif re.search(r'(?i)cause[:\s]+', message):
            cause_match = re.search(r'(?i)cause[:\s]+([^,.\n]+)', message)
            if cause_match:
                result['reason'] = cause_match.group(1).strip()[:50]
        # Check for specific issue types that imply a reason
        elif 'oomkilled' in message_lower:
            result['reason'] = 'Out of memory'
        elif 'crashloopbackoff' in message_lower:
            result['reason'] = 'Restart loop'
        elif 'imagepullbackoff' in message_lower or 'errimagepull' in message_lower:
            result['reason'] = 'Image pull failed'
        elif 'createcontainerconfigerror' in message_lower:
            result['reason'] = 'Config error'
        else:
            # Use issue type as reason or extract first part of message
            if issue_extracted and issue_extracted not in {'Error', 'Failed', 'Issue', 'Warning', 'Running', 'Pending'}:
                result['reason'] = issue_extracted
            else:
                # Extract first meaningful phrase (up to 50 chars)
                first_part = cleaned_message[:60].split('.')[0].split('|')[0].strip()
                # Remove "Exact Issue:" prefix if present
                if first_part.lower().startswith('exact issue:'):
                    first_part = first_part[12:].strip()
                result['reason'] = first_part if len(first_part) > 3 else (issue_extracted or 'Unknown')

    # Step 3: Enhance root_cause for known issues (but always keep original message)
    if issue_extracted in _ROOT_CAUSE_MAP:
        hint = _ROOT_CAUSE_MAP[issue_extracted]
        # Append hint but keep the actual error
        if len(cleaned_message) > 150:
            result['root_cause'] = f"{cleaned_message[:150]}... | Hint: {hint}"
        else:
            result['root_cause'] = f"{cleaned_message} | Hint: {hint}"

    return result


# Pod log fetching for actual error extraction
_pod_log_cache: Dict[str, Any] = {}
_pod_log_cache_lock = threading.Lock()
_POD_LOG_CACHE_TTL_SECONDS = 60  # Cache logs for 60 seconds

def _fetch_pod_actual_error(namespace: str, pod_name: str, container: str = '') -> str:
    """Fetch actual error from container logs for a pod with issues.

    This function runs kubectl logs to get the real error message from a crashing
    or failed container, instead of just showing generic "CrashLoopBackOff" status.

    Args:
        namespace: Kubernetes namespace
        pod_name: Name of the pod
        container: Optional container name (if pod has multiple containers)

    Returns:
        Actual error message from logs, or empty string if not found
    """
    cache_key = f"{namespace}/{pod_name}/{container}"
    now = time.time()

    # Check cache first
    with _pod_log_cache_lock:
        cached = _pod_log_cache.get(cache_key)
        if cached and (now - cached.get('ts', 0)) < _POD_LOG_CACHE_TTL_SECONDS:
            return cached.get('error', '')

    error_msg = ''
    try:
        # Try to get logs from the previous crashed container first
        cmd = ['kubectl', 'logs', pod_name, '-n', namespace, '--tail=100', '--previous']
        if container:
            cmd.extend(['-c', container])

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        log_output = result.stdout if result.returncode == 0 else ''

        # If --previous fails, try current logs
        if not log_output:
            cmd = ['kubectl', 'logs', pod_name, '-n', namespace, '--tail=100']
            if container:
                cmd.extend(['-c', container])
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            log_output = result.stdout if result.returncode == 0 else ''

        if log_output:
            error_msg = _extract_error_from_logs(log_output)
            if _is_dashboard_noise(error_msg):
                error_msg = ''

    except subprocess.TimeoutExpired:
        error_msg = ''
    except Exception:
        error_msg = ''

    # Cache the result
    with _pod_log_cache_lock:
        _pod_log_cache[cache_key] = {'ts': now, 'error': error_msg}
        # Clean old entries
        if len(_pod_log_cache) > 200:
            cutoff = now - _POD_LOG_CACHE_TTL_SECONDS * 2
            keys_to_remove = [k for k, v in _pod_log_cache.items() if v.get('ts', 0) < cutoff]
            for k in keys_to_remove[:50]:
                _pod_log_cache.pop(k, None)

    return error_msg


def _extract_error_from_logs(log_output: str) -> str:
    """Extract the most relevant error message from log output.

    Looks for exceptions, stack traces, error messages, and returns a clean,
    actionable error message.
    """
    if not log_output:
        return ''

    lines = log_output.strip().split('\n')
    if not lines:
        return ''

    # Patterns that indicate error lines (in priority order)
    error_patterns = [
        # Java exceptions
        (r'(?:Exception|Error|Throwable):\s*.+', True),
        (r'Caused by:\s*.+', True),
        (r'at\s+[\w.$]+\([\w.]+:\d+\)', False),  # Stack trace line (lower priority)
        # Python errors
        (r'(?:Error|Exception|Traceback).*', True),
        (r'raise\s+\w+', True),
        # Node.js errors
        (r'Error:\s*.+', True),
        (r'TypeError:\s*.+', True),
        (r'ReferenceError:\s*.+', True),
        # Generic error keywords
        (r'\b(?:FATAL|SEVERE|CRITICAL)\b.*', True),
        (r'\[ERROR\].*', True),
        (r'(?i)error[:\s].{10,}', True),
        (r'(?i)failed[:\s].{10,}', True),
        (r'(?i)exception[:\s].{10,}', True),
    ]

    best_error = ''
    best_priority = 999

    for i, line in enumerate(lines):
        line = line.strip()
        if not line or len(line) < 10:
            continue
        if _is_dashboard_noise(line):
            continue

        for priority, (pattern, is_primary) in enumerate(error_patterns):
            if re.search(pattern, line, re.IGNORECASE):
                if priority < best_priority or (priority == best_priority and is_primary):
                    best_priority = priority
                    # Get this line and potentially the next line for context
                    error_text = line
                    if i + 1 < len(lines) and len(lines[i + 1].strip()) > 10:
                        next_line = lines[i + 1].strip()
                        # Add next line if it's a continuation (stack trace, etc.)
                        if next_line.startswith('at ') or next_line.startswith('Caused by'):
                            error_text = f"{line} | {next_line}"
                    best_error = error_text
                    if is_primary:
                        break

        if best_priority == 0:  # Found highest priority error
            break

    # If no error found by patterns, try to get the last non-empty lines
    if not best_error:
        for line in reversed(lines[-20:]):
            line = line.strip()
            if line and len(line) > 20 and not line.startswith('#'):
                best_error = line
                break

    # Clean and truncate the error message
    if best_error:
        # Remove common prefixes
        best_error = re.sub(r'^\d{4}[-/]\d{2}[-/]\d{2}[T\s]\d{2}:\d{2}:\d{2}[.,\d]*\s*', '', best_error)
        best_error = re.sub(r'^\[[\w\-:]+\]\s*', '', best_error)
        best_error = re.sub(r'^[\w.]+\s*-\s*', '', best_error)
        best_error = best_error.strip()

        # Truncate if too long
        if len(best_error) > 300:
            best_error = best_error[:297] + '...'

    return best_error


def _jenkins_cfg() -> Dict[str, Any]:
    url = str(os.getenv('JENKINS_URL', '') or '').strip().rstrip('/')
    dispatcher = str(
        os.getenv('JENKINS_DISPATCHER_JOB', '') or
        os.getenv('JENKINS_JOB_NAME', '') or
        'ai-monitor-rebuild-dispatcher'
    ).strip().strip('/')
    trigger_token = str(os.getenv('JENKINS_JOB_TOKEN', '') or '').strip()
    user = str(os.getenv('JENKINS_USER', '') or '').strip()
    api_token = str(os.getenv('JENKINS_TOKEN', '') or '').strip()
    max_workers = int(os.getenv('JENKINS_BOT_MAX_CONCURRENT', '1') or 1)
    max_workers = max(1, min(max_workers, 10))
    return {
        'url': url,
        'dispatcher': dispatcher,
        'trigger_token': trigger_token,
        'user': user,
        'api_token': api_token,
        'max_workers': max_workers,
    }


def _jenkins_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    headers = {'Accept': 'application/json'}
    user = str(cfg.get('user', '') or '').strip()
    token = str(cfg.get('api_token', '') or '').strip()
    if user and token:
        encoded = base64.b64encode(f"{user}:{token}".encode('utf-8')).decode('ascii')
        headers['Authorization'] = f"Basic {encoded}"
    return headers


def _jenkins_read_json(url: str, cfg: Dict[str, Any], timeout: float = 4.0) -> Dict[str, Any]:
    req = urllib_request.Request(url=url, headers=_jenkins_headers(cfg), method='GET')
    with urllib_request.urlopen(req, timeout=max(1.0, float(timeout or 4.0))) as resp:
        payload = resp.read().decode('utf-8')
        data = json.loads(payload)
        return data if isinstance(data, dict) else {}


def _jenkins_dispatch_job_building_count(cfg: Dict[str, Any]) -> int:
    base = str(cfg.get('url', '') or '').strip()
    dispatcher = str(cfg.get('dispatcher', '') or '').strip()
    if not base or not dispatcher:
        return 0
    try:
        parts = [p for p in dispatcher.split('/') if p]
        job_path = '/'.join([f"job/{urllib_parse.quote(p)}" for p in parts])
        api_url = f"{base}/{job_path}/api/json?tree=builds[building]&depth=1"
        data = _jenkins_read_json(api_url, cfg, timeout=4.0)
        builds = data.get('builds', []) if isinstance(data.get('builds', []), list) else []
        return len([b for b in builds if isinstance(b, dict) and bool(b.get('building', False))])
    except Exception:
        return 0


def _jenkins_status_payload() -> Dict[str, Any]:
    cfg = _jenkins_cfg()
    out = {
        'configured': bool(cfg.get('url')),
        'status': 'unknown',
        'url': str(cfg.get('url', '') or ''),
        'dispatcher_job': str(cfg.get('dispatcher', '') or ''),
        'active_workers': 0,
        'max_workers': int(cfg.get('max_workers', 2) or 2),
        'message': '',
    }

    if not out['configured']:
        out['status'] = 'not_configured'
        out['message'] = 'JENKINS_URL is not configured'
        return out

    active = _jenkins_dispatch_job_building_count(cfg)
    if active <= 0:
        with _jenkins_dispatch_lock:
            active = int(_jenkins_dispatch_inflight)

    auto_active = 0
    with _jenkins_auto_rebuild_lock:
        for _, entry in _jenkins_auto_rebuild_state.items():
            if not isinstance(entry, dict):
                continue
            if not bool(entry.get('active_issue', False)):
                continue
            st = str(entry.get('status', '') or '').strip().lower()
            if st in {'pending', 'queued', 'running_wait', 'build_success', 'verifying'}:
                auto_active += 1

    # Busy means either Jenkins is building or auto worker is still
    # actively processing verification/queue for a service.
    out['active_workers'] = max(0, int(active), int(auto_active))

    try:
        _ = _jenkins_read_json(f"{cfg['url']}/api/json", cfg, timeout=4.0)
        out['status'] = 'connected'
        out['message'] = 'Jenkins reachable'
    except Exception as e:
        out['status'] = 'error'
        out['message'] = str(e)
    return out


def _jenkins_apply_env_overrides(params: Dict[str, str], target_env: str) -> Dict[str, str]:
    """Force dispatcher env-like params to the requested namespace."""
    out: Dict[str, str] = {
        str(k): str(v if v is not None else '')
        for k, v in (params or {}).items()
        if str(k or '').strip()
    }
    env_value = str(target_env or '').strip().lower()
    if env_value not in {'venus', 'jupiter'}:
        return out

    # Always provide canonical keys expected by dispatcher jobs.
    # Some dispatcher implementations read different key names/casing.
    out['TARGET_ENV'] = env_value
    out['ENV'] = env_value
    out['target_env'] = env_value
    out['env'] = env_value
    out['environment'] = env_value
    out['NAMESPACE'] = env_value
    out['namespace'] = env_value
    out['TARGET_NAMESPACE'] = env_value
    out['target_namespace'] = env_value
    out['TARGET_ENVIRONMENT'] = env_value
    out['target_environment'] = env_value

    # If inherited params already include equivalent env keys, override them too.
    for key in list(out.keys()):
        key_l = str(key or '').strip().lower()
        if key_l in {'target_env', 'env', 'environment', 'namespace', 'target_namespace'}:
            out[key] = env_value
            continue
        if re.search(r'(^|[_-])(env|environment|namespace)([_-]|$)', key_l):
            out[key] = env_value
    return out


def _jenkins_dispatch_result(service_name: str, target_env: str, source: str = 'manual', pipeline_name: str = ''):
    cfg = _jenkins_cfg()
    service_name = str(service_name or '').strip()
    target_env = str(target_env or '').strip().lower()
    source = str(source or 'manual').strip().lower() or 'manual'
    pipeline_name = str(pipeline_name or '').strip()

    if not service_name or target_env not in {'venus', 'jupiter'}:
        return {'status': 'error', 'message': 'service_name and target_env(venus|jupiter) are required'}, 400

    if not cfg.get('url') or not cfg.get('dispatcher') or not cfg.get('trigger_token'):
        return {'status': 'error', 'message': 'Jenkins dispatcher config missing'}, 400

    global _jenkins_dispatch_inflight
    with _jenkins_dispatch_lock:
        active = _jenkins_dispatch_job_building_count(cfg)
        if active <= 0:
            active = int(_jenkins_dispatch_inflight)
        limit = int(cfg.get('max_workers', 2) or 2)
        if active >= limit:
            return {
                'status': 'throttled',
                'message': f'max concurrent bot limit reached ({active}/{limit})',
                'active_workers': int(active),
                'max_workers': int(limit),
            }, 429
        _jenkins_dispatch_inflight += 1

    try:
        parts = [p for p in str(cfg['dispatcher']).split('/') if p]
        job_path = '/'.join([f"job/{urllib_parse.quote(p)}" for p in parts])
        inherited = _jenkins_dispatcher_latest_success_params(cfg, service_name, target_env)
        dispatch_params: Dict[str, str] = {
            str(k): str(v if v is not None else '')
            for k, v in inherited.items()
            if str(k or '').strip()
        }
        effective_pipeline = pipeline_name or str(dispatch_params.get('TARGET_JOB', '') or '').strip()
        if effective_pipeline:
            dispatch_params['TARGET_JOB'] = effective_pipeline
        dispatch_params.update({
            'token': cfg['trigger_token'],
            'SERVICE_NAME': service_name,
            'DRY_RUN': 'false',
            'REBUILD_TRIGGER_SOURCE': source,
        })
        dispatch_params = _jenkins_apply_env_overrides(dispatch_params, target_env)
        query = urllib_parse.urlencode(dispatch_params)
        trigger_url = f"{cfg['url']}/{job_path}/buildWithParameters?{query}"
        req = urllib_request.Request(url=trigger_url, headers=_jenkins_headers(cfg), method='POST')
        with urllib_request.urlopen(req, timeout=8.0) as resp:
            code = int(getattr(resp, 'status', 200) or 200)
        state = 'queued' if code in {200, 201, 202, 302} else 'error'
        return {
            'status': state,
            'http_code': code,
            'service_name': service_name,
            'target_env': target_env,
            'pipeline_name': effective_pipeline,
            'dispatcher_job': cfg['dispatcher'],
        }, (200 if state == 'queued' else 500)
    except urllib_error.HTTPError as e:
        return {'status': 'error', 'message': f'HTTP {int(e.code or 0)}', 'http_code': int(e.code or 0)}, 500
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500
    finally:
        with _jenkins_dispatch_lock:
            _jenkins_dispatch_inflight = max(0, int(_jenkins_dispatch_inflight) - 1)


def _jenkins_auto_rebuild_enabled() -> bool:
    raw = str(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_ENABLED', 'true') or '').strip().lower()
    return raw in {'1', 'true', 'yes', 'on', 'enabled'}


def _jenkins_auto_rebuild_interval_seconds() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_INTERVAL_SECONDS', '30') or 30)
    return max(10, min(value, 300))


def _jenkins_auto_rebuild_cooldown_seconds() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_COOLDOWN_SECONDS', '900') or 900)
    return max(60, min(value, 86400))


def _jenkins_auto_rebuild_max_per_cycle() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_MAX_PER_CYCLE', '2') or 2)
    return max(1, min(value, 2))


def _jenkins_auto_rebuild_window_minutes() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_WINDOW_MINUTES', '1') or 1)
    return max(1, min(value, 30))


def _jenkins_auto_rebuild_verify_timeout_seconds() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_VERIFY_TIMEOUT_SECONDS', '600') or 600)
    return max(120, min(value, 7200))


def _jenkins_auto_rebuild_queue_timeout_seconds() -> int:
    value = int(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_QUEUE_TIMEOUT_SECONDS', '900') or 900)
    return max(120, min(value, 7200))


def _jenkins_auto_rebuild_allowed_namespaces() -> set:
    raw = str(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_NAMESPACES', 'venus,jupiter') or '').strip().lower()
    if not raw:
        return {'venus', 'jupiter'}
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    out = {p for p in parts if p in {'venus', 'jupiter'}}
    return out if out else {'venus', 'jupiter'}


def _jenkins_auto_rebuild_wait_for_running() -> bool:
    raw = str(os.getenv('JENKINS_IMAGE_AUTO_REBUILD_WAIT_FOR_RUNNING', 'true') or '').strip().lower()
    return raw in {'1', 'true', 'yes', 'on', 'enabled'}


def _jenkins_auto_rebuild_effective_namespaces() -> set:
    configured = _jenkins_auto_rebuild_allowed_namespaces()
    with _jenkins_auto_rebuild_scope_lock:
        active = str(_jenkins_auto_rebuild_active_namespace or '').strip().lower()
    if active in {'venus', 'jupiter'}:
        return {active} if active in configured else set()
    if active in {'all', '*', ''}:
        return configured
    return configured


def _jenkins_dispatcher_running_keys(cfg: Dict[str, Any]) -> set:
    out = set()
    try:
        builds = _jenkins_dispatcher_builds(cfg, limit=30)
    except Exception:
        return out
    for row in builds:
        if not isinstance(row, dict) or not bool(row.get('building', False)):
            continue
        ns = str(row.get('target_env', '') or '').strip().lower()
        name = str(row.get('service_name', '') or '').strip().lower()
        if ns in {'venus', 'jupiter'} and name:
            out.add(f"{ns}/{name}")
    return out


def _jenkins_dispatcher_latest_build_for(cfg: Dict[str, Any], namespace: str, service_name: str, builds: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    rows = builds if isinstance(builds, list) else _jenkins_dispatcher_builds(cfg, limit=30)
    ns = str(namespace or '').strip().lower()
    svc = str(service_name or '').strip().lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get('target_env', '') or '').strip().lower() != ns:
            continue
        if str(row.get('service_name', '') or '').strip().lower() != svc:
            continue
        return row
    return {}


def _jenkins_dispatcher_latest_success_params(cfg: Dict[str, Any], service_name: str, namespace: str, builds: List[Dict[str, Any]] = None) -> Dict[str, str]:
    rows = builds if isinstance(builds, list) else _jenkins_dispatcher_builds(cfg, limit=60)
    svc = str(service_name or '').strip().lower()
    ns = str(namespace or '').strip().lower()

    for row in rows:
        if not isinstance(row, dict):
            continue
        result = str(row.get('result', '') or '').strip().lower()
        if result not in {'success', 'unstable'}:
            continue
        row_svc = str(row.get('service_name', '') or '').strip().lower()
        if not row_svc or row_svc != svc:
            continue
        row_ns = str(row.get('target_env', '') or '').strip().lower()
        if row_ns != ns:
            continue
        params = row.get('params', {}) if isinstance(row.get('params', {}), dict) else {}
        return {str(k): str(v if v is not None else '') for k, v in params.items()}
    return {}


def _jenkins_extract_image_pull_signature(service_row: Dict[str, Any]) -> str:
    patterns = [
        r'imagepullbackoff',
        r'errimagepull',
        r'invalidimagename',
        r'back[- ]?off pulling image',
        r'failed to pull image',
        r'failed to apply default image tag',
        r'invalid reference format',
        r'pull access denied',
        r'manifest unknown',
        r'image not found',
        r'no basic auth credentials',
        r'insufficient_scope',
    ]
    probe = []
    errors = service_row.get('recent_errors', []) if isinstance(service_row.get('recent_errors', []), list) else []
    for err in errors[:8]:
        if isinstance(err, dict):
            probe.append(str(err.get('message', '') or ''))
    pod_status = service_row.get('pod_status', {}) if isinstance(service_row.get('pod_status', {}), dict) else {}
    pods = pod_status.get('pods', []) if isinstance(pod_status.get('pods', []), list) else []
    for pod in pods[:10]:
        if not isinstance(pod, dict):
            continue
        probe.append(str(pod.get('reason', '') or ''))
        probe.append(str(pod.get('state', '') or ''))
        probe.append(str(pod.get('status', '') or ''))

    for raw in probe:
        text = str(raw or '').strip()
        if not text:
            continue
        low = text.lower()
        if any(re.search(p, low) for p in patterns):
            return text[:240]
    return ''


def _jenkins_normalize_issue_signature(signature: str) -> str:
    """Normalize noisy issue text so one issue does not requeue repeatedly."""
    raw = str(signature or '').strip().lower()
    if not raw:
        return ''
    if 'invalidimagename' in raw:
        return 'invalidimagename'
    if 'invalid reference format' in raw or 'failed to apply default image tag' in raw:
        return 'invalidimagename'
    if 'imagepullbackoff' in raw or 'back-off pulling image' in raw:
        return 'imagepullbackoff'
    if 'errimagepull' in raw or 'failed to pull image' in raw:
        return 'errimagepull'
    if 'no basic auth credentials' in raw or 'authorization failed' in raw:
        return 'registry-auth-failed'
    if 'manifest unknown' in raw or 'not found' in raw:
        return 'image-not-found'
    return raw[:120]


def _start_jenkins_auto_rebuild_worker_if_needed():
    global _jenkins_auto_rebuild_started
    if _jenkins_auto_rebuild_started:
        return
    if not _jenkins_auto_rebuild_enabled():
        return
    if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
        return

    cfg = _jenkins_cfg()
    if not cfg.get('url') or not cfg.get('dispatcher') or not cfg.get('trigger_token'):
        return

    interval_seconds = _jenkins_auto_rebuild_interval_seconds()
    cooldown_seconds = _jenkins_auto_rebuild_cooldown_seconds()
    window_minutes = _jenkins_auto_rebuild_window_minutes()
    verify_timeout_seconds = _jenkins_auto_rebuild_verify_timeout_seconds()
    queue_timeout_seconds = _jenkins_auto_rebuild_queue_timeout_seconds()
    wait_for_running = _jenkins_auto_rebuild_wait_for_running()

    def _worker_loop():
        while True:
            try:
                selected = agent._selected_services() if hasattr(agent, '_selected_services') else []
                status_map = agent.get_service_status(
                    minutes=window_minutes,
                    services_subset=selected,
                    include_deep_inspection=False
                )
                if not isinstance(status_map, dict):
                    status_map = {}

                queued = 0
                per_cycle_limit = _jenkins_auto_rebuild_max_per_cycle()
                now = datetime.now()
                builds = _jenkins_dispatcher_builds(cfg, limit=30)
                running_keys = {
                    f"{str(row.get('target_env', '') or '').strip().lower()}/{str(row.get('service_name', '') or '').strip().lower()}"
                    for row in builds if isinstance(row, dict) and bool(row.get('building', False))
                }
                active_issue_keys = set()
                allowed_namespaces = _jenkins_auto_rebuild_effective_namespaces()

                # Serialize automation: one active service at a time until success + deploy verification.
                active_state_key = ''
                active_entry = {}
                with _jenkins_auto_rebuild_lock:
                    for state_key, entry in list(_jenkins_auto_rebuild_state.items()):
                        if not isinstance(entry, dict):
                            continue
                        st = str(entry.get('status', '') or '').strip().lower()
                        if st in {'pending', 'queued', 'running_wait', 'build_success', 'verifying'} and bool(entry.get('active_issue', True)):
                            active_state_key = state_key
                            active_entry = dict(entry)
                            break

                if active_state_key:
                    ns, name = active_state_key.split('/', 1)
                    active_since = active_entry.get('active_since')
                    if not isinstance(active_since, datetime):
                        active_since = now
                    latest = _jenkins_dispatcher_latest_build_for(cfg, ns, name, builds=builds)

                    if active_state_key in running_keys:
                        with _jenkins_auto_rebuild_lock:
                            _jenkins_auto_rebuild_state[active_state_key] = {
                                **active_entry,
                                'status': 'running_wait',
                                'last_ts': now,
                                'active_since': active_since,
                                'active_issue': True,
                            }
                        time.sleep(interval_seconds)
                        continue

                    latest_result = str(latest.get('result', '') or '').strip().lower() if isinstance(latest, dict) else ''
                    build_number = int(latest.get('number', 0) or 0) if isinstance(latest, dict) else int(active_entry.get('build_number', 0) or 0)
                    if latest_result in {'success', 'unstable'}:
                        current_row = status_map.get(active_state_key, {}) if isinstance(status_map.get(active_state_key, {}), dict) else {}
                        if not current_row:
                            for k, row in status_map.items():
                                if not isinstance(row, dict):
                                    continue
                                row_ns = str(row.get('namespace', '') or '').strip().lower()
                                row_name = str(row.get('name', '') or '').strip().lower()
                                if row_ns == ns and row_name == name:
                                    current_row = row
                                    break

                        sig = str(active_entry.get('sig', '') or '')
                        _mark_service_fixed(name, ns, build_number, sig)

                        remaining_issue = _jenkins_extract_image_pull_signature(current_row) if isinstance(current_row, dict) else ''
                        metrics = current_row.get('metrics', {}) if isinstance(current_row.get('metrics', {}), dict) else {}
                        pod_ready = int(metrics.get('pod_ready_count', 0) or 0) > 0
                        pod_running = int(metrics.get('pod_running_count', 0) or 0) > 0
                        svc_status = str(current_row.get('status', 'unknown') or 'unknown').strip().lower()

                        argocd_ok = True
                        argocd_cfg = _argocd_cfg()
                        if bool(argocd_cfg.get('enabled', True)) and bool(argocd_cfg.get('url', '')):
                            argocd_state = _argocd_get_app_status(name, ns)
                            if isinstance(argocd_state, dict) and 'error' not in argocd_state:
                                argocd_ok = bool(argocd_state.get('synced', False)) and bool(argocd_state.get('healthy', False))
                            else:
                                argocd_ok = False

                        resolved = (not remaining_issue) and pod_ready and pod_running and svc_status in {'healthy', 'warning'} and argocd_ok
                        if resolved:
                            _mark_service_deployed(name, ns)
                            with _jenkins_auto_rebuild_lock:
                                _jenkins_auto_rebuild_state[active_state_key] = {
                                    **active_entry,
                                    'status': 'deployed',
                                    'last_ts': now,
                                    'active_since': active_since,
                                    'active_issue': False,
                                    'build_number': build_number,
                                }
                        else:
                            verify_elapsed = (now - active_since).total_seconds() if isinstance(active_since, datetime) else 0
                            if verify_elapsed >= verify_timeout_seconds:
                                logger.warning(
                                    "AUTO_REBUILD: verification timeout for %s after build #%s (elapsed=%ss)",
                                    active_state_key,
                                    build_number,
                                    int(verify_elapsed),
                                )
                                with _jenkins_auto_rebuild_lock:
                                    _jenkins_auto_rebuild_state[active_state_key] = {
                                        **active_entry,
                                        'status': 'failed',
                                        'last_ts': now,
                                        'active_since': active_since,
                                        'active_issue': True,
                                        'build_number': build_number,
                                        'failure_reason': (
                                            f"Verification timeout after build #{build_number}. "
                                            "ArgoCD/pod health did not converge in time; manual retry required."
                                        ),
                                    }
                                time.sleep(interval_seconds)
                                continue
                            with _jenkins_auto_rebuild_lock:
                                _jenkins_auto_rebuild_state[active_state_key] = {
                                    **active_entry,
                                    'status': 'verifying',
                                    'last_ts': now,
                                    'active_since': active_since,
                                    'active_issue': True,
                                    'build_number': build_number,
                                }
                        time.sleep(interval_seconds)
                        continue

                    if latest_result in {'failure', 'failed', 'error', 'aborted'}:
                        logger.warning(f"AUTO_REBUILD: Build #{build_number} FAILED for {active_state_key} - will NOT auto-retry")
                        with _jenkins_auto_rebuild_lock:
                            _jenkins_auto_rebuild_state[active_state_key] = {
                                **active_entry,
                                'status': 'failed',
                                'last_ts': now,
                                'active_since': active_since,
                                'active_issue': True,
                                'build_number': build_number,
                                'failure_reason': f"Build #{build_number} {latest_result}. Check Jenkins console for details.",
                            }
                        time.sleep(interval_seconds)
                        continue

                # Direct kubectl check for ImagePullBackOff pods (only non-running pods to reduce API load)
                kubectl_image_issues = {}  # {ns/service: signature}
                for check_ns in allowed_namespaces:
                    try:
                        import subprocess
                        import json as json_module
                        # Only fetch non-running pods to minimize kube-api load
                        result = subprocess.run(
                            ['kubectl', 'get', 'pods', '-n', check_ns, '--field-selector=status.phase!=Running,status.phase!=Succeeded', '-o', 'json'],
                            capture_output=True, text=True, timeout=15
                        )
                        if result.returncode == 0:
                            pods_data = json_module.loads(result.stdout or '{}')
                            for pod in pods_data.get('items', []):
                                if not isinstance(pod, dict):
                                    continue
                                metadata = pod.get('metadata', {}) or {}
                                pod_status = pod.get('status', {}) or {}
                                pod_name = str(metadata.get('name', '') or '')
                                owner_refs = metadata.get('ownerReferences', []) if isinstance(metadata.get('ownerReferences', []), list) else []
                                owner_kind = ''
                                if owner_refs and isinstance(owner_refs[0], dict):
                                    owner_kind = str(owner_refs[0].get('kind', '') or '').strip().lower()
                                # Jenkins auto-rebuild is deployment-only.
                                if owner_kind and owner_kind not in {'replicaset', 'deployment'}:
                                    continue

                                # Extract service name from labels or pod name
                                labels = metadata.get('labels', {}) or {}
                                service_name = str(labels.get('app', '') or labels.get('app.kubernetes.io/name', '') or '').strip()
                                if not service_name and pod_name:
                                    parts = pod_name.rsplit('-', 2)
                                    if len(parts) >= 2:
                                        service_name = parts[0]

                                if not service_name:
                                    continue

                                # Check for ImagePullBackOff in container statuses
                                container_statuses = pod_status.get('containerStatuses', []) or []
                                for cs in container_statuses:
                                    if not isinstance(cs, dict):
                                        continue
                                    state = cs.get('state', {}) or {}
                                    waiting = state.get('waiting', {}) or {}
                                    reason = str(waiting.get('reason', '') or '').lower()
                                    message = str(waiting.get('message', '') or '')

                                    msg_l = message.lower()
                                    is_image_issue = (
                                        reason in ('imagepullbackoff', 'errimagepull', 'invalidimagename') or
                                        bool(re.search(r'image|pull|manifest|invalid\s+reference\s+format|default\s+image\s+tag', msg_l))
                                    )
                                    if is_image_issue:
                                        svc_key = f"{check_ns}/{service_name.lower()}"
                                        if svc_key not in kubectl_image_issues:
                                            sig = f"{reason}: {message[:200]}" if message else reason
                                            kubectl_image_issues[svc_key] = sig
                                            logger.info(f"AUTO_REBUILD: Found ImagePull issue via kubectl: {svc_key}")
                                        break
                    except Exception as kubectl_err:
                        logger.warning(f"AUTO_REBUILD: kubectl check failed for {check_ns}: {kubectl_err}")

                # Merge kubectl findings into status_map for processing
                for svc_key, sig in kubectl_image_issues.items():
                    if svc_key not in status_map:
                        ns, name = svc_key.split('/', 1)
                        status_map[svc_key] = {
                            'name': name,
                            'namespace': ns,
                            'status': 'unhealthy',
                            'recent_errors': [{'message': sig}],
                            'pod_status': {'pods': [{'reason': sig}]}
                        }

                for svc_key, svc in sorted(status_map.items(), key=lambda x: str(x[0])):
                    if queued >= per_cycle_limit:
                        break
                    if not isinstance(svc, dict):
                        continue

                    ns = str(svc.get('namespace', '') or '').strip().lower()
                    name = str(svc.get('name', '') or '').strip().lower()
                    if not ns or not name:
                        raw = str(svc_key or '')
                        if '/' in raw:
                            ns_part, name_part = raw.split('/', 1)
                            ns = ns or str(ns_part or '').strip().lower()
                            name = name or str(name_part or '').strip().lower()
                    if ns not in {'venus', 'jupiter'} or not name:
                        continue
                    if ns not in allowed_namespaces:
                        continue

                    metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
                    workload_kind = str(metrics.get('workload_kind', '') or '').strip().lower()
                    # Jenkins automation is deployment-only.
                    if workload_kind and workload_kind != 'deployment':
                        continue

                    # Check kubectl findings first, then fallback to service status
                    signature = kubectl_image_issues.get(f"{ns}/{name}", '')
                    if not signature:
                        signature = _jenkins_extract_image_pull_signature(svc)
                    signature = _jenkins_normalize_issue_signature(signature)
                    if not signature:
                        continue

                    state_key = f"{ns}/{name}"
                    active_issue_keys.add(state_key)

                    with _jenkins_auto_rebuild_lock:
                        entry = _jenkins_auto_rebuild_state.get(state_key, {})
                        last_ts = entry.get('last_ts')
                        last_sig = str(entry.get('sig', '') or '')
                        last_status = str(entry.get('status', '') or '').strip().lower()
                        if state_key in running_keys:
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'active_since': entry.get('active_since', now),
                                'sig': signature,
                                'status': 'running_wait',
                                'active_issue': True,
                            }
                            continue
                        if isinstance(last_ts, datetime):
                            queued_age = (now - last_ts).total_seconds()
                            if last_sig == signature and last_status in {'pending', 'queued', 'running_wait'} and queued_age < queue_timeout_seconds:
                                continue
                        if isinstance(last_ts, datetime):
                            elapsed = (now - last_ts).total_seconds()
                            if elapsed < cooldown_seconds and last_sig == signature:
                                continue
                        if _is_service_recently_fixed(name, ns, signature):
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'sig': signature,
                                'status': 'recently_fixed',
                                'active_issue': False,
                            }
                            continue
                        # Don't auto-retry failed builds - require manual retry
                        if last_status == 'failed' and last_sig == signature:
                            logger.info(f"AUTO_REBUILD: Skipping {state_key} - previous build failed, requires manual retry")
                            continue
                        _jenkins_auto_rebuild_state[state_key] = {
                            'last_ts': now,
                            'active_since': entry.get('active_since', now),
                            'sig': signature,
                            'status': 'pending',
                            'active_issue': True,
                        }

                    override_pipeline = ''
                    with _jenkins_pipeline_override_lock:
                        override_pipeline = str(_jenkins_pipeline_overrides.get(state_key, '') or '').strip()
                    out, code = _jenkins_dispatch_result(name, ns, source='auto_image_pull', pipeline_name=override_pipeline)
                    if code == 200 and str(out.get('status', '')).lower() == 'queued':
                        queued += 1
                        with _jenkins_auto_rebuild_lock:
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'active_since': now,
                                'sig': signature,
                                'status': 'queued',
                                'active_issue': True,
                            }
                        logger.warning("AUTO_REBUILD queued for %s/%s (issue=%s)", ns, name, signature)
                    elif code == 429:
                        with _jenkins_auto_rebuild_lock:
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'active_since': entry.get('active_since', now),
                                'sig': signature,
                                'status': 'throttled',
                                'active_issue': True,
                            }
                        break
                    else:
                        fail_message = str(out.get('message', '') or out.get('error', '') or out.get('status', 'dispatch error')).strip()
                        logger.warning("AUTO_REBUILD: dispatch failed for %s: %s", state_key, fail_message)
                        with _jenkins_auto_rebuild_lock:
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'active_since': entry.get('active_since', now),
                                'sig': signature,
                                'status': 'failed',
                                'active_issue': True,
                                'failure_reason': fail_message,
                            }

                with _jenkins_auto_rebuild_lock:
                    for state_key, entry in list(_jenkins_auto_rebuild_state.items()):
                        if state_key in active_issue_keys:
                            continue
                        if not isinstance(entry, dict):
                            _jenkins_auto_rebuild_state[state_key] = {
                                'last_ts': now,
                                'sig': '',
                                'status': 'resolved',
                                'active_issue': False,
                            }
                            continue
                        # Keep failed entries visible for operators so they can
                        # see root cause and use manual pipeline override/retry.
                        if str(entry.get('status', '') or '').strip().lower() == 'failed':
                            entry['active_issue'] = False
                            _jenkins_auto_rebuild_state[state_key] = entry
                            continue
                        entry['active_issue'] = False
                        entry['sig'] = ''
                        entry['status'] = 'resolved'
                        entry['last_ts'] = now
                        _jenkins_auto_rebuild_state[state_key] = entry
            except Exception as e:
                logger.warning(f"AUTO_REBUILD worker loop error: {e}")
            time.sleep(interval_seconds)

    _jenkins_auto_rebuild_started = True
    threading.Thread(target=_worker_loop, daemon=True).start()


def _refresh_page_services_with_live_pods(services_page: Dict[str, Any]):
    if not isinstance(services_page, dict) or not services_page:
        return
    if not agent_available or agent is None:
        return
    service_monitor = getattr(agent, 'service_monitor', None)
    if service_monitor is None or not hasattr(service_monitor, 'get_pod_status'):
        return

    for key, svc in list(services_page.items()):
        if not isinstance(svc, dict):
            continue
        ns = str(svc.get('namespace', '') or '').strip().lower()
        name = str(svc.get('name', '') or '').strip()
        if not ns or not name:
            raw = str(key or '')
            if '/' in raw:
                ns_part, name_part = raw.split('/', 1)
                ns = ns or str(ns_part or '').strip().lower()
                name = name or str(name_part or '').strip()
        if ns not in {'venus', 'jupiter'} or not name:
            continue

        try:
            live = service_monitor.get_pod_status(name, namespace=ns)
        except Exception:
            continue
        if not isinstance(live, dict):
            continue

        pods = live.get('pods', []) if isinstance(live.get('pods', []), list) else []
        total = len(pods)
        running = len([p for p in pods if str((p or {}).get('status', '') or '').lower() == 'running'])
        ready = len([p for p in pods if bool((p or {}).get('ready', False))])
        issues = len([p for p in pods if not bool((p or {}).get('ready', False))])

        metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
        metrics['pod_total_count'] = total
        metrics['pod_running_count'] = running
        metrics['pod_ready_count'] = ready
        metrics['pod_issue_count'] = issues
        svc['metrics'] = metrics
        svc['pod_status'] = live

        live_state = str(live.get('status', 'unknown') or 'unknown').lower()
        svc_state = str(svc.get('status', 'unknown') or 'unknown').lower()

        if live_state == 'healthy':
            if svc_state in {'degraded', 'down', 'pending', 'warning', 'offline'}:
                errors = svc.get('recent_errors', []) if isinstance(svc.get('recent_errors', []), list) else []
                keep = []
                for err in errors:
                    if not isinstance(err, dict):
                        continue
                    msg = str(err.get('message', '') or '').lower()
                    if any(token in msg for token in ['imagepullbackoff', 'errimagepull', 'failed to pull image', 'back-off pulling image', 'no pods found']):
                        continue
                    keep.append(err)
                svc['recent_errors'] = keep[:5]
                if not keep:
                    svc['status'] = 'healthy'
        elif live_state in {'unhealthy', 'no_pods'}:
            if svc_state == 'healthy':
                svc['status'] = 'degraded' if live_state == 'unhealthy' else 'pending'

        services_page[key] = svc


def _jenkins_params_from_actions(actions: Any) -> Dict[str, str]:
    params: Dict[str, str] = {}
    if not isinstance(actions, list):
        return params
    for action in actions:
        if not isinstance(action, dict):
            continue
        values = action.get('parameters', [])
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            name = str(item.get('name', '') or '').strip()
            if not name:
                continue
            value = item.get('value', '')
            params[name] = str(value if value is not None else '')
    return params


def _jenkins_dispatcher_builds(cfg: Dict[str, Any], limit: int = 16) -> List[Dict[str, Any]]:
    base = str(cfg.get('url', '') or '').strip()
    dispatcher = str(cfg.get('dispatcher', '') or '').strip()
    if not base or not dispatcher:
        return []

    parts = [p for p in dispatcher.split('/') if p]
    job_path = '/'.join([f"job/{urllib_parse.quote(p)}" for p in parts])
    api_url = (
        f"{base}/{job_path}/api/json?"
        f"tree=builds[number,url,building,result,timestamp,duration,estimatedDuration,"
        f"actions[parameters[name,value]]]&depth=2"
    )
    data = _jenkins_read_json(api_url, cfg, timeout=5.0)
    raw_builds = data.get('builds', []) if isinstance(data.get('builds', []), list) else []
    rows: List[Dict[str, Any]] = []

    for build in raw_builds[:max(1, int(limit))]:
        if not isinstance(build, dict):
            continue
        params = _jenkins_params_from_actions(build.get('actions', []))
        service_name = str(params.get('SERVICE_NAME', '') or '').strip()
        target_env = str(params.get('TARGET_ENV', '') or params.get('ENV', '') or params.get('environment', '') or params.get('NAMESPACE', '') or '').strip().lower()
        target_job = str(params.get('TARGET_JOB', '') or '').strip()
        pipeline_name = target_job if target_job else 'resolved-by-dispatcher'
        rows.append({
            'number': int(build.get('number', 0) or 0),
            'url': str(build.get('url', '') or '').strip(),
            'building': bool(build.get('building', False)),
            'result': str(build.get('result', '') or '').strip().lower(),
            'timestamp': int(build.get('timestamp', 0) or 0),
            'duration_ms': int(build.get('duration', 0) or 0),
            'eta_ms': int(build.get('estimatedDuration', 0) or 0),
            'service_name': service_name,
            'target_env': target_env,
            'pipeline_name': pipeline_name,
            'params': params,
        })

    rows.sort(key=lambda r: int(r.get('timestamp', 0) or 0), reverse=True)
    return rows


def _jenkins_get_build_stages(cfg: Dict[str, Any], build_url: str) -> List[Dict[str, Any]]:
    """Parse pipeline stages from Jenkins console output."""
    if not build_url:
        return []

    console_url = f"{build_url.rstrip('/')}/consoleText"
    try:
        req = urllib_request.Request(url=console_url, headers=_jenkins_headers(cfg), method='GET')
        with urllib_request.urlopen(req, timeout=5.0) as resp:
            text = resp.read().decode('utf-8', errors='replace')
    except Exception:
        return []

    stages = []
    current_stage = None
    stage_order = []

    # Patterns for detecting stages in Jenkins Pipeline console output
    # Declarative Pipeline format: [Pipeline] stage
    # Then followed by { (Stage Name)
    stage_start_pattern = re.compile(r'\[Pipeline\]\s+\{\s*\(([^)]+)\)', re.IGNORECASE)
    stage_end_pattern = re.compile(r'\[Pipeline\]\s+\}', re.IGNORECASE)
    stage_started_pattern = re.compile(r'Stage\s*["\']?([^"\']+)["\']?\s+started', re.IGNORECASE)
    stage_skipped_pattern = re.compile(r'Stage\s*["\']?([^"\']+)["\']?\s+skipped', re.IGNORECASE)
    error_pattern = re.compile(r'\[ERROR\]|\bERROR\b|Exception|FAILED', re.IGNORECASE)

    lines = text.split('\n')
    last_500 = lines[-500:] if len(lines) > 500 else lines

    for line in last_500:
        # Check for stage start
        match = stage_start_pattern.search(line)
        if match:
            stage_name = match.group(1).strip()
            if stage_name and stage_name not in stage_order:
                stage_order.append(stage_name)
                current_stage = stage_name

        # Alternative format
        match = stage_started_pattern.search(line)
        if match:
            stage_name = match.group(1).strip()
            if stage_name and stage_name not in stage_order:
                stage_order.append(stage_name)
                current_stage = stage_name

        # Check for skipped stage
        match = stage_skipped_pattern.search(line)
        if match:
            stage_name = match.group(1).strip()
            if stage_name:
                if stage_name not in stage_order:
                    stage_order.append(stage_name)
                stages.append({
                    'name': stage_name,
                    'status': 'skipped',
                    'order': stage_order.index(stage_name) if stage_name in stage_order else len(stage_order)
                })

    # Build final stage list
    seen = set()
    for stage_name in stage_order:
        if stage_name in seen:
            continue
        seen.add(stage_name)

        # Determine status - last stage is likely running, others completed
        if stage_name == current_stage:
            status = 'running'
        else:
            status = 'completed'

        # Check if any stage was marked as skipped
        for s in stages:
            if s.get('name') == stage_name:
                status = s.get('status', status)
                break

        if not any(s.get('name') == stage_name for s in stages):
            stages.append({
                'name': stage_name,
                'status': status,
                'order': stage_order.index(stage_name)
            })

    # Sort by order
    stages.sort(key=lambda s: s.get('order', 0))

    # If no stages found, return common default stages as pending
    if not stages:
        return [
            {'name': 'Checkout', 'status': 'pending', 'order': 0},
            {'name': 'Build', 'status': 'pending', 'order': 1},
            {'name': 'Test', 'status': 'pending', 'order': 2},
            {'name': 'Deploy', 'status': 'pending', 'order': 3},
        ]

    return stages


def _jenkins_live_payload() -> Dict[str, Any]:
    status = _jenkins_status_payload()
    max_workers = int(status.get('max_workers', 2) or 2)
    out: Dict[str, Any] = {
        'status': str(status.get('status', 'unknown') or 'unknown'),
        'message': str(status.get('message', '') or ''),
        'url': str(status.get('url', '') or ''),
        'dispatcher_job': str(status.get('dispatcher_job', '') or ''),
        'active_workers': int(status.get('active_workers', 0) or 0),
        'max_workers': max_workers,
        'bots': [],
        'auto_queue': [],
        'recent': [],
        'recent_failures': 0,
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }

    if out['status'] != 'connected':
        out['bots'] = [{
            'bot_id': f'bot-{i + 1}',
            'state': 'disconnected',
            'service_name': '',
            'target_env': '',
            'pipeline_name': '',
            'build_number': None,
            'build_url': '',
            'started_at': None,
            'duration_ms': 0,
        } for i in range(max_workers)]
        return out

    try:
        builds = _jenkins_dispatcher_builds(_jenkins_cfg(), limit=18)
    except Exception as e:
        out['status'] = 'error'
        out['message'] = str(e)
        out['bots'] = [{
            'bot_id': f'bot-{i + 1}',
            'state': 'disconnected',
            'service_name': '',
            'target_env': '',
            'pipeline_name': '',
            'build_number': None,
            'build_url': '',
            'started_at': None,
            'duration_ms': 0,
        } for i in range(max_workers)]
        return out

    running = [b for b in builds if bool(b.get('building', False))]
    running.sort(key=lambda r: int(r.get('timestamp', 0) or 0), reverse=True)

    def _auto_state_stages(state: str) -> List[Dict[str, Any]]:
        state_l = str(state or '').strip().lower()
        stage_defs = [
            {'name': 'Detect Issue', 'status': 'completed', 'order': 0},
            {'name': 'Queue Build', 'status': 'pending', 'order': 1},
            {'name': 'Build Pipeline', 'status': 'pending', 'order': 2},
            {'name': 'Deploy Verify', 'status': 'pending', 'order': 3},
        ]
        if state_l in {'pending'}:
            stage_defs[1]['status'] = 'running'
        elif state_l in {'queued'}:
            stage_defs[1]['status'] = 'completed'
            stage_defs[2]['status'] = 'pending'
        elif state_l in {'running_wait'}:
            stage_defs[1]['status'] = 'completed'
            stage_defs[2]['status'] = 'running'
        elif state_l in {'build_success', 'verifying'}:
            stage_defs[1]['status'] = 'completed'
            stage_defs[2]['status'] = 'completed'
            stage_defs[3]['status'] = 'running'
        elif state_l in {'deployed'}:
            for item in stage_defs:
                item['status'] = 'completed'
        elif state_l in {'failed'}:
            stage_defs[1]['status'] = 'completed'
            stage_defs[2]['status'] = 'failed'
        return stage_defs

    active_auto_entries: List[Dict[str, Any]] = []
    failed_auto_entries: List[Dict[str, Any]] = []
    with _jenkins_auto_rebuild_lock:
        for state_key, entry in _jenkins_auto_rebuild_state.items():
            if not isinstance(entry, dict):
                continue
            if not bool(entry.get('active_issue', False)):
                continue
            st = str(entry.get('status', '') or '').strip().lower()
            if st not in {'pending', 'queued', 'running_wait', 'build_success', 'verifying', 'failed'}:
                continue
            if '/' not in str(state_key):
                continue
            ns, svc = str(state_key).split('/', 1)
            row = {
                'state_key': str(state_key),
                'namespace': str(ns or '').strip().lower(),
                'service_name': str(svc or '').strip().lower(),
                'status': st,
                'build_number': int(entry.get('build_number', 0) or 0),
                'failure_reason': str(entry.get('failure_reason', '') or ''),
                'last_ts': entry.get('last_ts'),
                'active_since': entry.get('active_since'),
            }
            if st == 'failed':
                failed_auto_entries.append(row)
            else:
                active_auto_entries.append(row)

    active_auto_entries.sort(
        key=lambda r: (
            r.get('last_ts').timestamp() if isinstance(r.get('last_ts'), datetime) else 0,
            r.get('build_number', 0),
        ),
        reverse=True,
    )
    failed_auto_entries.sort(
        key=lambda r: (
            r.get('last_ts').timestamp() if isinstance(r.get('last_ts'), datetime) else 0,
            r.get('build_number', 0),
        ),
        reverse=True,
    )

    completed = [b for b in builds if not bool(b.get('building', False))]
    success_like = [
        b for b in completed
        if str(b.get('result', '') or '').strip().lower() in {'success', 'unstable'}
    ]
    failure_like = [
        b for b in completed
        if str(b.get('result', '') or '').strip().lower() in {'failure', 'failed', 'error'}
    ]
    out['recent_failures'] = len(failure_like)
    # Keep history truthful for operators: show latest completed runs regardless
    # of result (success/failure/unstable), not success-only.
    recent = (running + completed)[:10]
    if not recent:
        recent = builds[:10]

    auto_by_key = {
        f"{str(row.get('namespace', '')).lower()}/{str(row.get('service_name', '')).lower()}": row
        for row in active_auto_entries
        if str(row.get('namespace', '')).strip() and str(row.get('service_name', '')).strip()
    }

    latest_build_number_by_key: Dict[str, int] = {}
    for row in recent:
        if not isinstance(row, dict):
            continue
        key = f"{str(row.get('target_env', '') or '').strip().lower()}/{str(row.get('service_name', '') or '').strip().lower()}"
        if key == '/':
            continue
        num = int(row.get('number', 0) or 0)
        latest_build_number_by_key[key] = max(int(latest_build_number_by_key.get(key, 0) or 0), num)

    enriched_recent: List[Dict[str, Any]] = []
    for row in recent:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        key = f"{str(item.get('target_env', '') or '').strip().lower()}/{str(item.get('service_name', '') or '').strip().lower()}"
        auto = auto_by_key.get(key)
        if auto and int(item.get('number', 0) or 0) == int(latest_build_number_by_key.get(key, 0) or 0):
            item['auto_state'] = str(auto.get('status', '') or '')
            item['auto_failure_reason'] = str(auto.get('failure_reason', '') or '')
        enriched_recent.append(item)

    # Surface failed auto-dispatch entries even when no Jenkins build row exists,
    # so operator can use Run w/pipeline with exact job name.
    with _jenkins_pipeline_override_lock:
        pipeline_overrides = dict(_jenkins_pipeline_overrides)
    existing_recent_keys = {
        f"{str(item.get('target_env', '') or '').strip().lower()}/{str(item.get('service_name', '') or '').strip().lower()}"
        for item in enriched_recent if isinstance(item, dict)
    }
    for row in failed_auto_entries:
        key = f"{str(row.get('namespace', '')).lower()}/{str(row.get('service_name', '')).lower()}"
        if key in existing_recent_keys:
            continue
        last_ts = row.get('last_ts')
        ts_ms = int(last_ts.timestamp() * 1000) if isinstance(last_ts, datetime) else 0
        enriched_recent.insert(0, {
            'number': int(row.get('build_number', 0) or 0),
            'url': '',
            'building': False,
            'result': 'failed',
            'timestamp': ts_ms,
            'duration_ms': 0,
            'eta_ms': 0,
            'service_name': str(row.get('service_name', '') or ''),
            'target_env': str(row.get('namespace', '') or ''),
            'pipeline_name': str(pipeline_overrides.get(key, '') or 'resolved-by-dispatcher'),
            'params': {},
            'auto_state': 'failed',
            'auto_failure_reason': str(row.get('failure_reason', '') or ''),
        })
        existing_recent_keys.add(key)
    recent = enriched_recent

    if out['recent_failures'] > 0:
        base_msg = str(out.get('message', '') or '').strip()
        fail_msg = f"recent failed dispatcher runs={out['recent_failures']}"
        out['message'] = f"{base_msg} | {fail_msg}" if base_msg else fail_msg

    bots = []
    cfg = _jenkins_cfg()
    running_keys = set()
    for idx in range(max_workers):
        if idx < len(running):
            build = running[idx]
            build_url = str(build.get('url', '') or '')
            # Fetch pipeline stages for running builds
            stages = _jenkins_get_build_stages(cfg, build_url) if build_url else []
            run_key = f"{str(build.get('target_env', '') or '').strip().lower()}/{str(build.get('service_name', '') or '').strip().lower()}"
            if run_key != '/':
                running_keys.add(run_key)
            bots.append({
                'bot_id': f'bot-{idx + 1}',
                'state': 'running',
                'service_name': str(build.get('service_name', '') or ''),
                'target_env': str(build.get('target_env', '') or ''),
                'pipeline_name': str(build.get('pipeline_name', '') or ''),
                'build_number': int(build.get('number', 0) or 0),
                'build_url': build_url,
                'started_at': int(build.get('timestamp', 0) or 0),
                'duration_ms': int(build.get('duration_ms', 0) or 0),
                'eta_ms': int(build.get('eta_ms', 0) or 0),
                'stages': stages,
            })
        else:
            next_auto = None
            for row in active_auto_entries:
                key = f"{str(row.get('namespace', '')).lower()}/{str(row.get('service_name', '')).lower()}"
                if key in running_keys:
                    continue
                next_auto = row
                running_keys.add(key)
                break

            if isinstance(next_auto, dict):
                state_l = str(next_auto.get('status', 'queued') or 'queued').strip().lower()
                started = next_auto.get('active_since')
                if not isinstance(started, datetime):
                    started = next_auto.get('last_ts')
                bots.append({
                    'bot_id': f'bot-{idx + 1}',
                    'state': state_l,
                    'service_name': str(next_auto.get('service_name', '') or ''),
                    'target_env': str(next_auto.get('namespace', '') or ''),
                    'pipeline_name': 'dispatcher-bot',
                    'build_number': int(next_auto.get('build_number', 0) or 0) or None,
                    'build_url': '',
                    'started_at': int(started.timestamp() * 1000) if isinstance(started, datetime) else None,
                    'duration_ms': 0,
                    'eta_ms': 0,
                    'stages': _auto_state_stages(state_l),
                    'detail': str(next_auto.get('failure_reason', '') or ''),
                })
            else:
                bots.append({
                    'bot_id': f'bot-{idx + 1}',
                    'state': 'idle',
                    'service_name': '',
                    'target_env': '',
                    'pipeline_name': '',
                    'build_number': None,
                    'build_url': '',
                    'started_at': None,
                    'duration_ms': 0,
                    'stages': [],
                })

    active_auto = [r for r in active_auto_entries if str(r.get('status', '')).strip().lower() in {'pending', 'queued', 'running_wait', 'build_success', 'verifying'}]
    if active_auto:
        base_msg = str(out.get('message', '') or '').strip()
        progress_msg = f"auto-progress active={len(active_auto)}"
        out['message'] = f"{base_msg} | {progress_msg}" if base_msg else progress_msg

    out['auto_queue'] = [
        {
            'service_name': str(r.get('service_name', '') or ''),
            'target_env': str(r.get('namespace', '') or ''),
            'status': str(r.get('status', '') or ''),
            'build_number': int(r.get('build_number', 0) or 0),
        }
        for r in active_auto_entries
    ]

    out['bots'] = bots
    out['recent'] = recent
    return out


# ============================================================================
# ArgoCD Integration
# ============================================================================

_argocd_app_cache: Dict[str, Any] = {}
_argocd_cache_lock = threading.Lock()
_argocd_cache_ttl_seconds = 30

# Track services fixed by Jenkins + ArgoCD deployment
_fix_tracking_state: Dict[str, Dict[str, Any]] = {}
_fix_tracking_lock = threading.Lock()


def _load_config() -> Dict[str, Any]:
    """Return runtime config with safe fallbacks for dashboard helpers."""
    try:
        if agent_available and agent is not None:
            cfg = getattr(agent, 'config', None)
            if isinstance(cfg, dict):
                return cfg
    except Exception:
        pass

    candidates = [
        os.path.join(current_dir, 'config.json'),
        '/app/config.json',
        'config.json',
    ]
    for path in candidates:
        try:
            with open(path, 'r') as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    return loaded
        except Exception:
            continue
    return {}


def _load_repo_mappings() -> Dict[str, str]:
    """Load service->repo mappings from configuration.

    Supported keys:
    - pr_automation.repo_overrides: {"namespace/service": "repo-name"}
    - codexa.repo_mappings: {"namespace/service": "repo-name"}

    Legacy value formats are also accepted, such as:
    - {"namespace/service": {"repo": "repo-name", "branch": "..."}}
    - {"service": "{'repo': 'repo-name', 'branch': '...'}"}
    """
    cfg = _load_config()
    out: Dict[str, str] = {}

    pr_auto = cfg.get('pr_automation', {}) if isinstance(cfg.get('pr_automation', {}), dict) else {}
    pr_overrides = pr_auto.get('repo_overrides', {}) if isinstance(pr_auto.get('repo_overrides', {}), dict) else {}
    codexa_cfg = cfg.get('codexa', {}) if isinstance(cfg.get('codexa', {}), dict) else {}
    codexa_map = codexa_cfg.get('repo_mappings', {}) if isinstance(codexa_cfg.get('repo_mappings', {}), dict) else {}

    def _extract_repo_name(raw_repo: Any) -> str:
        if isinstance(raw_repo, str):
            text = raw_repo.strip()
            if not text:
                return ''
            if text.startswith('{') and text.endswith('}'):
                parsed_obj = None
                try:
                    parsed_obj = json.loads(text)
                except Exception:
                    parsed_obj = None
                if parsed_obj is None:
                    try:
                        import ast
                        parsed_obj = ast.literal_eval(text)
                    except Exception:
                        parsed_obj = None
                if isinstance(parsed_obj, dict):
                    repo = str(parsed_obj.get('repo', '') or '').strip()
                    if repo:
                        return repo
            return text
        if isinstance(raw_repo, dict):
            return str(raw_repo.get('repo', '') or '').strip()
        return str(raw_repo or '').strip()

    def _normalize(src: Dict[str, Any]):
        for raw_key, raw_repo in src.items():
            key = str(raw_key or '').strip().lower()
            repo = _extract_repo_name(raw_repo)
            if not key or not repo:
                continue
            if '/' in key:
                ns, svc = key.split('/', 1)
                ns = ns.strip().lower()
                svc = svc.strip().lower()
                if ns in {'jupiter', 'venus'} and svc:
                    out[f"{ns}/{svc}"] = repo
            else:
                svc = key.strip().lower()
                if svc:
                    out[svc] = repo

    _normalize(pr_overrides)
    _normalize(codexa_map)
    return out


def _resolve_repo_for_service(namespace: str, service_name: str, mappings: Optional[Dict[str, str]] = None) -> str:
    mapping = mappings if isinstance(mappings, dict) else _load_repo_mappings()
    ns = str(namespace or '').strip().lower()
    svc = str(service_name or '').strip().lower()
    if not svc:
        return ''
    return str(mapping.get(f"{ns}/{svc}") or mapping.get(svc) or '')


def _argocd_cfg() -> Dict[str, Any]:
    """Load ArgoCD configuration from config.json or environment."""
    cfg = _load_config().get('argocd', {})
    return {
        'enabled': bool(cfg.get('enabled', True)),
        'url': str(os.getenv('ARGOCD_URL', '') or cfg.get('url', '') or '').strip().rstrip('/'),
        'token': str(os.getenv('ARGOCD_TOKEN', '') or cfg.get('token', '') or '').strip(),
        'insecure': bool(cfg.get('insecure_skip_verify', True)),
        'app_pattern': str(cfg.get('app_name_pattern', '{service}') or '{service}'),
        'namespace_patterns': cfg.get('namespace_app_patterns', {}),
        'timeout': float(cfg.get('timeout_seconds', 10) or 10),
        'sync_interval': int(cfg.get('sync_check_interval_seconds', 15) or 15),
    }


def _argocd_app_name(service: str, namespace: str) -> str:
    """Generate ArgoCD app name from service and namespace."""
    cfg = _argocd_cfg()
    patterns = cfg.get('namespace_patterns', {})
    pattern = patterns.get(namespace, cfg.get('app_pattern', '{service}'))
    return pattern.replace('{service}', service).replace('{namespace}', namespace)


def _argocd_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Build ArgoCD API headers."""
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }
    token = cfg.get('token', '')
    if token:
        headers['Authorization'] = f"Bearer {token}"
    return headers


def _argocd_request(path: str, cfg: Dict[str, Any] = None, method: str = 'GET') -> Dict[str, Any]:
    """Make request to ArgoCD API."""
    if cfg is None:
        cfg = _argocd_cfg()

    base_url = cfg.get('url', '')
    if not base_url:
        return {'error': 'ArgoCD URL not configured'}

    url = f"{base_url}{path}"
    timeout = cfg.get('timeout', 10)

    try:
        import ssl
        import urllib.request

        ctx = ssl.create_default_context()
        if cfg.get('insecure', True):
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, headers=_argocd_headers(cfg), method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return data if isinstance(data, dict) else {'data': data}
    except Exception as e:
        return {'error': str(e)}


def _argocd_get_app_status(service: str, namespace: str) -> Dict[str, Any]:
    """Get ArgoCD application status for a service."""
    cfg = _argocd_cfg()
    if not cfg.get('enabled') or not cfg.get('url'):
        return {'error': 'ArgoCD not configured', 'enabled': False}

    app_name = _argocd_app_name(service, namespace)
    cache_key = f"{namespace}/{service}"

    # Check cache
    with _argocd_cache_lock:
        cached = _argocd_app_cache.get(cache_key)
        if cached:
            cached_ts = cached.get('ts')
            if cached_ts and (datetime.now() - cached_ts).total_seconds() < _argocd_cache_ttl_seconds:
                return cached.get('data', {})

    # Fetch from ArgoCD API
    result = _argocd_request(f"/api/v1/applications/{app_name}", cfg)

    if 'error' in result:
        return result

    # Parse response
    status = result.get('status', {})
    sync_status = status.get('sync', {}).get('status', 'Unknown')
    health_status = status.get('health', {}).get('status', 'Unknown')

    # Extract current images
    images = []
    summary = status.get('summary', {})
    if isinstance(summary.get('images'), list):
        images = summary.get('images', [])

    # Get last sync time
    operation_state = status.get('operationState', {})
    finished_at = operation_state.get('finishedAt', '')

    parsed = {
        'app_name': app_name,
        'service': service,
        'namespace': namespace,
        'sync_status': sync_status,
        'health_status': health_status,
        'images': images,
        'current_image': images[0] if images else '',
        'last_sync_at': finished_at,
        'synced': sync_status == 'Synced',
        'healthy': health_status == 'Healthy',
    }

    # Update cache
    with _argocd_cache_lock:
        _argocd_app_cache[cache_key] = {
            'ts': datetime.now(),
            'data': parsed
        }

    return parsed


def _argocd_list_apps(namespace: str = '') -> List[Dict[str, Any]]:
    """List all ArgoCD applications, optionally filtered by namespace."""
    cfg = _argocd_cfg()
    if not cfg.get('enabled') or not cfg.get('url'):
        return []

    result = _argocd_request('/api/v1/applications', cfg)
    if 'error' in result:
        return []

    apps = []
    items = result.get('items', [])
    for item in items:
        if not isinstance(item, dict):
            continue

        metadata = item.get('metadata', {})
        status = item.get('status', {})
        spec = item.get('spec', {})

        app_name = metadata.get('name', '')
        dest_ns = spec.get('destination', {}).get('namespace', '')

        if namespace and dest_ns.lower() != namespace.lower():
            continue

        sync_status = status.get('sync', {}).get('status', 'Unknown')
        health_status = status.get('health', {}).get('status', 'Unknown')
        images = status.get('summary', {}).get('images', [])

        apps.append({
            'app_name': app_name,
            'namespace': dest_ns,
            'sync_status': sync_status,
            'health_status': health_status,
            'images': images,
            'synced': sync_status == 'Synced',
            'healthy': health_status == 'Healthy',
        })

    return apps


def _mark_service_fixed(service: str, namespace: str, build_number: int, issue_sig: str):
    """Mark a service as fixed after successful Jenkins build."""
    key = f"{namespace}/{service}"
    with _fix_tracking_lock:
        _fix_tracking_state[key] = {
            'status': 'built',
            'build_number': build_number,
            'issue_sig': issue_sig,
            'built_at': datetime.now(),
            'deployed_at': None,
            'fixed_until_new_issue': False,
        }


def _mark_service_deployed(service: str, namespace: str):
    """Mark a service as deployed after ArgoCD sync."""
    key = f"{namespace}/{service}"
    with _fix_tracking_lock:
        if key in _fix_tracking_state:
            _fix_tracking_state[key]['status'] = 'deployed'
            _fix_tracking_state[key]['deployed_at'] = datetime.now()
            _fix_tracking_state[key]['fixed_until_new_issue'] = True


def _is_service_recently_fixed(service: str, namespace: str, current_sig: str) -> bool:
    """Check if service was recently fixed and shouldn't be rebuilt."""
    key = f"{namespace}/{service}"
    with _fix_tracking_lock:
        state = _fix_tracking_state.get(key)
        if not state:
            return False

        # If same issue signature and already fixed, skip rebuild
        if state.get('fixed_until_new_issue') and state.get('issue_sig') == current_sig:
            deployed_at = state.get('deployed_at')
            if deployed_at and (datetime.now() - deployed_at).total_seconds() < 1800:  # 30 min
                return True

        return False


def _get_fix_tracking_status(service: str, namespace: str) -> Dict[str, Any]:
    """Get current fix tracking status for a service."""
    key = f"{namespace}/{service}"
    with _fix_tracking_lock:
        state = _fix_tracking_state.get(key)
        if not state:
            return {'status': 'none'}
        return dict(state)


# ============================================================================
# End ArgoCD Integration
# ============================================================================


def _selected_services_scope_key(selected_services) -> str:
    rows = []
    for item in selected_services or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name', '') or '').strip()
        namespace = str(item.get('namespace', '') or '').strip()
        if not name or not namespace:
            continue
        rows.append(f"{namespace}/{name}")
    rows.sort()
    return '|'.join(rows)


def _snapshot_cache_key(effective_window_minutes: int, selected_services: list) -> str:
    raw_scope = _selected_services_scope_key(selected_services)
    scope_hash = hashlib.sha1(raw_scope.encode('utf-8')).hexdigest() if raw_scope else 'none'
    return f"full:{int(effective_window_minutes)}:{scope_hash}"


def _progressive_cache_key(effective_window_minutes: int, selected_services: list) -> str:
    raw_scope = _selected_services_scope_key(selected_services)
    scope_hash = hashlib.sha1(raw_scope.encode('utf-8')).hexdigest() if raw_scope else 'none'
    return f"progress:{int(effective_window_minutes)}:{scope_hash}"


def _progressive_cache_get(cache_key: str) -> Dict[str, Any]:
    with _progressive_build_lock:
        local = _progressive_build_state.get(cache_key)
        if isinstance(local, dict):
            local_ts = local.get('ts')
            if local_ts is not None and (datetime.now() - local_ts).total_seconds() < 900:
                payload = local.get('payload')
                if isinstance(payload, dict):
                    return dict(payload)

    external = _external_cache_get(cache_key)
    payload = external.get('data') if isinstance(external.get('data'), dict) else {}
    if payload:
        with _progressive_build_lock:
            _progressive_build_state[cache_key] = {
                'running': bool(_progressive_build_state.get(cache_key, {}).get('running', False)),
                'ts': datetime.now(),
                'payload': dict(payload)
            }
    return payload if isinstance(payload, dict) else {}


def _progressive_cache_set(cache_key: str, payload: Dict[str, Any]):
    if not isinstance(payload, dict):
        return
    with _progressive_build_lock:
        current = _progressive_build_state.get(cache_key, {})
        _progressive_build_state[cache_key] = {
            'running': bool(current.get('running', False)),
            'ts': datetime.now(),
            'payload': dict(payload)
        }
    _external_cache_set(cache_key, {
        'ts': datetime.now().isoformat(),
        'data': payload
    })


def _run_progressive_service_build(cache_key: str, effective_window_minutes: int, selected_services: list):
    try:
        frame_size = 10
        selected_rows = [svc for svc in (selected_services or []) if isinstance(svc, dict)]
        total_selected = len(selected_rows)
        total_frames = (total_selected + frame_size - 1) // frame_size if total_selected > 0 else 0
        merged_services: Dict[str, Any] = {}

        for frame_index in range(total_frames):
            frame_start = frame_index * frame_size
            frame_end = frame_start + frame_size
            frame_services = selected_rows[frame_start:frame_end]
            frame_status = {}
            if agent_available and agent is not None and hasattr(agent, 'get_service_status'):
                try:
                    frame_status = agent.get_service_status(
                        minutes=effective_window_minutes,
                        services_subset=frame_services,
                        include_deep_inspection=False
                    )
                except Exception:
                    frame_status = {}
            if isinstance(frame_status, dict) and frame_status:
                merged_services.update(frame_status)

            progressive_payload = {
                'services': dict(merged_services),
                'total_selected': total_selected,
                'completed_frames': frame_index + 1,
                'total_frames': total_frames,
                'done': (frame_index + 1) >= total_frames,
                'frame_size': frame_size,
                'generated_at': datetime.now().isoformat()
            }
            _progressive_cache_set(cache_key, progressive_payload)
    finally:
        _release_progressive_distributed_lock(cache_key)
        with _progressive_build_lock:
            entry = _progressive_build_state.get(cache_key, {})
            entry['running'] = False
            entry['ts'] = datetime.now()
            _progressive_build_state[cache_key] = entry


def _schedule_progressive_build(cache_key: str, effective_window_minutes: int, selected_services: list):
    with _progressive_build_lock:
        state = _progressive_build_state.get(cache_key, {})
        if bool(state.get('running', False)):
            return
        _progressive_build_state[cache_key] = {
            'running': True,
            'ts': datetime.now(),
            'payload': state.get('payload', {}) if isinstance(state.get('payload', {}), dict) else {}
        }

    if not _acquire_progressive_distributed_lock(cache_key):
        with _progressive_build_lock:
            state = _progressive_build_state.get(cache_key, {})
            state['running'] = False
            state['ts'] = datetime.now()
            _progressive_build_state[cache_key] = state
        return

    if _queue_progressive_build_task(cache_key, effective_window_minutes, selected_services):
        return

    threading.Thread(
        target=_run_progressive_service_build,
        args=(cache_key, int(effective_window_minutes), list(selected_services or [])),
        daemon=True
    ).start()


def _celery_enabled() -> bool:
    if Celery is None:
        return False
    if redis_lib is None:
        return False
    return bool(os.getenv('CELERY_BROKER_URL') or os.getenv('REDIS_URL'))


def _get_celery_app():
    global _celery_app
    if _celery_app is not None:
        return _celery_app
    if not _celery_enabled():
        return None
    broker_url = os.getenv('CELERY_BROKER_URL') or os.getenv('REDIS_URL')
    if not broker_url:
        return None
    try:
        app = Celery('ai_monitoring_agent', broker=broker_url, backend=broker_url)
        app.conf.update(
            task_serializer='json',
            result_serializer='json',
            accept_content=['json'],
            task_ignore_result=True,
            worker_prefetch_multiplier=1,
            task_acks_late=False,
            task_default_queue='ai-monitoring-agent'
        )
        _celery_app = app
    except Exception as e:
        logger.warning(f"Celery initialization failed, fallback to thread worker: {e}")
        _celery_app = None
    return _celery_app


def _queue_progressive_build_task(cache_key: str, effective_window_minutes: int, selected_services: list) -> bool:
    celery_app = _get_celery_app()
    if celery_app is None:
        return False
    try:
        celery_app.send_task(
            'web_dashboard.progressive_build',
            args=[cache_key, int(effective_window_minutes), list(selected_services or [])],
            queue='ai-monitoring-agent'
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to enqueue Celery progressive build task, using thread fallback: {e}")
        return False


if _get_celery_app() is not None:
    @_get_celery_app().task(name='web_dashboard.progressive_build')
    def _celery_progressive_build(cache_key: str, effective_window_minutes: int, selected_services: list):
        _run_progressive_service_build(cache_key, effective_window_minutes, selected_services)


def _redis_cache_enabled() -> bool:
    if redis_lib is None:
        return False
    if not agent_available or agent is None:
        return False
    monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
    redis_cfg = monitoring_cfg.get('redis_cache', {}) if isinstance(monitoring_cfg.get('redis_cache', {}), dict) else {}
    return bool(redis_cfg.get('enabled', False))


def _get_external_cache_client():
    global _external_cache_client
    if not _redis_cache_enabled():
        return None

    with _external_cache_lock:
        if _external_cache_client is not None:
            return _external_cache_client

        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
        redis_cfg = monitoring_cfg.get('redis_cache', {}) if isinstance(monitoring_cfg.get('redis_cache', {}), dict) else {}
        redis_url = os.getenv('REDIS_URL', redis_cfg.get('url', 'redis://localhost:6379/0'))
        connect_timeout = float(redis_cfg.get('connect_timeout_seconds', 1.0) or 1.0)
        socket_timeout = float(redis_cfg.get('socket_timeout_seconds', 1.5) or 1.5)

        try:
            client = redis_lib.Redis.from_url(
                redis_url,
                socket_connect_timeout=max(0.2, connect_timeout),
                socket_timeout=max(0.2, socket_timeout),
                decode_responses=True
            )
            client.ping()
            _external_cache_client = client
        except Exception as e:
            logger.warning(f"Redis cache unavailable, continuing with in-process cache only: {e}")
            _external_cache_client = None

        return _external_cache_client


def _get_snapshot_cache_client():
    global _snapshot_cache_client
    if redis_lib is None:
        return None
    with _external_cache_lock:
        if _snapshot_cache_client is not None:
            return _snapshot_cache_client

        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        try:
            client = redis_lib.Redis.from_url(
                redis_url,
                socket_connect_timeout=1.0,
                socket_timeout=1.5,
                decode_responses=False
            )
            client.ping()
            _snapshot_cache_client = client
        except Exception:
            _snapshot_cache_client = None
        return _snapshot_cache_client


def _snapshot_decode_blob(blob: Any) -> Dict[str, Any]:
    if not blob:
        return {}
    try:
        raw_blob = blob if isinstance(blob, (bytes, bytearray)) else str(blob).encode('utf-8')
        try:
            raw = gzip.decompress(raw_blob)
        except Exception:
            # backward compatibility with base64(gzip(json)) payloads
            compressed = base64.b64decode(raw_blob)
            raw = gzip.decompress(compressed)
        payload = json.loads(raw.decode('utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_namespace_snapshot_blob(namespace: str, stale: bool = False) -> Any:
    ns = str(namespace or '').strip().lower()
    if ns not in {'venus', 'jupiter', 'all'}:
        ns = 'venus'
    client = _get_snapshot_cache_client()
    if client is None:
        return b''
    key = f"snapshot:{ns}:{'stale' if stale else 'latest'}"
    try:
        return client.get(key)
    except Exception:
        return b''


def _read_namespace_snapshot(namespace: str, stale: bool = False) -> Dict[str, Any]:
    return _snapshot_decode_blob(_read_namespace_snapshot_blob(namespace, stale=stale))


def _build_status_distribution(services_map: Dict[str, Any]) -> Dict[str, Any]:
    status_keys = ['healthy', 'warning', 'degraded', 'down', 'pending', 'unknown', 'scaled_down', 'offline']
    counts = {key: 0 for key in status_keys}
    for _, svc in services_map.items():
        if not isinstance(svc, dict):
            continue
        raw_status = str(svc.get('status', 'unknown') or 'unknown').strip().lower()
        mapped = raw_status if raw_status in counts else 'unknown'
        counts[mapped] = int(counts.get(mapped, 0) or 0) + 1
    total = int(sum(counts.values()) or 0)
    percentages = {
        key: (round((float(val) / float(total) * 100.0), 2) if total > 0 else 0.0)
        for key, val in counts.items()
    }
    return {'counts': counts, 'percentages': percentages, 'total': total}


def _snapshot_safe_empty(page_size: int = 20) -> Dict[str, Any]:
    return {
        'services': {},
        'stale': True,
        'message': 'Collecting live data, retry in 5s',
        '_cache': 'empty',
        '_data_age_seconds': 0.0,
        'summary': {'all_services': 0, 'down_degraded': 0, 'unresolved_alerts': 0},
        'status_distribution': _build_status_distribution({}),
        'pagination': {
            'page': 1,
            'page_size': int(page_size or 20),
            'total': 0,
            'total_pages': 1,
            'has_next': False,
            'has_prev': False,
        }
    }


def _snapshot_filter_paginate(
    payload: Dict[str, Any],
    namespace_filter: str,
    workload_filter: str,
    status_filter: str,
    search_filter: str,
    page: int,
    page_size: int,
    stale: bool,
    message: str = ''
) -> Dict[str, Any]:
    services_raw = payload.get('services', {}) if isinstance(payload.get('services', {}), dict) else {}
    allowed_workload_kinds = {'deployment', 'statefulset', 'daemonset'}
    filtered_map: Dict[str, Dict[str, Any]] = {}

    def _status_rank(value: str) -> int:
        order = {
            'down': 6,
            'offline': 6,
            'degraded': 5,
            'pending': 4,
            'warning': 3,
            'unknown': 2,
            'scaled_down': 1,
            'healthy': 0,
        }
        return int(order.get(str(value or '').strip().lower(), 2))

    for key, svc in services_raw.items():
        if not isinstance(svc, dict):
            continue
        name = str(svc.get('name', '') or '').strip()
        ns = str(svc.get('namespace', '') or '').strip().lower()
        metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
        workload_kind = str(metrics.get('workload_kind', '') or '').strip().lower()
        workload_name = str(metrics.get('workload_name', '') or '').strip()

        if not name and '/' in str(key):
            _, name = str(key).split('/', 1)
        if not ns and '/' in str(key):
            ns, _ = str(key).split('/', 1)
            ns = str(ns or '').strip().lower()

        if workload_kind in allowed_workload_kinds and workload_name:
            name = workload_name

        if not name or ns not in {'venus', 'jupiter'}:
            continue

        if workload_filter in allowed_workload_kinds:
            if workload_kind != workload_filter:
                continue
        else:
            if workload_kind not in allowed_workload_kinds:
                continue

        st = str(svc.get('status', 'unknown') or 'unknown').strip().lower()
        if namespace_filter in {'venus', 'jupiter'} and ns != namespace_filter:
            continue
        if status_filter and st != status_filter:
            continue
        if search_filter and search_filter not in name.lower() and search_filter not in str(key).lower():
            continue

        row_key = f"{ns}/{name}"
        fixed = dict(svc)
        fixed['name'] = name
        fixed['namespace'] = ns

        existing = filtered_map.get(row_key)
        if not isinstance(existing, dict):
            filtered_map[row_key] = fixed
            continue

        existing_status = str(existing.get('status', 'unknown') or 'unknown')
        incoming_status = str(fixed.get('status', 'unknown') or 'unknown')
        if _status_rank(incoming_status) > _status_rank(existing_status):
            filtered_map[row_key] = fixed
            continue

        existing_errors = existing.get('recent_errors', []) if isinstance(existing.get('recent_errors', []), list) else []
        incoming_errors = fixed.get('recent_errors', []) if isinstance(fixed.get('recent_errors', []), list) else []
        existing['recent_errors'] = (existing_errors + incoming_errors)[:5]
        filtered_map[row_key] = existing

    filtered_rows = sorted(filtered_map.items(), key=lambda item: (str(item[1].get('namespace', '')), str(item[1].get('name', ''))))
    total = len(filtered_rows)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 1
    current_page = max(1, min(page, total_pages))
    start = (current_page - 1) * page_size
    end = start + page_size
    page_items = filtered_rows[start:end]
    page_services = {k: v for k, v in page_items}

    # Post-process: Ensure all services have structured issue/reason/root_cause fields
    # AND fetch actual errors from container logs for pod issues
    POD_ISSUES_NEEDING_LOGS = {'crashloopbackoff', 'oomkilled', 'error', 'failed', 'backoff'}

    for svc_key, svc in page_services.items():
        if not isinstance(svc, dict):
            continue
        errors = svc.get('recent_errors', [])
        if not isinstance(errors, list):
            continue

        # Get pod info for potential log fetching
        pod_status = svc.get('pod_status', {}) if isinstance(svc.get('pod_status', {}), dict) else {}
        pod_entries = pod_status.get('pods', []) if isinstance(pod_status.get('pods', []), list) else []
        namespace = str(svc.get('namespace', '') or '').strip()

        for err in errors:
            if not isinstance(err, dict):
                continue

            msg = str(err.get('message', '') or err.get('root_cause', '') or '')
            if not msg:
                continue

            # Parse structured fields from message
            parsed = _parse_error_to_structured(msg, pod_status, str(svc.get('status', '')))
            if not err.get('issue'):
                err['issue'] = parsed.get('issue', '')
            if not err.get('reason'):
                err['reason'] = parsed.get('reason', '')

            # Check if this is a pod-level issue that needs actual log fetching
            issue_lower = str(err.get('issue', '') or '').lower()
            msg_lower = msg.lower()
            needs_log_fetch = any(marker in issue_lower or marker in msg_lower for marker in POD_ISSUES_NEEDING_LOGS)

            # For pod issues, fetch actual error from container logs
            if needs_log_fetch and namespace and pod_entries:
                actual_error = ''
                # Try each pod to get the error
                for pod in pod_entries[:3]:  # Limit to first 3 pods
                    if not isinstance(pod, dict):
                        continue
                    pod_name = str(pod.get('name', '') or '').strip()
                    if not pod_name:
                        continue
                    # Only fetch logs for pods with issues
                    pod_ready = pod.get('ready', False)
                    pod_reason = str(pod.get('reason', '') or '').lower()
                    if pod_ready and not pod_reason:
                        continue
                    actual_error = _fetch_pod_actual_error(namespace, pod_name)
                    if actual_error:
                        break

                if actual_error and not _is_dashboard_noise(actual_error):
                    err['root_cause'] = actual_error
                elif not err.get('root_cause'):
                    err['root_cause'] = parsed.get('root_cause', '') or msg
            elif not err.get('root_cause'):
                err['root_cause'] = parsed.get('root_cause', '') or msg

    summary_payload = payload.get('summary', {}) if isinstance(payload.get('summary', {}), dict) else {}

    out = {
        'services': page_services,
        'stale': bool(stale),
        'message': str(message or ''),
        'summary': summary_payload if summary_payload else {
            'all_services': total,
            'down_degraded': 0,
            'unresolved_alerts': 0,
        },
        'status_distribution': _build_status_distribution({k: v for k, v in filtered_rows}),
        'pagination': {
            'page': current_page,
            'page_size': page_size,
            'total': total,
            'total_pages': total_pages,
            'has_prev': current_page > 1,
            'has_next': current_page < total_pages,
        }
    }
    generated_at = str(payload.get('generated_at', '') or '')
    if generated_at:
        out['evidence_generated_at'] = generated_at
    return out


def _snapshot_service_status_payload_from_request(allow_stale: bool = True) -> Dict[str, Any]:
    page = request.args.get('page', default=1, type=int)
    page_size = request.args.get('page_size', default=20, type=int)
    if not page or page < 1:
        page = 1
    if not page_size or page_size < 1:
        page_size = 20
    page_size = min(page_size, 100)

    namespace_filter = request.args.get('namespace', 'venus').strip().lower()
    if namespace_filter not in {'jupiter', 'venus', 'all'}:
        namespace_filter = 'venus'
    workload_filter = str(request.args.get('workload_kind', 'deployment') or '').strip().lower()
    if workload_filter not in {'deployment', 'statefulset', 'daemonset', 'all'}:
        workload_filter = 'deployment'
    status_filter = request.args.get('status', '').strip().lower()
    search_filter = request.args.get('search', '').strip().lower()

    latest = _read_namespace_snapshot(namespace_filter, stale=False)
    age_seconds = 0.0
    stale_message = ''
    if isinstance(latest, dict) and latest.get('services'):
        age_seconds = max(0.0, round(time.time() - float(latest.get('collected_at', 0) or 0), 1))
        payload = _snapshot_filter_paginate(
            latest,
            namespace_filter=namespace_filter,
            workload_filter=workload_filter,
            status_filter=status_filter,
            search_filter=search_filter,
            page=page,
            page_size=page_size,
            stale=bool(age_seconds > 15),
            message=''
        )
        payload['_cache'] = 'live_build'
        payload['_data_age_seconds'] = age_seconds
        return payload

    if allow_stale:
        stale_payload = _read_namespace_snapshot(namespace_filter, stale=True)
        if isinstance(stale_payload, dict) and stale_payload.get('services'):
            age_seconds = max(0.0, round(time.time() - float(stale_payload.get('collected_at', 0) or 0), 1))
            stale_message = 'Using stale snapshot'
            payload = _snapshot_filter_paginate(
                stale_payload,
                namespace_filter=namespace_filter,
                workload_filter=workload_filter,
                status_filter=status_filter,
                search_filter=search_filter,
                page=page,
                page_size=page_size,
                stale=True,
                message=stale_message
            )
            payload['_cache'] = 'stale'
            payload['_data_age_seconds'] = age_seconds
            return payload

    return _snapshot_safe_empty(page_size=page_size)


def _external_cache_get(cache_key: str) -> Dict[str, Any]:
    client = _get_external_cache_client()
    if client is None:
        return {}
    try:
        payload = client.get(f"ai-agent:{cache_key}")
        if not payload:
            return {}
        loaded = json.loads(payload)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _external_cache_set(cache_key: str, data: Dict[str, Any]):
    client = _get_external_cache_client()
    if client is None:
        return
    if not isinstance(data, dict):
        return
    try:
        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
        redis_cfg = monitoring_cfg.get('redis_cache', {}) if isinstance(monitoring_cfg.get('redis_cache', {}), dict) else {}
        ttl_seconds = int(redis_cfg.get('ttl_seconds', 120) or 120)
        ttl_seconds = max(15, min(ttl_seconds, 600))
        client.setex(f"ai-agent:{cache_key}", ttl_seconds, json.dumps(data, separators=(',', ':')))
    except Exception:
        return


def _acquire_progressive_distributed_lock(cache_key: str) -> bool:
    client = _get_external_cache_client()
    if client is None:
        return True
    lock_key = f"ai-agent:progress-lock:{cache_key}"
    try:
        return bool(client.set(lock_key, '1', nx=True, ex=180))
    except Exception:
        return True


def _release_progressive_distributed_lock(cache_key: str):
    client = _get_external_cache_client()
    if client is None:
        return
    lock_key = f"ai-agent:progress-lock:{cache_key}"
    try:
        client.delete(lock_key)
    except Exception:
        return


def _get_recent_nonempty_full_snapshot(max_age_seconds: int = 300) -> Dict[str, Any]:
    """Return recent non-empty full snapshot from in-process cache."""
    with _full_service_status_lock:
        cached_ts = _full_service_status_cache.get('ts')
        cached_data = _full_service_status_cache.get('data')

    if not isinstance(cached_data, dict) or not cached_data:
        return {}
    if cached_ts is None:
        return {}
    if (datetime.now() - cached_ts).total_seconds() > max(30, min(int(max_age_seconds or 300), 1800)):
        return {}
    return cached_data


def _refresh_full_status_snapshot(cache_key: str, effective_window_minutes: int, selected_services: list):
    try:
        if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
            data = {}
        else:
            data = agent.get_service_status(
                minutes=effective_window_minutes,
                services_subset=selected_services,
                include_deep_inspection=False
            )
            if not isinstance(data, dict):
                data = {}
    except Exception:
        data = {}

    with _full_service_status_lock:
        existing_key = _full_service_status_cache.get('key')
        existing_data = _full_service_status_cache.get('data') if isinstance(_full_service_status_cache.get('data'), dict) else {}
        if not data and existing_key == cache_key and existing_data:
            # Keep last known-good snapshot when refresh returns transient empty payload.
            data = existing_data

        _full_service_status_cache['key'] = cache_key
        _full_service_status_cache['ts'] = datetime.now()
        _full_service_status_cache['data'] = data
        _full_service_status_cache['refreshing'] = False
        _full_service_status_cache['refresh_key'] = None

    if data:
        _external_cache_set(cache_key, {
            'ts': datetime.now().isoformat(),
            'data': data
        })


def _schedule_full_status_refresh(cache_key: str, effective_window_minutes: int, selected_services: list):
    with _full_service_status_lock:
        if (
            _full_service_status_cache.get('refreshing') and
            _full_service_status_cache.get('refresh_key') == cache_key
        ):
            return
        _full_service_status_cache['refreshing'] = True
        _full_service_status_cache['refresh_key'] = cache_key

    worker = threading.Thread(
        target=_refresh_full_status_snapshot,
        args=(cache_key, int(effective_window_minutes), list(selected_services or [])),
        daemon=True
    )
    worker.start()


def _start_snapshot_warmer_if_needed():
    global _snapshot_warmer_started
    if _snapshot_warmer_started:
        return
    if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
        return

    monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
    enabled = bool(monitoring_cfg.get('background_snapshot_enabled', True))
    if not enabled:
        return

    interval_seconds = int(monitoring_cfg.get('background_snapshot_interval_seconds', 30) or 30)
    interval_seconds = max(10, min(interval_seconds, 300))

    def _worker_loop():
        while True:
            try:
                selected = agent._selected_services() if hasattr(agent, '_selected_services') else []
                requested_window = int(monitoring_cfg.get('background_snapshot_window_minutes', 30) or 30)
                threshold = int(monitoring_cfg.get('service_status_window_cap_threshold', 100) or 100)
                cap_minutes = int(monitoring_cfg.get('service_status_window_cap_minutes', 360) or 360)
                effective_window = min(requested_window, cap_minutes) if len(selected) >= threshold else requested_window
                cache_key = _snapshot_cache_key(effective_window, selected)
                _schedule_full_status_refresh(cache_key, effective_window, selected)
            except Exception:
                pass
            time.sleep(interval_seconds)

    _snapshot_warmer_started = True
    threading.Thread(target=_worker_loop, daemon=True).start()


def _update_troubleshoot_job(job_id: str, **updates):
    with _troubleshoot_jobs_lock:
        job = _troubleshoot_jobs.get(job_id)
        if not isinstance(job, dict):
            return
        job.update(updates)


def _run_troubleshoot_job(job_id: str, service: str, namespace: str, time_scope: Dict = None):
    if not agent_available or agent is None:
        _update_troubleshoot_job(job_id, status='failed', progress=100, error='Agent unavailable')
        return
    service_monitor = getattr(agent, 'service_monitor', None)
    if service_monitor is None or not hasattr(service_monitor, 'run_troubleshooting'):
        _update_troubleshoot_job(job_id, status='failed', progress=100, error='Service monitor unavailable')
        return

    # Parse time scope for ES queries
    time_scope = time_scope or {}
    window_minutes = int(time_scope.get('window_minutes', 30) or 30)
    start_time_iso = time_scope.get('start_time')
    end_time_iso = time_scope.get('end_time')

    # Calculate time range for ES queries
    if start_time_iso and end_time_iso:
        try:
            start_dt = datetime.fromisoformat(start_time_iso.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time_iso.replace('Z', '+00:00'))
            # Handle timezone-aware datetimes
            if start_dt.tzinfo is not None:
                start_dt = start_dt.astimezone().replace(tzinfo=None)
            if end_dt.tzinfo is not None:
                end_dt = end_dt.astimezone().replace(tzinfo=None)
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
        except Exception:
            end_ms = int(time.time() * 1000)
            start_ms = int((time.time() - window_minutes * 60) * 1000)
    else:
        end_ms = int(time.time() * 1000)
        start_ms = int((time.time() - window_minutes * 60) * 1000)

    try:
        def _progress(stage: str, percent: int, detail: str):
            with _troubleshoot_jobs_lock:
                job = _troubleshoot_jobs.get(job_id)
                if not isinstance(job, dict):
                    return
                steps = job.get('steps', []) if isinstance(job.get('steps', []), list) else []
                steps.append({
                    'ts': datetime.now().isoformat(),
                    'stage': str(stage or ''),
                    'percent': int(percent or 0),
                    'detail': str(detail or '')
                })
                job['steps'] = steps[-100:]
                job['progress'] = max(0, min(100, int(percent or 0)))

        _update_troubleshoot_job(job_id, status='running', progress=5)
        result = service_monitor.run_troubleshooting(service, namespace=namespace, progress_callback=_progress)

        # Enrich with Elasticsearch evidence using time-scoped queries
        es_client = getattr(agent, 'elasticsearch', None)
        is_actionable = getattr(agent, '_is_actionable_error_message', None)
        sanitize = getattr(agent, '_sanitize_root_cause_message', None)
        if es_client is not None and callable(is_actionable):
            try:
                # Use time-scoped start/end times calculated from time_scope
                es_logs = es_client.get_logs(
                    service=service,
                    start_time=start_ms,
                    end_time=end_ms,
                    limit=500,  # Increased limit for more evidence
                    namespace=namespace
                )
                es_candidates = []
                for row in es_logs if isinstance(es_logs, list) else []:
                    if not isinstance(row, dict):
                        continue
                    msg = str(
                        row.get('message') or
                        row.get('body') or
                        row.get('log') or
                        row.get('msg') or
                        ''
                    ).strip()
                    if not msg:
                        continue
                    if callable(sanitize):
                        try:
                            msg = str(sanitize(msg) or '').strip()
                        except Exception:
                            pass
                    if not msg:
                        continue
                    try:
                        if not bool(is_actionable(msg)):
                            continue
                    except Exception:
                        continue
                    compact = re.sub(r'\s+', ' ', msg).strip()
                    if compact:
                        es_candidates.append(compact[:500] + ('...' if len(compact) > 500 else ''))

                # Deduplicate preserving order
                dedup = []
                seen = set()
                for item in es_candidates:
                    key = item.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    dedup.append(item)

                if dedup:
                    result['es_error_lines'] = dedup[:20]
                    root_causes = result.get('root_causes', []) if isinstance(result.get('root_causes', []), list) else []
                    for item in reversed(dedup[:5]):
                        if item not in root_causes:
                            root_causes.insert(0, item)
                    result['root_causes'] = root_causes[:30]
            except Exception:
                pass

        # Build exact RCA using full troubleshooting evidence.
        try:
            pods = result.get('pods', []) if isinstance(result.get('pods', []), list) else []
            logs_payload = []
            events_payload = []
            describe_payload = []
            for pod in pods:
                if not isinstance(pod, dict):
                    continue
                pod_name = str(pod.get('name', '') or '')
                for entry in (pod.get('log_excerpt', []) or []):
                    text = str(entry or '').strip()
                    if text:
                        logs_payload.append({'pod': pod_name, 'message': text})
                for entry in (pod.get('error_lines', []) or []):
                    text = str(entry or '').strip()
                    if text:
                        logs_payload.append({'pod': pod_name, 'message': text})
                for key in ('log_evidence_line', 'log_previous', 'log_current'):
                    text = str(pod.get(key, '') or '').strip()
                    if text:
                        logs_payload.append({'pod': pod_name, 'message': text})
                event_text = str(pod.get('event_summary', '') or '').strip()
                if event_text:
                    events_payload.append({'pod': pod_name, 'message': event_text})
                desc_text = str(pod.get('describe_excerpt', '') or '').strip()
                if desc_text:
                    describe_payload.append({'pod': pod_name, 'message': desc_text})

            if isinstance(result.get('es_error_lines', []), list):
                for line in result.get('es_error_lines', [])[:200]:
                    text = str(line or '').strip()
                    if text:
                        logs_payload.append({'pod': '', 'message': text})

            rca_metrics = {
                'status': result.get('status', ''),
                'pod_counts': result.get('pod_counts', {}),
            }
            exact_rca = asyncio.run(
                get_exact_rca(
                    service_name=service,
                    namespace=namespace,
                    logs=logs_payload,
                    events=events_payload,
                    describe=describe_payload,
                    metrics=rca_metrics,
                    force_llm=True,
                )
            )
            if isinstance(exact_rca, dict) and exact_rca.get('exact_issue'):
                result['exact_rca'] = exact_rca
                root_causes = result.get('root_causes', []) if isinstance(result.get('root_causes', []), list) else []
                exact_issue = str(exact_rca.get('exact_issue', '') or '').strip()
                if exact_issue and exact_issue not in root_causes:
                    root_causes.insert(0, exact_issue)
                result['root_causes'] = root_causes[:30]
                active_model = str(_load_ai_llm_selection().get('model', _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL) or (_ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL)).strip().lower()
                ns_patterns = get_top_patterns(namespace=namespace, limit=20, llm_model=active_model)
                similar = []
                sig = str(exact_rca.get('error_signature', '') or '')
                for row in ns_patterns:
                    if not isinstance(row, dict):
                        continue
                    if sig and str(row.get('error_signature', '') or '').strip() != sig:
                        continue
                    similar.append({
                        'id': row.get('id', ''),
                        'exact_issue': row.get('exact_issue', ''),
                        'root_cause': row.get('root_cause', ''),
                        'fix_command': row.get('fix_command', ''),
                        'confidence_score': row.get('confidence_score', 0.0),
                        'occurrence_count': row.get('occurrence_count', 0),
                        'resolved_in_minutes': row.get('resolved_in_minutes', 0),
                    })
                    if len(similar) >= 3:
                        break
                result['similar_past_issues'] = similar
        except Exception:
            pass

        _update_troubleshoot_job(
            job_id,
            status='completed',
            progress=100,
            result=result,
            finished_at=datetime.now().isoformat()
        )
    except Exception as e:
        _update_troubleshoot_job(
            job_id,
            status='failed',
            progress=100,
            error=str(e),
            finished_at=datetime.now().isoformat()
        )


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
        status_for_window = agent.get_service_status(
            minutes=window_minutes,
            services_subset=services_subset,
            include_deep_inspection=False
        )
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

# ============================================================================
# Authentication Routes
# ============================================================================

@app.route('/login')
@app.route('/ai-agent/login')
def login_page():
    """Login page"""
    if session.get('user_id'):
        return redirect(url_for('ai_agent_redirect'))
    return render_template('login.html')

@app.route('/login', methods=['POST'])
@app.route('/ai-agent/login', methods=['POST'])
def login_submit():
    """Handle login form submission"""
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')

    if not username or not password:
        return render_template('login.html', error='Please enter username and password')

    user = _authenticate_user(username, password)
    if not user:
        return render_template('login.html', error='Invalid username or password')

    session.permanent = True
    session['user_id'] = user.get('id')
    session['username'] = user.get('username')
    session['role'] = user.get('role')
    session['name'] = user.get('name', user.get('username'))

    return redirect(url_for('ai_agent_redirect'))

@app.route('/logout')
@app.route('/ai-agent/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    return redirect(url_for('login_page'))

@app.route('/admin')
@app.route('/ai-agent/admin')
@admin_required
def admin_panel():
    """Admin panel for user management"""
    users_data = _load_users()
    current_user = _get_user_by_id(session.get('user_id', ''))
    return render_template('admin.html',
                         users=users_data.get('users', []),
                         settings=users_data.get('settings', {}),
                         current_user=current_user)

@app.route('/api/admin/users', methods=['GET'])
@app.route('/ai-agent/api/admin/users', methods=['GET'])
@admin_required
def api_admin_get_users():
    """Get all users"""
    users_data = _load_users()
    # Remove passwords from response
    users = []
    for user in users_data.get('users', []):
        user_copy = dict(user)
        user_copy.pop('password', None)
        users.append(user_copy)
    return jsonify({'users': users, 'settings': users_data.get('settings', {})})

@app.route('/api/admin/users', methods=['POST'])
@app.route('/ai-agent/api/admin/users', methods=['POST'])
@admin_required
def api_admin_create_user():
    """Create new user"""
    data = request.get_json() or {}
    username = str(data.get('username', '')).strip().lower()
    password = str(data.get('password', '')).strip()
    name = str(data.get('name', '')).strip() or username
    role = str(data.get('role', 'viewer')).strip()

    if not username or len(username) < 3:
        return jsonify({'error': 'Username must be at least 3 characters'}), 400
    if not password or len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    if role not in ['admin', 'operator', 'viewer']:
        role = 'viewer'

    # Check if username exists
    if _get_user_by_username(username):
        return jsonify({'error': 'Username already exists'}), 400

    users_data = _load_users()
    new_user = {
        'id': str(uuid4()),
        'username': username,
        'password': _hash_password(password),
        'role': role,
        'name': name,
        'created_at': datetime.now().isoformat(),
        'active': True
    }
    users_data['users'].append(new_user)

    if _save_users(users_data):
        new_user_copy = dict(new_user)
        new_user_copy.pop('password', None)
        return jsonify({'success': True, 'user': new_user_copy})
    return jsonify({'error': 'Failed to save user'}), 500

@app.route('/api/admin/users/<user_id>', methods=['PUT'])
@app.route('/ai-agent/api/admin/users/<user_id>', methods=['PUT'])
@admin_required
def api_admin_update_user(user_id: str):
    """Update user"""
    data = request.get_json() or {}
    users_data = _load_users()

    user_index = None
    for i, user in enumerate(users_data.get('users', [])):
        if user.get('id') == user_id:
            user_index = i
            break

    if user_index is None:
        return jsonify({'error': 'User not found'}), 404

    user = users_data['users'][user_index]

    protected_admin = bool(user.get('protected', False)) and str(user.get('role', '')).lower() == 'admin'

    # Update fields
    if 'name' in data:
        user['name'] = str(data['name']).strip()
    if 'role' in data and data['role'] in ['admin', 'operator', 'viewer']:
        if protected_admin and str(data['role']).lower() != 'admin':
            return jsonify({'error': 'Protected admin role cannot be changed'}), 400
        user['role'] = str(data['role']).lower()
    if 'active' in data:
        next_active = bool(data['active'])
        if protected_admin and not next_active:
            return jsonify({'error': 'Protected admin cannot be deactivated'}), 400
        user['active'] = next_active
    if 'password' in data and len(str(data['password'])) >= 6:
        user['password'] = _hash_password(str(data['password']))

    # Ensure there is always at least one admin.
    prospective_users = list(users_data.get('users', []))
    prospective_users[user_index] = user
    if _count_admin_users({'users': prospective_users}) < 1:
        return jsonify({'error': 'At least one admin user is required'}), 400

    users_data['users'][user_index] = user

    if _save_users(users_data):
        user_copy = dict(user)
        user_copy.pop('password', None)
        return jsonify({'success': True, 'user': user_copy})
    return jsonify({'error': 'Failed to save user'}), 500

@app.route('/api/admin/users/<user_id>', methods=['DELETE'])
@app.route('/ai-agent/api/admin/users/<user_id>', methods=['DELETE'])
@admin_required
def api_admin_delete_user(user_id: str):
    """Delete user"""
    # Prevent deleting own account
    if session.get('user_id') == user_id:
        return jsonify({'error': 'Cannot delete your own account'}), 400

    users_data = _load_users()
    target_user = None
    for user in users_data.get('users', []):
        if user.get('id') == user_id:
            target_user = user
            break

    if not target_user:
        return jsonify({'error': 'User not found'}), 404

    if str(target_user.get('role', '')).lower() == 'admin':
        return jsonify({'error': 'Admin users cannot be deleted'}), 400

    users_data['users'] = [u for u in users_data.get('users', []) if u.get('id') != user_id]

    if _save_users(users_data):
        return jsonify({'success': True})
    return jsonify({'error': 'Failed to delete user'}), 500

@app.route('/api/slack/status')
@app.route('/ai-agent/api/slack/status')
@login_required
def api_slack_status():
    """Return Slack integration status for the Settings UI."""
    bot_token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "")
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if bot_token and channel:
        mode = "bot_token"
        configured = True
        display_channel = channel
        display_token = "xoxb-****" + bot_token[-6:] if len(bot_token) > 6 else "****"
    elif webhook:
        mode = "webhook"
        configured = True
        display_channel = channel or "#alerts"
        display_token = ""
    else:
        mode = "none"
        configured = False
        display_channel = ""
        display_token = ""
    return jsonify({
        "configured": configured,
        "mode": mode,
        "channel": display_channel,
        "token_hint": display_token,
        "active": _slack_notifier is not None,
    })


@app.route('/api/admin/settings', methods=['PUT'])
@app.route('/ai-agent/api/admin/settings', methods=['PUT'])
@admin_required
def api_admin_update_settings():
    """Update admin settings"""
    data = request.get_json() or {}
    users_data = _load_users()

    if 'auth_enabled' in data:
        users_data['settings']['auth_enabled'] = bool(data['auth_enabled'])
    if 'session_timeout_hours' in data:
        users_data['settings']['session_timeout_hours'] = max(1, min(168, int(data['session_timeout_hours'])))

    if _save_users(users_data):
        return jsonify({'success': True, 'settings': users_data['settings']})
    return jsonify({'error': 'Failed to save settings'}), 500

@app.route('/api/auth/status')
@app.route('/ai-agent/api/auth/status')
def api_auth_status():
    """Get current auth status"""
    if not session.get('user_id'):
        return jsonify({'authenticated': False, 'auth_enabled': _is_auth_enabled()})
    user = _get_user_by_id(session.get('user_id', ''))
    return jsonify({
        'authenticated': True,
        'auth_enabled': _is_auth_enabled(),
        'user': {
            'id': user.get('id'),
            'username': user.get('username'),
            'name': user.get('name'),
            'role': user.get('role')
        }
    })

@app.route('/api/user/me')
@app.route('/ai-agent/api/user/me')
def api_user_me():
    """Get current user info for role-based permissions"""
    if not session.get('user_id'):
        return jsonify({'role': 'viewer', 'authenticated': False, 'auth_enabled': _is_auth_enabled()})
    user = _get_user_by_id(session.get('user_id', ''))
    if not user:
        session.clear()
        return jsonify({'role': 'viewer', 'authenticated': False, 'auth_enabled': _is_auth_enabled()})
    return jsonify({
        'id': user.get('id'),
        'username': user.get('username'),
        'name': user.get('name'),
        'role': user.get('role', 'viewer'),
        'authenticated': True,
        'auth_enabled': _is_auth_enabled(),
    })

@app.route('/api/admin/auth', methods=['POST'])
@app.route('/ai-agent/api/admin/auth', methods=['POST'])
@admin_required
def api_admin_toggle_auth():
    """Toggle authentication on/off"""
    data = request.get_json() or {}
    enabled = bool(data.get('enabled', False))

    if not enabled:
        return jsonify({'error': 'Authentication cannot be disabled'}), 400

    users_data = _load_users()
    users_data['settings'] = users_data.get('settings', {})
    users_data['settings']['auth_enabled'] = enabled

    if _save_users(users_data):
        return jsonify({'success': True, 'auth_enabled': enabled})
    return jsonify({'error': 'Failed to save settings'}), 500


@app.route('/api/admin/users/<user_id>/reset-password', methods=['POST'])
@app.route('/ai-agent/api/admin/users/<user_id>/reset-password', methods=['POST'])
@admin_required
def api_admin_reset_password(user_id: str):
    """Admin password reset for a specific user."""
    data = request.get_json() or {}
    new_password = str(data.get('new_password', '') or '')
    if len(new_password) < 8:
        return jsonify({'error': 'New password must be at least 8 characters'}), 400

    users_data = _load_users()
    user_index = None
    for i, user in enumerate(users_data.get('users', [])):
        if user.get('id') == user_id:
            user_index = i
            break

    if user_index is None:
        return jsonify({'error': 'User not found'}), 404

    users_data['users'][user_index]['password'] = _hash_password(new_password)
    users_data['users'][user_index]['password_updated_at'] = datetime.now().isoformat()

    if _save_users(users_data):
        return jsonify({'success': True})
    return jsonify({'error': 'Failed to save password reset'}), 500


@app.route('/api/user/password', methods=['POST'])
@app.route('/ai-agent/api/user/password', methods=['POST'])
@login_required
def api_user_change_password():
    """Allow logged-in user to change own password."""
    data = request.get_json() or {}
    current_password = str(data.get('current_password', '') or '')
    new_password = str(data.get('new_password', '') or '')

    if len(new_password) < 8:
        return jsonify({'error': 'New password must be at least 8 characters'}), 400

    user_id = str(session.get('user_id', '') or '')
    user = _get_user_by_id(user_id)
    if not user:
        session.clear()
        return jsonify({'error': 'Session user not found'}), 401

    if user.get('password') != _hash_password(current_password):
        return jsonify({'error': 'Current password is incorrect'}), 400

    users_data = _load_users()
    updated = False
    for row in users_data.get('users', []):
        if row.get('id') == user_id:
            row['password'] = _hash_password(new_password)
            row['password_updated_at'] = datetime.now().isoformat()
            updated = True
            break

    if not updated:
        return jsonify({'error': 'User not found'}), 404

    if _save_users(users_data):
        return jsonify({'success': True})
    return jsonify({'error': 'Failed to update password'}), 500

# ============================================================================
# End Authentication Routes
# ============================================================================

# ============================================================================
# PR Automation Routes
# ============================================================================

# In-memory PR tracking (would be Redis in production)
_pr_tracking: Dict[str, Dict] = {}
# Dismissed OOMKilled services (won't appear in pending)
_pr_dismissed: Set[str] = set()
_pr_pending_cache: Dict[str, Any] = {'ts': None, 'data': {'pending': []}}
_pr_pending_cache_lock = threading.Lock()
_pr_issue_first_seen: Dict[str, datetime] = {}
_pr_issue_first_seen_lock = threading.Lock()


def _pr_issue_persistence_seconds() -> int:
    loaded = _load_config()
    cfg = loaded.get('pr_automation', {}) if isinstance(loaded, dict) else {}
    value = int((cfg or {}).get('issue_persistence_seconds', 300) or 300)
    return max(60, min(value, 3600))


def _pr_extract_issue_type(svc: Dict[str, Any]) -> str:
    issue_type = ''
    recent_errors = svc.get('recent_errors', []) or []
    for err in recent_errors:
        if not isinstance(err, dict):
            continue
        issue_field = str(err.get('issue', '') or '').lower()
        msg = str(err.get('message', '') or '').lower()
        reason_field = str(err.get('reason', '') or '').lower()
        root_cause = str(err.get('root_cause', '') or '').lower()
        combined = f"{issue_field} {msg} {reason_field} {root_cause}"
        if 'oomkilled' in combined or 'out of memory' in combined or 'exitcode=137' in combined or 'exit code 137' in combined:
            issue_type = 'OOMKilled'
            break
        if 'insufficient cpu' in combined or ('failedscheduling' in combined and 'cpu' in combined) or 'cpu throttl' in combined:
            issue_type = 'CPUStarvation'
            break

    if issue_type:
        return issue_type

    pod_status = svc.get('pod_status', {}) or {}
    pods = pod_status.get('pods', []) or []
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        reason = str(pod.get('reason', '') or '').lower()
        if 'oomkilled' in reason or 'exitcode=137' in reason or 'exit code 137' in reason:
            return 'OOMKilled'
        if 'insufficient cpu' in reason or ('failedscheduling' in reason and 'cpu' in reason):
            return 'CPUStarvation'

    deep = svc.get('deep_inspection', {}) or {}
    root_cause = str(deep.get('root_cause', '') or '').lower()
    issue = str(deep.get('exact_issue', '') or '').lower()
    if 'oomkilled' in root_cause or 'oomkilled' in issue:
        return 'OOMKilled'
    if 'insufficient cpu' in root_cause or 'insufficient cpu' in issue:
        return 'CPUStarvation'
    return ''


def _pr_extract_infra_issue_type(svc: Dict[str, Any]) -> str:
    """Detect infra-level issue type (vault, port, yaml) from service error data."""
    if not INFRA_FIX_AVAILABLE:
        return ''
    recent_errors = svc.get('recent_errors', []) or []
    texts = []
    for err in recent_errors:
        if not isinstance(err, dict):
            continue
        texts.extend([
            str(err.get('message', '') or ''),
            str(err.get('root_cause', '') or ''),
            str(err.get('issue', '') or ''),
            str(err.get('reason', '') or ''),
        ])
    deep = svc.get('deep_inspection', {}) or {}
    texts.append(str(deep.get('root_cause', '') or ''))
    texts.append(str(deep.get('exact_issue', '') or ''))
    return _infra_fix.detect_infra_issue(texts)


def _pr_collect_issue_candidates(namespaces: Set[str] = None) -> Dict[str, Dict[str, Any]]:
    """Collect only services already showing issue signals on dashboard snapshots."""
    candidates: Dict[str, Dict[str, Any]] = {}
    now = datetime.now()
    persistence_seconds = _pr_issue_persistence_seconds()
    scan_namespaces = set(namespaces or {'venus', 'jupiter'})
    if not scan_namespaces:
        scan_namespaces = {'venus', 'jupiter'}
    for ns in scan_namespaces:
        if ns not in {'venus', 'jupiter'}:
            continue
        payload = _read_namespace_snapshot(ns, stale=False)
        if not isinstance(payload, dict) or not payload.get('services'):
            payload = _read_namespace_snapshot(ns, stale=True)
        services = payload.get('services', {}) if isinstance(payload, dict) else {}
        if not isinstance(services, dict):
            continue
        for svc_key, svc in services.items():
            if not isinstance(svc, dict):
                continue
            namespace = str(svc.get('namespace', '') or '').strip().lower()
            service_name = str(svc.get('name', '') or '').strip()
            if not namespace or not service_name:
                if '/' in str(svc_key):
                    parts = str(svc_key).split('/', 1)
                    namespace = namespace or str(parts[0]).strip().lower()
                    service_name = service_name or str(parts[1]).strip()
            if namespace not in {'venus', 'jupiter'} or not service_name:
                continue
            full_key = f"{namespace}/{service_name}"
            status = str(svc.get('status', 'unknown') or 'unknown').lower()
            metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
            workload_kind = str(metrics.get('workload_kind', '') or '').strip().lower()
            # PR automation is deployment-only to avoid noisy/non-actionable PRs
            # for statefulsets/daemonsets and transient pod-only entries.
            if workload_kind != 'deployment':
                continue
            restart_10m = int(metrics.get('prometheus_restarts_10m', 0) or 0)
            has_recent_errors = bool(svc.get('recent_errors'))
            has_issue_signal = status in {'degraded', 'down', 'pending', 'warning'} or has_recent_errors or restart_10m > 0

            with _pr_issue_first_seen_lock:
                if not has_issue_signal:
                    _pr_issue_first_seen.pop(full_key, None)
                    continue

                first_seen = _pr_issue_first_seen.get(full_key)
                if not isinstance(first_seen, datetime):
                    _pr_issue_first_seen[full_key] = now
                    continue

            if (now - first_seen).total_seconds() < persistence_seconds:
                continue
            candidates[full_key] = svc
    return candidates


def _svc_has_active_pod_issue(svc: Dict[str, Any]) -> bool:
    """Return True only when a pod is genuinely NOT running.

    'warning' status alone (log-level anomalies on a healthy pod) is NOT
    treated as a pod issue — it would produce false positives for services
    whose pods are fully ready but have noisy error logs.
    """
    if not isinstance(svc, dict):
        return False
    metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}
    status = str(svc.get('status', '') or '').strip().lower()
    total = int(metrics.get('pod_total_count', 0) or 0)
    running = int(metrics.get('pod_running_count', 0) or 0)
    ready = int(metrics.get('pod_ready_count', 0) or 0)
    issue_count = int(metrics.get('pod_issue_count', 0) or 0)

    # Only statuses that indicate pods are genuinely not running
    if status in {'down', 'degraded', 'pending'}:
        return True
    if issue_count > 0:
        return True
    if total > 0 and running == 0:
        return True
    if running > 0 and ready < running:
        return True
    return False

@app.route('/api/pr/pending')
@app.route('/ai-agent/api/pr/pending')
@operator_required
def api_pr_pending():
    """Get services with pending PR opportunities (OOMKilled/CPUStarvation services)."""
    try:
        force_refresh = str(request.args.get('refresh', '') or '').strip().lower() in {'1', 'true', 'yes'}
        now = datetime.now()
        with _pr_pending_cache_lock:
            cached_ts = _pr_pending_cache.get('ts')
            cached_data = _pr_pending_cache.get('data')
            if (
                not force_refresh and
                isinstance(cached_ts, datetime) and
                isinstance(cached_data, dict) and
                (now - cached_ts).total_seconds() < 12
            ):
                return jsonify(cached_data)

        pending = []
        issue_services: Dict[str, str] = {}
        namespace_filter = str(request.args.get('namespace', '') or '').strip().lower()
        if namespace_filter in {'venus', 'jupiter'}:
            candidate_services = _pr_collect_issue_candidates({namespace_filter})
        else:
            candidate_services = _pr_collect_issue_candidates({'venus', 'jupiter'})
        logging.info(f"PR Pending: candidate services with issues on dashboard={len(candidate_services)}")

        for svc_key, svc in candidate_services.items():
            issue_type = _pr_extract_issue_type(svc)
            if issue_type:
                issue_services[svc_key] = issue_type

        # Build pending list from collected issue services
        for svc_key, issue_type in issue_services.items():
            if '/' not in svc_key:
                continue
            namespace, service_name = svc_key.split('/', 1)

            # Skip if dismissed by user
            if svc_key in _pr_dismissed:
                continue

            # Only skip if PR was successfully generated or merged (not failed)
            existing_success = [p for p in _pr_tracking.values()
                       if p.get('service_key') == svc_key and p.get('status') in ['generated', 'merged']]
            if existing_success:
                continue

            # Check if there was a failed attempt (for retry indicator)
            failed_attempt = [p for p in _pr_tracking.values()
                       if p.get('service_key') == svc_key and p.get('status') == 'failed']
            last_error = failed_attempt[-1].get('error', '') if failed_attempt else None

            # Get current memory/cpu from cached issue candidate snapshot first.
            current_memory = '512Mi'
            current_cpu = '500m'
            svc_data = candidate_services.get(svc_key, {}) if isinstance(candidate_services.get(svc_key, {}), dict) else {}
            if svc_data:
                try:
                    pod_status = svc_data.get('pod_status', {}) or {}
                    if pod_status.get('memory_limit'):
                        current_memory = str(pod_status.get('memory_limit'))
                    if pod_status.get('cpu_limit'):
                        current_cpu = str(pod_status.get('cpu_limit'))
                except Exception:
                    pass

            # Override with live deployment resources when available (real values, not defaults).
            live_resources = _read_workload_resources(namespace, service_name)
            if str(live_resources.get('memory_limit', '') or '').strip():
                current_memory = str(live_resources.get('memory_limit'))
            elif str(live_resources.get('memory_request', '') or '').strip():
                current_memory = str(live_resources.get('memory_request'))
            if str(live_resources.get('cpu_limit', '') or '').strip():
                current_cpu = str(live_resources.get('cpu_limit'))
            elif str(live_resources.get('cpu_request', '') or '').strip():
                current_cpu = str(live_resources.get('cpu_request'))

            fail_count = len(failed_attempt)
            mem_factor = 1.25 if fail_count <= 0 else (1.4 if fail_count == 1 else 1.6)
            cpu_factor = 1.2 if fail_count <= 0 else (1.35 if fail_count == 1 else 1.5)

            pending.append({
                'namespace': namespace,
                'service': service_name,
                'issue_type': issue_type,
                'current_memory': current_memory,
                'recommended_memory': _calculate_recommended_memory(current_memory, factor=mem_factor),
                'current_cpu': current_cpu,
                'recommended_cpu': _calculate_recommended_cpu(current_cpu, factor=cpu_factor),
                'status': 'retry' if last_error else ('cpu_detected' if issue_type == 'CPUStarvation' else 'oom_detected'),
                'last_error': last_error
            })

        logging.info(f"PR Pending: Returning {len(pending)} pending PRs")
        payload = {'pending': pending}
        with _pr_pending_cache_lock:
            _pr_pending_cache['ts'] = datetime.now()
            _pr_pending_cache['data'] = payload
        return jsonify(payload)
    except Exception as e:
        logging.error(f"Error getting pending PRs: {e}", exc_info=True)
        return jsonify({'pending': [], 'error': str(e)})

def _read_workload_resources(namespace: str, service_name: str) -> Dict[str, str]:
    out = {
        'memory_request': '',
        'memory_limit': '',
        'cpu_request': '',
        'cpu_limit': '',
    }
    ns = str(namespace or '').strip().lower()
    svc = str(service_name or '').strip()
    if ns not in {'venus', 'jupiter'} or not svc:
        return out
    try:
        result = subprocess.run(
            ['kubectl', 'get', 'deployment', svc, '-n', ns, '-o', 'json'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return out
        deploy = json.loads(result.stdout or '{}')
        containers = (
            deploy.get('spec', {})
            .get('template', {})
            .get('spec', {})
            .get('containers', [])
        )
        if not containers or not isinstance(containers, list):
            return out
        primary = containers[0] if isinstance(containers[0], dict) else {}
        resources = primary.get('resources', {}) if isinstance(primary.get('resources', {}), dict) else {}
        requests = resources.get('requests', {}) if isinstance(resources.get('requests', {}), dict) else {}
        limits = resources.get('limits', {}) if isinstance(resources.get('limits', {}), dict) else {}
        out['memory_request'] = str(requests.get('memory', '') or '').strip()
        out['memory_limit'] = str(limits.get('memory', '') or '').strip()
        out['cpu_request'] = str(requests.get('cpu', '') or '').strip()
        out['cpu_limit'] = str(limits.get('cpu', '') or '').strip()
    except Exception:
        return out
    return out


def _calculate_recommended_memory(current: str, factor: float = 1.5) -> str:
    """Calculate recommended memory (1.5x increase)."""
    try:
        # Parse memory string (e.g., "512Mi", "1Gi", "2048Mi")
        current = str(current).strip()
        factor = float(factor or 1.5)
        if factor < 1.05:
            factor = 1.05
        if current.endswith('Gi'):
            val = float(current[:-2])
            new_val = val * factor
            return f"{new_val:.1f}Gi" if new_val != int(new_val) else f"{int(new_val)}Gi"
        elif current.endswith('Mi'):
            val = int(current[:-2])
            new_val = int(val * factor)
            if new_val >= 1024:
                return f"{new_val / 1024:.1f}Gi"
            return f"{new_val}Mi"
        return "1Gi"  # Default if parsing fails
    except:
        return "1Gi"


def _calculate_recommended_cpu(current: str, factor: float = 1.5) -> str:
    """Calculate recommended CPU (1.5x increase)."""
    try:
        cur = str(current or '').strip().lower()
        factor = float(factor or 1.5)
        if factor < 1.05:
            factor = 1.05
        if cur.endswith('m'):
            val = int(float(cur[:-1] or 0))
            return f"{max(100, int(val * factor))}m"
        val = float(cur)
        bumped = val * factor
        if bumped < 1:
            return f"{int(bumped * 1000)}m"
        return f"{bumped:.1f}" if bumped != int(bumped) else f"{int(bumped)}"
    except Exception:
        return "750m"

@app.route('/api/pr/history')
@app.route('/ai-agent/api/pr/history')
@operator_required
def api_pr_history():
    """Get PR history."""
    history = list(_pr_tracking.values())
    days = max(1, min(int(request.args.get('days', 7) or 7), 7))
    cutoff = datetime.now() - timedelta(days=days)
    history = [
        item for item in history
        if (_parse_iso_datetime(str(item.get('created_at', '') or '')) or datetime.min) >= cutoff
    ]
    # Sort by created_at descending
    history.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    return jsonify({'history': history[:100], 'days': days})

@app.route('/api/pr/dismiss', methods=['POST'])
@app.route('/ai-agent/api/pr/dismiss', methods=['POST'])
@operator_required
def api_pr_dismiss():
    """Dismiss an OOMKilled service from pending list."""
    data = request.get_json() or {}
    namespace = data.get('namespace', '')
    service = data.get('service', '')

    if not namespace or not service:
        return jsonify({'error': 'Missing namespace or service'}), 400

    svc_key = f"{namespace}/{service}"
    _pr_dismissed.add(svc_key)
    logging.info(f"PR Dismiss: Service {svc_key} dismissed from pending")
    return jsonify({'success': True, 'dismissed': svc_key})

@app.route('/api/pr/generate', methods=['POST'])
@app.route('/ai-agent/api/pr/generate', methods=['POST'])
@operator_required
def api_pr_generate():
    """Generate a PR to fix resource issue by increasing memory/cpu."""
    data = request.get_json() or {}
    namespace = data.get('namespace', '')
    service = data.get('service', '')
    recommended_memory = data.get('recommended_memory', '')
    recommended_cpu = data.get('recommended_cpu', '')
    issue_type = str(data.get('issue_type', 'OOMKilled') or 'OOMKilled')

    if not namespace or not service:
        return jsonify({'error': 'Missing required fields'}), 400
    if issue_type == 'CPUStarvation' and not recommended_cpu:
        return jsonify({'error': 'Missing recommended_cpu'}), 400
    if issue_type != 'CPUStarvation' and not recommended_memory:
        return jsonify({'error': 'Missing recommended_memory'}), 400

    try:
        import subprocess
        import tempfile
        import os
        from uuid import uuid4

        pr_id = str(uuid4())[:8]
        svc_key = f"{namespace}/{service}"
        branch_name = f"ai-fix-oom-{service}-{pr_id}"

        # Get current user
        current_user = session.get('username', 'ai-agent')

        # Create PR tracking entry
        _pr_tracking[pr_id] = {
            'id': pr_id,
            'service_key': svc_key,
            'namespace': namespace,
            'service': service,
            'issue_type': issue_type,
            'current_memory': data.get('current_memory', '?'),
            'recommended_memory': recommended_memory,
            'current_cpu': data.get('current_cpu', '?'),
            'recommended_cpu': recommended_cpu,
            'status': 'pending',
            'pr_url': None,
            'created_at': datetime.now().isoformat(),
            'created_by': current_user,
            'merged_at': None
        }

        # Clone k8s-manifest repo from azure branch
        gh_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
        if not gh_token:
            _pr_tracking[pr_id]['status'] = 'failed'
            _pr_tracking[pr_id]['error'] = 'GitHub token not configured'
            return jsonify({'error': 'GitHub token not configured'}), 500

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_url = f"https://{gh_token}@github.com/fabhotelstech/k8s-manifest.git"

            # Clone azure branch
            result = subprocess.run(
                ['git', 'clone', '--depth', '1', '--branch', 'azure', repo_url, tmpdir],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode != 0:
                _pr_tracking[pr_id]['status'] = 'failed'
                _pr_tracking[pr_id]['error'] = 'Failed to clone repo'
                return jsonify({'error': 'Failed to clone k8s-manifest repo (azure branch)'}), 500

            # Find deployment file — use fuzzy name resolution so e.g.
            # "invoice-reader-service" matches directory "invoice-reader".
            deployment_path = None
            relative_path = None

            # Accepted deployment file names (in priority order)
            _DEPLOY_FILENAMES = ['deployment.yaml', 'deployment.yml', 'deploy.yaml', 'deploy.yml']

            def _find_deploy_file(svc_dir):
                """Return (abs_path, filename) for the first deployment file found."""
                for fname in _DEPLOY_FILENAMES:
                    p = os.path.join(svc_dir, fname)
                    if os.path.exists(p):
                        return p, fname
                return None, None

            if INFRA_FIX_AVAILABLE:
                try:
                    resolved = _infra_fix.resolve_service_dir(tmpdir, namespace, service)
                    logging.info(f"PR Gen: resolve_service_dir({namespace}, {service}) -> {resolved}")
                    if resolved:
                        svc_dir, rel_dir = resolved
                        dep_path, dep_fname = _find_deploy_file(svc_dir)
                        if dep_path:
                            deployment_path = dep_path
                            relative_path = f"{rel_dir}/{dep_fname}"
                        else:
                            # Log what IS in the directory to help diagnose
                            try:
                                found_files = os.listdir(svc_dir)
                            except Exception:
                                found_files = []
                            logging.warning(
                                f"PR Gen: Directory found at {rel_dir} but no deployment file. "
                                f"Files present: {found_files}"
                            )
                except Exception as _rsd_err:
                    logging.error(f"PR Gen: resolve_service_dir raised: {_rsd_err}", exc_info=True)
            else:
                # Fallback: exact path + fuzzy suffix-strip without infra_fix module
                logging.warning(f"PR Gen: infra_fix not available, using manual path search for {service}")
                backend_techs = ['java17', 'java8', 'nodejs', 'python']

                def _strip_suffixes(name):
                    for sfx in ['-service', '-api', '-svc', '-server', '-app', '-backend', '-worker']:
                        if name.lower().endswith(sfx):
                            return name[:-len(sfx)]
                    return name

                candidates_to_try = list({service, _strip_suffixes(service), service.lower(), _strip_suffixes(service.lower())})
                for tech in backend_techs:
                    for svc_candidate in candidates_to_try:
                        svc_dir = os.path.join(tmpdir, namespace, 'backend', tech, svc_candidate)
                        if os.path.isdir(svc_dir):
                            dep_path, dep_fname = _find_deploy_file(svc_dir)
                            if dep_path:
                                deployment_path = dep_path
                                relative_path = f"{namespace}/backend/{tech}/{svc_candidate}/{dep_fname}"
                                break
                    if deployment_path:
                        break
                if not deployment_path:
                    for svc_candidate in candidates_to_try:
                        svc_dir = os.path.join(tmpdir, namespace, 'frontend', svc_candidate)
                        if os.path.isdir(svc_dir):
                            dep_path, dep_fname = _find_deploy_file(svc_dir)
                            if dep_path:
                                deployment_path = dep_path
                                relative_path = f"{namespace}/frontend/{svc_candidate}/{dep_fname}"
                                break

            if not deployment_path:
                # Log the actual directory tree to diagnose
                try:
                    ns_dir = os.path.join(tmpdir, namespace)
                    ns_tree = []
                    for root, dirs, files in os.walk(ns_dir):
                        rel = os.path.relpath(root, tmpdir)
                        ns_tree.append(f"{rel}/: {files}")
                        if len(ns_tree) > 40:
                            ns_tree.append('...(truncated)')
                            break
                    logging.warning(f"PR Gen: {namespace} tree:\n" + "\n".join(ns_tree))
                except Exception:
                    pass
                _pr_tracking[pr_id]['status'] = 'failed'
                _pr_tracking[pr_id]['error'] = f'Deployment file not found for {service}'
                logging.warning(f"PR Gen: Could not find deployment for {service} in {namespace}.")
                return jsonify({'error': f'Deployment file not found for {service} in {namespace}. Checked backend and frontend paths.'}), 404

            logging.info(f"PR Gen: Found deployment at {relative_path}")

            # Update resource in deployment file
            with open(deployment_path, 'r') as fp:
                content = fp.read()

            import re
            # More flexible regex patterns to match various YAML formats:
            # - limits:\n  memory: 512Mi
            # - limits:\n  cpu: 500m\n  memory: 512Mi
            # - memory: "512Mi" or memory: '512Mi' or memory: 512Mi

            # Pattern 1: Match memory under limits block (handles cpu before memory)
            # This looks for "limits:" then any content until "memory:" on subsequent lines
            def replace_limits_memory(match):
                return match.group(1) + recommended_memory

            def replace_limits_cpu(match):
                return match.group(1) + recommended_cpu

            if issue_type == 'CPUStarvation':
                new_content = re.sub(
                    r'(limits:\s*\n(?:\s+\w+:\s*["\']?[\w.]+["\']?\s*\n)*\s*cpu:\s*)["\']?[\d.]+m?["\']?',
                    replace_limits_cpu,
                    content
                )
                new_content = re.sub(
                    r'(limits:\s*\n\s*cpu:\s*)["\']?[\d.]+m?["\']?',
                    f'\\g<1>{recommended_cpu}',
                    new_content
                )
                new_content = re.sub(
                    r'(requests:\s*\n(?:\s+\w+:\s*["\']?[\w.]+["\']?\s*\n)*\s*cpu:\s*)["\']?[\d.]+m?["\']?',
                    replace_limits_cpu,
                    new_content
                )
                new_content = re.sub(
                    r'(requests:\s*\n\s*cpu:\s*)["\']?[\d.]+m?["\']?',
                    f'\\g<1>{recommended_cpu}',
                    new_content
                )
            else:
                new_content = re.sub(
                    r'(limits:\s*\n(?:\s+\w+:\s*["\']?[\w.]+["\']?\s*\n)*\s*memory:\s*)["\']?[\d.]+[GMK]i["\']?',
                    replace_limits_memory,
                    content
                )

                # Pattern 2: Simple case - limits followed directly by memory
                new_content = re.sub(
                    r'(limits:\s*\n\s*memory:\s*)["\']?[\d.]+[GMK]i["\']?',
                    f'\\g<1>{recommended_memory}',
                    new_content
                )

                # Pattern 3: Update requests memory (for consistency)
                new_content = re.sub(
                    r'(requests:\s*\n(?:\s+\w+:\s*["\']?[\w.]+["\']?\s*\n)*\s*memory:\s*)["\']?[\d.]+[GMK]i["\']?',
                    replace_limits_memory,
                    new_content
                )
                new_content = re.sub(
                    r'(requests:\s*\n\s*memory:\s*)["\']?[\d.]+[GMK]i["\']?',
                    f'\\g<1>{recommended_memory}',
                    new_content
                )

            # Verify something changed
            if new_content == content:
                logging.warning(f"PR Gen: Resource pattern not matched in {relative_path}, trying alternate patterns")
                if issue_type == 'CPUStarvation':
                    new_content = re.sub(
                        r'(\s+cpu:\s*)["\']?[\d.]+m?["\']?(\s*#.*)?$',
                        f'\\g<1>{recommended_cpu}\\g<2>',
                        content,
                        flags=re.MULTILINE
                    )
                else:
                    # Try a simpler direct replacement for memory lines under resources
                    new_content = re.sub(
                        r'(\s+memory:\s*)["\']?[\d.]+[GMK]i["\']?(\s*#.*)?$',
                        f'\\g<1>{recommended_memory}\\g<2>',
                        content,
                        flags=re.MULTILINE
                    )

            with open(deployment_path, 'w') as fp:
                fp.write(new_content)

            # Create branch and commit
            subprocess.run(['git', 'config', 'user.email', 'ai-agent@fabhotels.com'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'AI Monitoring Agent'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'add', deployment_path], cwd=tmpdir, capture_output=True)

            commit_msg = f"""fix({service}): increase {'cpu' if issue_type == 'CPUStarvation' else 'memory'} limit to fix {issue_type}

Service: {service}
Namespace: {namespace}
Issue: {issue_type}
Change: Increased {'cpu' if issue_type == 'CPUStarvation' else 'memory'} limit to {recommended_cpu if issue_type == 'CPUStarvation' else recommended_memory}

Generated by AI Monitoring Agent
Approved by: {current_user}
"""
            subprocess.run(['git', 'commit', '-m', commit_msg], cwd=tmpdir, capture_output=True)
            push_result = subprocess.run(
                ['git', 'push', '-u', 'origin', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=60
            )
            if push_result.returncode != 0:
                err_text = push_result.stderr.replace(gh_token, '***') if gh_token else push_result.stderr
                _pr_tracking[pr_id]['status'] = 'failed'
                _pr_tracking[pr_id]['error'] = f'Push failed: {err_text[:200]}'
                return jsonify({'error': f'Failed to push branch: {err_text[:200]}'}), 500

            # Create PR using gh CLI (base branch: azure)
            pr_title = f"[AI-Fix] {service}: Increase {'cpu' if issue_type == 'CPUStarvation' else 'memory'} to {recommended_cpu if issue_type == 'CPUStarvation' else recommended_memory} ({issue_type})"
            pr_body = f"""## Summary
- **Service:** {service}
- **Namespace:** {namespace}
- **Issue:** {issue_type}
- **Change:** {'CPU' if issue_type == 'CPUStarvation' else 'Memory'} limit increased to {recommended_cpu if issue_type == 'CPUStarvation' else recommended_memory}
- **File:** `{relative_path}`

## Details
This PR was automatically generated by the AI Monitoring Agent to address a resource issue.

## Approved By
{current_user}

---
*Generated with AI Monitoring Agent*
"""
            result = subprocess.run(
                ['gh', 'pr', 'create', '--title', pr_title, '--body', pr_body, '--base', 'azure', '--head', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0:
                pr_url = result.stdout.strip()
                _pr_tracking[pr_id]['status'] = 'generated'
                _pr_tracking[pr_id]['pr_url'] = pr_url
                return jsonify({'success': True, 'pr_id': pr_id, 'pr_url': pr_url})
            else:
                _pr_tracking[pr_id]['status'] = 'failed'
                _pr_tracking[pr_id]['error'] = result.stderr
                return jsonify({'error': f'Failed to create PR: {result.stderr}'}), 500

    except Exception as e:
        logging.error(f"Error generating PR: {e}", exc_info=True)
        if pr_id in _pr_tracking:
            _pr_tracking[pr_id]['status'] = 'failed'
            _pr_tracking[pr_id]['error'] = str(e)
        return jsonify({'error': str(e)}), 500

@app.route('/api/pr/merge/<pr_id>', methods=['POST'])
@app.route('/ai-agent/api/pr/merge/<pr_id>', methods=['POST'])
@admin_required
def api_pr_merge(pr_id: str):
    """Merge a generated PR (admin only)."""
    if pr_id not in _pr_tracking:
        return jsonify({'error': 'PR not found'}), 404

    pr = _pr_tracking[pr_id]
    if pr.get('status') != 'generated':
        return jsonify({'error': f"PR cannot be merged (status: {pr.get('status')})"}), 400

    if not pr.get('pr_url'):
        return jsonify({'error': 'PR URL not available'}), 400

    try:
        import subprocess
        # Extract PR number from URL
        pr_url = pr['pr_url']
        # Merge using gh CLI
        result = subprocess.run(
            ['gh', 'pr', 'merge', pr_url, '--merge', '--delete-branch'],
            capture_output=True, text=True, timeout=60
        )

        if result.returncode == 0:
            pr['status'] = 'merged'
            pr['merged_at'] = datetime.now().isoformat()
            pr['merged_by'] = session.get('username', 'admin')
            return jsonify({'success': True, 'message': 'PR merged successfully'})
        else:
            return jsonify({'error': f'Failed to merge PR: {result.stderr}'}), 500

    except Exception as e:
        logging.error(f"Error merging PR: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# End PR Automation Routes
# ============================================================================

# ============================================================================
# Infra Fix Routes (vault secret, port mismatch, yaml syntax)
# ============================================================================

_infra_pr_tracking: Dict[str, Dict[str, Any]] = {}  # infra PR history

# Proactive manifest-audit results (populated by _infra_audit_loop every 5 min)
_infra_audit_results: Dict[str, Dict[str, Any]] = {}  # key: "namespace/service"
_infra_audit_results_lock = threading.Lock()

@app.route('/api/pr/infra-pending')
@app.route('/ai-agent/api/pr/infra-pending')
@operator_required
def api_pr_infra_pending():
    """Return services with detected infra issues (vault, port, yaml, node selector, etc.)."""
    if not INFRA_FIX_AVAILABLE:
        return jsonify({'pending': []})
    try:
        namespace_filter = str(request.args.get('namespace', '') or '').strip().lower()
        namespaces = {namespace_filter} if namespace_filter in {'venus', 'jupiter'} else {'venus', 'jupiter'}
        candidates = _pr_collect_issue_candidates(namespaces)
        pending = []
        seen_keys: set = set()

        # Pass 1: log/error-text-based detection (existing approach)
        for svc_key, svc in candidates.items():
            if '/' not in svc_key:
                continue
            namespace, service_name = svc_key.split('/', 1)
            existing_ok = [p for p in _infra_pr_tracking.values()
                           if p.get('service_key') == svc_key and p.get('status') in ('generated', 'merged')]
            if existing_ok:
                continue
            if svc_key in _pr_dismissed:
                continue
            if not _svc_has_active_pod_issue(svc):
                continue
            resource_issue = _pr_extract_issue_type(svc)
            if resource_issue:
                continue
            infra_type = _pr_extract_infra_issue_type(svc)
            if not infra_type:
                continue
            failed_attempt = [p for p in _infra_pr_tracking.values()
                              if p.get('service_key') == svc_key and p.get('status') == 'failed']
            last_error = failed_attempt[-1].get('error', '') if failed_attempt else None
            recent_errors = svc.get('recent_errors', []) or []
            sample_msg = ''
            for err in recent_errors:
                if isinstance(err, dict):
                    sample_msg = str(err.get('message', '') or err.get('root_cause', '') or '')[:120]
                    if sample_msg:
                        break
            seen_keys.add(svc_key)
            pending.append({
                'namespace': namespace,
                'service': service_name,
                'issue_type': infra_type,
                'description': sample_msg,
                'status': 'retry' if last_error else 'detected',
                'last_error': last_error,
                'source': 'log_analysis',
            })

        # Pass 2: proactive manifest-audit results from _infra_audit_loop
        with _infra_audit_results_lock:
            audit_snapshot = dict(_infra_audit_results)

        for svc_key, audit in audit_snapshot.items():
            if svc_key in seen_keys:
                continue
            if svc_key in _pr_dismissed:
                continue
            namespace = audit.get('namespace', '')
            service_name = audit.get('service', '')
            if not namespace or not service_name:
                continue
            if namespace_filter and namespace != namespace_filter:
                continue
            existing_ok = [p for p in _infra_pr_tracking.values()
                           if p.get('service_key') == svc_key and p.get('status') in ('generated', 'merged')]
            if existing_ok:
                continue
            issues = audit.get('issues', []) or []
            if not issues:
                continue
            svc = candidates.get(svc_key)
            active_pod_issue = _svc_has_active_pod_issue(svc) if isinstance(svc, dict) else False
            issue_types = list(dict.fromkeys(i.get('type', '') for i in issues if i.get('type')))
            issue_type_str = ', '.join(issue_types) if issue_types else 'InfraIssue'
            description = '; '.join(i.get('description', '') for i in issues[:3])
            failed_attempt = [p for p in _infra_pr_tracking.values()
                              if p.get('service_key') == svc_key and p.get('status') == 'failed']
            last_error = failed_attempt[-1].get('error', '') if failed_attempt else None
            item = {
                'namespace': namespace,
                'service': service_name,
                'issue_type': issue_type_str,
                'description': description,
                'status': 'retry' if last_error else 'detected',
                'last_error': last_error,
                'source': 'manifest_audit',
                'pod_reason': audit.get('pod_reason', ''),
                'last_checked': audit.get('last_checked', ''),
            }
            if active_pod_issue:
                seen_keys.add(svc_key)
                pending.append(item)
            # Healthy services are silently dropped — no suggestions section any more

        # Pass 3: rollout monitor root-cause issues (pod describe/events based)
        if ROLLOUT_MONITOR_AVAILABLE:
            try:
                rollout_issues = _rollout_monitor.get_monitor().get_issues(namespace=namespace_filter or None)
            except Exception:
                rollout_issues = []

            for issue in rollout_issues:
                if not isinstance(issue, dict):
                    continue
                namespace = str(issue.get('namespace', '') or '').strip().lower()
                service_name = str(issue.get('service', '') or '').strip()
                if not namespace or not service_name:
                    continue
                if namespace_filter and namespace != namespace_filter:
                    continue

                svc_key = f"{namespace}/{service_name}"
                if svc_key in seen_keys or svc_key in _pr_dismissed:
                    continue

                svc = candidates.get(svc_key)
                if isinstance(svc, dict) and not _svc_has_active_pod_issue(svc):
                    continue

                existing_ok = [p for p in _infra_pr_tracking.values()
                               if p.get('service_key') == svc_key and p.get('status') in ('generated', 'merged')]
                if existing_ok:
                    continue

                rc = issue.get('root_cause', {}) if isinstance(issue.get('root_cause', {}), dict) else {}
                if not bool(rc.get('fix_possible')):
                    continue

                rc_type = str(rc.get('type', '') or '').strip() or 'RolloutFix'
                rc_desc = str(rc.get('description', '') or '').strip()
                failed_attempt = [p for p in _infra_pr_tracking.values()
                                  if p.get('service_key') == svc_key and p.get('status') == 'failed']
                last_error = failed_attempt[-1].get('error', '') if failed_attempt else None
                seen_keys.add(svc_key)
                pending.append({
                    'namespace': namespace,
                    'service': service_name,
                    'issue_type': rc_type,
                    'description': rc_desc or str(issue.get('summary', '') or 'Rollout issue detected from failed pod/events'),
                    'status': 'retry' if last_error else 'detected',
                    'last_error': last_error,
                    'source': 'rollout_monitor',
                })

        return jsonify({'pending': pending})
    except Exception as e:
        logging.error(f"Error getting infra pending: {e}", exc_info=True)
        return jsonify({'pending': [], 'error': str(e)})


@app.route('/api/pr/infra-generate', methods=['POST'])
@app.route('/ai-agent/api/pr/infra-generate', methods=['POST'])
@operator_required
def api_pr_infra_generate():
    """Clone k8s-manifest, detect all infra issues for the service, apply fixes, create PR."""
    if not INFRA_FIX_AVAILABLE:
        return jsonify({'error': 'infra_fix module not available'}), 503

    data = request.get_json() or {}
    namespace = str(data.get('namespace', '') or '').strip().lower()
    service = str(data.get('service', '') or '').strip()
    if not namespace or not service:
        return jsonify({'error': 'namespace and service are required'}), 400

    import tempfile

    pr_id = str(uuid4())[:8]
    svc_key = f"{namespace}/{service}"
    branch_name = f"ai-infra-fix/{service}-{pr_id}"
    current_user = session.get('username', 'ai-agent')

    _infra_pr_tracking[pr_id] = {
        'id': pr_id,
        'service_key': svc_key,
        'namespace': namespace,
        'service': service,
        'issue_type': 'InfraFix',
        'status': 'pending',
        'pr_url': None,
        'created_at': datetime.now().isoformat(),
        'created_by': current_user,
    }

    gh_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
    if not gh_token:
        _infra_pr_tracking[pr_id]['status'] = 'failed'
        _infra_pr_tracking[pr_id]['error'] = 'GitHub token not configured'
        return jsonify({'error': 'GitHub token not configured'}), 500

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_url = f"https://{gh_token}@github.com/fabhotelstech/k8s-manifest.git"
            clone_result = subprocess.run(
                ['git', 'clone', '--depth', '1', '--branch', 'azure', repo_url, tmpdir],
                capture_output=True, text=True, timeout=120
            )
            if clone_result.returncode != 0:
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = 'Failed to clone k8s-manifest repo'
                return jsonify({'error': 'Failed to clone k8s-manifest (azure branch)'}), 500

            files = _infra_fix.find_k8s_files(tmpdir, namespace, service)
            if not files.get('relative_dir'):
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = f'No k8s files found for {service} in {namespace}'
                return jsonify({'error': f'No k8s manifest directory found for {namespace}/{service}'}), 404

            # Audit all issues
            all_issues = []
            if files.get('deployment'):
                all_issues.extend(_infra_fix.audit_deployment(files['deployment'], namespace))
            if files.get('deployment') and files.get('service'):
                all_issues.extend(_infra_fix.audit_service(files['deployment'], files['service']))

            if not all_issues:
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = 'No fixable issues found in k8s manifests'
                return jsonify({'error': 'No fixable issues detected in the k8s manifests'}), 200

            # Apply all fixes
            changes = _infra_fix.apply_all_fixes(files, namespace, all_issues)
            if not changes:
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = 'All issues already fixed or unfixable'
                return jsonify({'error': 'Nothing to change — manifests already correct'}), 200

            # Git commit
            subprocess.run(['git', 'config', 'user.email', 'ai-agent@fabhotels.com'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'AI Infra Fix'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'add', '-A'], cwd=tmpdir, capture_output=True)

            issue_summary = '; '.join(f"{i['type']}: {i['description']}" for i in all_issues[:3])
            changes_summary = '\n'.join(f"  - {c}" for c in changes[:10])
            commit_msg = (
                f"fix({service}): infra manifest fixes\n\n"
                f"Issues detected: {issue_summary}\n\n"
                f"Changes applied:\n{changes_summary}\n\n"
                f"Namespace: {namespace}\n"
                f"Generated by AI Monitoring Agent\n"
                f"Approved by: {current_user}"
            )
            subprocess.run(['git', 'commit', '-m', commit_msg], cwd=tmpdir, capture_output=True)
            push_result = subprocess.run(
                ['git', 'push', '-u', 'origin', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=120
            )
            if push_result.returncode != 0:
                err_text = push_result.stderr.replace(gh_token, '***') if gh_token else push_result.stderr
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = f'Push failed: {err_text[:200]}'
                return jsonify({'error': f'Push failed: {err_text[:200]}'}), 500

            # Create PR
            issue_types_str = ', '.join({i['type'] for i in all_issues})
            pr_title = f"[AI-InfraFix] {service} ({namespace}): {issue_types_str}"
            issue_list = '\n'.join(f"- **{i['type']}**: {i['description']}" for i in all_issues)
            pr_body = (
                f"## Infra Fix: {service} ({namespace})\n\n"
                f"### Issues Detected\n{issue_list}\n\n"
                f"### Changes Applied\n{changes_summary}\n\n"
                f"### Files Modified\n"
                f"- `{files['relative_dir']}/deployment.yaml`"
                + (f"\n- `{files['relative_dir']}/service.yaml`" if files.get('service') else "")
                + (f"\n- `{files['relative_dir']}/hpa.yaml`" if files.get('hpa') else "")
                + f"\n\n**Approved by:** {current_user}\n\n"
                f"---\n*Generated by AI Monitoring Agent — Infra Fix*"
            )
            pr_result = subprocess.run(
                ['gh', 'pr', 'create',
                 '--title', pr_title,
                 '--body', pr_body,
                 '--base', 'azure',
                 '--head', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=60
            )
            if pr_result.returncode == 0:
                pr_url = pr_result.stdout.strip()
                _infra_pr_tracking[pr_id]['status'] = 'generated'
                _infra_pr_tracking[pr_id]['pr_url'] = pr_url
                _infra_pr_tracking[pr_id]['issue_type'] = issue_types_str
                _infra_pr_tracking[pr_id]['changes'] = changes
                return jsonify({'success': True, 'pr_id': pr_id, 'pr_url': pr_url,
                                'changes': changes, 'issues': all_issues})
            else:
                err_text = pr_result.stderr.replace(gh_token, '***') if gh_token else pr_result.stderr
                _infra_pr_tracking[pr_id]['status'] = 'failed'
                _infra_pr_tracking[pr_id]['error'] = err_text[:300]
                return jsonify({'error': f'PR creation failed: {err_text[:300]}'}), 500

    except Exception as e:
        logging.error(f"Infra fix PR error: {e}", exc_info=True)
        _infra_pr_tracking[pr_id]['status'] = 'failed'
        _infra_pr_tracking[pr_id]['error'] = str(e)
        return jsonify({'error': str(e)}), 500


@app.route('/api/pr/infra-history')
@app.route('/ai-agent/api/pr/infra-history')
@operator_required
def api_pr_infra_history():
    """Return infra fix PR history."""
    days = max(1, min(int(request.args.get('days', 7) or 7), 7))
    cutoff = datetime.now() - timedelta(days=days)
    history = [
        item for item in _infra_pr_tracking.values()
        if (_parse_iso_datetime(str(item.get('created_at', '') or '')) or datetime.min) >= cutoff
    ]
    history.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    return jsonify({'history': history[:100], 'days': days})

# ============================================================================
# End Infra Fix Routes
# ============================================================================

# ============================================================================
# Rollout Monitor Routes
# ============================================================================

@app.route('/api/rollout/issues')
@app.route('/ai-agent/api/rollout/issues')
@operator_required
def api_rollout_issues():
    """Return active unhealthy rollout issues across monitored namespaces."""
    if not ROLLOUT_MONITOR_AVAILABLE:
        return jsonify({'issues': [], 'error': 'rollout_monitor not available'})
    namespace = str(request.args.get('namespace', '') or '').strip().lower()
    monitor = _rollout_monitor.get_monitor()
    issues = monitor.get_issues(namespace=namespace or None)
    age = monitor.last_scan_age_seconds()
    return jsonify({
        'issues': issues,
        'last_scan_age_seconds': round(age, 0) if age is not None else None,
        'total': len(issues),
    })


@app.route('/api/rollout/scan', methods=['POST'])
@app.route('/ai-agent/api/rollout/scan', methods=['POST'])
@operator_required
def api_rollout_scan():
    """Force an immediate rollout scan and return results."""
    if not ROLLOUT_MONITOR_AVAILABLE:
        return jsonify({'error': 'rollout_monitor not available'}), 503
    count = _rollout_monitor.get_monitor().force_scan()
    return jsonify({'issues_found': count})


@app.route('/api/rollout/fix-pr', methods=['POST'])
@app.route('/ai-agent/api/rollout/fix-pr', methods=['POST'])
@operator_required
def api_rollout_fix_pr():
    """
    Create a k8s-manifest PR to fix a detected rollout issue.

    Body: { namespace, service }
    The root cause and fix details come from the live issue stored by the monitor.
    """
    if not ROLLOUT_MONITOR_AVAILABLE or not INFRA_FIX_AVAILABLE:
        missing = []
        if not ROLLOUT_MONITOR_AVAILABLE:
            missing.append('rollout_monitor')
        if not INFRA_FIX_AVAILABLE:
            missing.append('infra_fix')
        return jsonify({'error': f"missing backend module(s): {', '.join(missing)}"}), 503

    data = request.get_json() or {}
    namespace = str(data.get('namespace', '') or '').strip().lower()
    service = str(data.get('service', '') or '').strip()
    if not namespace or not service:
        return jsonify({'error': 'namespace and service are required'}), 400

    monitor = _rollout_monitor.get_monitor()
    issue = monitor.get_issue(namespace, service)
    if not issue:
        return jsonify({'error': f'No active rollout issue found for {namespace}/{service}'}), 404

    root_cause = issue.get('root_cause', {}) or {}
    if not root_cause.get('fix_possible'):
        return jsonify({
            'error': f"No auto-fix available: {root_cause.get('type', 'Unknown')} — {root_cause.get('description', '')}",
            'fix_possible': False,
        }), 422

    import tempfile

    gh_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
    if not gh_token:
        return jsonify({'error': 'GitHub token not configured'}), 500

    pr_id = str(uuid4())[:8]
    branch_name = f"ai-rollout-fix/{service}-{pr_id}"
    current_user = session.get('username', 'ai-agent')

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_url = f"https://{gh_token}@github.com/fabhotelstech/k8s-manifest.git"
            clone = subprocess.run(
                ['git', 'clone', '--depth', '1', '--branch', 'azure', repo_url, tmpdir],
                capture_output=True, text=True, timeout=120
            )
            if clone.returncode != 0:
                return jsonify({'error': 'Failed to clone k8s-manifest'}), 500

            files = _infra_fix.find_k8s_files(tmpdir, namespace, service)
            if not files.get('relative_dir'):
                return jsonify({'error': f'No k8s manifest directory found for {namespace}/{service}'}), 404

            fix_details = root_cause.get('fix_details', {})
            changes = _infra_fix.apply_rollout_fix(files, namespace, fix_details)
            if not changes:
                return jsonify({'error': 'apply_rollout_fix produced no changes'}), 422

            subprocess.run(['git', 'config', 'user.email', 'ai-agent@fabhotels.com'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'AI Rollout Fix'], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'checkout', '-b', branch_name], cwd=tmpdir, capture_output=True)
            subprocess.run(['git', 'add', '-A'], cwd=tmpdir, capture_output=True)

            evidence_text = '\n'.join(f"  - {e}" for e in (root_cause.get('evidence') or [])[:6])
            changes_text = '\n'.join(f"  - {c}" for c in changes)
            failing_pods_text = '\n'.join(
                f"  - `{p['name']}`: {p.get('waiting_reason') or p.get('phase', '?')}"
                + (f" (restarts: {p['restart_count']})" if p.get('restart_count') else "")
                for p in (issue.get('failing_pods') or [])[:4]
            )

            commit_msg = (
                f"fix({service}): fix rollout failure — {root_cause.get('type', 'ProbePortMismatch')}\n\n"
                f"Root cause: {root_cause.get('description', '')}\n\n"
                f"Changes:\n{changes_text}\n\n"
                f"Namespace: {namespace} | Approved by: {current_user}\n"
                f"Generated by AI Monitoring Agent — Rollout Fix"
            )
            subprocess.run(['git', 'commit', '-m', commit_msg], cwd=tmpdir, capture_output=True)

            push = subprocess.run(
                ['git', 'push', '-u', 'origin', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=120
            )
            if push.returncode != 0:
                err = push.stderr.replace(gh_token, '***') if gh_token else push.stderr
                return jsonify({'error': f'Push failed: {err[:200]}'}), 500

            pr_title = f"[AI-RolloutFix] {service} ({namespace}): {root_cause.get('type', 'fix')}"
            pr_body = (
                f"## Rollout Fix: `{service}` ({namespace})\n\n"
                f"### Issue\n{issue.get('summary', '')}\n\n"
                f"### Root Cause\n**Type:** {root_cause.get('type', '?')}  \n"
                f"**Description:** {root_cause.get('description', '')}\n\n"
                f"### Evidence\n{evidence_text}\n\n"
                f"### Failing Pods\n{failing_pods_text}\n\n"
                f"### Changes Applied\n{changes_text}\n\n"
                f"### Files Modified\n"
                f"- `{files['relative_dir']}/deployment.yaml`"
                + (f"\n- `{files['relative_dir']}/service.yaml`" if files.get('service') else "")
                + f"\n\n**Approved by:** {current_user}\n\n"
                f"---\n*Generated by AI Monitoring Agent — Rollout Fix*"
            )
            pr_result = subprocess.run(
                ['gh', 'pr', 'create', '--title', pr_title, '--body', pr_body, '--base', 'azure', '--head', branch_name],
                cwd=tmpdir, capture_output=True, text=True, timeout=60
            )
            if pr_result.returncode == 0:
                pr_url = pr_result.stdout.strip()
                monitor.set_fix_pr_url(namespace, service, pr_url)
                logging.info(f"Rollout fix PR created: {pr_url}")
                return jsonify({
                    'success': True,
                    'pr_url': pr_url,
                    'changes': changes,
                    'root_cause': root_cause,
                })
            else:
                err = pr_result.stderr.replace(gh_token, '***') if gh_token else pr_result.stderr
                return jsonify({'error': f'PR creation failed: {err[:300]}'}), 500

    except Exception as e:
        logging.error(f"Rollout fix PR error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# End Rollout Monitor Routes
# ============================================================================


# ============================================================================
# K8s AI Troubleshooting Agent Routes
# ============================================================================

@app.route('/api/k8s-agent/status')
@app.route('/ai-agent/api/k8s-agent/status')
@login_required
def api_k8s_agent_status():
    if not K8S_AGENT_AVAILABLE:
        return jsonify({'available': False, 'error': 'k8s_agent module not available'})
    return jsonify({'available': True, **_k8s_agent.get_agent().status()})


@app.route('/api/k8s-agent/incidents')
@app.route('/ai-agent/api/k8s-agent/incidents')
@login_required
def api_k8s_agent_incidents():
    if not K8S_AGENT_AVAILABLE:
        return jsonify({'incidents': [], 'error': 'k8s_agent not available'})
    ns = request.args.get('namespace', '').strip().lower() or None
    limit = int(request.args.get('limit', 50))
    incidents = _k8s_agent.get_agent().get_incidents(namespace=ns, limit=limit)
    return jsonify({'incidents': incidents, 'count': len(incidents)})


@app.route('/api/k8s-agent/analyze', methods=['POST'])
@app.route('/ai-agent/api/k8s-agent/analyze', methods=['POST'])
@operator_required
def api_k8s_agent_analyze():
    """Trigger manual LLM analysis for a specific deployment."""
    if not K8S_AGENT_AVAILABLE:
        return jsonify({'error': 'k8s_agent not available'}), 503
    data = request.get_json() or {}
    namespace = str(data.get('namespace', '') or '').strip().lower()
    service   = str(data.get('service', '') or '').strip()
    if not namespace or not service:
        return jsonify({'error': 'namespace and service are required'}), 400
    try:
        incident_id = _k8s_agent.get_agent().force_analyze(namespace, service)
        return jsonify({'incident_id': incident_id, 'status': 'analyzing'})
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logging.error(f"K8s agent analyze error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/k8s-agent/fix-pr', methods=['POST'])
@app.route('/ai-agent/api/k8s-agent/fix-pr', methods=['POST'])
@operator_required
def api_k8s_agent_fix_pr():
    """Create a GitHub PR for a diagnosed incident."""
    if not K8S_AGENT_AVAILABLE:
        return jsonify({'error': 'k8s_agent not available'}), 503
    data = request.get_json() or {}
    incident_id  = str(data.get('incident_id', '') or '').strip()
    triggered_by = session.get('username', 'ai-agent')
    if not incident_id:
        return jsonify({'error': 'incident_id is required'}), 400
    try:
        pr_url = _k8s_agent.get_agent().create_pr_for_incident(incident_id, triggered_by)
        if pr_url:
            return jsonify({'success': True, 'pr_url': pr_url})
        return jsonify({'error': 'PR creation failed — check server logs'}), 500
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logging.error(f"K8s agent fix-pr error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# End K8s AI Agent Routes
# ============================================================================

@app.route('/')
@login_required
def dashboard():
    """Main dashboard page"""
    try:
        # Ensure model is downloaded before serving dashboard
        ensure_model_downloaded()
        return render_template('dashboard.html', user=session)
    except Exception as e:
        logging.error(f"Error loading template: {str(e)}", exc_info=True)
        return f"Error loading template: {str(e)}", 500

@app.route('/ai-agent')
@app.route('/ai-agent/')
@login_required
def ai_agent_redirect():
    """Handle /ai-agent path"""
    try:
        # Ensure model is downloaded before serving dashboard
        ensure_model_downloaded()
        return render_template('dashboard.html', user=session)
    except Exception as e:
        return f"Error loading template: {str(e)}", 500


@app.route('/service-issues')
@app.route('/ai-agent/service-issues')
@login_required
def service_issues_page():
    """Per-service issue explorer page for selected time window."""
    service = str(request.args.get('service', '') or '').strip()
    namespace = str(request.args.get('namespace', '') or '').strip().lower()
    if namespace not in {'jupiter', 'venus'}:
        namespace = 'venus'
    return render_template(
        'service_issues.html',
        user=session,
        service_name=service,
        namespace=namespace,
    )

@app.route('/api/pod-details')
@app.route('/ai-agent/api/pod-details')
@login_required
def api_pod_details():
    """
    Return live k8s data for a service: pod describe, events, logs, rollout
    issue, and infra audit. Used by the service-issues detail page.
    """
    namespace = str(request.args.get('namespace', '') or '').strip().lower()
    service   = str(request.args.get('service', '') or '').strip()
    if namespace not in {'jupiter', 'venus'} or not service:
        return jsonify({'error': 'namespace and service are required'}), 400

    def _krun(args, timeout=12):
        try:
            r = subprocess.run(['kubectl'] + args, capture_output=True, text=True, timeout=timeout)
            return r.stdout if r.returncode == 0 else ''
        except Exception:
            return ''

    def _kjson_local(args, timeout=12):
        out = _krun(args + ['-o', 'json'], timeout=timeout)
        try:
            return json.loads(out) if out else None
        except Exception:
            return None

    # Background noise patterns that are NOT the root cause of a crash.
    # OTEL exporters, metrics reporters, and their stack traces are logged by
    # background threads throughout the pod's life — they appear at the very
    # end of a crashed container's log tail and drown out the real startup error.
    _NOISE_PATTERNS = [
        'io.opentelemetry', 'PeriodicMetricReader', 'HttpExporter',
        'Failed to export metrics', 'Exporter failed', 'otel-collector',
        'signoz-prod', 'okhttp3.internal', 'RetryInterceptor',
        'RealInterceptorChain.proceed', 'java.net.InetAddress',
        'RouteSelector', 'io.grpc', 'grpc.netty',
    ]
    _CRASH_PATTERNS = [
        'BeanCreationException', 'NoSuchBeanDefinitionException',
        'APPLICATION FAILED TO START', 'Error starting ApplicationContext',
        'Context initialization failed', 'LifecycleException',
        'could not locate propertysource', 'could not resolve placeholder',
        'SM_CONFIG', 'VAULT', 'application-vault',
        'GOOGLE_APPLICATION_CREDENTIALS', 'gcpKey.json',
        'FileNotFoundException', 'Unable to start', 'startup failed',
        'Failed to start', 'SEVERE', 'FATAL',
        'ErrImagePull', 'ImagePullBackOff',
    ]

    def _filter_noise(text: str) -> str:
        """Strip OTEL/metrics exporter noise, leaving crash-relevant lines."""
        if not text:
            return text
        out, skip_trace = [], False
        for line in text.splitlines():
            s = line.strip()
            if any(p in line for p in _NOISE_PATTERNS):
                skip_trace = True
                continue
            if skip_trace and (s.startswith('at ') or s.startswith('Caused by:') or s.startswith('...')):
                continue
            skip_trace = False
            out.append(line)
        return '\n'.join(out).strip()

    def _extract_crash_reason(text: str) -> str:
        """Pull out only the lines that describe the crash root cause."""
        if not text:
            return ''
        relevant = []
        for line in text.splitlines():
            if any(p.lower() in line.lower() for p in _CRASH_PATTERNS):
                relevant.append(line)
        return '\n'.join(relevant[:50])

    result = {
        'namespace': namespace, 'service': service,
        'pods': [], 'describe': '', 'logs': '', 'logs_previous': '',
        'logs_crash_reason': '',
        'events': [], 'rollout_issue': None, 'infra_issues': [],
        'infra_pr_url': None,
    }

    # --- Pod list ---
    pods_doc = _kjson_local(['get', 'pods', '-n', namespace, '-l', f'app={service}'], timeout=15)
    pod_items = (pods_doc or {}).get('items', []) or []

    failing_pod = ''
    for pod in pod_items:
        meta   = pod.get('metadata', {}) or {}
        status = pod.get('status', {}) or {}
        phase  = status.get('phase', '')
        ready  = False
        for c in (status.get('conditions', []) or []):
            if c.get('type') == 'Ready' and c.get('status') == 'True':
                ready = True
        pod_name = meta.get('name', '')
        result['pods'].append({
            'name': pod_name,
            'phase': phase,
            'ready': ready,
            'restarts': sum(
                cs.get('restartCount', 0)
                for cs in (status.get('containerStatuses', []) or [])
            ),
        })
        if not failing_pod and not ready and phase in ('Running', 'Pending', 'Failed', 'Unknown'):
            failing_pod = pod_name

    # use any pod if all are ready (user may click for healthy service too)
    if not failing_pod and pod_items:
        failing_pod = (pod_items[0].get('metadata', {}) or {}).get('name', '')

    if failing_pod:
        result['describe'] = '\n'.join(
            _krun(['describe', 'pod', failing_pod, '-n', namespace], timeout=15).splitlines()[:100]
        )
        # Current pod — more lines, filter background thread noise
        raw_logs = _krun(
            ['logs', failing_pod, '-n', namespace, '--tail=200'], timeout=15
        )
        result['logs'] = _filter_noise(raw_logs)

        # Previous crashed container — get the STARTUP section (first ~25 KB)
        # using --limit-bytes (no --tail) so we see the beginning of the run
        # where vault/config startup failures are printed, not the end where
        # background OTEL exporters spam noise before the container dies.
        raw_prev = _krun(
            ['logs', failing_pod, '-n', namespace, '--limit-bytes=25000', '--previous'],
            timeout=15
        )
        if not raw_prev:
            # Fallback: some runtimes don't support limit-bytes cleanly
            raw_prev = _krun(
                ['logs', failing_pod, '-n', namespace, '--tail=150', '--previous'],
                timeout=15
            )
        result['logs_previous'] = _filter_noise(raw_prev)
        result['logs_crash_reason'] = _extract_crash_reason(raw_prev) or _extract_crash_reason(raw_logs)
        events_doc = _kjson_local([
            'get', 'events', '-n', namespace,
            f'--field-selector=involvedObject.name={failing_pod}',
        ], timeout=12)
        for ev in (events_doc or {}).get('items', []) or []:
            result['events'].append({
                'time': ev.get('lastTimestamp', ev.get('eventTime', '')),
                'type': ev.get('type', ''),
                'reason': ev.get('reason', ''),
                'message': ev.get('message', ''),
            })
        result['events'].sort(key=lambda e: e.get('time', ''), reverse=True)
        result['events'] = result['events'][:20]

    # --- Rollout monitor issue ---
    if ROLLOUT_MONITOR_AVAILABLE:
        try:
            issue = _rollout_monitor.get_monitor().get_issue(namespace, service)
            if issue:
                result['rollout_issue'] = {
                    'summary': issue.get('summary', ''),
                    'root_cause': issue.get('root_cause', {}),
                    'fix_pr_url': issue.get('fix_pr_url'),
                }
        except Exception:
            pass

    # --- Infra audit ---
    # Pass 1: use in-memory results from _infra_audit_loop (populated every 5 min)
    svc_key = f"{namespace}/{service}"
    with _infra_audit_results_lock:
        cached_audit = _infra_audit_results.get(svc_key)
    if cached_audit:
        result['infra_issues'] = cached_audit.get('issues', [])

    # Pass 2: inline audit using live kubectl YAML (immediate, no repo clone needed)
    # Runs when cached result is absent or empty so the page is never blank
    if not result['infra_issues'] and INFRA_FIX_AVAILABLE:
        try:
            import tempfile as _tmpf
            import yaml as _audit_yaml
            dep_doc = _kjson_local(['get', 'deployment', service, '-n', namespace], timeout=12)
            if dep_doc:
                with _tmpf.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tf:
                    _audit_yaml.dump(dep_doc, tf, default_flow_style=False)
                    tmp_dep = tf.name
                dep_issues = _infra_fix.audit_deployment(tmp_dep, namespace)
                svc_issues: list = []
                svc_doc = _kjson_local(['get', 'service', service, '-n', namespace], timeout=12)
                if svc_doc:
                    with _tmpf.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tf2:
                        _audit_yaml.dump(svc_doc, tf2, default_flow_style=False)
                        tmp_svc = tf2.name
                    svc_issues = _infra_fix.audit_service(tmp_dep, tmp_svc)
                    try:
                        os.unlink(tmp_svc)
                    except OSError:
                        pass
                try:
                    os.unlink(tmp_dep)
                except OSError:
                    pass
                result['infra_issues'] = dep_issues + svc_issues
        except Exception as _ae:
            logging.debug(f"[pod-details] inline audit error: {_ae}")

    return jsonify(result)


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
            window_minutes = 5

        requested_limit = request.args.get('limit', type=int)
        if not requested_limit or requested_limit <= 0:
            requested_limit = 120 if window_minutes >= 1440 else (80 if window_minutes >= 720 else 40)
        incident_limit = min(max(requested_limit, 10), 500)

        namespace_filter = request.args.get('namespace', 'venus').strip().lower()
        if namespace_filter not in {'jupiter', 'venus'}:
            namespace_filter = 'venus'

        selected_scope_keys = set()
        selected_scope_namespaces = set()
        if agent_available and agent is not None and hasattr(agent, '_selected_services'):
            try:
                selected_services = agent._selected_services() or []
            except Exception:
                selected_services = []
            for item in selected_services:
                if not isinstance(item, dict):
                    continue
                svc_name = str(item.get('name', '') or '').strip()
                svc_ns = str(item.get('namespace', '') or '').strip().lower()
                if not svc_name or not svc_ns:
                    continue
                selected_scope_keys.add(f"{svc_ns}/{svc_name}")
                selected_scope_namespaces.add(svc_ns)

        now = datetime.now()
        cache_key = (
            f"incidents:{window_minutes}:{incident_limit}:{namespace_filter}:"
            f"{len(selected_scope_keys)}:{','.join(sorted(selected_scope_namespaces))}"
        )
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
                anomaly_services = []
                anomalies = incident.get('anomalies', []) if isinstance(incident.get('anomalies', []), list) else []
                for an in anomalies:
                    if not isinstance(an, dict):
                        continue
                    raw_service = str(an.get('service', '') or '').strip()
                    if not raw_service:
                        continue
                    if '/' in raw_service:
                        svc_ns, svc_name = raw_service.split('/', 1)
                        anomaly_services.append((svc_ns.strip().lower(), svc_name.strip()))
                    else:
                        anomaly_services.append(('', raw_service))

                if namespace_filter:
                    if not any(ns == namespace_filter for ns, _ in anomaly_services):
                        continue

                # Keep incidents only for currently monitored scope when available.
                if selected_scope_keys:
                    in_scope = False
                    for ns, name in anomaly_services:
                        if not ns or not name:
                            continue
                        if f"{ns}/{name}" in selected_scope_keys:
                            in_scope = True
                            break
                    if anomaly_services and not in_scope:
                        continue

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


@app.route('/api/codexa/repo-mappings', methods=['GET', 'POST'])
@app.route('/ai-agent/api/codexa/repo-mappings', methods=['GET', 'POST'])
@operator_required
def api_codexa_repo_mappings():
    """Get or update service->repo mapping used by CodeXA."""
    if request.method == 'GET':
        mapping = _load_repo_mappings()
        rows = []
        for key, repo in sorted(mapping.items()):
            ns = ''
            svc = key
            if '/' in key:
                ns, svc = key.split('/', 1)
            rows.append({'namespace': ns, 'service': svc, 'repo': repo})
        return jsonify({'status': 'success', 'count': len(rows), 'mappings': rows})

    payload = request.get_json(silent=True) or {}
    rows = payload.get('mappings', []) if isinstance(payload.get('mappings', []), list) else []
    normalized: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ns = str(row.get('namespace', '') or '').strip().lower()
        svc = str(row.get('service', '') or '').strip().lower()
        repo = str(row.get('repo', '') or '').strip()
        if not svc or not repo:
            continue
        key = f"{ns}/{svc}" if ns in {'jupiter', 'venus'} else svc
        normalized[key] = repo

    try:
        if not agent_available or agent is None or not isinstance(getattr(agent, 'config', None), dict):
            return jsonify({'status': 'error', 'message': 'Agent unavailable for config update'}), 503
        new_cfg = json.loads(json.dumps(agent.config))
        if not isinstance(new_cfg, dict):
            new_cfg = {}
        new_cfg.setdefault('pr_automation', {})
        if not isinstance(new_cfg.get('pr_automation', {}), dict):
            new_cfg['pr_automation'] = {}
        new_cfg.setdefault('codexa', {})
        if not isinstance(new_cfg.get('codexa', {}), dict):
            new_cfg['codexa'] = {}

        # Only update codexa.repo_mappings — pr_automation.repo_overrides has
        # branch-specific entries that should only be changed via configmap.
        new_cfg['codexa']['repo_mappings'] = dict(normalized)

        ok = agent.update_config(new_cfg)
        # Always push live mappings into repo_resolver memory so analysis
        # works immediately even if config.json write failed (ConfigMap is read-only).
        try:
            from codexa.services.repo_resolver import set_live_mappings as _slm
            _slm(normalized)
        except Exception:
            pass
        if not ok:
            note = str(getattr(agent, 'last_config_update_note', '') or '').strip()
            # Don't hard-fail — live mappings are already updated in memory
            logging.warning(f"config.json write failed ({note}), repo mappings live in memory only")

        out_rows = []
        for key, repo in sorted(normalized.items()):
            ns = ''
            svc = key
            if '/' in key:
                ns, svc = key.split('/', 1)
            out_rows.append({'namespace': ns, 'service': svc, 'repo': repo})
        return jsonify({'status': 'success', 'count': len(out_rows), 'mappings': out_rows})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/llm/settings', methods=['GET', 'POST'])
@app.route('/ai-agent/api/llm/settings', methods=['GET', 'POST'])
@operator_required
def api_llm_settings():
    """Get or update active AI-agent LLM model selection."""
    if request.method == 'GET':
        return jsonify(_current_ai_llm_state())

    payload = request.get_json(silent=True) or {}
    model = str(payload.get('model', '') or '').strip()
    if model.startswith('openai:'):
        bare = model.split(':', 1)[1]
        if bare in AI_AGENT_ALLOWED_MODELS:
            model = bare
    if model not in AI_AGENT_ALLOWED_MODELS:
        return jsonify({'error': 'Unsupported model'}), 400

    with _ai_llm_selection_lock:
        _save_ai_llm_selection(model)
        applied = _apply_ai_llm_model(model)
    return jsonify({'status': 'success', **applied})

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
        requested_model = str(request.args.get('model', '') or '').strip().lower()
        if not requested_model:
            requested_model = str(_load_ai_llm_selection().get('model', _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL) or (_ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL)).strip().lower()
        if not agent_available or agent is None:
            return jsonify({
                'corrections': 0,
                'patterns': 0,
                'accuracy': 0,
                'model_accuracy': 0,
                'feedback_count': 0,
                'incident_history': 0,
                'training_status': 'Not Connected',
                'last_training': 'Never',
                'model': requested_model,
            })

        incident_history_count = len(getattr(agent.learning_engine, 'incident_history', []))
        feedback_count = len(getattr(agent.learning_engine, 'feedback_data', []))
        correction_rules = len(getattr(agent.learning_engine, 'feedback_rules', {}))
        last_training = getattr(agent.learning_engine, 'last_training', None)

        learning_stats = get_learning_stats(llm_model=requested_model)
        runtime = get_rca_runtime_metrics(llm_model=requested_model)
        corrections = int(learning_stats.get('total_learnings', 0) or feedback_count or 0)
        patterns = int(learning_stats.get('namespaces_covered_count', 0) or len(learning_stats.get('namespaces_covered', []) or []) or correction_rules or 0)
        accuracy = float(learning_stats.get('accuracy_rate', 0) or 0)
        if accuracy <= 0:
            accuracy = float(runtime.get('rca_accuracy_rate', 0) or 0)

        return jsonify({
            'corrections': corrections,
            'patterns': patterns,
            'accuracy': int(round(accuracy)),
            'model_accuracy': int(round(accuracy)) if accuracy > 0 else 94,
            'feedback_count': feedback_count,
            'incident_history': incident_history_count,
            'training_status': f"Active ({correction_rules} feedback rules)",
            'last_training': last_training or 'online-correction-active',
            'model': requested_model,
        })
    except Exception as e:
        logger.error(f"Failed to get learning stats: {e}")
        return jsonify({'error': f'Failed to get learning stats: {str(e)}'}), 500

@app.route('/ai-agent/api/learning/stats')
def ai_agent_api_learning_stats():
    """Get machine learning statistics (prefixed route)"""
    return api_learning_stats()


@app.route('/api/jenkins/status')
@app.route('/ai-agent/api/jenkins/status')
def api_jenkins_status():
    """Return Jenkins connectivity and bot worker activity."""
    try:
        _start_jenkins_auto_rebuild_worker_if_needed()
    except Exception:
        pass
    return jsonify(_jenkins_status_payload())


@app.route('/api/jenkins/dispatch', methods=['POST'])
@app.route('/ai-agent/api/jenkins/dispatch', methods=['POST'])
def api_jenkins_dispatch():
    """Trigger Jenkins dispatcher job with hard concurrent worker limit."""
    payload = request.get_json(silent=True) or {}
    service_name = str(payload.get('service_name', '') or '').strip()
    target_env = str(payload.get('target_env', '') or payload.get('namespace', '') or '').strip().lower()
    pipeline_name = str(payload.get('pipeline_name', '') or '').strip()
    if service_name and target_env in {'venus', 'jupiter'} and pipeline_name:
        svc_key = str(service_name or '').strip().lower()
        with _jenkins_pipeline_override_lock:
            _jenkins_pipeline_overrides[f"{target_env}/{svc_key}"] = pipeline_name
            _save_jenkins_pipeline_overrides(_jenkins_pipeline_overrides)
    result, code = _jenkins_dispatch_result(service_name, target_env, source='manual_api', pipeline_name=pipeline_name)
    return jsonify(result), int(code)


@app.route('/api/jenkins/retry', methods=['POST'])
@app.route('/ai-agent/api/jenkins/retry', methods=['POST'])
def api_jenkins_retry():
    """Manually retry a failed Jenkins build - clears failed state and triggers rebuild."""
    payload = request.get_json(silent=True) or {}
    service_name = str(payload.get('service_name', '') or '').strip()
    namespace = str(payload.get('namespace', '') or '').strip().lower()

    if not service_name or namespace not in {'venus', 'jupiter'}:
        return jsonify({'status': 'error', 'message': 'service_name and namespace (venus/jupiter) required'}), 400

    state_key = f"{namespace}/{service_name}"

    # Clear the failed state to allow retry
    with _jenkins_auto_rebuild_lock:
        if state_key in _jenkins_auto_rebuild_state:
            entry = _jenkins_auto_rebuild_state[state_key]
            if entry.get('status') == 'failed':
                logger.info(f"JENKINS_RETRY: Clearing failed state for {state_key}")
                del _jenkins_auto_rebuild_state[state_key]
            else:
                return jsonify({'status': 'error', 'message': f'Service {state_key} is not in failed state (current: {entry.get("status")})'}), 400
        else:
            return jsonify({'status': 'error', 'message': f'No state found for {state_key}'}), 404

    # Trigger the rebuild
    result, code = _jenkins_dispatch_result(service_name, namespace, source='manual_retry')
    return jsonify(result), int(code)


@app.route('/api/jenkins/failed')
@app.route('/ai-agent/api/jenkins/failed')
def api_jenkins_failed():
    """Get list of services with failed builds."""
    failed = []
    with _jenkins_auto_rebuild_lock:
        for state_key, entry in _jenkins_auto_rebuild_state.items():
            if not isinstance(entry, dict):
                continue
            if entry.get('status') == 'failed':
                if '/' in state_key:
                    ns, svc = state_key.split('/', 1)
                else:
                    ns, svc = '', state_key
                failed.append({
                    'service': svc,
                    'namespace': ns,
                    'build_number': entry.get('build_number'),
                    'failure_reason': entry.get('failure_reason', 'Build failed'),
                    'failed_at': entry.get('last_ts').isoformat() if isinstance(entry.get('last_ts'), datetime) else str(entry.get('last_ts', '')),
                    'signature': entry.get('sig', ''),
                })
    return jsonify({'failed': failed, 'count': len(failed)})


@app.route('/api/jenkins/live')
@app.route('/ai-agent/api/jenkins/live')
def api_jenkins_live():
    """Return live Jenkins bot-to-pipeline assignment for dashboard."""
    return jsonify(_jenkins_live_payload())


@app.route('/api/jenkins/build/<int:build_number>/stages')
@app.route('/ai-agent/api/jenkins/build/<int:build_number>/stages')
def api_jenkins_build_stages(build_number: int):
    """Get pipeline stages for a specific build."""
    cfg = _jenkins_cfg()
    base_url = cfg.get('url', '')
    dispatcher = cfg.get('dispatcher', '')

    if not base_url or not dispatcher:
        return jsonify({'error': 'Jenkins not configured', 'stages': []}), 400

    build_url = f"{base_url}/job/{dispatcher}/{build_number}"
    stages = _jenkins_get_build_stages(cfg, build_url)
    return jsonify({'build_number': build_number, 'stages': stages})


@app.route('/api/jenkins/build/<int:build_number>/console')
@app.route('/ai-agent/api/jenkins/build/<int:build_number>/console')
def api_jenkins_build_console(build_number: int):
    """Get last N lines of console output for a build."""
    cfg = _jenkins_cfg()
    base_url = cfg.get('url', '')
    dispatcher = cfg.get('dispatcher', '')
    lines = int(request.args.get('lines', 200) or 200)

    if not base_url or not dispatcher:
        return jsonify({'error': 'Jenkins not configured'}), 400

    console_url = f"{base_url}/job/{dispatcher}/{build_number}/consoleText"
    try:
        req = urllib_request.Request(url=console_url, headers=_jenkins_headers(cfg), method='GET')
        with urllib_request.urlopen(req, timeout=10.0) as resp:
            text = resp.read().decode('utf-8', errors='replace')
            all_lines = text.split('\n')
            last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return jsonify({
                'build_number': build_number,
                'total_lines': len(all_lines),
                'returned_lines': len(last_lines),
                'console': '\n'.join(last_lines)
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jenkins/auto-scope', methods=['POST'])
@app.route('/ai-agent/api/jenkins/auto-scope', methods=['POST'])
def api_jenkins_auto_scope():
    """Set active namespace scope for auto image rebuild worker."""
    payload = request.get_json(silent=True) or {}
    ns = str(payload.get('namespace', '') or '').strip().lower()
    if ns not in {'venus', 'jupiter', 'all'}:
        return jsonify({'status': 'error', 'message': 'namespace must be venus|jupiter|all'}), 400
    global _jenkins_auto_rebuild_active_namespace
    with _jenkins_auto_rebuild_scope_lock:
        _jenkins_auto_rebuild_active_namespace = ns
    return jsonify({'status': 'success', 'active_namespace': ns})


# ============================================================================
# ArgoCD API Endpoints
# ============================================================================

@app.route('/api/argocd/status')
@app.route('/ai-agent/api/argocd/status')
def api_argocd_status():
    """Get ArgoCD connectivity and configuration status."""
    cfg = _argocd_cfg()
    if not cfg.get('enabled'):
        return jsonify({'status': 'disabled', 'enabled': False})

    if not cfg.get('url'):
        return jsonify({'status': 'not_configured', 'enabled': True, 'error': 'ArgoCD URL not set'})

    # Test connectivity
    try:
        result = _argocd_request('/api/v1/applications?limit=1', cfg)
        if 'error' in result:
            return jsonify({'status': 'error', 'enabled': True, 'url': cfg.get('url', ''), 'error': result['error']})
        return jsonify({
            'status': 'ok',
            'enabled': True,
            'url': cfg.get('url', ''),
            'app_pattern': cfg.get('app_pattern', ''),
        })
    except Exception as e:
        return jsonify({'status': 'error', 'enabled': True, 'url': cfg.get('url', ''), 'error': str(e)})


@app.route('/api/argocd/apps')
@app.route('/ai-agent/api/argocd/apps')
def api_argocd_apps():
    """List all ArgoCD applications."""
    namespace = request.args.get('namespace', '')
    apps = _argocd_list_apps(namespace)
    return jsonify({'apps': apps, 'count': len(apps)})


@app.route('/api/argocd/app/<namespace>/<service>')
@app.route('/ai-agent/api/argocd/app/<namespace>/<service>')
def api_argocd_app_status(namespace: str, service: str):
    """Get ArgoCD app status for a specific service."""
    status = _argocd_get_app_status(service, namespace)
    return jsonify(status)


@app.route('/api/fix-tracking/<namespace>/<service>')
@app.route('/ai-agent/api/fix-tracking/<namespace>/<service>')
def api_fix_tracking_status(namespace: str, service: str):
    """Get fix tracking status for a service."""
    fix_status = _get_fix_tracking_status(service, namespace)
    argocd_status = _argocd_get_app_status(service, namespace)
    return jsonify({
        'fix': fix_status,
        'argocd': argocd_status,
    })


# ============================================================================
# End ArgoCD API Endpoints
# ============================================================================


@app.route('/api/connection-test')
@app.route('/ai-agent/api/connection-test')
def api_connection_test():
    """Connectivity checks for dashboard integrations."""
    prom_status = {'status': 'unknown', 'url': ''}
    es_status = []

    if agent_available and agent is not None:
        prom = getattr(agent, 'prometheus', None)
        if prom is not None:
            prom_status['url'] = str(getattr(prom, 'base_url', '') or '')
            try:
                _ = prom.get_api_metrics()
                prom_status['status'] = 'ok'
            except Exception as e:
                prom_status['status'] = 'error'
                prom_status['error'] = str(e)

        configured_hosts = []
        if isinstance(getattr(agent, 'config', {}), dict):
            es_cfg = agent.config.get('elasticsearch', {}) if isinstance(agent.config.get('elasticsearch', {}), dict) else {}
            configured_hosts = es_cfg.get('hosts', []) if isinstance(es_cfg.get('hosts', []), list) else []

        for host in configured_hosts:
            row = {'host': str(host), 'status': 'unknown'}
            try:
                es_client = getattr(agent, 'elasticsearch', None)
                if es_client is None:
                    row['status'] = 'disabled'
                elif es_client.is_connected():
                    row['status'] = 'ok'
                else:
                    row['status'] = 'error'
            except Exception as e:
                row['status'] = 'error'
                row['error'] = str(e)
            es_status.append(row)

    return jsonify({
        'prometheus': prom_status,
        'elasticsearch': es_status,
        'jenkins': _jenkins_status_payload(),
    })

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
            return jsonify({'services': [], 'selected_services': [], 'max_selected_services': 0, 'source': 'none'})

        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(agent.config, dict) else {}
        configured_namespaces = monitoring_cfg.get('discovery_namespaces', [])
        if not isinstance(configured_namespaces, list):
            configured_namespaces = []
        base_namespaces = ['jupiter', 'venus']
        namespaces = []
        for ns in base_namespaces + configured_namespaces:
            value = str(ns or '').strip()
            if value and value not in namespaces:
                namespaces.append(value)
        if not namespaces:
            namespaces = ['jupiter', 'venus']

        discovered = []
        source = 'cluster'
        service_monitor = getattr(agent, 'service_monitor', None)
        if service_monitor is not None:
            try:
                discovered = service_monitor.discover_services(namespaces=namespaces)
            except Exception as e:
                logger.warning(f"Cluster service discovery failed: {e}")
                discovered = []

        if not discovered:
            es_client = getattr(agent, 'elasticsearch', None)
            if es_client is not None:
                try:
                    discovered = es_client.discover_services(limit=2000) or []
                    source = 'elasticsearch'
                except Exception as e:
                    logger.warning(f"Elasticsearch service discovery failed: {e}")

        selected = agent.config.get('monitored_services', []) if isinstance(agent.config, dict) else []
        if not isinstance(selected, list):
            selected = []

        return jsonify({
            'services': discovered,
            'selected_services': selected,
            'max_selected_services': int(agent.config.get('max_selected_services', 0) or 0),
            'source': source,
            'namespaces': namespaces
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
        return jsonify(_snapshot_service_status_payload_from_request(allow_stale=True))

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
                last_status_snapshot = {}
                try:
                    if isinstance(getattr(agent, '_last_service_status', None), dict):
                        last_status_snapshot = dict(getattr(agent, '_last_service_status', {}) or {})
                except Exception:
                    last_status_snapshot = {}

                if isinstance(last_status_snapshot, dict) and last_status_snapshot:
                    for key, svc in last_status_snapshot.items():
                        if not isinstance(svc, dict):
                            continue
                        name = str(svc.get('name', '') or '').strip()
                        namespace = str(svc.get('namespace', '') or '').strip() or 'unknown'
                        if not name and isinstance(key, str) and '/' in key:
                            _, name = key.split('/', 1)
                        if not name:
                            continue
                        norm_key = f"{namespace}/{name}"
                        fallback[norm_key] = dict(svc)

                registry_snapshot = {}
                try:
                    if hasattr(agent, 'service_registry') and hasattr(agent.service_registry, 'get_services_map'):
                        registry_snapshot = agent.service_registry.get_services_map() or {}
                except Exception:
                    registry_snapshot = {}

                if isinstance(registry_snapshot, dict) and registry_snapshot:
                    for key, svc in registry_snapshot.items():
                        if not isinstance(svc, dict):
                            continue
                        name = str(svc.get('name', '') or '').strip()
                        namespace = str(svc.get('namespace', '') or '').strip() or 'unknown'
                        if not name and isinstance(key, str) and '/' in key:
                            _, name = key.split('/', 1)
                        if not name:
                            continue
                        norm_key = f"{namespace}/{name}"
                        fallback[norm_key] = dict(svc)

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
        
        namespace_filter = request.args.get('namespace', 'venus').strip().lower()
        if namespace_filter not in {'jupiter', 'venus'}:
            namespace_filter = 'venus'
        status_filter = request.args.get('status', '').strip().lower()
        search_filter = request.args.get('search', '').strip().lower()
        request_cache_key = f"status-req:{window_minutes}:{page}:{page_size}:{namespace_filter}:{status_filter}:{search_filter}"

        cached_req_key = _service_status_cache.get('req_key')
        cached_ts = _service_status_cache.get('ts')
        cached_payload = _service_status_cache.get('data')
        if (
            cached_req_key == request_cache_key and
            cached_ts is not None and
            isinstance(cached_payload, dict) and
            (datetime.now() - cached_ts).total_seconds() < 1.2
        ):
            return jsonify(cached_payload)

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
                'scaled_down': 0,
                'unknown': 0
            }
            return ranking.get(status, 0)

        # Get selected services once
        logger.info("Getting real service status")
        selected_services_override = request.args.get('selected_services', default='', type=str) or ''
        selected_services = []
        if selected_services_override:
            try:
                parsed_override = json.loads(selected_services_override)
                if isinstance(parsed_override, list):
                    cleaned_override = []
                    seen_override = set()
                    for item in parsed_override:
                        if not isinstance(item, dict):
                            continue
                        svc_name = _canonical_name(str(item.get('name', '') or '').strip())
                        svc_ns = str(item.get('namespace', '') or '').strip().lower()
                        if not svc_name or svc_ns not in {'jupiter', 'venus'}:
                            continue
                        dedupe = (svc_ns, svc_name)
                        if dedupe in seen_override:
                            continue
                        seen_override.add(dedupe)
                        cleaned_override.append({'name': svc_name, 'namespace': svc_ns})
                    if cleaned_override:
                        selected_services = cleaned_override
            except Exception as parse_err:
                logger.warning(f"Ignoring invalid selected_services override: {parse_err}")

        if not selected_services:
            selected_services = agent._selected_services() if hasattr(agent, '_selected_services') else []
        selected_services = [
            svc for svc in selected_services
            if str((svc or {}).get('namespace', '') or '').strip().lower() == namespace_filter
        ]
        if search_filter:
            needle = search_filter.lower()
            selected_services = [
                svc for svc in selected_services
                if needle in str((svc or {}).get('name', '') or '').strip().lower()
            ]

        # Reconcile selected services with live namespace discovery to avoid stale/invalid names.
        discovered_names = set()
        if hasattr(agent, 'service_monitor') and agent.service_monitor is not None and hasattr(agent.service_monitor, 'discover_services'):
            try:
                discovered_rows = agent.service_monitor.discover_services(namespaces=[namespace_filter]) or []
                for item in discovered_rows:
                    if not isinstance(item, dict):
                        continue
                    item_ns = str(item.get('namespace', '') or '').strip().lower()
                    item_name = _canonical_name(str(item.get('name', '') or '').strip())
                    if item_ns == namespace_filter and item_name:
                        discovered_names.add(item_name)
            except Exception:
                discovered_names = set()

        if discovered_names:
            selected_services = [
                svc for svc in selected_services
                if _canonical_name(str((svc or {}).get('name', '') or '').strip()) in discovered_names
            ]

        if not selected_services:
            # Hard guard: never run a full status cycle with zero scope when there is
            # still actionable service evidence in config/incidents.
            fallback_selected = []
            seen_selected = set()

            configured_selected = []
            if isinstance(getattr(agent, 'config', {}), dict):
                configured_selected = agent.config.get('monitored_services', []) or []
            for item in configured_selected:
                if not isinstance(item, dict):
                    continue
                svc_name = _canonical_name(str(item.get('name', '') or '').strip())
                svc_ns = str(item.get('namespace', '') or '').strip().lower()
                if not svc_name or svc_ns not in {'jupiter', 'venus'}:
                    continue
                if namespace_filter and svc_ns != namespace_filter.lower():
                    continue
                if search_filter and search_filter not in svc_name.lower():
                    continue
                key = (svc_ns, svc_name)
                if key in seen_selected:
                    continue
                seen_selected.add(key)
                fallback_selected.append({'name': svc_name, 'namespace': svc_ns})

            if not fallback_selected:
                for incident in list(getattr(agent, 'active_incidents', []) or [])[-200:]:
                    if not isinstance(incident, dict):
                        continue
                    raw_service = str(incident.get('service', '') or '').strip()
                    if not raw_service:
                        continue
                    if '/' in raw_service:
                        inc_ns, inc_name = raw_service.split('/', 1)
                    else:
                        inc_ns, inc_name = 'jupiter', raw_service
                    inc_ns = str(inc_ns or '').strip().lower()
                    inc_name = _canonical_name(str(inc_name or '').strip())
                    if inc_ns not in {'jupiter', 'venus'} or not inc_name:
                        continue
                    if namespace_filter and inc_ns != namespace_filter.lower():
                        continue
                    if search_filter and search_filter not in inc_name.lower():
                        continue
                    key = (inc_ns, inc_name)
                    if key in seen_selected:
                        continue
                    seen_selected.add(key)
                    fallback_selected.append({'name': inc_name, 'namespace': inc_ns})

            if fallback_selected:
                selected_services = fallback_selected
                logger.warning(
                    "Recovered selected service scope from fallback sources: %s services",
                    len(selected_services)
                )

        selected_total_count = len([svc for svc in selected_services if isinstance(svc, dict)])

        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
        status_window_cap_minutes = int(monitoring_cfg.get('service_status_window_cap_minutes', 360) or 360)
        status_window_cap_minutes = max(5, min(status_window_cap_minutes, 720))
        # Always cap heavy live service-status queries to avoid ingress/gateway timeouts.
        # requested_window_minutes is still returned to UI for transparency.
        effective_window_minutes = min(window_minutes, status_window_cap_minutes)

        total_selected = selected_total_count
        now = datetime.now()

        full_cache_seconds = int(monitoring_cfg.get('full_status_cache_seconds', 45) or 45)
        full_key = _snapshot_cache_key(effective_window_minutes, selected_services)
        max_cache_age = max(5, min(full_cache_seconds, 300))

        full_status = {}
        progressive_key = _progressive_cache_key(effective_window_minutes, selected_services)
        progressive_payload = {}
        if total_selected > 0:
            _schedule_progressive_build(progressive_key, effective_window_minutes, selected_services)
            progressive_payload = _progressive_cache_get(progressive_key)
            progressive_services = progressive_payload.get('services') if isinstance(progressive_payload.get('services'), dict) else {}
            if progressive_services:
                full_status = progressive_services

        if not full_status and total_selected <= 0:
            with _full_service_status_lock:
                cached_key = _full_service_status_cache.get('key')
                cached_ts = _full_service_status_cache.get('ts')
                cached_data = _full_service_status_cache.get('data')

            cache_matches = cached_key == full_key and isinstance(cached_data, dict)
            cache_fresh = (
                cache_matches and
                cached_ts is not None and
                (now - cached_ts).total_seconds() < max_cache_age
            )

            if not cache_fresh:
                external = _external_cache_get(full_key)
                external_data = external.get('data') if isinstance(external.get('data'), dict) else None
                external_ts_raw = str(external.get('ts', '') or '')
                external_fresh = False
                if external_data is not None and external_ts_raw:
                    try:
                        external_ts = datetime.fromisoformat(external_ts_raw.replace('Z', '+00:00'))
                        if external_ts.tzinfo is not None:
                            external_ts = external_ts.astimezone().replace(tzinfo=None)
                        external_fresh = (now - external_ts).total_seconds() < max_cache_age
                    except Exception:
                        external_fresh = False

                if external_fresh and external_data is not None:
                    full_status = external_data
                    with _full_service_status_lock:
                        _full_service_status_cache['key'] = full_key
                        _full_service_status_cache['ts'] = now
                        _full_service_status_cache['data'] = full_status
                        _full_service_status_cache['refreshing'] = False
                        _full_service_status_cache['refresh_key'] = None
                    cache_matches = True
                    cache_fresh = True

            if cache_fresh:
                full_status = cached_data or {}
            elif cache_matches:
                # Stale-while-revalidate: serve stale snapshot immediately and refresh in background.
                full_status = cached_data or {}
                _schedule_full_status_refresh(full_key, effective_window_minutes, selected_services)
            else:
                # First load for this scope/window: compute once, then cache.
                full_status = agent.get_service_status(
                    minutes=effective_window_minutes,
                    services_subset=selected_services,
                    include_deep_inspection=False
                ) if hasattr(agent, 'get_service_status') else {}
                if not isinstance(full_status, dict):
                    full_status = {}
                if full_status:
                    with _full_service_status_lock:
                        _full_service_status_cache['key'] = full_key
                        _full_service_status_cache['ts'] = now
                        _full_service_status_cache['data'] = full_status
                        _full_service_status_cache['refreshing'] = False
                        _full_service_status_cache['refresh_key'] = None
                else:
                    # Do not drop to empty immediately on transient first-load misses.
                    # Reuse recent non-empty snapshot and trigger background refresh.
                    full_status = _get_recent_nonempty_full_snapshot(max_age_seconds=max_cache_age * 3)
                    _schedule_full_status_refresh(full_key, effective_window_minutes, selected_services)

        if not full_status and total_selected > 0:
            # Return lightweight selected-scope placeholder quickly while progressive
            # worker keeps filling Redis/in-memory frame cache in background.
            quick_fallback = {}
            for item in selected_services:
                if not isinstance(item, dict):
                    continue
                svc_name = _canonical_name(str(item.get('name', '') or '').strip())
                svc_ns = str(item.get('namespace', '') or '').strip().lower()
                if not svc_name or not svc_ns:
                    continue
                k = f"{svc_ns}/{svc_name}"
                quick_fallback[k] = {
                    'name': svc_name,
                    'namespace': svc_ns,
                    'status': 'unknown',
                    'metrics': {
                        'service': svc_name,
                        'total_log_entries': 0,
                        'error_count': 0,
                        'warning_count': 0,
                        'error_rate': 0.0,
                        'timestamp': datetime.now().isoformat(),
                        'latest_timestamp': ''
                    },
                    'recent_errors': [],
                    'pod_status': {'status': 'unknown', 'pods': [], 'namespace': svc_ns}
                }
            if quick_fallback:
                full_status = quick_fallback

        # Guard against transient collector/query failures returning empty payloads.
        # Keep configured services visible in dashboard instead of flipping to blank.
        if not full_status:
            full_status = _get_recent_nonempty_full_snapshot(max_age_seconds=max_cache_age * 3)

        if not full_status:
            full_status = _selected_services_fallback()
            if namespace_filter:
                full_status = {
                    k: v for k, v in full_status.items()
                if str((v or {}).get('namespace', '') or '').strip().lower() == namespace_filter
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

        if not full_status:
            incident_seed = {}

            def _collect_incident_seed(incident_obj):
                if not isinstance(incident_obj, dict):
                    return
                raw_service = str(incident_obj.get('service', '') or '').strip()
                if not raw_service:
                    anomalies = incident_obj.get('anomalies', []) if isinstance(incident_obj.get('anomalies', []), list) else []
                    if anomalies and isinstance(anomalies[0], dict):
                        raw_service = str(anomalies[0].get('service', '') or '').strip()
                if not raw_service:
                    return
                if '/' in raw_service:
                    seed_ns, seed_name = raw_service.split('/', 1)
                else:
                    seed_ns, seed_name = 'jupiter', raw_service
                seed_ns = str(seed_ns or '').strip().lower()
                seed_name = _canonical_name(str(seed_name or '').strip())
                if seed_ns not in {'jupiter', 'venus'} or not seed_name:
                    return
                if namespace_filter and seed_ns != namespace_filter.lower():
                    return
                if search_filter and search_filter not in seed_name.lower():
                    return
                seed_key = f"{seed_ns}/{seed_name}"
                if seed_key in incident_seed:
                    return
                incident_issue = str(
                    incident_obj.get('issue', '') or
                    incident_obj.get('issue_summary', '') or
                    f"Exact Issue: {seed_key} incident detected"
                )
                incident_ts = str(incident_obj.get('timestamp', '') or datetime.now().isoformat())
                incident_seed[seed_key] = {
                    'name': seed_name,
                    'namespace': seed_ns,
                    'status': 'degraded',
                    'metrics': {
                        'service': seed_name,
                        'total_log_entries': 0,
                        'error_count': 1,
                        'warning_count': 0,
                        'error_rate': 0.0,
                        'pod_total_count': 0,
                        'pod_ready_count': 0,
                        'pod_running_count': 0,
                        'pod_issue_count': 0,
                        'timestamp': datetime.now().isoformat(),
                        'latest_timestamp': incident_ts
                    },
                    'recent_errors': [{
                        'timestamp': incident_ts,
                        'message': incident_issue,
                        'severity': 'ERROR'
                    }],
                    'pod_status': {'status': 'unknown', 'pods': [], 'namespace': seed_ns}
                }

            for inc in list(getattr(agent, 'active_incidents', []) or [])[-400:]:
                _collect_incident_seed(inc)

            if hasattr(agent, 'get_recent_incidents'):
                try:
                    for inc in (agent.get_recent_incidents(200) or []):
                        _collect_incident_seed(inc)
                except Exception:
                    pass

            if incident_seed:
                full_status = incident_seed
                logger.warning("Recovered full_status from incident seed: %s", len(full_status))

        if not full_status and hasattr(agent, 'service_monitor') and agent.service_monitor is not None:
            try:
                monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
                discovery_namespaces = monitoring_cfg.get('discovery_namespaces', ['jupiter', 'venus'])
                if not isinstance(discovery_namespaces, list) or not discovery_namespaces:
                    discovery_namespaces = ['jupiter', 'venus']

                discovered = agent.service_monitor.discover_services(namespaces=discovery_namespaces)
                discovered_map = {}
                for item in discovered or []:
                    if not isinstance(item, dict):
                        continue
                    ns = str(item.get('namespace', '') or '').strip().lower()
                    name = _canonical_name(str(item.get('name', '') or '').strip())
                    if ns not in {'jupiter', 'venus'} or not name:
                        continue
                    if namespace_filter and ns != namespace_filter.lower():
                        continue
                    if search_filter and search_filter not in name.lower():
                        continue
                    key = f"{ns}/{name}"
                    discovered_map[key] = {
                        'name': name,
                        'namespace': ns,
                        'status': 'unknown',
                        'metrics': {
                            'service': name,
                            'total_log_entries': 0,
                            'error_count': 0,
                            'warning_count': 0,
                            'error_rate': 0.0,
                            'pod_total_count': 0,
                            'pod_ready_count': 0,
                            'pod_running_count': 0,
                            'pod_issue_count': 0,
                            'timestamp': datetime.now().isoformat(),
                            'latest_timestamp': ''
                        },
                        'recent_errors': [],
                        'pod_status': {'status': 'unknown', 'pods': [], 'namespace': ns}
                    }

                if discovered_map:
                    full_status = discovered_map
                    logger.warning("Recovered full_status from cluster discovery: %s", len(full_status))
            except Exception as discover_err:
                logger.warning(f"Cluster discovery fallback failed: {discover_err}")

        selected_scope_key = _selected_services_scope_key(selected_services)
        cache_key = f"status:{window_minutes}:{page}:{page_size}:{namespace_filter}:{status_filter}:{search_filter}:{selected_scope_key}"

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

        # Ensure service table root-cause column is never empty for unhealthy rows.
        # If log-derived exact errors are unavailable, derive deterministic issue text
        # from pod state/restart/readiness evidence.
        for svc_key, svc in list(service_status.items()):
            if not isinstance(svc, dict):
                continue
            svc_state = str(svc.get('status', 'unknown') or 'unknown').lower()
            if svc_state not in {'degraded', 'offline', 'down', 'pending', 'warning'}:
                continue

            current_errors = svc.get('recent_errors', []) if isinstance(svc.get('recent_errors', []), list) else []
            has_actionable_error = False
            for err in current_errors:
                if not isinstance(err, dict):
                    continue
                msg = str(err.get('message', '') or '').strip()
                if msg and msg.lower() not in {'n/a', 'unknown', 'none'}:
                    has_actionable_error = True
                    break
            if has_actionable_error:
                continue

            pod_status = svc.get('pod_status', {}) if isinstance(svc.get('pod_status', {}), dict) else {}
            pod_entries = pod_status.get('pods', []) if isinstance(pod_status.get('pods', []), list) else []
            metrics = svc.get('metrics', {}) if isinstance(svc.get('metrics', {}), dict) else {}

            reasons = []
            restart_hints = []
            for pod in pod_entries:
                if not isinstance(pod, dict):
                    continue
                pod_name = str(pod.get('name', '') or '').strip()
                reason = str(pod.get('reason', '') or '').strip()
                ready = bool(pod.get('ready', False))
                restarts = int(pod.get('restarts', 0) or 0)
                if reason:
                    reasons.append(f"{pod_name}:{reason}" if pod_name else reason)
                if restarts > 0:
                    restart_hints.append(f"{pod_name}:{restarts}" if pod_name else str(restarts))
                if not ready and not reason and pod_name:
                    reasons.append(f"{pod_name}:not-ready")

            pod_total = int(metrics.get('pod_total_count', 0) or 0)
            pod_running = int(metrics.get('pod_running_count', 0) or 0)
            pod_ready = int(metrics.get('pod_ready_count', 0) or 0)
            pod_issue = int(metrics.get('pod_issue_count', 0) or 0)

            issue_text = ''
            structured_issue = ''
            structured_reason = ''
            structured_root_cause = ''

            if reasons:
                issue_text = f"Pod issues: {' | '.join(reasons[:3])}"
                # Parse structured from first pod's reason
                first_reason = reasons[0].split(':', 1)[-1] if reasons else ''
                parsed = _parse_error_to_structured(first_reason, pod_status, svc_state)
                structured_issue = parsed.get('issue', '') or 'PodIssue'
                structured_reason = parsed.get('reason', '') or first_reason
                structured_root_cause = parsed.get('root_cause', '') or issue_text
            elif pod_total == 0:
                issue_text = f"No pods found for {svc_key}; deployment may have replicas=0 or selector mismatch"
                structured_issue = 'NoPods'
                structured_reason = 'No pods found'
                structured_root_cause = 'Deployment may have replicas=0 or selector mismatch'
            elif pod_ready == 0 and pod_total > 0:
                issue_text = f"Pods are not ready (total={pod_total}, running={pod_running}, ready={pod_ready}, issue={pod_issue})"
                structured_issue = 'PodsNotReady'
                structured_reason = f"Ready: {pod_ready}/{pod_total}"
                structured_root_cause = 'Pods exist but none are in ready state'
            elif restart_hints:
                issue_text = f"Frequent pod restarts: {' | '.join(restart_hints[:3])}"
                structured_issue = 'FrequentRestarts'
                structured_reason = f"{len(restart_hints)} pod(s) restarting"
                structured_root_cause = 'Pods are restarting frequently - check application logs'
            else:
                issue_text = f"Service unhealthy ({svc_state}) with pod health mismatch (total={pod_total}, running={pod_running}, ready={pod_ready}, issue={pod_issue})"
                structured_issue = 'Unhealthy'
                structured_reason = f"Status: {svc_state}"
                structured_root_cause = issue_text

            svc['recent_errors'] = [{
                'timestamp': datetime.now().isoformat(),
                'message': issue_text,
                'severity': 'POD_ERROR',
                # Structured fields for 3-column display
                'issue': structured_issue,
                'reason': structured_reason,
                'root_cause': structured_root_cause
            }]
            service_status[svc_key] = svc

        if not service_status:
            incident_seed = {}
            for incident in list(getattr(agent, 'active_incidents', []) or [])[-300:]:
                if not isinstance(incident, dict):
                    continue
                raw_service = str(incident.get('service', '') or '').strip()
                if not raw_service:
                    continue
                if '/' in raw_service:
                    seed_ns, seed_name = raw_service.split('/', 1)
                else:
                    seed_ns, seed_name = 'jupiter', raw_service
                seed_ns = str(seed_ns or '').strip().lower()
                seed_name = _canonical_name(str(seed_name or '').strip())
                if seed_ns not in {'jupiter', 'venus'} or not seed_name:
                    continue
                seed_key = f"{seed_ns}/{seed_name}"
                if seed_key in incident_seed:
                    continue
                incident_issue_text = str(incident.get('issue', '') or incident.get('issue_summary', '') or f"Exact Issue: {seed_key} incident detected")
                incident_parsed = _parse_error_to_structured(incident_issue_text, {}, 'degraded')
                incident_seed[seed_key] = {
                    'name': seed_name,
                    'namespace': seed_ns,
                    'status': 'degraded',
                    'metrics': {
                        'service': seed_name,
                        'total_log_entries': 0,
                        'error_count': 1,
                        'warning_count': 0,
                        'error_rate': 0.0,
                        'pod_total_count': 0,
                        'pod_ready_count': 0,
                        'pod_running_count': 0,
                        'pod_issue_count': 0,
                        'timestamp': datetime.now().isoformat(),
                        'latest_timestamp': str(incident.get('timestamp', '') or '')
                    },
                    'recent_errors': [{
                        'timestamp': str(incident.get('timestamp', datetime.now().isoformat()) or datetime.now().isoformat()),
                        'message': incident_issue_text,
                        'severity': 'ERROR',
                        # Structured fields for 3-column display
                        'issue': incident_parsed.get('issue', '') or 'Incident',
                        'reason': incident_parsed.get('reason', '') or 'Incident detected',
                        'root_cause': incident_parsed.get('root_cause', '') or incident_issue_text
                    }],
                    'pod_status': {'status': 'unknown', 'pods': [], 'namespace': seed_ns}
                }
            if incident_seed:
                service_status = incident_seed
                logger.warning("Recovered service status from active incidents: %s", len(service_status))

        # Enforce strict selected-window evidence for logs/exceptions/traces-backed root cause.
        # On very large service sets this can be expensive; make it adaptive.
        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
        strict_recent_error_max_services = int(monitoring_cfg.get('strict_recent_error_max_services', 0) or 0)
        strict_recent_error_map = {}
        if strict_recent_error_max_services > 0 and len(selected_services) <= max(1, strict_recent_error_max_services):
            strict_recent_error_map = _compute_service_recent_errors_for_window(
                effective_window_minutes,
                selected_services=selected_services
            )

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

                    # Parse structured fields from exact_issue
                    parsed = _parse_error_to_structured(exact_issue, svc.get('pod_status', {}), svc_status)

                    if svc_errors:
                        svc_errors[0]['message'] = exact_issue
                        svc_errors[0]['issue'] = parsed.get('issue', '')
                        svc_errors[0]['reason'] = parsed.get('reason', '')
                        svc_errors[0]['root_cause'] = parsed.get('root_cause', '') or exact_issue
                    else:
                        svc_errors = [{
                            'timestamp': datetime.now().isoformat(),
                            'message': exact_issue,
                            'severity': 'ERROR',
                            'issue': parsed.get('issue', ''),
                            'reason': parsed.get('reason', ''),
                            'root_cause': parsed.get('root_cause', '') or exact_issue
                        }]
                    svc['recent_errors'] = svc_errors

                filtered[key] = svc
            service_status = filtered

            # Post-process: Ensure ALL services have structured fields in recent_errors
            for svc_key, svc in list(service_status.items()):
                if not isinstance(svc, dict):
                    continue
                errors = svc.get('recent_errors', [])
                if not isinstance(errors, list):
                    continue
                for err in errors:
                    if not isinstance(err, dict):
                        continue
                    # If issue/reason not set, parse from message
                    if not err.get('issue') or not err.get('reason'):
                        msg = str(err.get('message', '') or err.get('root_cause', '') or '')
                        if msg:
                            parsed = _parse_error_to_structured(msg, svc.get('pod_status', {}), str(svc.get('status', '')))
                            if not err.get('issue'):
                                err['issue'] = parsed.get('issue', '')
                            if not err.get('reason'):
                                err['reason'] = parsed.get('reason', '')
                            if not err.get('root_cause'):
                                err['root_cause'] = parsed.get('root_cause', '') or msg

            # Attach repository mapping for CodeXA clone/analysis.
            repo_mappings = _load_repo_mappings()
            for _, svc in list(service_status.items()):
                if not isinstance(svc, dict):
                    continue
                ns = str(svc.get('namespace', '') or '').strip().lower()
                name = str(svc.get('name', '') or '').strip()
                mapped_repo = _resolve_repo_for_service(ns, name, repo_mappings)
                if mapped_repo:
                    svc['repo_name'] = mapped_repo

            # Store structured issues for per-service error explorer (6h retention).
            _record_service_status_issues(service_status)

            # If filters intentionally narrow results to zero, return empty dataset (no fallback warning)
            if not service_status and (namespace_filter or status_filter or search_filter):
                # For reliability, reuse recent non-empty filtered snapshot when available.
                base_filter_key = f"base:{namespace_filter}:{status_filter}:{search_filter}"
                cached_base_key = _last_nonempty_service_status_cache.get('key')
                cached_base_ts = _last_nonempty_service_status_cache.get('ts')
                cached_base_items = _last_nonempty_service_status_cache.get('items')
                if (
                    cached_base_key == base_filter_key and
                    cached_base_ts is not None and
                    isinstance(cached_base_items, dict) and
                    cached_base_items and
                    (datetime.now() - cached_base_ts).total_seconds() < 600
                ):
                    service_status = dict(cached_base_items)
                else:
                    return jsonify(_empty_paginated())
        
        # Stable ordering + page slicing after filters
        total = len(service_status)

        # Keep base fallback key independent from time window so table does not
        # blank when operators switch 5m/30m/12h filters during transient lag.
        base_filter_key = f"base:{namespace_filter}:{status_filter}:{search_filter}"
        if total > 0:
            _last_nonempty_service_status_cache['key'] = base_filter_key
            _last_nonempty_service_status_cache['ts'] = now
            _last_nonempty_service_status_cache['items'] = dict(service_status)
        else:
            cached_base_key = _last_nonempty_service_status_cache.get('key')
            cached_base_ts = _last_nonempty_service_status_cache.get('ts')
            cached_base_items = _last_nonempty_service_status_cache.get('items')
            if (
                cached_base_key == base_filter_key and
                cached_base_ts is not None and
                isinstance(cached_base_items, dict) and
                cached_base_items and
                (now - cached_base_ts).total_seconds() < 300
            ):
                # Reuse last known-good filtered payload to avoid blank table flicker.
                service_status = dict(cached_base_items)
                total = len(service_status)

            # Final fallback: derive filtered view from recent non-empty full snapshot.
            if total == 0:
                recent_full = _get_recent_nonempty_full_snapshot(max_age_seconds=900)
                if isinstance(recent_full, dict) and recent_full:
                    recovered = {}
                    for key, svc in recent_full.items():
                        if not isinstance(svc, dict):
                            continue
                        svc_name = str((svc or {}).get('name', '') or '').strip().lower()
                        svc_ns = str((svc or {}).get('namespace', '') or '').strip().lower()
                        svc_state = str((svc or {}).get('status', '') or '').strip().lower()
                        if namespace_filter and svc_ns != namespace_filter.lower():
                            continue
                        if status_filter and svc_state != status_filter:
                            continue
                        if search_filter and search_filter not in svc_name and search_filter not in str(key).lower():
                            continue
                        recovered[key] = svc

                    if recovered:
                        service_status = recovered
                        total = len(service_status)
                        _last_nonempty_service_status_cache['key'] = base_filter_key
                        _last_nonempty_service_status_cache['ts'] = now
                        _last_nonempty_service_status_cache['items'] = dict(service_status)

        # Compute summary from the final post-fallback filtered payload.
        summary_all_services = selected_total_count if selected_total_count > 0 else len(service_status)
        summary_down_degraded = len([
            1 for _, s in service_status.items()
            if str(s.get('status', '')).lower() in {'degraded', 'offline', 'down', 'pending'}
        ])
        summary_unresolved_alerts = len([
            1 for incident in getattr(agent, 'active_incidents', [])
            if str(incident.get('status', '')).lower() == 'active'
        ])

        status_keys = ['healthy', 'warning', 'degraded', 'down', 'pending', 'unknown', 'scaled_down', 'offline']
        status_counts = {key: 0 for key in status_keys}
        for _, svc in service_status.items():
            if not isinstance(svc, dict):
                continue
            raw_status = str(svc.get('status', 'unknown') or 'unknown').strip().lower()
            mapped = raw_status if raw_status in status_counts else 'unknown'
            status_counts[mapped] = int(status_counts.get(mapped, 0) or 0) + 1
        status_total = int(sum(status_counts.values()) or 0)
        status_percentages = {}
        for key, count in status_counts.items():
            status_percentages[key] = round((float(count) / float(status_total) * 100.0), 2) if status_total > 0 else 0.0

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

        try:
            _refresh_page_services_with_live_pods(service_status)
        except Exception:
            pass

        page_filter_key = f"page:{namespace_filter}:{status_filter}:{search_filter}:{current_page}:{page_size}"

        # If we got an empty response, prefer recent non-empty payload for same page/filter.
        if not service_status:
            cached_page_key = _last_nonempty_page_payload_cache.get('key')
            cached_page_ts = _last_nonempty_page_payload_cache.get('ts')
            cached_page_payload = _last_nonempty_page_payload_cache.get('payload')
            if (
                cached_page_key == page_filter_key and
                cached_page_ts is not None and
                isinstance(cached_page_payload, dict) and
                (now - cached_page_ts).total_seconds() < 900
            ):
                stale_payload = dict(cached_page_payload)
                stale_payload['stale'] = True
                stale_payload['stale_reason'] = 'reused_last_nonempty_page'
                _service_status_cache['req_key'] = request_cache_key
                _service_status_cache['key'] = cache_key
                _service_status_cache['ts'] = now
                _service_status_cache['data'] = stale_payload
                return jsonify(stale_payload)
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
            'summary': {
                'all_services': summary_all_services,
                'down_degraded': summary_down_degraded,
                'unresolved_alerts': summary_unresolved_alerts
            },
            'status_distribution': {
                'counts': status_counts,
                'percentages': status_percentages,
                'total': status_total
            },
            'debug': {
                'selected_scope_count': len(selected_services or []),
                'selected_scope_total': selected_total_count,
                'returned_count': len(service_status or {}),
                'source_window_minutes': effective_window_minutes,
                'progressive_completed_frames': int(progressive_payload.get('completed_frames', 0) or 0),
                'progressive_total_frames': int(progressive_payload.get('total_frames', 0) or 0),
                'progressive_done': bool(progressive_payload.get('done', False))
            },
            'requested_window_minutes': window_minutes,
            'evidence_window_minutes': effective_window_minutes,
            'evidence_generated_at': datetime.now().isoformat(),
            'stale': False
        }
        if service_status:
            _last_nonempty_page_payload_cache['key'] = page_filter_key
            _last_nonempty_page_payload_cache['ts'] = now
            _last_nonempty_page_payload_cache['payload'] = dict(payload)
        _service_status_cache['req_key'] = request_cache_key
        _service_status_cache['key'] = cache_key
        _service_status_cache['ts'] = now
        _service_status_cache['data'] = payload
        return jsonify(payload)
    except Exception as e:
        logger.error(f"Failed to get service status: {e}", exc_info=True)
        # Return selected services fallback when live status query errors out.
        fallback_services = _selected_services_fallback()
        fallback_page = request.args.get('page', default=1, type=int)
        if not fallback_page or fallback_page < 1:
            fallback_page = 1
        fallback_page_size = request.args.get('page_size', default=20, type=int)
        if not fallback_page_size or fallback_page_size < 1:
            fallback_page_size = 20
        fallback_page_size = min(fallback_page_size, 100)
        ordered = sorted(fallback_services.items(), key=lambda item: item[0])
        total = len(ordered)
        total_pages = (total + fallback_page_size - 1) // fallback_page_size if total > 0 else 0
        current_page = min(fallback_page, total_pages) if total_pages > 0 else 1
        start = (current_page - 1) * fallback_page_size
        end = start + fallback_page_size
        fallback_items = ordered[start:end]
        fallback_payload = {key: value for key, value in fallback_items}
        return jsonify({
            'services': fallback_payload,
            'pagination': {
                'page': current_page,
                'page_size': fallback_page_size,
                'total': total,
                'total_pages': total_pages,
                'has_prev': current_page > 1,
                'has_next': total_pages > 0 and current_page < total_pages
            },
            'stale': True,
            'stale_reason': 'exception_fallback'
        })
    
@app.route('/ai-agent/api/service-status')
def ai_agent_api_service_status():
    """Get real-time status of all monitored services (prefixed route)"""
    return api_service_status()


@app.route('/api/service-issues')
@app.route('/ai-agent/api/service-issues')
@login_required
def api_service_issues():
    """Return all captured issues/errors for one service in selected time window."""
    namespace = str(request.args.get('namespace', '') or '').strip().lower()
    service = str(request.args.get('service', '') or '').strip()
    if namespace not in {'jupiter', 'venus'}:
        return jsonify({'status': 'error', 'message': 'namespace must be jupiter or venus'}), 400
    if not service:
        return jsonify({'status': 'error', 'message': 'service is required'}), 400

    start_dt, end_dt, effective_minutes, clamped = _resolve_time_window_from_request(default_minutes=60, max_minutes=360)
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    limit = request.args.get('limit', default=500, type=int) or 500
    limit = max(50, min(limit, 1000))

    service_key = _service_issue_storage_key(namespace, service)
    with _service_issue_store_lock:
        _prune_service_issue_store_locked(datetime.utcnow())
        stored_rows = list(_service_issue_store.get(service_key, []))

    in_window_rows = []
    for row in stored_rows:
        seen_dt = _parse_iso_datetime(str(row.get('last_seen_at', '') or row.get('detected_at', '') or ''))
        if seen_dt is None:
            continue
        if start_dt <= seen_dt <= end_dt:
            in_window_rows.append(dict(row))

    es_rows = _collect_es_actionable_service_issues(
        namespace=namespace,
        service=service,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=limit,
    )

    merged_by_fp: Dict[str, Dict[str, Any]] = {}
    for item in in_window_rows + es_rows:
        fp = str(item.get('fingerprint', '') or '')
        if not fp:
            fp = _event_fingerprint(
                namespace,
                service,
                str(item.get('issue', '') or ''),
                str(item.get('reason', '') or ''),
                str(item.get('root_cause', '') or ''),
                str(item.get('stacktrace', '') or ''),
            )
            item['fingerprint'] = fp

        existing = merged_by_fp.get(fp)
        if not existing:
            merged_by_fp[fp] = dict(item)
            continue

        existing_seen = _parse_iso_datetime(str(existing.get('last_seen_at', '') or existing.get('detected_at', '') or '')) or datetime.min
        item_seen = _parse_iso_datetime(str(item.get('last_seen_at', '') or item.get('detected_at', '') or '')) or datetime.min
        if item_seen > existing_seen:
            existing['last_seen_at'] = item_seen.isoformat()
            existing['detected_at'] = str(item.get('detected_at', '') or item_seen.isoformat())
            existing['message'] = str(item.get('message', '') or existing.get('message', '') or '')
            existing['root_cause'] = str(item.get('root_cause', '') or existing.get('root_cause', '') or '')
            existing['stacktrace'] = str(item.get('stacktrace', '') or existing.get('stacktrace', '') or '')
            existing['source'] = str(item.get('source', '') or existing.get('source', '') or '')
        existing['occurrences'] = int(existing.get('occurrences', 1) or 1) + int(item.get('occurrences', 1) or 1)
        if not str(existing.get('issue', '') or '').strip() and str(item.get('issue', '') or '').strip():
            existing['issue'] = str(item.get('issue', '') or '')
        if not str(existing.get('reason', '') or '').strip() and str(item.get('reason', '') or '').strip():
            existing['reason'] = str(item.get('reason', '') or '')

    items = list(merged_by_fp.values())
    items.sort(
        key=lambda row: _parse_iso_datetime(str(row.get('last_seen_at', '') or row.get('detected_at', '') or '')) or datetime.min,
        reverse=True,
    )
    items = items[:limit]

    issue_counter: Dict[str, int] = {}
    for index, item in enumerate(items, start=1):
        item['number'] = index
        issue_name = str(item.get('issue', '') or '').strip() or 'UnknownIssue'
        issue_counter[issue_name] = issue_counter.get(issue_name, 0) + int(item.get('occurrences', 1) or 1)

    caught_issues = [
        {'issue': issue, 'count': count}
        for issue, count in sorted(issue_counter.items(), key=lambda entry: entry[1], reverse=True)
    ]

    return jsonify({
        'status': 'success',
        'namespace': namespace,
        'service': service,
        'window': {
            'start_time': start_iso,
            'end_time': end_iso,
            'minutes': effective_minutes,
            'clamped_to_retention': bool(clamped),
            'retention_hours': 6,
        },
        'counts': {
            'total_errors': len(items),
            'unique_issue_types': len(caught_issues),
            'stored_rows_in_window': len(in_window_rows),
            'es_rows_in_window': len(es_rows),
        },
        'caught_issues': caught_issues,
        'errors': items,
    })


@app.route('/api/rca/feedback', methods=['POST'])
@app.route('/ai-agent/api/rca/feedback', methods=['POST'])
@operator_required
def api_rca_feedback():
    """Capture operator feedback for RCA self-learning."""
    try:
        payload = request.get_json(silent=True) or {}
        service_name = str(payload.get('service_name', '') or '').strip()
        namespace = str(payload.get('namespace', '') or '').strip().lower()
        rca_id = str(payload.get('rca_id', '') or '').strip()
        feedback = str(payload.get('feedback', '') or '').strip().lower()
        operator_fix = str(payload.get('operator_fix', '') or '').strip()
        resolved_in_minutes = int(payload.get('resolved_in_minutes', 0) or 0)

        if not service_name or namespace not in {'jupiter', 'venus'} or not rca_id:
            return jsonify({'status': 'error', 'message': 'service_name, namespace, rca_id are required'}), 400
        if feedback not in {'correct', 'wrong', 'edited'}:
            return jsonify({'status': 'error', 'message': 'feedback must be one of correct|wrong|edited'}), 400

        existing = get_saved_rca_result(rca_id) or {}
        feedback_data = {
            'service_name': service_name,
            'namespace': namespace,
            'feedback': feedback,
            'operator_fix': operator_fix,
            'resolved_in_minutes': resolved_in_minutes,
            'error_signature': str(payload.get('error_signature', '') or existing.get('error_signature', '')),
            'llm_model': str(payload.get('llm_model', '') or existing.get('llm_model', '') or _load_ai_llm_selection().get('model', _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL)).strip().lower(),
        }

        # Process synchronously so learning is effective on the very next troubleshoot run.
        saved = process_rca_feedback(rca_id, feedback_data)
        task_id = enqueue_feedback_task(rca_id, feedback_data)
        if task_id:
            return jsonify({'status': 'success', 'learning': saved, 'task_id': task_id})
        return jsonify({'status': 'success', 'learning': saved})
    except Exception as e:
        logger.error(f"Failed to capture RCA feedback: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/rca/learnings')
@app.route('/ai-agent/api/rca/learnings')
def api_rca_learnings():
    """Get top learned RCA patterns for namespace."""
    try:
        namespace = str(request.args.get('namespace', '') or '').strip().lower()
        requested_model = str(request.args.get('model', '') or '').strip().lower()
        if not requested_model:
            requested_model = str(_load_ai_llm_selection().get('model', _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL) or (_ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL)).strip().lower()
        limit = int(request.args.get('limit', 20) or 20)
        patterns = get_top_patterns(namespace=namespace, limit=limit, llm_model=requested_model)
        return jsonify({'status': 'success', 'items': patterns, 'model': requested_model})
    except Exception as e:
        logger.error(f"Failed to fetch RCA learnings: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/rca/stats')
@app.route('/ai-agent/api/rca/stats')
def api_rca_stats():
    """Get RCA learning statistics."""
    learning_stats: Dict[str, Any] = {}
    runtime: Dict[str, Any] = {}
    requested_model = str(request.args.get('model', '') or '').strip().lower()
    if not requested_model:
        requested_model = str(_load_ai_llm_selection().get('model', _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL) or (_ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL)).strip().lower()
    try:
        learning_stats = get_learning_stats(llm_model=requested_model) or {}
    except Exception as e:
        logger.warning(f"RCA Stats: learning stats unavailable: {e}")
        learning_stats = {}

    try:
        runtime = get_rca_runtime_metrics(llm_model=requested_model) or {}
    except Exception as e:
        logger.warning(f"RCA Stats: runtime metrics unavailable: {e}")
        runtime = {}

    l0_hits = max(
        int(learning_stats.get('l0_hits_today', 0) or 0),
        int(runtime.get('rca_l0_hits_total', 0) or 0)
    )
    llm_calls = max(
        int(learning_stats.get('llm_calls_today', 0) or 0),
        int(runtime.get('rca_llm_calls_total', 0) or 0)
    )
    accuracy_rate = float(learning_stats.get('accuracy_rate', 0) or 0)
    if accuracy_rate <= 0:
        accuracy_rate = float(runtime.get('rca_accuracy_rate', 0) or 0)

    codexa_metrics: Dict[str, Any] = {}
    codexa_model = ''
    codexa_provider = ''
    try:
        if _init_codexa() and _codexa_analyzer is not None and getattr(_codexa_analyzer, 'llm', None) is not None:
            codexa_metrics = _codexa_analyzer.llm.get_metrics() or {}
            llm_cfg = getattr(_codexa_analyzer.llm, 'config', None)
            codexa_model = str(getattr(llm_cfg, 'model', '') or '').strip()
            codexa_provider = str(getattr(llm_cfg, 'provider', '') or '').strip()
    except Exception:
        codexa_metrics = {}
        codexa_model = ''
        codexa_provider = ''

    rca_avg_tokens = float(
        runtime.get('rca_avg_tokens_per_call', 0)
        or runtime.get('rca_llm_tokens_avg', 0)
        or runtime.get('avg_tokens_per_call', 0)
        or 0
    )
    model_confidence = float(
        runtime.get('rca_model_confidence', 0)
        or learning_stats.get('model_confidence', 0)
        or 0
    )

    result = {
        'total_learnings': int(learning_stats.get('total_learnings', 0) or 0),
        'l0_hits_today': l0_hits,
        'rca_l0_hits_total': l0_hits,
        'rca_l1_hits_total': int(runtime.get('rca_l1_hits_total', 0) or 0),
        'rca_llm_calls_total': llm_calls,
        'llm_calls_today': llm_calls,
        'rca_fallback_total': int(runtime.get('rca_fallback_total', 0) or 0),
        'rca_llm_latency_seconds_avg': float(runtime.get('rca_llm_latency_seconds_avg', 0.0) or 0.0),
        'accuracy_rate': accuracy_rate,
        'top_issues': learning_stats.get('top_issues', []) or [],
        'namespaces': sorted(list(learning_stats.get('namespaces_covered', [])) if isinstance(learning_stats.get('namespaces_covered'), (list, set)) else []),
        'model': requested_model,
        'avg_tokens_per_call': rca_avg_tokens,
        'model_confidence': model_confidence,
        'codexa_llm_calls_total': int(codexa_metrics.get('total_calls', 0) or 0),
        'codexa_avg_tokens_per_call': float(codexa_metrics.get('avg_tokens_per_call', 0) or 0),
        'codexa_avg_response_time': float(codexa_metrics.get('avg_response_time', 0) or 0),
        'codexa_total_tokens': int(codexa_metrics.get('total_tokens', 0) or 0),
        'codexa_model': codexa_model,
        'codexa_provider': codexa_provider,
    }
    logger.info(f"RCA Stats - returning: {result}")
    return jsonify(result)


@app.route('/metrics')
@app.route('/ai-agent/metrics')
def prometheus_metrics():
    """Prometheus metrics endpoint for RCA counters/histogram."""
    try:
        payload = get_rca_prometheus_metrics()
        return Response(payload, mimetype='text/plain; version=0.0.4; charset=utf-8')
    except Exception as e:
        logger.error(f"Failed to render prometheus metrics: {e}")
        return Response('', mimetype='text/plain; version=0.0.4; charset=utf-8', status=500)


@app.route('/api/service-status/stream')
@app.route('/ai-agent/api/service-status/stream')
def ai_agent_api_service_status_stream():
    """Stream near-live service status updates via SSE."""
    interval_seconds = 5.0
    namespace_filter = str(request.args.get('namespace', 'venus') or '').strip().lower()
    if namespace_filter not in {'venus', 'jupiter', 'all'}:
        namespace_filter = 'venus'

    @stream_with_context
    def _event_stream():
        yield f"data: {json.dumps({'type': 'ping', 'ts': datetime.now().isoformat()}, separators=(',', ':'))}\n\n"
        last_digest = ''
        while True:
            try:
                raw = _read_namespace_snapshot_blob(namespace_filter, stale=False)
                stale_read = False
                if not raw:
                    raw = _read_namespace_snapshot_blob(namespace_filter, stale=True)
                    stale_read = True

                digest = hashlib.md5(raw).hexdigest() if raw else ''
                if digest != last_digest:
                    payload = _snapshot_service_status_payload_from_request(allow_stale=True)
                    if stale_read and isinstance(payload, dict):
                        payload['stale'] = True
                    payload['_stream_status_code'] = 200
                    payload['_stream_ts'] = datetime.now().isoformat()
                    yield f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
                    last_digest = digest
                else:
                    heartbeat = {'type': 'ping', 'ts': datetime.now().isoformat()}
                    yield f"data: {json.dumps(heartbeat, separators=(',', ':'))}\n\n"
            except GeneratorExit:
                break
            except Exception as e:
                err_payload = {
                    'type': 'error',
                    'message': str(e),
                    'ts': datetime.now().isoformat()
                }
                yield f"data: {json.dumps(err_payload, separators=(',', ':'))}\n\n"
            time.sleep(interval_seconds)

    return Response(
        _event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/api/troubleshoot', methods=['POST'])
def api_troubleshoot_start():
    """Start asynchronous troubleshooting workflow for a service."""
    try:
        payload = request.get_json(silent=True) or {}
        service = str(payload.get('service', '') or '').strip()
        namespace = str(payload.get('namespace', '') or '').strip()
        if not service or not namespace:
            return jsonify({'status': 'error', 'message': 'service and namespace are required'}), 400

        # Extract time scope from request (for time-scoped TS queries)
        window_minutes = int(payload.get('window_minutes', 30) or 30)
        start_time = payload.get('start_time')  # ISO format string
        end_time = payload.get('end_time')  # ISO format string
        time_scope = {
            'window_minutes': window_minutes,
            'start_time': start_time,
            'end_time': end_time
        }

        job_id = uuid4().hex
        with _troubleshoot_jobs_lock:
            _troubleshoot_jobs[job_id] = {
                'id': job_id,
                'status': 'queued',
                'progress': 0,
                'service': service,
                'namespace': namespace,
                'time_scope': time_scope,
                'created_at': datetime.now().isoformat(),
                'steps': [],
                'result': None,
                'error': ''
            }

        worker = threading.Thread(target=_run_troubleshoot_job, args=(job_id, service, namespace, time_scope), daemon=True)
        worker.start()
        return jsonify({'status': 'accepted', 'job_id': job_id})
    except Exception as e:
        logger.error(f"Failed to start troubleshoot job: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/troubleshoot/<job_id>')
def api_troubleshoot_status(job_id):
    """Get troubleshooting job progress and result."""
    with _troubleshoot_jobs_lock:
        job = _troubleshoot_jobs.get(str(job_id), None)
        if not isinstance(job, dict):
            return jsonify({'status': 'not_found'}), 404
        return jsonify(job)


@app.route('/ai-agent/api/troubleshoot', methods=['POST'])
def ai_agent_api_troubleshoot_start():
    """Start asynchronous troubleshooting workflow (prefixed route)."""
    return api_troubleshoot_start()


@app.route('/ai-agent/api/troubleshoot/<job_id>')
def ai_agent_api_troubleshoot_status(job_id):
    """Get troubleshooting status (prefixed route)."""
    return api_troubleshoot_status(job_id)

@app.route('/api/services/summary')
def api_services_summary():
    """Get summary counts for all/down/unresolved sections."""
    try:
        if not agent_available or agent is None or not hasattr(agent, 'get_service_status'):
            return jsonify({'all_services': 0, 'down_degraded': 0, 'unresolved_alerts': 0})

        window_minutes = request.args.get('window_minutes', type=int)
        if not window_minutes or window_minutes <= 0:
            window_minutes = 5

        monitoring_cfg = agent.config.get('monitoring', {}) if isinstance(getattr(agent, 'config', {}), dict) else {}
        large_set_threshold = int(monitoring_cfg.get('service_status_window_cap_threshold', 100) or 100)
        status_window_cap_minutes = int(monitoring_cfg.get('service_status_window_cap_minutes', 360) or 360)

        selected_services = agent._selected_services() if hasattr(agent, '_selected_services') else []
        effective_window_minutes = window_minutes
        if len(selected_services) >= large_set_threshold and window_minutes > status_window_cap_minutes:
            effective_window_minutes = status_window_cap_minutes

        cache_key = f"summary:{window_minutes}:{effective_window_minutes}:{len(selected_services)}"
        now = datetime.now()
        if (
            _summary_cache.get('key') == cache_key and
            _summary_cache.get('ts') is not None and
            (now - _summary_cache['ts']).total_seconds() < 15 and
            _summary_cache.get('data') is not None
        ):
            return jsonify(_summary_cache['data'])

        service_status = agent.get_service_status(
            minutes=effective_window_minutes,
            services_subset=selected_services,
            include_deep_inspection=False
        )
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


try:
    _start_snapshot_warmer_if_needed()
except Exception:
    pass

try:
    _start_jenkins_auto_rebuild_worker_if_needed()
except Exception:
    pass

try:
    _apply_ai_llm_model(_load_ai_llm_selection().get('model', AI_AGENT_DEFAULT_MODEL))
except Exception:
    pass

# ============================================================================
# CodeXA Routes - Autonomous Code Fix Engine
# ============================================================================

# Initialize CodeXA services
_codexa_initialized = False
_codexa_repository = None
_codexa_detector = None
_codexa_analyzer = None
_codexa_fixer = None
_codexa_git_ops = None
_codexa_llm_selection_lock = threading.Lock()
# Set while any issue is being analyzed — background poll skips detection until clear.
_codexa_pipeline_busy = threading.Event()
CODEXA_LLM_SELECTION_FILE = os.getenv(
    'CODEXA_LLM_SELECTION_FILE',
    os.path.join(
        os.getenv('LEARNING_STORE_PATH', '/var/otel/rca-learning').strip() or '/var/otel/rca-learning',
        'codexa_llm_selection.json'
    )
)
_ai_llm_selection_lock = threading.Lock()
AI_LLM_SELECTION_FILE = os.getenv(
    'AI_LLM_SELECTION_FILE',
    os.path.join(
        os.getenv('LEARNING_STORE_PATH', '/var/otel/rca-learning').strip() or '/var/otel/rca-learning',
        'ai_agent_llm_selection.json'
    )
)
AI_AGENT_DEFAULT_MODEL = 'ollama:qwen2.5:1.5b'
AI_AGENT_ALLOWED_MODELS = {
    # Keep AI-Agent and CodeXA model catalog aligned.
    'ollama:qwen2.5:1.5b': {'provider': 'ollama', 'label': 'Qwen 2.5 1.5B (default)'},
    'ollama:qwen2.5:7b-instruct-q4_K_M': {'provider': 'ollama', 'label': 'Qwen 2.5 7B'},
    'gemini:gemini-1.5-pro-latest': {'provider': 'gemini', 'label': 'Gemini 1.5 Pro'},
    'gemini:gemini-1.5-flash-latest': {'provider': 'gemini', 'label': 'Gemini 1.5 Flash'},
    'gemini:gemini-2.0-flash': {'provider': 'gemini', 'label': 'Gemini 2.0 Flash'},
    # Backward-compatibility selections
    'qwen3': {'provider': 'qwen', 'label': 'qwen3 (legacy)'},
    'gemini-1.5-pro': {'provider': 'gemini', 'label': 'gemini-1.5-pro (legacy)'},
    'gemini-1.5-flash': {'provider': 'gemini', 'label': 'gemini-1.5-flash (legacy)'},
}
CODEXA_UI_ALLOWED_MODELS = {
    # Local Ollama models
    'ollama:qwen2.5:1.5b',  # Default - fast on CPU
    'qwen2.5:1.5b',  # backwards compatibility
    'ollama:qwen2.5:7b-instruct-q4_K_M',
    'qwen2.5:7b-instruct-q4_K_M',  # backwards compatibility
    # Gemini cloud models (use -latest suffix for stable API access)
    'gemini:gemini-1.5-pro-latest',
    'gemini:gemini-1.5-flash-latest',
    'gemini:gemini-2.0-flash',
    # Legacy names for backwards compatibility
    'gemini:gemini-1.5-pro',
    'gemini:gemini-1.5-flash',
}

# CodeXA default model (kept local/ollama by default).
CODEXA_DEFAULT_MODEL = 'ollama:qwen2.5:1.5b'
CODEXA_DEFAULT_PROVIDER = 'ollama'

# OpenAI/GPT wiring is intentionally disabled for now.
# Keep this commented block for quick re-enable later when needed.
# _CODEX_GPT_MODEL_RAW  = os.getenv('CODEX_GPT_MODEL', '').strip()
# _CODEX_GPT_API_KEY    = os.getenv('CODEX_GPT_API_KEY', '').strip()
# _CODEX_GPT_BASE_URL   = os.getenv('CODEX_GPT_BASE_URL', '').strip()
# if _CODEX_GPT_MODEL_RAW:
#     _codex_gpt_model_id = (
#         _CODEX_GPT_MODEL_RAW
#         if _CODEX_GPT_MODEL_RAW.startswith('openai:')
#         else f"openai:{_CODEX_GPT_MODEL_RAW}"
#     )
#     CODEXA_UI_ALLOWED_MODELS.add(_codex_gpt_model_id)

_ORIGINAL_AI_API_URL = os.getenv('AI_API_URL', 'http://20.244.11.93/v1').strip() or 'http://20.244.11.93/v1'
_ORIGINAL_AI_API_KEY = os.getenv('AI_API_KEY', '').strip()
_ORIGINAL_AI_MODEL = os.getenv('AI_MODEL_NAME', AI_AGENT_DEFAULT_MODEL).strip() or AI_AGENT_DEFAULT_MODEL


def _load_ai_llm_selection() -> Dict[str, str]:
    path = str(AI_LLM_SELECTION_FILE or '').strip()
    if not path or not os.path.exists(path):
        return {'model': _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL}
    try:
        with open(path, 'r') as f:
            raw = json.load(f)
    except Exception:
        return {'model': _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL}
    if not isinstance(raw, dict):
        return {'model': _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL}
    model = str(raw.get('model', '') or '').strip()
    if model.startswith('openai:'):
        bare = model.split(':', 1)[1]
        if bare in AI_AGENT_ALLOWED_MODELS:
            model = bare
    if not model:
        model = _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL
    return {'model': model}


def _save_ai_llm_selection(model: str) -> bool:
    path = str(AI_LLM_SELECTION_FILE or '').strip()
    if not path:
        return False
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            'model': str(model or '').strip(),
            'updated_at': datetime.now().isoformat(),
        }
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2)
        return True
    except Exception:
        return False


def _apply_ai_llm_model(model: str) -> Dict[str, Any]:
    chosen = str(model or '').strip()
    if not chosen:
        chosen = _ORIGINAL_AI_MODEL or AI_AGENT_DEFAULT_MODEL

    provider = AI_AGENT_ALLOWED_MODELS.get(chosen, {}).get('provider')
    if not provider:
        if chosen.lower().startswith('openai:'):
            provider = 'openai'
        elif chosen.lower().startswith('ollama:'):
            provider = 'ollama'
        else:
            provider = 'gemini' if chosen.lower().startswith('gemini') else 'qwen'
    runtime_model = chosen
    if ':' in chosen:
        prefix, rest = chosen.split(':', 1)
        if prefix.strip().lower() in {'openai', 'gemini', 'ollama', 'anthropic'}:
            runtime_model = rest
    os.environ['AI_MODEL_NAME'] = runtime_model

    if provider == 'gemini':
        os.environ['OLLAMA_ENABLED'] = 'false'
        gemini_key = str(os.getenv('GEMINI_API_KEY', '') or '').strip()
        if gemini_key:
            os.environ['AI_API_KEY'] = gemini_key
        gemini_api_url = str(os.getenv('GEMINI_OPENAI_API_URL', '') or '').strip()
        if gemini_api_url:
            os.environ['AI_API_URL'] = gemini_api_url
    elif provider == 'ollama':
        os.environ['OLLAMA_ENABLED'] = 'true'
        os.environ['OLLAMA_MODEL'] = runtime_model
        # Disable remote OpenAI-compatible path; use Ollama fallback directly.
        os.environ['AI_API_KEY'] = ''
        os.environ['AI_API_URL'] = _ORIGINAL_AI_API_URL
    elif provider == 'openai':
        os.environ['OLLAMA_ENABLED'] = 'false'
        gpt_key = str(os.getenv('CODEX_GPT_API_KEY', '') or '').strip()
        gpt_url = str(os.getenv('CODEX_GPT_BASE_URL', '') or '').strip()
        if gpt_key:
            os.environ['AI_API_KEY'] = gpt_key
        if gpt_url:
            os.environ['AI_API_URL'] = gpt_url
    else:
        os.environ['OLLAMA_ENABLED'] = 'false'
        os.environ['AI_API_KEY'] = _ORIGINAL_AI_API_KEY
        os.environ['AI_API_URL'] = _ORIGINAL_AI_API_URL

    # Apply live to already-initialized analyzers.
    try:
        if agent_available and agent is not None:
            llm_clients = []
            rca = getattr(agent, 'root_cause_analyzer', None)
            if rca is not None and getattr(rca, 'llm_client', None) is not None:
                llm_clients.append(rca.llm_client)
            le = getattr(agent, 'learning_engine', None)
            if le is not None and getattr(le, 'llm_client', None) is not None:
                llm_clients.append(le.llm_client)
            for client in llm_clients:
                client.model = chosen
                client.api_url = str(os.getenv('AI_API_URL', _ORIGINAL_AI_API_URL) or _ORIGINAL_AI_API_URL).rstrip('/')
                client.api_key = str(os.getenv('AI_API_KEY', '') or '').strip()
    except Exception:
        pass

    return {
        'model': chosen,
        'provider': provider,
        'api_url': str(os.getenv('AI_API_URL', _ORIGINAL_AI_API_URL) or _ORIGINAL_AI_API_URL),
        'remote_key_present': bool(str(os.getenv('AI_API_KEY', '') or '').strip()),
    }


def _current_ai_llm_state() -> Dict[str, Any]:
    selected = _load_ai_llm_selection()
    model = str(selected.get('model', AI_AGENT_DEFAULT_MODEL) or AI_AGENT_DEFAULT_MODEL)
    provider = AI_AGENT_ALLOWED_MODELS.get(model, {}).get('provider')
    if not provider:
        if model.lower().startswith('openai:'):
            provider = 'openai'
        else:
            provider = 'gemini' if model.lower().startswith('gemini') else 'qwen'
    model_options = [
        {'id': model_id, 'label': meta.get('label', model_id), 'provider': meta.get('provider', 'qwen')}
        for model_id, meta in AI_AGENT_ALLOWED_MODELS.items()
    ]
    if model not in {m.get('id') for m in model_options}:
        model_options.insert(0, {'id': model, 'label': f"{model} (current)", 'provider': provider})
    return {
        'model': model,
        'provider': provider,
        'api_url': str(os.getenv('AI_API_URL', _ORIGINAL_AI_API_URL) or _ORIGINAL_AI_API_URL),
        'remote_key_present': bool(str(os.getenv('AI_API_KEY', '') or '').strip()),
        'timeout': int(os.getenv('AI_API_TIMEOUT', '20') or '20'),
        'models': model_options,
    }


_CODEXA_PROVIDER_PREFIXES = {'gemini', 'openai', 'anthropic', 'ollama'}


def _codexa_provider_from_model(model_id: str) -> str:
    model = str(model_id or '').strip().lower()
    if not model:
        return 'ollama'
    if ':' in model:
        prefix = model.split(':', 1)[0].strip()
        if prefix in {'gemini', 'openai', 'anthropic'}:
            return prefix
    return 'ollama'


def _codexa_bare_model_name(model_id: str) -> str:
    """Strip provider prefix so only the bare model name is sent to the API.

    'openai:GPT-5.3 Codex'            -> 'GPT-5.3 Codex'
    'gemini:gemini-1.5-pro-latest'     -> 'gemini-1.5-pro-latest'
    'ollama:qwen2.5:7b-instruct-q4_K_M' -> 'qwen2.5:7b-instruct-q4_K_M'
    'qwen2.5:7b-instruct-q4_K_M'      -> 'qwen2.5:7b-instruct-q4_K_M'  (unchanged)
    """
    s = str(model_id or '').strip()
    if ':' in s:
        prefix, rest = s.split(':', 1)
        if prefix.lower() in _CODEXA_PROVIDER_PREFIXES:
            return rest
    return s


def _load_codexa_llm_selection() -> Dict[str, str]:
    path = str(CODEXA_LLM_SELECTION_FILE or '').strip()
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            raw = json.load(f)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    model = str(raw.get('model', '') or '').strip()
    provider = str(raw.get('provider', '') or '').strip().lower()
    if not model:
        return {}
    if provider not in {'ollama', 'gemini', 'openai', 'anthropic'}:
        provider = _codexa_provider_from_model(model)
    return {'model': model, 'provider': provider}


def _save_codexa_llm_selection(model: str, provider: str) -> bool:
    path = str(CODEXA_LLM_SELECTION_FILE or '').strip()
    if not path:
        return False
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            'model': str(model or '').strip(),
            'provider': str(provider or '').strip().lower(),
            'updated_at': datetime.now().isoformat(),
        }
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2)
        return True
    except Exception:
        return False


def _apply_codexa_llm_selection(config=None) -> Dict[str, str]:
    """Apply persisted model/provider override to active CodeXA config."""
    if not CODEXA_AVAILABLE:
        return {}
    try:
        from codexa.config import get_config
        cfg = config or get_config()
        persisted = _load_codexa_llm_selection()

        # OpenAI/GPT selection has been deprecated for CodeXA.
        if persisted and str(persisted.get('provider', '') or '').strip().lower() == 'openai':
            persisted = {
                'model': CODEXA_DEFAULT_MODEL,
                'provider': CODEXA_DEFAULT_PROVIDER,
            }
            _save_codexa_llm_selection(persisted['model'], persisted['provider'])

        if not persisted:
            return {'model': str(cfg.llm.model or ''), 'provider': str(cfg.llm.provider or 'ollama')}

        # Validate the persisted model is still allowed.
        persisted_full = persisted['model']
        if ':' not in persisted_full:
            persisted_full = f"{persisted['provider']}:{persisted['model']}"
        if persisted['provider'] == 'openai':
            return {'model': str(cfg.llm.model or ''), 'provider': str(cfg.llm.provider or 'ollama')}

        # Strip provider prefix — cfg.llm.model must be the bare name sent to the API
        cfg.llm.model = _codexa_bare_model_name(persisted['model'])
        cfg.llm.provider = persisted['provider']
        return {'model': persisted['model'], 'provider': persisted['provider']}
    except Exception:
        return {}

def _init_codexa():
    """Initialize CodeXA services on first request."""
    global _codexa_initialized, _codexa_repository, _codexa_detector, _codexa_analyzer, _codexa_fixer, _codexa_git_ops

    if _codexa_initialized:
        return True

    if not CODEXA_AVAILABLE:
        return False

    try:
        from codexa.config import get_config
        from codexa.db.repository import IssueRepository
        from codexa.services.detector import IssueDetector
        from codexa.services.analyzer import CodeAnalyzer
        from codexa.services.fixer import FixGenerator
        from codexa.services.git_ops import GitOperations

        config = get_config()
        _apply_codexa_llm_selection(config)
        # Get Redis client if available, otherwise use in-memory storage
        redis_cli = _get_external_cache_client()
        _data_dir = os.getenv('LEARNING_STORE_PATH', '/var/otel/rca-learning').strip() or '/var/otel/rca-learning'
        _codexa_pr_history_file = os.path.join(_data_dir, 'codexa_pr_history.json')
        _codexa_issue_history_file = os.path.join(_data_dir, 'codexa_issue_history.json')
        _codexa_fix_history_file = os.path.join(_data_dir, 'codexa_fix_history.json')
        _codexa_repository = IssueRepository(
            redis_cli,
            pr_history_file=_codexa_pr_history_file,
            issue_history_file=_codexa_issue_history_file,
            fix_history_file=_codexa_fix_history_file,
        )

        # Get Elasticsearch client from the monitoring agent for direct log access
        es_client = None
        if agent is not None and hasattr(agent, 'elasticsearch'):
            es_client = agent.elasticsearch
            logging.info("CodeXA: Using Elasticsearch client from monitoring agent")
        else:
            logging.info("CodeXA: No Elasticsearch client available, using API-only detection")

        _codexa_detector = IssueDetector(config, _codexa_repository, es_client)
        _codexa_analyzer = CodeAnalyzer(config)
        _codexa_fixer = FixGenerator(config, _codexa_analyzer)
        _codexa_git_ops = GitOperations(config)
        _codexa_initialized = True
        logging.info("CodeXA services initialized successfully")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize CodeXA: {e}", exc_info=True)
        return False

@app.route('/ai-agent/codexa')
@app.route('/ai-agent/codexa/')
@login_required
def codexa_dashboard():
    """Serve CodeXA dashboard."""
    if not CODEXA_AVAILABLE:
        return "CodeXA not available", 503

    # Serve the CodeXA template
    codexa_template_dir = os.path.join(os.path.dirname(__file__), 'codexa', 'templates')
    template_path = os.path.join(codexa_template_dir, 'codexa.html')

    if os.path.exists(template_path):
        with open(template_path, 'r') as f:
            return f.read()
    else:
        return "CodeXA template not found", 404


def _codexa_parse_window_minutes(raw_value: Any, default_minutes: int = 60) -> int:
    try:
        value = int(raw_value or default_minutes)
    except Exception:
        value = int(default_minutes)
    return max(5, min(value, 24 * 60))


def _codexa_issue_in_window(issue: Any, window_minutes: int) -> bool:
    def _parse_ts(val):
        if isinstance(val, datetime):
            return val.replace(tzinfo=None) if val.tzinfo else val
        if isinstance(val, str) and val.strip():
            for raw in (val.strip().replace('Z', '+00:00'), val.strip()):
                try:
                    dt = datetime.fromisoformat(raw)
                    return dt.replace(tzinfo=None) if dt.tzinfo else dt
                except Exception:
                    continue
        return None

    detected  = _parse_ts(getattr(issue, 'detected_at', None))
    last_seen = _parse_ts(getattr(issue, 'last_seen_at', None))

    # Use the most recent of detected_at / last_seen_at so issues that were
    # first seen hours ago but occurred again just now still appear in the window.
    ts = max((t for t in (detected, last_seen) if t is not None),
             default=None)

    if ts is None:
        ts = _parse_ts(getattr(issue, 'created_at', None))
    if ts is None:
        return False

    cutoff = datetime.utcnow() - timedelta(minutes=max(1, int(window_minutes or 60)))
    return ts >= cutoff


def _codexa_issue_timestamp_iso(issue: Any) -> str:
    detected = getattr(issue, 'detected_at', None)
    if isinstance(detected, datetime):
        return detected.isoformat()
    created = getattr(issue, 'created_at', None)
    if isinstance(created, datetime):
        return created.isoformat()
    return datetime.utcnow().isoformat()


def _codexa_issue_group_key(issue: Any) -> Tuple[str, str, str, str, int]:
    return (
        str(getattr(issue, 'service_name', '') or '').strip().lower(),
        str(getattr(issue, 'namespace', '') or '').strip().lower(),
        str(getattr(issue, 'exception_type', '') or '').strip().lower(),
        str(getattr(issue, 'file_path', '') or '').strip().lower(),
        int(getattr(issue, 'line_number', 0) or 0),
    )


def _codexa_issue_is_actionable(issue: Any) -> bool:
    exception_type = str(getattr(issue, 'exception_type', '') or '').strip().lower()
    message = str(getattr(issue, 'exception_message', '') or '')
    stack = str(getattr(issue, 'stack_trace', '') or '')
    evidence = f"{message}\n{stack}"
    evidence_l = evidence.lower()

    # Drop known benign log lines that were wrongly captured as issues.
    if 'status=true' in evidence_l and 'errors=null' in evidence_l:
        return False

    file_path = str(getattr(issue, 'file_path', '') or '').strip()
    line_number = int(getattr(issue, 'line_number', 0) or 0)
    has_location = bool(file_path) and line_number > 0

    has_strong_signal = (
        ('caused by:' in evidence_l)
        or ('traceback' in evidence_l)
        or (re.search(r"\bat\s+[\w.$]+\([\w.$]+:\d+\)", evidence) is not None)
        or (re.search(r"\b\w+\.(java|kt|py|js|ts|go):\d+\b", evidence, re.IGNORECASE) is not None)
        or (re.search(r"\b\w+(Exception|Error)\b", evidence) is not None)
    )

    if exception_type in {'error', 'exception', 'npe', 'runtimeexception'} and not has_location and not has_strong_signal:
        return False

    return True


def _codexa_background_poll():
    """Background thread: auto-detect issues every CODEXA_POLL_INTERVAL seconds, then auto-analyze."""
    poll_interval = max(30, int(os.environ.get('CODEXA_POLL_INTERVAL', '60') or '60'))
    window_minutes = max(5, int(os.environ.get('CODEXA_DETECT_WINDOW_MINUTES', '30') or '30'))
    import asyncio
    while True:
        try:
            time.sleep(poll_interval)
            if not CODEXA_AVAILABLE:
                continue
            if not _init_codexa():
                continue
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                detected = loop.run_until_complete(_codexa_detector.detect_issues(window_minutes=window_minutes))
            finally:
                loop.close()
            if detected:
                logging.info(f"CodeXA background poll: {len(detected)} issue(s) detected (new/updated)")

            # Always check ALL PENDING issues — not just newly detected ones.
            # A previous cycle may have added issues while the pipeline was busy;
            # they stay PENDING until the pipeline is free to pick them up.
            if not _codexa_pipeline_busy.is_set():
                try:
                    from codexa.models import IssueStatus as _IS
                    all_pending = _codexa_repository.get_issues(status=_IS.PENDING, limit=500)
                    # One per service — most recently detected first
                    seen_svc: dict = {}
                    for iss in sorted(all_pending,
                                      key=lambda i: getattr(i, 'detected_at', None) or '',
                                      reverse=True):
                        svc_key = f"{getattr(iss, 'namespace', '')}/{getattr(iss, 'service_name', '')}"
                        if svc_key not in seen_svc:
                            seen_svc[svc_key] = iss
                    to_analyze_ids = [str(i.id) for i in seen_svc.values() if getattr(i, 'id', None)]
                    if to_analyze_ids:
                        logging.info(f"CodeXA background poll: queuing {len(to_analyze_ids)} pending issue(s) for analysis")
                        threading.Thread(
                            target=_codexa_run_auto_pipeline,
                            args=(to_analyze_ids, False),  # auto_pr=False — user clicks Generate PR
                            daemon=True,
                            name='codexa-auto-analyze',
                        ).start()
                except Exception as _q_err:
                    logging.warning(f"CodeXA poll queue error: {_q_err}")
            else:
                if detected:
                    logging.info("CodeXA background poll: pipeline busy — new issues stored as PENDING, will be picked up next cycle")
        except Exception as _poll_err:
            logging.warning(f"CodeXA background poll error: {_poll_err}")


def _codexa_run_auto_pipeline(issue_ids: List[str], auto_pr: bool = True, min_pr_confidence: float = 0.75):
    """Background worker: analyze detected issues and optionally create PRs."""
    if not issue_ids:
        return
    if not _init_codexa():
        return

    try:
        from codexa.models import IssueStatus
    except Exception:
        return

    _codexa_pipeline_busy.set()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for issue_id in issue_ids:
            try:
                issue = _codexa_repository.get_issue(issue_id)
                if not issue:
                    continue

                # Set up step logging so the Analysis Log modal shows progress
                _codexa_repository.start_analysis_log(issue_id)

                def _record(name, status="ok", detail="", _iid=issue_id):
                    _codexa_repository.add_analysis_step(_iid, name, status, detail)

                _record("Analysis started", "ok",
                        f"service={issue.service_name} | exception={issue.exception_type}")
                _codexa_repository.update_issue_status(issue_id, IssueStatus.ANALYZING)

                analysis = loop.run_until_complete(
                    _codexa_analyzer.analyze(issue, on_step=_record)
                )
                if not analysis or not analysis.get("fix"):
                    _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                    _record("Result", "error", "No fix could be generated for this issue")
                    continue

                _record("Generate fix", "running")
                fix = loop.run_until_complete(_codexa_fixer.generate_fix(issue, analysis))
                if not fix:
                    _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                    _record("Generate fix", "error", "Fix generation produced no output")
                    continue

                _codexa_repository.add_fix(fix)
                _codexa_repository.update_issue_status(issue_id, IssueStatus.FIX_READY)
                _record("Fix ready", "ok", "Click 'Create PR' to open a pull request")

                if not auto_pr:
                    continue
                if float(getattr(fix, 'confidence', 0.0) or 0.0) < float(min_pr_confidence or 0.75):
                    continue

                _record("Create PR", "running")
                pr = loop.run_until_complete(_codexa_git_ops.create_pr(issue, fix))
                if pr:
                    _codexa_repository.add_pr(pr)
                    _codexa_repository.update_issue_status(issue_id, IssueStatus.PR_CREATED)
                    _record("PR created", "ok", pr.pr_url or "")
            except Exception as item_err:
                logging.error(f"CodeXA auto pipeline failed for issue {issue_id}: {item_err}")
                try:
                    _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                    _codexa_repository.add_analysis_step(issue_id, "Pipeline error", "error", str(item_err)[:300])
                except Exception:
                    pass
    finally:
        loop.close()
        _codexa_pipeline_busy.clear()

@app.route('/ai-agent/codexa/api/stats')
@login_required
def codexa_api_stats():
    """Get CodeXA dashboard statistics."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        window_minutes = _codexa_parse_window_minutes(request.args.get('window_minutes', 60), 60)
        stats = _codexa_repository.get_stats()
        trend = _codexa_repository.get_trend_data(7)
        types = _codexa_repository.get_issue_type_distribution()
        all_issues = _codexa_repository.get_issues(limit=5000)
        recent_issues = [
            i for i in all_issues
            if _codexa_issue_in_window(i, window_minutes) and _codexa_issue_is_actionable(i)
        ]
        recent_fix_ready = [
            i for i in recent_issues
            if str(getattr(getattr(i, 'status', ''), 'value', getattr(i, 'status', '')) or '').strip().lower() == 'fix_ready'
        ]
        recent_analyzing = [
            i for i in recent_issues
            if str(getattr(getattr(i, 'status', ''), 'value', getattr(i, 'status', '')) or '').strip().lower() == 'analyzing'
        ]

        return jsonify({
            "detected": len(recent_issues),
            "analyzing": len(recent_analyzing),
            "fix_ready": len(recent_fix_ready),
            "prs_created": stats["prs"]["total"],
            "prs_merged": stats["prs"]["merged"],
            "success_rate": stats["success_rate"],
            "trend": trend,
            "issue_types": types,
            "window_minutes": window_minutes,
        })
    except Exception as e:
        logging.error(f"CodeXA stats error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/issues')
@login_required
def codexa_api_issues():
    """List CodeXA issues."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        status = request.args.get('status')
        window_minutes = _codexa_parse_window_minutes(request.args.get('window_minutes', 60), 60)
        limit = max(1, min(int(request.args.get('limit', 50) or 50), 500))
        from codexa.models import IssueStatus

        status_filter = None
        if status:
            try:
                status_filter = IssueStatus(status)
            except ValueError:
                pass

        issues = _codexa_repository.get_issues(status=status_filter, limit=max(limit * 20, 1000))
        # Always apply the time-window — status filter + window filter are both active.
        # Old behaviour (window skipped when status filter set) caused 6h-old issues
        # to appear even when the user had selected the 1h window.
        issues = [
            i for i in issues
            if _codexa_issue_in_window(i, window_minutes)
            and _codexa_issue_is_actionable(i)
        ]
        grouped: Dict[Tuple[str, str, str, str, int], Dict[str, Any]] = {}
        for issue in issues:
            key = _codexa_issue_group_key(issue)
            row = grouped.get(key)
            if row is None:
                grouped[key] = {
                    "id": issue.id,
                    "service_name": issue.service_name,
                    "namespace": issue.namespace,
                    "exception_type": issue.exception_type,
                    "exception_message": issue.exception_message[:200] if issue.exception_message else None,
                    "file_path": issue.file_path,
                    "line_number": issue.line_number,
                    "status": issue.status.value,
                    "confidence": issue.confidence,
                    "created_at": _codexa_issue_timestamp_iso(issue),
                    "occurrence_count": int(getattr(issue, 'occurrence_count', 1) or 1),
                    "last_seen_at": issue.last_seen_at.isoformat() if getattr(issue, 'last_seen_at', None) else None,
                    "repeat_count": 1,
                }
            else:
                row["repeat_count"] = int(row.get("repeat_count", 1) or 1) + 1
                # Take max occurrence_count across grouped duplicates
                row["occurrence_count"] = max(
                    int(row.get("occurrence_count", 1) or 1),
                    int(getattr(issue, 'occurrence_count', 1) or 1)
                )

        rows = list(grouped.values())
        rows.sort(key=lambda r: str(r.get("created_at", "")), reverse=True)
        rows = rows[:limit]

        return jsonify({
            "window_minutes": window_minutes,
            "issues": rows
        })
    except Exception as e:
        logging.error(f"CodeXA issues error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/issues/<issue_id>')
@login_required
def codexa_api_issue_detail(issue_id):
    """Get issue details."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        issue = _codexa_repository.get_issue(issue_id)
        if not issue:
            return jsonify({"error": "Issue not found"}), 404

        fixes = _codexa_repository.get_fixes_for_issue(issue_id)

        return jsonify({
            "issue": {
                "id": issue.id,
                "service_name": issue.service_name,
                "namespace": issue.namespace,
                "exception_type": issue.exception_type,
                "exception_message": issue.exception_message,
                "stack_trace": issue.stack_trace,
                "file_path": issue.file_path,
                "line_number": issue.line_number,
                "status": issue.status.value,
                "created_at": _codexa_issue_timestamp_iso(issue)
            },
            "fixes": [
                {
                    "id": f.id,
                    "file_path": f.file_path,
                    "diff_patch": f.diff_patch,
                    "fix_description": f.fix_description,
                    "confidence": f.confidence,
                    "verification_status": f.verification_status
                }
                for f in fixes
            ]
        })
    except Exception as e:
        logging.error(f"CodeXA issue detail error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/ai-agent/codexa/api/issues/<issue_id>/report')
@login_required
def codexa_api_issue_report(issue_id):
    """Get a concise AI fix report for a CodeXA issue."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        issue = _codexa_repository.get_issue(issue_id)
        if not issue:
            return jsonify({"error": "Issue not found"}), 404

        fixes = _codexa_repository.get_fixes_for_issue(issue_id)
        latest_fix = fixes[0] if fixes else None
        prs = _codexa_repository.get_prs(limit=500)
        linked_pr = next((p for p in prs if str(getattr(p, 'issue_id', '')) == str(issue_id)), None)

        changed_files = []
        if latest_fix is not None:
            file_path = str(getattr(latest_fix, 'file_path', '') or '').strip()
            if file_path:
                changed_files.append(file_path)

        report = {
            "issue": {
                "id": issue.id,
                "service_name": issue.service_name,
                "namespace": issue.namespace,
                "status": issue.status.value,
                "detected_at": issue.detected_at.isoformat() if issue.detected_at else None,
                "exception_type": issue.exception_type,
                "exception_message": issue.exception_message,
                "file_path": issue.file_path,
                "line_number": issue.line_number,
                "confidence": issue.confidence,
                "stack_trace": issue.stack_trace,
                "log_evidence": issue.log_evidence,
            },
            "analysis": {
                "fix_ready": latest_fix is not None,
                "fix_description": str(getattr(latest_fix, 'fix_description', '') or '') if latest_fix is not None else '',
                "llm_reasoning": str(getattr(latest_fix, 'llm_reasoning', '') or '') if latest_fix is not None else '',
                "verification_status": str(getattr(latest_fix, 'verification_status', '') or '') if latest_fix is not None else '',
                "verification_output": str(getattr(latest_fix, 'verification_output', '') or '') if latest_fix is not None else '',
                "changed_files": changed_files,
                "diff_patch": str(getattr(latest_fix, 'diff_patch', '') or '') if latest_fix is not None else '',
            },
            "pr": {
                "created": linked_pr is not None,
                "pr_number": int(getattr(linked_pr, 'pr_number', 0) or 0) if linked_pr is not None else 0,
                "pr_url": str(getattr(linked_pr, 'pr_url', '') or '') if linked_pr is not None else '',
                "status": str(getattr(getattr(linked_pr, 'status', ''), 'value', getattr(linked_pr, 'status', '')) or '') if linked_pr is not None else '',
                "title": str(getattr(linked_pr, 'pr_title', '') or '') if linked_pr is not None else '',
            }
        }
        return jsonify(report)
    except Exception as e:
        logging.error(f"CodeXA issue report error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/issues/<issue_id>/analyze', methods=['POST'])
@operator_required
def codexa_api_analyze_issue(issue_id):
    """Trigger analysis for an issue."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        issue = _codexa_repository.get_issue(issue_id)
        if not issue:
            return jsonify({"error": "Issue not found"}), 404

        from codexa.models import IssueStatus

        if issue.status == IssueStatus.ANALYZING:
            return jsonify({"status": "already_analyzing", "message": "Analysis already in progress"})

        # Re-analysis: clear any existing fix so a fresh one is generated
        if issue.status in (IssueStatus.FIX_READY, IssueStatus.PR_CREATED):
            try:
                old_fixes = _codexa_repository.get_fixes_for_issue(issue_id)
                for f in old_fixes:
                    _codexa_repository._fixes.pop(f.id, None)
            except Exception:
                pass

        # Update status and reset step log for this run
        _codexa_repository.update_issue_status(issue_id, IssueStatus.ANALYZING)
        _codexa_repository.start_analysis_log(issue_id)

        def record(name, status, detail=""):
            _codexa_repository.add_analysis_step(issue_id, name, status, detail)

        record("Analysis started", "ok",
               f"service={issue.service_name} | exception={issue.exception_type}")

        # Run analysis in background thread
        def run_analysis():
            import asyncio
            _codexa_pipeline_busy.set()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                analysis = loop.run_until_complete(_codexa_analyzer.analyze(issue, on_step=record))
                if analysis and analysis.get("fix"):
                    fix = loop.run_until_complete(_codexa_fixer.generate_fix(issue, analysis))
                    if fix and fix.fixed_code:
                        _codexa_repository.add_fix(fix)
                        _codexa_repository.update_issue_status(issue_id, IssueStatus.FIX_READY)
                        record("Fix ready", "ok", "Click 'Create PR' to open a pull request")
                    else:
                        _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                        record("Save fix", "error", "No applicable fix was produced")
                else:
                    _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                    record("Result", "error", "No fix generated — see the failing step above")
            except Exception as e:
                logging.error(f"Analysis failed: {e}", exc_info=True)
                _codexa_repository.update_issue_status(issue_id, IssueStatus.FAILED)
                record("Analysis crashed", "error", str(e))
            finally:
                loop.close()
                _codexa_pipeline_busy.clear()

        thread = threading.Thread(target=run_analysis)
        thread.daemon = True
        thread.start()

        return jsonify({"status": "analyzing", "issue_id": issue_id})
    except Exception as e:
        logging.error(f"CodeXA analyze error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/ai-agent/codexa/api/issues/<issue_id>/analysis-log')
@login_required
def codexa_api_analysis_log(issue_id):
    """Live step log for the most recent analysis run (polled by dashboard modal)."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503
    try:
        issue = _codexa_repository.get_issue(issue_id)
        return jsonify({
            "issue_id": issue_id,
            "status": issue.status.value if issue else "unknown",
            "steps": _codexa_repository.get_analysis_steps(issue_id),
        })
    except Exception as e:
        logging.error(f"CodeXA analysis-log error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/issues/<issue_id>/fix', methods=['POST'])
@operator_required
def codexa_api_create_fix(issue_id):
    """Create PR for an issue fix."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        issue = _codexa_repository.get_issue(issue_id)
        if not issue:
            return jsonify({"error": "Issue not found"}), 404

        fixes = _codexa_repository.get_fixes_for_issue(issue_id)
        if not fixes:
            return jsonify({"error": "No fix available"}), 400

        # Sort fixes by created_at descending and take the most recent
        fixes.sort(key=lambda x: getattr(x, 'created_at', None) or __import__('datetime').datetime.min, reverse=True)
        fix = fixes[0]

        logging.info(f"CodeXA: creating PR for issue={issue_id} service={issue.service_name} "
                     f"repo={getattr(issue, 'repo_name', '?')} branch={getattr(issue, 'branch', '?')} "
                     f"fix_file={fix.file_path}")

        # Create PR synchronously in a fresh event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            pr = loop.run_until_complete(_codexa_git_ops.create_pr(issue, fix))
            if pr and pr.pr_url:
                _codexa_repository.add_pr(pr)
                from codexa.models import IssueStatus
                _codexa_repository.update_issue_status(issue_id, IssueStatus.PR_CREATED)
                # Send ONE consolidated Slack doc with all 1h issues for this service
                threading.Thread(
                    target=_slack_send_pr_report,
                    args=(issue, fix, pr.pr_url),
                    daemon=True
                ).start()
                return jsonify({
                    "status": "created",
                    "pr_url": pr.pr_url,
                    "pr_number": pr.pr_number,
                    "slack_queued": _slack_notifier is not None
                })
            else:
                detail = str(getattr(_codexa_git_ops, 'last_error', '') or '').strip()
                logging.error(f"CodeXA: create_pr returned None for issue={issue_id} — "
                              f"check pod logs for clone/push/API error details: {detail}")
                return jsonify({"error": detail or "Failed to create PR — check pod logs for details"}), 500
        finally:
            loop.close()
    except Exception as e:
        logging.error(f"CodeXA create PR error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/issues/<issue_id>/dismiss', methods=['POST'])
@operator_required
def codexa_api_dismiss_issue(issue_id):
    """Dismiss an issue."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        from codexa.models import IssueStatus

        if _codexa_repository.update_issue_status(issue_id, IssueStatus.DISMISSED):
            return jsonify({"status": "dismissed"})
        else:
            return jsonify({"error": "Issue not found"}), 404
    except Exception as e:
        logging.error(f"CodeXA dismiss error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/prs')
@login_required
def codexa_api_prs():
    """List CodeXA pull requests."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        hours = request.args.get('hours', 24, type=int)
        hours = max(1, min(hours, 24 * 7))  # clamp 1h – 7d
        prs = _codexa_repository.get_prs(limit=200, hours=hours)

        return jsonify({
            "prs": [
                {
                    "id": p.id,
                    "service_name": p.service_name,
                    "issue_type": "Fix",
                    "pr_number": p.pr_number,
                    "pr_url": p.pr_url,
                    "status": p.status.value,
                    "created_at": p.created_at.isoformat()
                }
                for p in prs
            ],
            "hours": hours
        })
    except Exception as e:
        logging.error(f"CodeXA PRs error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/llm/models')
@login_required
def codexa_api_llm_models():
    """Get available LLM models."""
    if not CODEXA_AVAILABLE:
        return jsonify({"error": "CodeXA not available"}), 503

    try:
        from codexa.config import get_available_models, get_config

        config = get_config()
        _apply_codexa_llm_selection(config)
        models = get_available_models()

        normalized_models = []
        for m in models:
            if isinstance(m, dict):
                normalized_models.append({
                    'id': str(m.get('id', '') or ''),
                    'name': str(m.get('name', m.get('id', '')) or ''),
                    'description': str(m.get('description', '') or ''),
                })
            else:
                normalized_models.append({
                    'id': str(getattr(m, 'id', '') or ''),
                    'name': str(getattr(m, 'name', getattr(m, 'id', '')) or ''),
                    'description': str(getattr(m, 'description', '') or ''),
                })

        persisted = _load_codexa_llm_selection()
        current_provider = str((persisted.get('provider') if isinstance(persisted, dict) else '') or config.llm.provider or 'ollama').strip().lower()
        current_bare = str((persisted.get('model') if isinstance(persisted, dict) else '') or config.llm.model or '').strip()
        if ':' in current_bare and current_bare.split(':', 1)[0].strip().lower() in _CODEXA_PROVIDER_PREFIXES:
            current_full_model = current_bare
        else:
            current_full_model = f"{current_provider}:{current_bare}" if current_bare else ''

        filtered_models = [
            m for m in normalized_models
            if m.get('id') and str(m.get('id')).strip() in CODEXA_UI_ALLOWED_MODELS and not str(m.get('id')).strip().lower().startswith('openai:')
        ]
        # Keep currently selected model visible even if it falls outside the
        # curated low-resource list, so operators can see current state.
        current_model = current_full_model or str(config.llm.model or '')
        if current_model and current_model not in {str(m.get('id')) for m in filtered_models}:
            current_item = next((m for m in normalized_models if str(m.get('id')) == current_model), None)
            if current_item:
                filtered_models.insert(0, current_item)

        return jsonify({
            "models": filtered_models,
            "current_model": current_model,
            "current": str(config.llm.model or ''),
            "provider": str(config.llm.provider or 'ollama'),
            "gpt_configured": False,
        })
    except Exception as e:
        logging.error(f"CodeXA models error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/llm/model', methods=['POST'])
@operator_required
def codexa_api_set_model():
    """Set the active LLM model."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        data = request.get_json() or {}
        model_id = str(data.get('model_id', '') or '').strip()

        if not model_id:
            return jsonify({"error": "model_id required"}), 400

        from codexa.config import get_config, get_available_models

        available = get_available_models()
        allowed_ids = CODEXA_UI_ALLOWED_MODELS | {
            str(m.get('id', '')).strip()
            for m in available
            if isinstance(m, dict) and str(m.get('id', '')).strip()
        }
        allowed_ids = {m for m in allowed_ids if not str(m).lower().startswith('openai:')}
        if model_id not in allowed_ids:
            return jsonify({"error": "Unsupported model_id"}), 400

        config = get_config()

        provider = _codexa_provider_from_model(model_id)

        with _codexa_llm_selection_lock:
            config.llm.model = _codexa_bare_model_name(model_id)
            config.llm.provider = provider
            _save_codexa_llm_selection(model_id, provider)

        if provider == 'openai':
            return jsonify({"error": "openai/gpt models are disabled for CodeXA"}), 400

        # Ensure live analyzer picks latest provider/model immediately.
        if _codexa_analyzer is not None and getattr(_codexa_analyzer, 'llm', None) is not None:
            llm_cfg = getattr(_codexa_analyzer.llm, 'config', None)
            if llm_cfg is not None:
                llm_cfg.model = _codexa_bare_model_name(model_id)
                llm_cfg.provider = provider
                if provider == 'openai':
                    return jsonify({"error": "openai/gpt models are disabled for CodeXA"}), 400

        return jsonify({"status": "ok", "model": model_id, "provider": provider})
    except Exception as e:
        logging.error(f"CodeXA set model error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/llm/stats')
@login_required
def codexa_api_llm_stats():
    """Get LLM performance stats."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        from codexa.config import get_config

        config = get_config()
        _apply_codexa_llm_selection(config)
        metrics = _codexa_analyzer.llm.get_metrics() if _codexa_analyzer else {}
        llm_cfg = getattr(getattr(_codexa_analyzer, 'llm', None), 'config', None)
        provider_name = str(getattr(llm_cfg, 'provider', '') or config.llm.provider or 'ollama')
        bare_model = str(getattr(llm_cfg, 'model', '') or config.llm.model)
        selected = _load_codexa_llm_selection()
        selected_model = str((selected.get('model') if isinstance(selected, dict) else '') or '').strip()
        if selected_model:
            model_name = selected_model
        elif bare_model:
            model_name = f"{provider_name}:{bare_model}" if ':' not in bare_model else bare_model
        else:
            model_name = ''

        return jsonify({
            "model": model_name,
            "provider": provider_name,
            "total_calls": metrics.get("total_calls", 0),
            "successful_calls": metrics.get("successful_calls", 0),
            "failed_calls": metrics.get("failed_calls", 0),
            "avg_response_time": metrics.get("avg_response_time", 0),
            "total_tokens": metrics.get("total_tokens", 0),
            "avg_tokens_per_call": metrics.get("avg_tokens_per_call", 0),
            "success_rate": metrics.get("success_rate", 0)
        })
    except Exception as e:
        logging.error(f"CodeXA LLM stats error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/ai-agent/codexa/api/llm/test', methods=['POST'])
@operator_required
def codexa_api_llm_test():
    """Test LLM connectivity."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(_codexa_analyzer.test_connection())
            return jsonify({"status": "ok", "response": result})
        finally:
            loop.close()
    except Exception as e:
        logging.error(f"CodeXA LLM test error: {e}")
        return jsonify({"error": str(e), "status": "failed"}), 500

@app.route('/ai-agent/codexa/api/detect', methods=['POST'])
@operator_required
def codexa_api_detect():
    """Manually trigger issue detection from AI Monitoring Agent."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        import asyncio
        payload = request.get_json(silent=True) or {}
        window_minutes = _codexa_parse_window_minutes(payload.get('window_minutes', 60), 60)
        auto_analyze = bool(payload.get('auto_analyze', True))
        auto_pr = bool(payload.get('auto_pr', False))  # never auto-create; user must click Generate PR
        max_auto_issues = max(1, min(int(payload.get('max_auto_issues', 20) or 20), 100))
        min_pr_confidence = max(0.0, min(float(payload.get('min_pr_confidence', 0.75) or 0.75), 1.0))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            detected = loop.run_until_complete(_codexa_detector.detect_issues(window_minutes=window_minutes))
            queued_issue_ids: List[str] = []
            if auto_analyze and detected:
                queued_issue_ids = [str(i.id) for i in detected[:max_auto_issues] if getattr(i, 'id', None)]
                if queued_issue_ids:
                    worker = threading.Thread(
                        target=_codexa_run_auto_pipeline,
                        args=(queued_issue_ids, auto_pr, min_pr_confidence),
                        daemon=True,
                    )
                    worker.start()
            return jsonify({
                "status": "ok",
                "window_minutes": window_minutes,
                "detected_count": len(detected),
                "auto": {
                    "auto_analyze": auto_analyze,
                    "auto_pr": auto_pr,
                    "max_auto_issues": max_auto_issues,
                    "min_pr_confidence": min_pr_confidence,
                    "queued_count": len(queued_issue_ids),
                    "queued_issue_ids": queued_issue_ids,
                },
                "issues": [
                    {
                        "id": i.id,
                        "service": i.service_name,
                        "namespace": i.namespace,
                        "type": i.exception_type,
                        "message": i.exception_message[:200] if i.exception_message else ""
                    }
                    for i in detected
                ]
            })
        finally:
            loop.close()
    except Exception as e:
        logging.error(f"CodeXA detect error: {e}")
        return jsonify({"error": str(e), "status": "failed"}), 500


@app.route('/ai-agent/codexa/api/history')
@login_required
def codexa_api_history():
    """Get CodeXA issue detection history for the last N hours."""
    if not _init_codexa():
        return jsonify({"error": "CodeXA not initialized"}), 503

    try:
        hours = max(1, min(int(request.args.get('hours', 24) or 24), 24 * 7))
        limit = max(1, min(int(request.args.get('limit', 200) or 200), 2000))
        dedupe = str(request.args.get('dedupe', 'true') or 'true').strip().lower() not in {'0', 'false', 'no'}
        items = _codexa_repository.get_issue_history(hours=hours, limit=limit)
        if dedupe:
            # Keep only the most recent entry per (service, namespace) so each
            # service appears exactly once in history regardless of how many
            # distinct exceptions it produced.
            filtered = []
            seen = set()
            for item in items:
                svc = str(item.get('service', '') if isinstance(item, dict) else '')
                ns  = str(item.get('namespace', '') if isinstance(item, dict) else '')
                key = (svc, ns)
                if key in seen:
                    continue
                seen.add(key)
                filtered.append(item)
            items = filtered
        return jsonify({
            "hours": hours,
            "dedupe": dedupe,
            "count": len(items),
            "items": items,
        })
    except Exception as e:
        logging.error(f"CodeXA history error: {e}")
        return jsonify({"error": str(e)}), 500

if ROLLOUT_MONITOR_AVAILABLE:
    try:
        _rollout_monitor.get_monitor().start()
        logging.info("RolloutMonitor background thread started")
    except Exception as _rm_err:
        logging.warning(f"RolloutMonitor failed to start: {_rm_err}")


def _auto_infra_fix_loop():
    """
    Background thread: after every rollout-monitor scan cycle, check for
    fixable rollout issues that don't have a PR yet and auto-create them.
    Infra-level issues are fixed without any manual button press.
    """
    import tempfile
    _seen_auto_prs: set = set()

    def _do_auto_pr(namespace, service, issue):
        key = f"{namespace}/{service}"
        if key in _seen_auto_prs:
            return
        root_cause = issue.get('root_cause', {}) or {}
        if not root_cause.get('fix_possible'):
            return
        gh_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN', '')
        if not gh_token:
            return
        if not INFRA_FIX_AVAILABLE:
            return
        _seen_auto_prs.add(key)
        try:
            pr_id = str(uuid4())[:8]
            branch_name = f"ai-infra-autofix/{service}-{pr_id}"
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_url = f"https://{gh_token}@github.com/fabhotelstech/k8s-manifest.git"
                clone = subprocess.run(
                    ['git', 'clone', '--depth', '1', '--branch', 'azure', repo_url, tmpdir],
                    capture_output=True, text=True, timeout=120
                )
                if clone.returncode != 0:
                    logging.warning(f"[auto-infra] clone failed for {key}")
                    _seen_auto_prs.discard(key)
                    return
                files = _infra_fix.find_k8s_files(tmpdir, namespace, service)
                if not files.get('relative_dir'):
                    logging.warning(f"[auto-infra] no manifest dir for {key}")
                    return
                fix_details = root_cause.get('fix_details', {})
                changes = _infra_fix.apply_rollout_fix(files, namespace, fix_details)
                if not changes:
                    logging.info(f"[auto-infra] no YAML changes for {key}")
                    return
                subprocess.run(['git', 'config', 'user.email', 'ai-agent@fabhotels.com'], cwd=tmpdir, capture_output=True)
                subprocess.run(['git', 'config', 'user.name', 'AI Infra Auto-Fix'], cwd=tmpdir, capture_output=True)
                subprocess.run(['git', 'checkout', '-b', branch_name], cwd=tmpdir, capture_output=True)
                subprocess.run(['git', 'add', '-A'], cwd=tmpdir, capture_output=True)
                changes_text = '\n'.join(f'  - {c}' for c in changes)
                commit_msg = (
                    f"fix({service}): auto-fix infra issue — {root_cause.get('type', 'InfraFix')}\n\n"
                    f"{root_cause.get('description', '')}\n\nChanges:\n{changes_text}\n\n"
                    f"Namespace: {namespace} | Auto-generated by AI Infra Agent"
                )
                subprocess.run(['git', 'commit', '-m', commit_msg], cwd=tmpdir, capture_output=True)
                push = subprocess.run(
                    ['git', 'push', 'origin', branch_name],
                    cwd=tmpdir, capture_output=True, text=True, timeout=120
                )
                if push.returncode != 0:
                    logging.warning(f"[auto-infra] push failed for {key}")
                    _seen_auto_prs.discard(key)
                    return
                pr_title = f"[AI-InfraFix] {service} ({namespace}): {root_cause.get('type', 'fix')}"
                ev_text = '\n'.join(f"  - {e}" for e in (root_cause.get('evidence') or [])[:5])
                ch_text = '\n'.join(f"  - {c}" for c in changes)
                pr_body = (
                    f"## Infra Auto-Fix: `{service}` ({namespace})\n\n"
                    f"### Root Cause\n**{root_cause.get('type', '?')}**: {root_cause.get('description', '')}\n\n"
                    f"### Evidence\n{ev_text}\n\n"
                    f"### Changes\n{ch_text}\n\n"
                    f"---\n*Auto-generated by AI Infrastructure Agent — no manual action required.*"
                )
                pr_run = subprocess.run(
                    ['gh', 'pr', 'create', '--title', pr_title, '--body', pr_body, '--base', 'azure'],
                    cwd=tmpdir, capture_output=True, text=True, timeout=60
                )
                if pr_run.returncode == 0:
                    pr_url = pr_run.stdout.strip()
                    _rollout_monitor.get_monitor().set_fix_pr_url(namespace, service, pr_url)
                    logging.info(f"[auto-infra] PR created: {pr_url}")
                else:
                    logging.warning(f"[auto-infra] PR create failed for {key}: {pr_run.stderr[:200]}")
                    _seen_auto_prs.discard(key)
        except Exception as e:
            logging.error(f"[auto-infra] error for {key}: {e}")
            _seen_auto_prs.discard(key)

    while True:
        try:
            if ROLLOUT_MONITOR_AVAILABLE:
                issues = _rollout_monitor.get_monitor().get_issues()
                for issue in issues:
                    ns  = issue.get('namespace', '')
                    svc = issue.get('service', '')
                    if ns and svc and not issue.get('fix_pr_url'):
                        _do_auto_pr(ns, svc, issue)
        except Exception as e:
            logging.error(f"[auto-infra] loop error: {e}")
        time.sleep(150)


# Auto-infra-fix background thread is DISABLED — PRs are created manually via the
# "Generate PR" button in the dashboard to prevent runaway branch creation.
# if ROLLOUT_MONITOR_AVAILABLE and INFRA_FIX_AVAILABLE:
#     _auto_fix_thread = threading.Thread(
#         target=_auto_infra_fix_loop, daemon=True, name='auto-infra-fix'
#     )
#     _auto_fix_thread.start()


def _infra_audit_loop():
    """
    Background thread: every 5 minutes find failing pods in venus/jupiter,
    clone k8s-manifest once, audit deployment.yaml + service.yaml for each
    failing service, and store results in _infra_audit_results.
    These issues are surfaced in /api/pr/infra-pending for manual 'Generate PR'.
    """
    import json as _json

    def _failing_services(namespace: str) -> Dict[str, str]:
        """Return {svc_key: pod_reason} for pods that are genuinely unhealthy.

        A pod is only considered failing when it has an explicit bad container
        state (CrashLoopBackOff, OOMKilled, ImagePullBackOff, etc.) OR when
        the pod-level Ready condition is False AND the phase is not Pending.
        Pods whose Ready condition is True are always skipped — they are healthy
        regardless of what individual sidecar/init containers report.
        """
        result: Dict[str, str] = {}
        try:
            out = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json'],
                capture_output=True, text=True, timeout=30
            )
            if out.returncode != 0:
                return result
            data = _json.loads(out.stdout)
            for item in data.get('items', []):
                meta = item.get('metadata', {})
                status = item.get('status', {})
                pod_name = meta.get('name', '')
                labels = meta.get('labels', {})
                svc_name = (
                    labels.get('app') or
                    labels.get('app.kubernetes.io/name') or
                    labels.get('app.kubernetes.io/component') or
                    ''
                )
                if not svc_name:
                    parts = pod_name.rsplit('-', 2)
                    svc_name = parts[0] if len(parts) >= 2 else pod_name
                if not svc_name:
                    continue

                phase = status.get('phase', '')

                # Pod-level Ready condition is the ground truth.
                # If it's True, ALL main containers passed readiness — pod is healthy.
                conditions = status.get('conditions') or []
                pod_ready = any(
                    c.get('type') == 'Ready' and c.get('status') == 'True'
                    for c in conditions
                )
                if pod_ready:
                    continue  # healthy — skip regardless of individual container states

                # Only examine explicit bad exit/wait reasons (never the generic NotReady
                # catch-all, which fires on vault-agent sidecars and completed init containers)
                bad = {
                    'CrashLoopBackOff', 'ImagePullBackOff', 'ErrImagePull',
                    'Error', 'CreateContainerConfigError', 'RunContainerError',
                    'OOMKilled',
                }
                is_failing = phase == 'Failed'
                reason = phase if is_failing else ''

                # Check main containers only (not init containers) for bad states
                for cs in (status.get('containerStatuses') or []):
                    if cs.get('ready'):
                        continue
                    state = cs.get('state', {})
                    w_reason = (state.get('waiting') or {}).get('reason', '')
                    t_reason = (state.get('terminated') or {}).get('reason', '')
                    if w_reason in bad or t_reason in bad:
                        is_failing = True
                        reason = w_reason or t_reason
                        break

                # Skip Pending pods unless we already have an explicit bad reason
                if phase == 'Pending' and not reason:
                    continue

                if is_failing and svc_name:
                    key = f"{namespace}/{svc_name}"
                    result[key] = reason or 'Unknown'
        except Exception as e:
            logging.warning(f"[infra-audit] pod list error ({namespace}): {e}")
        return result

    def _fetch_pod_crash_logs(namespace: str, svc_name: str, tail: int = 200) -> str:
        """Return recent crash logs for a failing pod. Empty string on any failure."""
        try:
            # Resolve pod name via app label
            out = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', namespace, '-l', f'app={svc_name}',
                 '--no-headers', '-o', 'custom-columns=NAME:.metadata.name'],
                capture_output=True, text=True, timeout=10
            )
            if out.returncode != 0 or not out.stdout.strip():
                return ''
            pod_name = out.stdout.strip().split('\n')[0]
            # --previous gets the last crashed container's logs
            r = subprocess.run(
                ['kubectl', 'logs', pod_name, '-n', namespace,
                 '--tail', str(tail), '--previous'],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout
            # Fall back to current container logs
            r = subprocess.run(
                ['kubectl', 'logs', pod_name, '-n', namespace, '--tail', str(tail)],
                capture_output=True, text=True, timeout=15
            )
            return r.stdout if r.returncode == 0 else ''
        except Exception as _le:
            logging.debug(f"[infra-audit] log fetch failed {namespace}/{svc_name}: {_le}")
        return ''

    while True:
        try:
            import tempfile
            gh_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN', '')
            if not INFRA_FIX_AVAILABLE or not gh_token:
                time.sleep(300)
                continue

            # Collect failing services from both namespaces
            failing: Dict[str, str] = {}
            for ns in ('venus', 'jupiter'):
                failing.update(_failing_services(ns))

            if not failing:
                logging.debug("[infra-audit] no failing pods found")
                # Clear stale results if all services recovered
                with _infra_audit_results_lock:
                    _infra_audit_results.clear()
                time.sleep(300)
                continue

            logging.info(f"[infra-audit] {len(failing)} failing service(s): {list(failing.keys())}")

            new_results: Dict[str, Dict[str, Any]] = {}
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_url = f"https://{gh_token}@github.com/fabhotelstech/k8s-manifest.git"
                clone = subprocess.run(
                    ['git', 'clone', '--depth', '1', '--branch', 'azure', repo_url, tmpdir],
                    capture_output=True, text=True, timeout=120
                )
                if clone.returncode != 0:
                    logging.warning(f"[infra-audit] clone failed: {clone.stderr[:200]}")
                    time.sleep(300)
                    continue

                for svc_key, pod_reason in failing.items():
                    ns, svc = svc_key.split('/', 1)
                    try:
                        files = _infra_fix.find_k8s_files(tmpdir, ns, svc)
                        fixable = bool(files.get('relative_dir'))
                        issues: List[Dict[str, str]] = []

                        if fixable:
                            if files.get('deployment'):
                                issues.extend(_infra_fix.audit_deployment(files['deployment'], ns))
                            if files.get('deployment') and files.get('service'):
                                issues.extend(_infra_fix.audit_service(files['deployment'], files['service']))
                        else:
                            # Manifest not in k8s-manifest repo — fall back to live kubectl audit
                            logging.debug(f"[infra-audit] no manifest dir for {svc_key}, trying inline kubectl audit")
                            try:
                                import tempfile as _tf2, yaml as _kyaml
                                _dep_out = subprocess.run(
                                    ['kubectl', 'get', 'deployment', svc, '-n', ns, '-o', 'json'],
                                    capture_output=True, text=True, timeout=15
                                )
                                if _dep_out.returncode == 0:
                                    _dep_doc = json.loads(_dep_out.stdout)
                                    with _tf2.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as _tf:
                                        _kyaml.dump(_dep_doc, _tf, default_flow_style=False)
                                        _tmp_dep = _tf.name
                                    issues.extend(_infra_fix.audit_deployment(_tmp_dep, ns))
                                    _svc_out = subprocess.run(
                                        ['kubectl', 'get', 'service', svc, '-n', ns, '-o', 'json'],
                                        capture_output=True, text=True, timeout=15
                                    )
                                    if _svc_out.returncode == 0:
                                        _svc_doc = json.loads(_svc_out.stdout)
                                        with _tf2.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as _tf3:
                                            _kyaml.dump(_svc_doc, _tf3, default_flow_style=False)
                                            _tmp_svc = _tf3.name
                                        issues.extend(_infra_fix.audit_service(_tmp_dep, _tmp_svc))
                                        try: os.unlink(_tmp_svc)
                                        except OSError: pass
                                    try: os.unlink(_tmp_dep)
                                    except OSError: pass
                            except Exception as _ke:
                                logging.warning(f"[infra-audit] inline kubectl audit failed for {svc_key}: {_ke}")

                        if issues:
                            # Verify against actual pod logs before surfacing the PR card.
                            # Manifest analysis alone produces false positives (e.g. MySQL
                            # flagged for missing vault env vars it never uses).
                            issue_types = {i['type'] for i in issues}
                            log_text = _fetch_pod_crash_logs(ns, svc)
                            if log_text:
                                confirmed = _infra_fix.detect_infra_issue([log_text])
                                if not confirmed or confirmed not in issue_types:
                                    logging.info(
                                        f"[infra-audit] {svc_key}: manifest has "
                                        f"{issue_types} but logs confirm "
                                        f"'{confirmed or 'nothing'}' — skipping"
                                    )
                                    continue  # logs don't back up the manifest finding
                            else:
                                # No logs means pod never got to run app code.
                                # Only CreateContainerConfigError/RunContainerError are
                                # genuine secret/config failures worth surfacing.
                                if pod_reason not in (
                                    'CreateContainerConfigError', 'RunContainerError'
                                ):
                                    logging.debug(
                                        f"[infra-audit] {svc_key}: no logs, "
                                        f"reason={pod_reason} — skipping"
                                    )
                                    continue

                            new_results[svc_key] = {
                                'namespace': ns,
                                'service': svc,
                                'issues': issues,
                                'pod_reason': pod_reason,
                                'last_checked': datetime.now().isoformat(),
                                'fixable': fixable,
                            }
                            logging.info(
                                f"[infra-audit] {svc_key}: {len(issues)} issue(s) "
                                f"(log-verified, "
                                f"{'manifest' if fixable else 'kubectl'}): "
                                f"{[i['type'] for i in issues]}"
                            )
                        else:
                            logging.debug(f"[infra-audit] {svc_key}: "
                                          f"{'manifests' if fixable else 'kubectl audit'} clean")
                    except Exception as e:
                        logging.warning(f"[infra-audit] error auditing {svc_key}: {e}")

            with _infra_audit_results_lock:
                # Remove entries for services that are no longer failing
                # (pod recovered since last audit cycle)
                stale = [k for k in _infra_audit_results if k not in failing]
                for k in stale:
                    del _infra_audit_results[k]
                _infra_audit_results.update(new_results)

            logging.info(f"[infra-audit] cycle done: {len(new_results)} service(s) with manifest issues")
            # Infra audit results are surfaced in the dashboard only — no Slack

        except Exception as e:
            logging.error(f"[infra-audit] loop error: {e}", exc_info=True)

        time.sleep(300)


_init_slack()

if INFRA_FIX_AVAILABLE:
    try:
        threading.Thread(target=_infra_audit_loop, daemon=True, name='infra-audit').start()
        logging.info("Infra-audit background thread started (5-min manifest scan)")
    except Exception as _ia_err:
        logging.warning(f"Infra-audit thread failed to start: {_ia_err}")

if CODEXA_AVAILABLE:
    try:
        threading.Thread(target=_codexa_background_poll, daemon=True, name='codexa-poll').start()
        logging.info("CodeXA background detection thread started")
    except Exception as _cp_err:
        logging.warning(f"CodeXA background poll failed to start: {_cp_err}")

if K8S_AGENT_AVAILABLE:
    try:
        _k8s_agent.get_agent().start()
        logging.info("KubernetesAIAgent background thread started")
    except Exception as _ka_err:
        logging.warning(f"KubernetesAIAgent failed to start: {_ka_err}")

if __name__ == '__main__':
    print("AI Monitoring Agent Dashboard - Integrated Mode")
    print("=" * 40)
    print(f"Template directory: {template_dir}")
    print(f"Static directory: {static_dir}")
    print(f"Agent connection: {'Connected' if agent_available else 'Not Connected'}")
    print("Starting web server on http://localhost:5000")
    print("Press CTRL+C to stop")

    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
