"""
Kubernetes AI Troubleshooting Agent

Full lifecycle:
  1. Poll all pod/deployment state across monitored namespaces every POLL_INTERVAL s
  2. Detect newly unhealthy pods (new failures not seen before)
  3. Collect evidence: pod describe, logs, events, deployment YAML, service YAML,
     a reference healthy peer for comparison
  4. Send structured evidence to LLM (qwen2.5:7b via Ollama) for diagnosis
  5. Parse LLM response → root cause type + list of YAML file changes
  6. Apply changes to k8s-manifest clone and open a PR on the azure branch
  7. Store incidents in memory; expose via Flask routes

Completely LLM-driven: no hardcoded issue types, ports, service names, or
health-check paths. The LLM reasons from raw evidence.
"""

import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import requests
import yaml

logger = logging.getLogger("k8s_agent")

# ── Configuration ─────────────────────────────────────────────────────────────

MONITORED_NAMESPACES = ["venus", "jupiter"]
POLL_INTERVAL        = int(os.getenv("K8S_AGENT_POLL_INTERVAL", "90"))
ANALYSIS_COOLDOWN    = int(os.getenv("K8S_AGENT_COOLDOWN", "600"))   # re-analyze after 10 min
CONFIDENCE_THRESHOLD = float(os.getenv("K8S_AGENT_CONFIDENCE", "0.55"))
AUTO_PR              = os.getenv("K8S_AGENT_AUTO_PR", "false").lower() in ("1", "true", "yes")

# LLM — reuse the codexa-tuned Ollama settings (7b model, long timeout)
_LLM_HOST      = os.getenv("CODEXA_LLM_HOST", os.getenv("OLLAMA_HOST", "http://ollama:11434")).rstrip("/")
_LLM_MODEL     = os.getenv("CODEXA_LLM_MODEL", os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct-q4_K_M"))
_LLM_TIMEOUT   = int(os.getenv("CODEXA_LLM_TIMEOUT", "900"))
_LLM_MAX_TOK   = int(os.getenv("CODEXA_LLM_MAX_TOKENS", "1024"))

# Ollama is single-threaded. Concurrent requests from multiple analysis
# threads cause idle TCP connections that get dropped by the k8s network
# layer, producing RemoteDisconnected errors. Serialize all LLM calls.
_LLM_SEM = threading.Semaphore(1)

# GitHub
_GH_ORG   = os.getenv("CODEXA_GITHUB_ORG", "fabhotelstech")
_GH_BRANCH = "azure"     # k8s-manifest base branch

# ── kubectl helpers ────────────────────────────────────────────────────────────

def _kstr(args: List[str], timeout: int = 12) -> str:
    """Run kubectl; return stdout (empty on failure)."""
    try:
        r = subprocess.run(["kubectl"] + args, capture_output=True, text=True, timeout=timeout)
        return r.stdout if r.returncode == 0 else ""
    except Exception:
        return ""


def _kjson(args: List[str], timeout: int = 12) -> Optional[Any]:
    out = _kstr(args + ["-o", "json"], timeout=timeout)
    try:
        return json.loads(out) if out else None
    except json.JSONDecodeError:
        return None


def _top_error_lines(log_text: str, max_lines: int = 50) -> str:
    """Return the most informative log lines, prioritising ERROR/WARN lines."""
    lines = (log_text or "").splitlines()
    priority = [l for l in lines if re.search(r"(?i)error|exception|warn|fail|fatal|caused by|at ", l)]
    rest     = [l for l in lines if l not in priority]
    selected = priority[-max_lines:] if len(priority) >= max_lines else (priority + rest)[-(max_lines):]
    return "\n".join(selected)


# ── Evidence collector ─────────────────────────────────────────────────────────

def collect_evidence(namespace: str, pod_name: str, deploy_name: str) -> Dict[str, str]:
    """
    Gather all kubectl data for an unhealthy pod and its workload.
    Returns a dict of text blobs keyed by section name.
    """
    ev: Dict[str, str] = {}

    # Pod describe (conditions, events, probe config)
    desc = _kstr(["describe", "pod", pod_name, "-n", namespace], timeout=15)
    # Keep first 80 lines — contains all relevant sections
    ev["pod_describe"] = "\n".join(desc.splitlines()[:80])

    # Current container logs
    cur = _kstr(["logs", pod_name, "-n", namespace, "--tail=60"], timeout=15)
    ev["pod_logs_current"] = _top_error_lines(cur, 50)

    # Previous container logs (after crash)
    prev = _kstr(["logs", pod_name, "-n", namespace, "--tail=60", "--previous"], timeout=15)
    ev["pod_logs_previous"] = _top_error_lines(prev, 40)

    # Kubernetes Warning events for the pod
    events_doc = _kjson([
        "get", "events", "-n", namespace,
        f"--field-selector=involvedObject.name={pod_name},type=Warning",
    ])
    ev_lines = []
    for e in (events_doc or {}).get("items", []):
        ts  = e.get("lastTimestamp", "")
        msg = e.get("message", "")
        rsn = e.get("reason", "")
        ev_lines.append(f"[{ts}] {rsn}: {msg}")
    ev["pod_events"] = "\n".join(ev_lines[-15:])

    # Deployment YAML
    deploy_doc = _kjson(["get", "deployment", deploy_name, "-n", namespace])
    if deploy_doc:
        # Strip managed fields to reduce tokens
        deploy_doc.get("metadata", {}).pop("managedFields", None)
        deploy_doc.get("metadata", {}).pop("annotations", None)
        ev["deployment_yaml"] = yaml.dump(deploy_doc, default_flow_style=False, sort_keys=False)[:3000]
    else:
        ev["deployment_yaml"] = ""

    # Service YAML
    svc_doc = _kjson(["get", "service", deploy_name, "-n", namespace])
    if not svc_doc:
        svcs = _kjson(["get", "services", "-n", namespace, "-l", f"app={deploy_name}"])
        svc_items = (svcs or {}).get("items", [])
        svc_doc = svc_items[0] if svc_items else None
    if svc_doc:
        svc_doc.get("metadata", {}).pop("managedFields", None)
        ev["service_yaml"] = yaml.dump(svc_doc, default_flow_style=False, sort_keys=False)[:1500]
    else:
        ev["service_yaml"] = "(not found)"

    # Reference: one healthy deployment in same namespace for pattern comparison
    all_deploys = _kjson(["get", "deployments", "-n", namespace])
    healthy_ref = ""
    for d in (all_deploys or {}).get("items", []):
        dname = d.get("metadata", {}).get("name", "")
        if dname == deploy_name:
            continue
        ready    = int(d.get("status", {}).get("readyReplicas", 0) or 0)
        desired  = int(d.get("spec", {}).get("replicas", 1) or 1)
        if ready >= desired:
            # Extract just env + probe + port section for reference
            containers = (
                d.get("spec", {}).get("template", {})
                .get("spec", {}).get("containers", [])
            )
            if containers:
                ref_c = containers[0]
                ref_snip = {
                    "name": dname,
                    "env_keys": [e.get("name") for e in (ref_c.get("env") or [])],
                    "ports": ref_c.get("ports"),
                    "readinessProbe": ref_c.get("readinessProbe"),
                    "livenessProbe": ref_c.get("livenessProbe"),
                    "volumeMounts": [m.get("name") for m in (ref_c.get("volumeMounts") or [])],
                }
                healthy_ref = json.dumps(ref_snip, indent=2)[:800]
                break
    ev["healthy_reference"] = healthy_ref or "(none found)"

    return ev


# ── LLM integration ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are a senior Kubernetes SRE. "
    "Analyze the given evidence and respond ONLY with a single valid JSON object. "
    "No markdown, no prose, no explanation outside the JSON."
)

_DIAGNOSIS_SCHEMA = """
{
  "root_cause_type": "<short label, e.g. ReadinessProbePortMismatch>",
  "root_cause_description": "<one paragraph explaining what is wrong and why>",
  "severity": "critical|high|medium|low",
  "confidence": <0.0-1.0>,
  "fix_description": "<what needs to change and why it will fix the issue>",
  "changes": [
    {
      "file": "deployment.yaml|service.yaml|hpa.yaml",
      "yaml_path": "<dot-notation path, array index as [N], e.g. spec.template.spec.containers[0].readinessProbe.httpGet.port>",
      "current_value": "<current value as string>",
      "new_value": "<correct value — same type as the field>",
      "description": "<why this change fixes the issue>"
    }
  ],
  "pr_title": "<concise PR title>",
  "pr_body": "<markdown PR body with root cause, evidence summary, impact analysis>"
}
"""

def _build_prompt(namespace: str, service: str, evidence: Dict[str, str]) -> str:
    def _section(title: str, body: str) -> str:
        body = (body or "").strip()
        if not body:
            return ""
        return f"\n## {title}\n```\n{body}\n```\n"

    return (
        f"A pod in the `{namespace}` namespace belonging to deployment `{service}` "
        f"is failing. Diagnose the root cause and propose a fix.\n"
        + _section("Pod Describe (truncated)", evidence.get("pod_describe", ""))
        + _section("Pod Events (Warnings)", evidence.get("pod_events", ""))
        + _section("Container Logs (current)", evidence.get("pod_logs_current", ""))
        + _section("Container Logs (previous crash)", evidence.get("pod_logs_previous", ""))
        + _section("Deployment YAML", evidence.get("deployment_yaml", ""))
        + _section("Service YAML", evidence.get("service_yaml", ""))
        + _section("Reference: Healthy Peer in Same Namespace", evidence.get("healthy_reference", ""))
        + f"\n## Required Response Schema\n```json\n{_DIAGNOSIS_SCHEMA}\n```\n"
        + "\nRespond with ONLY the JSON object. No other text."
    )


def call_llm(prompt: str) -> str:
    """Call Ollama and return the raw response text.

    Serialized via _LLM_SEM: Ollama is single-threaded, so queuing HTTP
    connections from multiple threads causes TCP idle-timeout disconnects.
    """
    payload = {
        "model": _LLM_MODEL,
        "prompt": f"{_SYSTEM_PROMPT}\n\n{prompt}",
        "stream": False,
        "options": {
            "temperature": 0.05,
            "top_p": 0.9,
            "num_predict": _LLM_MAX_TOK,
        },
    }
    with _LLM_SEM:
        try:
            resp = requests.post(
                f"{_LLM_HOST}/api/generate",
                json=payload,
                timeout=_LLM_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return str(data.get("response", "") or "").strip()
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return ""


def parse_llm_response(raw: str) -> Optional[Dict[str, Any]]:
    """
    Extract the JSON diagnosis from an LLM response.
    Tries: direct parse → json block → brace extraction.
    """
    text = (raw or "").strip()
    # 1. Direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # 2. Markdown code block
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # 3. First {...} block
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # 4. Minimal extraction
    def _grab(key: str) -> str:
        km = re.search(rf'"{key}"\s*:\s*"([^"]*)"', text)
        return km.group(1) if km else ""
    if _grab("root_cause_type"):
        return {
            "root_cause_type": _grab("root_cause_type"),
            "root_cause_description": _grab("root_cause_description"),
            "severity": _grab("severity") or "high",
            "confidence": 0.4,
            "fix_description": _grab("fix_description"),
            "changes": [],
            "pr_title": _grab("pr_title"),
            "pr_body": _grab("pr_body"),
        }
    return None


# ── YAML path applier (generic) ────────────────────────────────────────────────

def _parse_path(path_str: str) -> List[Any]:
    """'containers[0].readinessProbe.httpGet.port' → ['containers', 0, 'readinessProbe', ...]"""
    parts: List[Any] = []
    for seg in path_str.split("."):
        m = re.match(r"^(\w+)\[(\d+)\]$", seg)
        if m:
            parts.append(m.group(1))
            parts.append(int(m.group(2)))
        else:
            parts.append(seg)
    return parts


def _coerce(value: str, current: Any) -> Any:
    """Coerce the string new_value to match the type of current."""
    if isinstance(current, int):
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
    if isinstance(current, float):
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
    if isinstance(current, bool):
        return str(value).lower() in ("true", "1", "yes")
    # Also try int/float from string when current is absent
    try:
        return int(value)
    except (ValueError, TypeError):
        pass
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


def apply_yaml_path(doc: Any, path_str: str, new_value: str) -> Tuple[bool, str]:
    """
    Set *new_value* at the dotted YAML *path_str* inside *doc* (in-place).
    Returns (changed, description).
    """
    parts = _parse_path(path_str)
    node = doc
    try:
        for part in parts[:-1]:
            if isinstance(node, list):
                node = node[int(part)]
            elif isinstance(node, dict):
                if part not in node:
                    return False, f"Path not found: {part} in {list(node.keys())}"
                node = node[part]
            else:
                return False, f"Cannot traverse into {type(node)} at {part}"

        last = parts[-1]
        if isinstance(node, list):
            current = node[int(last)]
            coerced = _coerce(new_value, current)
            if node[int(last)] == coerced:
                return False, "Value already correct"
            node[int(last)] = coerced
        elif isinstance(node, dict):
            current = node.get(last)
            coerced = _coerce(new_value, current)
            if node.get(last) == coerced:
                return False, "Value already correct"
            node[last] = coerced
        else:
            return False, f"Cannot set on {type(node)}"

        return True, f"{path_str}: {current!r} → {coerced!r}"
    except (IndexError, KeyError, TypeError) as e:
        return False, f"Path error at '{path_str}': {e}"


def apply_llm_changes(
    deploy_path: Optional[str],
    service_path: Optional[str],
    hpa_path: Optional[str],
    changes: List[Dict[str, Any]],
) -> List[str]:
    """
    Apply all LLM-suggested changes to the appropriate manifest files.
    Returns list of human-readable change descriptions.
    """
    file_map: Dict[str, Optional[str]] = {
        "deployment.yaml": deploy_path,
        "service.yaml": service_path,
        "hpa.yaml": hpa_path,
    }
    # Cache loaded docs per file path to avoid double-load
    docs: Dict[str, Any] = {}

    applied: List[str] = []
    for change in changes:
        fname   = change.get("file", "")
        path    = change.get("yaml_path", "")
        new_val = str(change.get("new_value", ""))
        desc    = change.get("description", "")

        fpath = file_map.get(fname)
        if not fpath:
            logger.warning(f"LLM suggested change to {fname} but file not found — skipping")
            continue
        if fpath not in docs:
            try:
                with open(fpath) as f:
                    docs[fpath] = yaml.safe_load(f)
            except Exception as e:
                logger.error(f"Cannot load {fpath}: {e}")
                continue

        ok, detail = apply_yaml_path(docs[fpath], path, new_val)
        if ok:
            applied.append(f"{fname} [{path}]: {detail}")
            logger.info(f"Applied: {fname} {detail}")
        else:
            logger.warning(f"Skipped change to {fname} at {path}: {detail}")

    # Write modified docs back to disk
    for fpath, doc in docs.items():
        try:
            with open(fpath, "w") as f:
                yaml.dump(doc, f, default_flow_style=False, sort_keys=False)
        except Exception as e:
            logger.error(f"Cannot write {fpath}: {e}")

    return applied


# ── PR creation ────────────────────────────────────────────────────────────────

def create_fix_pr(
    namespace: str,
    service: str,
    diagnosis: Dict[str, Any],
    changes_applied: List[str],
    files: Dict[str, Optional[str]],
    files_rel: Dict[str, Optional[str]],
    tmpdir: str,
    gh_token: str,
    triggered_by: str = "ai-agent",
) -> Optional[str]:
    """Commit applied changes and open a GitHub PR. Returns PR URL or None."""
    branch = f"ai-k8s-fix/{service}-{str(uuid4())[:8]}"

    subprocess.run(["git", "config", "user.email", "ai-agent@fabhotels.com"], cwd=tmpdir, capture_output=True)
    subprocess.run(["git", "config", "user.name", "K8s AI Agent"], cwd=tmpdir, capture_output=True)
    subprocess.run(["git", "checkout", "-b", branch], cwd=tmpdir, capture_output=True)
    subprocess.run(["git", "add", "-A"], cwd=tmpdir, capture_output=True)

    rc_type = diagnosis.get("root_cause_type", "UnknownFailure")
    rc_desc = diagnosis.get("root_cause_description", "")
    changes_md = "\n".join(f"  - {c}" for c in (changes_applied or ["(no YAML changes — see PR body)"]))

    commit_msg = (
        f"fix({service}): {rc_type}\n\n"
        f"{rc_desc[:200]}\n\n"
        f"Changes:\n{changes_md}\n\n"
        f"Namespace: {namespace} | Triggered by: {triggered_by}\n"
        f"Generated by K8s AI Troubleshooting Agent"
    )
    subprocess.run(["git", "commit", "-m", commit_msg], cwd=tmpdir, capture_output=True)

    push = subprocess.run(
        ["git", "push", "origin", branch],
        cwd=tmpdir, capture_output=True, text=True, timeout=120
    )
    if push.returncode != 0:
        err = push.stderr.replace(gh_token, "***")
        logger.error(f"Push failed: {err}")
        return None

    modified_files = "\n".join(
        f"- `{rel}`" for key, rel in files_rel.items() if rel and files.get(key)
    )
    pr_title  = diagnosis.get("pr_title") or f"[K8s-AI-Fix] {service} ({namespace}): {rc_type}"
    pr_body   = (
        (diagnosis.get("pr_body") or "")
        + f"\n\n---\n**Modified files:**\n{modified_files}"
        + f"\n\n**Changes applied:**\n{changes_md}"
        + f"\n\n*Generated by K8s AI Troubleshooting Agent · Approved by: {triggered_by}*"
    )

    pr_run = subprocess.run(
        ["gh", "pr", "create", "--title", pr_title, "--body", pr_body, "--base", _GH_BRANCH],
        cwd=tmpdir, capture_output=True, text=True, timeout=60
    )
    if pr_run.returncode == 0:
        url = pr_run.stdout.strip()
        logger.info(f"PR created: {url}")
        return url
    err = pr_run.stderr.replace(gh_token, "***")
    logger.error(f"PR create failed: {err}")
    return None


# ── Pod state tracker ──────────────────────────────────────────────────────────

def _pod_failure_signature(pod: Dict) -> str:
    """Stable fingerprint for a pod failure; used for deduplication."""
    meta   = pod.get("metadata", {})
    status = pod.get("status", {})
    name   = meta.get("name", "")
    uid    = meta.get("uid", "")[:8]
    phase  = status.get("phase", "")
    reason = ""
    for cs in (status.get("containerStatuses", []) or []):
        w = (cs.get("state", {}) or {}).get("waiting", {}) or {}
        if w.get("reason"):
            reason = w["reason"]
            break
    return f"{name}|{uid}|{phase}|{reason}"


def _is_pod_failing(pod: Dict) -> bool:
    """Return True if the pod is in an unhealthy / failing state."""
    status = pod.get("status", {}) or {}
    phase  = status.get("phase", "")
    if phase in ("Failed", "Unknown"):
        return True
    for cs in (status.get("containerStatuses", []) or []):
        waiting = (cs.get("state", {}) or {}).get("waiting", {}) or {}
        reason  = waiting.get("reason", "")
        if reason in ("CrashLoopBackOff", "Error", "ErrImagePull", "ImagePullBackOff",
                      "CreateContainerConfigError", "InvalidImageName"):
            return True
        if not cs.get("ready", True) and cs.get("restartCount", 0) > 2:
            return True
    conditions = {c["type"]: c["status"] for c in (status.get("conditions", []) or [])}
    if conditions.get("Ready") == "False" and conditions.get("ContainersReady") == "False":
        if phase == "Running":
            # Running but not ready — check if it's been long enough (avoid transient)
            start = status.get("startTime", "")
            if start:
                try:
                    age = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(start.replace("Z", "+00:00"))).total_seconds()
                    return age > 120     # failing for >2 min
                except Exception:
                    pass
    return False


def _deploy_for_pod(pod: Dict) -> str:
    """Infer deployment name from pod owner chain (pod → rs → deploy)."""
    for ref in (pod.get("metadata", {}).get("ownerReferences", []) or []):
        if isinstance(ref, dict) and ref.get("kind") == "ReplicaSet":
            # RS name = deploy-name + hash; strip the hash
            rs_name = ref.get("name", "")
            return re.sub(r"-[a-z0-9]{8,10}$", "", rs_name)
    # Fallback: app label
    return pod.get("metadata", {}).get("labels", {}).get("app", "")


# ── Main agent class ───────────────────────────────────────────────────────────

class KubernetesAIAgent:
    """
    Background agent: polls pods, detects failures, calls LLM, creates PRs.
    """

    def __init__(self, namespaces: Optional[List[str]] = None):
        self._namespaces     = namespaces or MONITORED_NAMESPACES
        self._lock           = threading.Lock()
        self._stop           = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Fingerprint → timestamp of last analysis (dedup)
        self._analyzed: Dict[str, float] = {}
        # Incidents: key = incident_id
        self._incidents: Dict[str, Dict[str, Any]] = {}
        self._last_scan: Optional[datetime] = None

    # ── lifecycle ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="k8s-ai-agent")
        self._thread.start()
        logger.info(f"KubernetesAIAgent started (namespaces={self._namespaces}, interval={POLL_INTERVAL}s)")

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        # Stagger first scan by 15s to let the app finish starting
        self._stop.wait(timeout=15)
        while not self._stop.is_set():
            try:
                self._scan_all()
            except Exception as e:
                logger.error(f"Agent scan error: {e}", exc_info=True)
            self._stop.wait(timeout=POLL_INTERVAL)

    # ── scan ───────────────────────────────────────────────────────────────────

    def _scan_all(self) -> None:
        now = datetime.now(timezone.utc)
        for ns in self._namespaces:
            try:
                self._scan_namespace(ns)
            except Exception as e:
                logger.error(f"Namespace scan error ({ns}): {e}")
        with self._lock:
            self._last_scan = now
        # Expire old analyzed fingerprints (keep last hour)
        cutoff = time.time() - 3600
        with self._lock:
            self._analyzed = {k: v for k, v in self._analyzed.items() if v > cutoff}

    def _scan_namespace(self, namespace: str) -> None:
        pods_doc = _kjson(["get", "pods", "-n", namespace])
        if not pods_doc:
            return
        for pod in pods_doc.get("items", []) or []:
            if not _is_pod_failing(pod):
                continue
            sig        = _pod_failure_signature(pod)
            pod_name   = pod.get("metadata", {}).get("name", "")
            deploy     = _deploy_for_pod(pod)
            if not pod_name or not deploy:
                continue
            now_ts = time.time()
            with self._lock:
                last = self._analyzed.get(sig, 0)
            if now_ts - last < ANALYSIS_COOLDOWN:
                continue
            with self._lock:
                self._analyzed[sig] = now_ts
            # Analyze in a separate thread so the scanner keeps running
            t = threading.Thread(
                target=self._analyze_pod,
                args=(namespace, pod_name, deploy),
                daemon=True,
                name=f"k8s-analyze-{pod_name[:20]}"
            )
            t.start()

    # ── analysis ───────────────────────────────────────────────────────────────

    def _analyze_pod(self, namespace: str, pod_name: str, deploy: str) -> None:
        logger.info(f"Analyzing failure: {namespace}/{pod_name} (deploy={deploy})")
        incident_id = str(uuid4())[:12]
        incident: Dict[str, Any] = {
            "id": incident_id,
            "namespace": namespace,
            "service": deploy,
            "pod_name": pod_name,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "status": "analyzing",
            "diagnosis": None,
            "fix_pr_url": None,
            "changes_applied": [],
        }
        with self._lock:
            self._incidents[incident_id] = incident

        try:
            evidence  = collect_evidence(namespace, pod_name, deploy)
            prompt    = _build_prompt(namespace, deploy, evidence)
            raw_resp  = call_llm(prompt)

            if not raw_resp:
                incident["status"] = "llm_unavailable"
                logger.warning(f"LLM returned empty for {namespace}/{pod_name}")
                return

            diagnosis = parse_llm_response(raw_resp)
            if not diagnosis:
                incident["status"] = "parse_failed"
                incident["raw_llm"] = raw_resp[:500]
                logger.warning(f"Could not parse LLM response for {namespace}/{pod_name}")
                return

            incident["diagnosis"] = diagnosis
            confidence = float(diagnosis.get("confidence", 0) or 0)

            if not diagnosis.get("changes") or confidence < CONFIDENCE_THRESHOLD:
                incident["status"] = "diagnosed_no_fix"
                logger.info(
                    f"Diagnosed {namespace}/{pod_name}: {diagnosis.get('root_cause_type')} "
                    f"(confidence={confidence:.2f}, no auto-fix)"
                )
                return

            incident["status"] = "fix_ready"
            logger.info(
                f"Fix ready for {namespace}/{pod_name}: {diagnosis.get('root_cause_type')} "
                f"(confidence={confidence:.2f})"
            )

            if AUTO_PR:
                self._create_pr_for_incident(incident_id)

        except Exception as e:
            logger.error(f"Analysis failed for {namespace}/{pod_name}: {e}", exc_info=True)
            incident["status"] = "error"
            incident["error"] = str(e)

    # ── PR creation ────────────────────────────────────────────────────────────

    def create_pr_for_incident(self, incident_id: str, triggered_by: str = "ai-agent") -> Optional[str]:
        """Public method: create a fix PR for a diagnosed incident."""
        with self._lock:
            incident = self._incidents.get(incident_id)
        if not incident:
            raise ValueError(f"Incident {incident_id} not found")
        if incident.get("status") not in ("fix_ready", "diagnosed_no_fix"):
            raise ValueError(f"Incident status is {incident.get('status')} — not ready for PR")
        return self._create_pr_for_incident(incident_id, triggered_by)

    def _create_pr_for_incident(self, incident_id: str, triggered_by: str = "ai-agent") -> Optional[str]:
        with self._lock:
            incident = dict(self._incidents.get(incident_id, {}))
        if not incident:
            return None
        namespace = incident["namespace"]
        service   = incident["service"]
        diagnosis = incident.get("diagnosis") or {}

        gh_token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN", "")
        if not gh_token:
            logger.error("No GitHub token — cannot create PR")
            with self._lock:
                if incident_id in self._incidents:
                    self._incidents[incident_id]["status"] = "pr_failed"
                    self._incidents[incident_id]["error"] = "GitHub token not configured"
            return None

        try:
            from infra_fix import find_k8s_files
        except ImportError:
            logger.error("infra_fix not available — cannot find k8s files")
            return None

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_url = f"https://{gh_token}@github.com/{_GH_ORG}/k8s-manifest.git"
                clone = subprocess.run(
                    ["git", "clone", "--depth", "1", "--branch", _GH_BRANCH, repo_url, tmpdir],
                    capture_output=True, text=True, timeout=120
                )
                if clone.returncode != 0:
                    logger.error(f"Clone failed: {clone.stderr.replace(gh_token, '***')}")
                    return None

                k8s_files = find_k8s_files(tmpdir, namespace, service)
                if not k8s_files.get("relative_dir"):
                    logger.error(f"No k8s manifest dir found for {namespace}/{service}")
                    with self._lock:
                        if incident_id in self._incidents:
                            self._incidents[incident_id]["status"] = "pr_failed"
                            self._incidents[incident_id]["error"] = "manifest directory not found"
                    return None

                rel = k8s_files["relative_dir"]
                files_rel = {
                    "deployment.yaml": f"{rel}/deployment.yaml" if k8s_files.get("deployment") else None,
                    "service.yaml":    f"{rel}/service.yaml"    if k8s_files.get("service")    else None,
                    "hpa.yaml":        f"{rel}/hpa.yaml"        if k8s_files.get("hpa")        else None,
                }

                changes_applied = apply_llm_changes(
                    k8s_files.get("deployment"),
                    k8s_files.get("service"),
                    k8s_files.get("hpa"),
                    diagnosis.get("changes", []),
                )

                if not changes_applied:
                    logger.warning(f"No YAML changes applied for {service} — creating diagnostic PR")

                pr_url = create_fix_pr(
                    namespace, service, diagnosis, changes_applied,
                    k8s_files, files_rel, tmpdir, gh_token, triggered_by
                )

                with self._lock:
                    if incident_id in self._incidents:
                        self._incidents[incident_id]["fix_pr_url"]       = pr_url
                        self._incidents[incident_id]["changes_applied"]  = changes_applied
                        self._incidents[incident_id]["status"]            = "pr_created" if pr_url else "pr_failed"

                return pr_url

        except Exception as e:
            logger.error(f"PR creation error for {service}: {e}", exc_info=True)
            with self._lock:
                if incident_id in self._incidents:
                    self._incidents[incident_id]["status"] = "pr_failed"
                    self._incidents[incident_id]["error"]  = str(e)
            return None

    # ── public API ─────────────────────────────────────────────────────────────

    def get_incidents(
        self,
        namespace: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._incidents.values())
        if namespace:
            items = [i for i in items if i.get("namespace") == namespace]
        items.sort(key=lambda i: i.get("detected_at", ""), reverse=True)
        return items[:limit]

    def force_analyze(self, namespace: str, service: str) -> str:
        """Manually trigger analysis for a specific deployment. Returns incident_id."""
        pods_doc = _kjson(["get", "pods", "-n", namespace, "-l", f"app={service}"])
        pod_name = ""
        for pod in (pods_doc or {}).get("items", []) or []:
            if _is_pod_failing(pod):
                pod_name = pod.get("metadata", {}).get("name", "")
                break
        if not pod_name:
            # Take any pod for the deployment
            for pod in (pods_doc or {}).get("items", []) or []:
                pod_name = pod.get("metadata", {}).get("name", "")
                if pod_name:
                    break
        if not pod_name:
            raise ValueError(f"No pods found for {namespace}/{service}")

        incident_id = str(uuid4())[:12]
        incident: Dict[str, Any] = {
            "id": incident_id,
            "namespace": namespace,
            "service": service,
            "pod_name": pod_name,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "status": "analyzing",
            "diagnosis": None,
            "fix_pr_url": None,
            "changes_applied": [],
        }
        with self._lock:
            self._incidents[incident_id] = incident

        t = threading.Thread(
            target=self._analyze_pod_by_incident,
            args=(incident_id, namespace, pod_name, service),
            daemon=True,
        )
        t.start()
        return incident_id

    def _analyze_pod_by_incident(
        self, incident_id: str, namespace: str, pod_name: str, deploy: str
    ) -> None:
        with self._lock:
            incident = self._incidents.get(incident_id)
        if not incident:
            return
        try:
            evidence  = collect_evidence(namespace, pod_name, deploy)
            prompt    = _build_prompt(namespace, deploy, evidence)
            raw_resp  = call_llm(prompt)
            diagnosis = parse_llm_response(raw_resp) if raw_resp else None
            with self._lock:
                if incident_id in self._incidents:
                    self._incidents[incident_id]["diagnosis"] = diagnosis
                    confidence = float((diagnosis or {}).get("confidence", 0) or 0)
                    has_changes = bool((diagnosis or {}).get("changes"))
                    if not diagnosis:
                        self._incidents[incident_id]["status"] = "parse_failed"
                    elif not has_changes or confidence < CONFIDENCE_THRESHOLD:
                        self._incidents[incident_id]["status"] = "diagnosed_no_fix"
                    else:
                        self._incidents[incident_id]["status"] = "fix_ready"
        except Exception as e:
            with self._lock:
                if incident_id in self._incidents:
                    self._incidents[incident_id]["status"] = "error"
                    self._incidents[incident_id]["error"] = str(e)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            total     = len(self._incidents)
            fix_ready = sum(1 for i in self._incidents.values() if i.get("status") == "fix_ready")
            pr_done   = sum(1 for i in self._incidents.values() if i.get("status") == "pr_created")
            last_scan = self._last_scan
        age = None
        if last_scan:
            age = round((datetime.now(timezone.utc) - last_scan).total_seconds())
        return {
            "running": self._thread is not None and self._thread.is_alive(),
            "namespaces": self._namespaces,
            "poll_interval": POLL_INTERVAL,
            "auto_pr": AUTO_PR,
            "llm_model": _LLM_MODEL,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "incidents_total": total,
            "incidents_fix_ready": fix_ready,
            "incidents_pr_created": pr_done,
            "last_scan_age_seconds": age,
        }


# ── module-level singleton ─────────────────────────────────────────────────────

_agent: Optional[KubernetesAIAgent] = None


def get_agent() -> KubernetesAIAgent:
    global _agent
    if _agent is None:
        _agent = KubernetesAIAgent()
    return _agent
