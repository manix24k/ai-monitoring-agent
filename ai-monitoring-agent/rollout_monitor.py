"""
Rollout Monitor: detect unhealthy Kubernetes deployment rollouts.

Polls all Deployments in monitored namespaces every POLL_INTERVAL seconds.
Specifically catches the "half-stuck rollout" pattern: new pods fail readiness /
liveness checks while old pods remain healthy, so the service looks fine overall.

Thread-safe; issues are stored in memory keyed by (namespace, deployment_name).
"""

import json
import logging
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rollout_monitor")

MONITORED_NAMESPACES = ["venus", "jupiter"]
POLL_INTERVAL = int(__import__("os").getenv("ROLLOUT_POLL_INTERVAL", "120"))
KUBECTL_TIMEOUT = 10
# How long (seconds) an issue must be absent before it is auto-cleared
ISSUE_TTL = 600

# ── low-level kubectl helpers ────────────────────────────────────────────────

def _kubectl(args: List[str], timeout: int = KUBECTL_TIMEOUT) -> Optional[str]:
    """Run kubectl; return stdout on success, None on failure."""
    try:
        r = subprocess.run(
            ["kubectl"] + args,
            capture_output=True, text=True, timeout=timeout
        )
        return r.stdout if r.returncode == 0 else None
    except Exception:
        return None


def _kj(args: List[str], timeout: int = KUBECTL_TIMEOUT) -> Optional[Any]:
    """Run kubectl -o json and parse result."""
    out = _kubectl(args + ["-o", "json"], timeout=timeout)
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


# ── data helpers ─────────────────────────────────────────────────────────────

def _conditions_map(status: dict) -> Dict[str, dict]:
    return {c["type"]: c for c in status.get("conditions", []) or []}


def _pod_template_hash(rs: dict) -> str:
    return (
        rs.get("metadata", {})
        .get("labels", {})
        .get("pod-template-hash", "")
    )


def _rs_matches_current(rs: dict, current_revision: str) -> bool:
    ann = rs.get("metadata", {}).get("annotations", {}) or {}
    return ann.get("deployment.kubernetes.io/revision") == current_revision


# ── pod / event analysis ─────────────────────────────────────────────────────

def _pod_events(namespace: str, pod_name: str) -> List[dict]:
    """Fetch Warning events for a pod."""
    doc = _kj([
        "get", "events",
        "-n", namespace,
        f"--field-selector=involvedObject.name={pod_name},type=Warning",
    ])
    if not doc:
        return []
    return doc.get("items", []) or []


def _extract_probe_failure(events: List[dict]) -> Optional[Dict[str, str]]:
    """
    Parse readiness/liveness probe failure events.

    Returns {probe_type, failing_port, failing_path, message} or None.
    """
    for ev in sorted(events, key=lambda e: e.get("lastTimestamp", ""), reverse=True):
        reason = ev.get("reason", "")
        msg = ev.get("message", "")
        if reason not in ("Unhealthy", "ProbeError"):
            continue
        probe_type = (
            "readiness" if "Readiness" in msg
            else "liveness" if "Liveness" in msg
            else "startup" if "Startup" in msg
            else "probe"
        )
        port_m = re.search(r":(\d{2,5})[/\s\"']", msg)
        path_m = re.search(r"https?://[^/]+(/[^\s\"']*)", msg)
        return {
            "probe_type": probe_type,
            "failing_port": port_m.group(1) if port_m else "",
            "failing_path": path_m.group(1) if path_m else "",
            "message": msg[:300],
        }
    return None


def _pod_phase_summary(pod: dict) -> dict:
    """Return a compact summary of a pod's current state."""
    meta = pod.get("metadata", {})
    status = pod.get("status", {})
    container_statuses = status.get("containerStatuses", []) or []

    waiting_reason = ""
    last_exit_code = None
    restart_count = 0
    for cs in container_statuses:
        restart_count = max(restart_count, cs.get("restartCount", 0))
        state = cs.get("state", {}) or {}
        waiting = state.get("waiting") or {}
        if waiting.get("reason"):
            waiting_reason = waiting["reason"]
        terminated = state.get("terminated") or {}
        if terminated.get("exitCode") is not None:
            last_exit_code = terminated["exitCode"]

    conditions = {c["type"]: c["status"] for c in (status.get("conditions", []) or [])}
    ready = conditions.get("Ready") == "True"

    return {
        "name": meta.get("name", ""),
        "phase": status.get("phase", "Unknown"),
        "ready": ready,
        "waiting_reason": waiting_reason,
        "restart_count": restart_count,
        "last_exit_code": last_exit_code,
        "pod_template_hash": (meta.get("labels", {}) or {}).get("pod-template-hash", ""),
        "start_time": status.get("startTime", ""),
    }


# ── deployment-level analysis ────────────────────────────────────────────────

def _spec_probe_port(container: dict, probe_key: str) -> Optional[int]:
    probe = container.get(probe_key) or {}
    http_get = probe.get("httpGet") or {}
    p = http_get.get("port")
    if p is None:
        return None
    try:
        return int(p)
    except (ValueError, TypeError):
        return None


def _spec_container_port(container: dict) -> Optional[int]:
    for p in container.get("ports", []) or []:
        if isinstance(p, dict) and p.get("containerPort"):
            try:
                return int(p["containerPort"])
            except (ValueError, TypeError):
                pass
    return None


def analyze_deployment_root_cause(
    namespace: str,
    deploy: dict,
    failing_pods: List[dict],
    svc_doc: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Examine a failing deployment and its pods to identify a root cause.

    Returns a dict with keys:
      type, description, evidence, fix_possible, fix_details
    """
    result: Dict[str, Any] = {
        "type": "Unknown",
        "description": "",
        "evidence": [],
        "fix_possible": False,
        "fix_details": {},
    }

    spec_containers = (
        deploy.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", []) or []
    )
    container = spec_containers[0] if spec_containers else {}
    container_port = _spec_container_port(container)

    # Static manifest validation first (works even when pod events are sparse):
    # readiness/liveness probe port should match containerPort.
    readiness_port = _spec_probe_port(container, "readinessProbe")
    liveness_port = _spec_probe_port(container, "livenessProbe")
    if container_port is not None:
        for probe_type, probe_port in (("readiness", readiness_port), ("liveness", liveness_port)):
            if probe_port is not None and probe_port != container_port:
                return {
                    "type": "ProbePortMismatch",
                    "description": (
                        f"{probe_type}Probe checks port {probe_port} but containerPort is {container_port}"
                    ),
                    "evidence": [
                        f"Deployment containerPort = {container_port}",
                        f"Deployment {probe_type}Probe.port = {probe_port}",
                    ],
                    "fix_possible": True,
                    "fix_details": {
                        "fix_type": "ProbePortMismatch",
                        "probe_type": probe_type,
                        "current_probe_port": probe_port,
                        "correct_port": container_port,
                        "probe_path": "",
                        "files": ["deployment.yaml"],
                    },
                }

    # Optional service targetPort check (when service manifest is available).
    if svc_doc and container_port is not None:
        try:
            svc_ports = (svc_doc.get("spec", {}) or {}).get("ports", []) or []
            target_port = None
            for p in svc_ports:
                if isinstance(p, dict) and p.get("targetPort") is not None:
                    target_port = int(p.get("targetPort"))
                    break
            if target_port is not None and target_port != container_port:
                return {
                    "type": "PortMismatch",
                    "description": (
                        f"service targetPort is {target_port} but containerPort is {container_port}"
                    ),
                    "evidence": [
                        f"Service targetPort = {target_port}",
                        f"Deployment containerPort = {container_port}",
                    ],
                    "fix_possible": True,
                    "fix_details": {
                        "fix_type": "PortMismatch",
                        "correct_port": container_port,
                        "files": ["service.yaml"],
                    },
                }
        except Exception:
            pass

    for pod in failing_pods:
        pod_name = pod.get("name", "")
        events = _pod_events(namespace, pod_name)
        probe_fail = _extract_probe_failure(events)

        if probe_fail:
            failing_port = int(probe_fail["failing_port"]) if probe_fail["failing_port"] else None
            probe_type = probe_fail["probe_type"]
            spec_probe_port = _spec_probe_port(container, f"{probe_type}Probe")

            evidence = [f"Pod {pod_name}: {probe_fail['message']}"]
            if container_port:
                evidence.append(f"Deployment containerPort = {container_port}")
            if spec_probe_port:
                evidence.append(f"Deployment {probe_type}Probe.port = {spec_probe_port}")

            # Case 1: probe port != container port → clear mismatch in manifest
            if (
                spec_probe_port is not None
                and container_port is not None
                and spec_probe_port != container_port
            ):
                result.update({
                    "type": "ProbePortMismatch",
                    "description": (
                        f"{probe_type}Probe checks port {spec_probe_port} but "
                        f"containerPort is {container_port}"
                    ),
                    "evidence": evidence,
                    "fix_possible": True,
                    "fix_details": {
                        "probe_type": probe_type,
                        "current_probe_port": spec_probe_port,
                        "correct_port": container_port,
                        "probe_path": probe_fail["failing_path"],
                        "files": ["deployment.yaml"],
                    },
                })
                return result

            # Case 2: probe port matches spec but connection refused
            # → app not listening; surface for human, but we can also
            # check the service targetPort
            if failing_port and container_port and failing_port == container_port:
                result.update({
                    "type": "AppNotListeningOnPort",
                    "description": (
                        f"App not responding on port {failing_port}. "
                        f"containerPort and probe port agree ({container_port}); "
                        f"likely the app itself uses a different port (env var/config)."
                    ),
                    "evidence": evidence,
                    "fix_possible": False,
                    "fix_details": {},
                })
                return result

            # Case 3: we know failing port and spec probe port but they're
            # different from what the event says (rare)
            if failing_port and spec_probe_port and failing_port != spec_probe_port:
                result.update({
                    "type": "ProbePortMismatch",
                    "description": (
                        f"{probe_type}Probe is checking port {failing_port} "
                        f"(from event), but spec says {spec_probe_port}"
                    ),
                    "evidence": evidence,
                    "fix_possible": True,
                    "fix_details": {
                        "probe_type": probe_type,
                        "current_probe_port": spec_probe_port,
                        "correct_port": container_port or failing_port,
                        "probe_path": probe_fail["failing_path"],
                        "files": ["deployment.yaml"],
                    },
                })
                return result

            # Generic probe failure — not enough info to auto-fix
            result.update({
                "type": "ProbeFailed",
                "description": f"{probe_type} probe failing: {probe_fail['message'][:150]}",
                "evidence": evidence,
                "fix_possible": False,
            })
            return result

        # No probe event — check CrashLoopBackOff
        waiting_reason = pod.get("waiting_reason", "")
        if waiting_reason in ("CrashLoopBackOff", "Error"):
            result.update({
                "type": "CrashLoopBackOff",
                "description": f"Pod {pod_name} is in {waiting_reason} (exit code {pod.get('last_exit_code')})",
                "evidence": [f"Pod {pod_name}: state={waiting_reason}, restarts={pod.get('restart_count', 0)}"],
                "fix_possible": False,
            })
            return result

        if waiting_reason in ("ErrImagePull", "ImagePullBackOff"):
            result.update({
                "type": "ImagePullError",
                "description": f"Pod {pod_name}: {waiting_reason}",
                "evidence": [f"Cannot pull image for pod {pod_name}"],
                "fix_possible": False,
            })
            return result

    return result


# ── per-namespace scan ───────────────────────────────────────────────────────

def scan_namespace(namespace: str) -> List[Dict[str, Any]]:
    """
    Scan all Deployments in *namespace*.

    Returns a list of issue dicts for unhealthy rollouts.
    """
    issues: List[Dict[str, Any]] = []

    doc = _kj(["get", "deployments", "-n", namespace])
    if not doc:
        return issues

    for deploy in doc.get("items", []) or []:
        meta = deploy.get("metadata", {})
        dep_name = meta.get("name", "")
        if not dep_name:
            continue

        status = deploy.get("status", {}) or {}
        spec_replicas = deploy.get("spec", {}).get("replicas", 1) or 1
        ready = int(status.get("readyReplicas", 0) or 0)
        updated = int(status.get("updatedReplicas", 0) or 0)
        unavailable = int(status.get("unavailableReplicas", 0) or 0)
        conditions = _conditions_map(status)

        # Is this rollout healthy?
        progressing = conditions.get("Progressing", {})
        deadline_exceeded = progressing.get("reason") == "ProgressDeadlineExceeded"
        partial_rollout = updated < spec_replicas and ready >= 1
        has_unavailable = unavailable > 0
        is_stuck = deadline_exceeded or (partial_rollout and has_unavailable)

        if not (has_unavailable or deadline_exceeded or partial_rollout):
            continue

        # Find the current ReplicaSet (newest revision)
        current_rev = (meta.get("annotations", {}) or {}).get(
            "deployment.kubernetes.io/revision", ""
        )
        rs_doc = _kj(["get", "rs", "-n", namespace, "-l", f"app={dep_name}"])
        current_hash = ""
        if rs_doc:
            for rs in rs_doc.get("items", []) or []:
                if _rs_matches_current(rs, current_rev):
                    current_hash = _pod_template_hash(rs)
                    break

        # Get all pods for this deployment
        all_pods_doc = _kj(["get", "pods", "-n", namespace, "-l", f"app={dep_name}"])
        all_pods = []
        if all_pods_doc:
            for pod in all_pods_doc.get("items", []) or []:
                all_pods.append(_pod_phase_summary(pod))

        new_pods = [p for p in all_pods if current_hash and p["pod_template_hash"] == current_hash]
        failing_pods = [p for p in (new_pods or all_pods) if not p["ready"]]

        if not failing_pods and not has_unavailable and not deadline_exceeded:
            continue

        # Build human-readable summary
        if deadline_exceeded:
            summary = f"Deployment progress deadline exceeded ({updated}/{spec_replicas} updated)"
        elif partial_rollout:
            summary = (
                f"Rollout in progress: {updated}/{spec_replicas} updated, "
                f"{ready}/{spec_replicas} ready, {unavailable} unavailable"
            )
        else:
            summary = (
                f"{ready}/{spec_replicas} pods ready — "
                f"{len(failing_pods)} pod(s) failing"
                + (f" (new replica)" if new_pods else "")
            )

        if new_pods and failing_pods:
            old_ready = sum(1 for p in all_pods if p not in new_pods and p["ready"])
            if old_ready > 0:
                summary += f"; {old_ready} old replica(s) still healthy"

        severity = "critical" if deadline_exceeded or ready == 0 else "warning"

        # Detect root cause inline
        svc_doc = _kj(["get", "service", dep_name, "-n", namespace])
        root_cause = analyze_deployment_root_cause(namespace, deploy, failing_pods, svc_doc=svc_doc)

        issues.append({
            "namespace": namespace,
            "service": dep_name,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
            "summary": summary,
            "pods_total": spec_replicas,
            "pods_ready": ready,
            "pods_updated": updated,
            "pods_unavailable": unavailable,
            "failing_pods": [
                {
                    "name": p["name"],
                    "phase": p["phase"],
                    "waiting_reason": p["waiting_reason"],
                    "restart_count": p["restart_count"],
                    "last_exit_code": p["last_exit_code"],
                    "is_new_replica": current_hash != "" and p["pod_template_hash"] == current_hash,
                }
                for p in failing_pods
            ],
            "root_cause": root_cause,
            "fix_pr_url": None,
        })

    return issues


# ── background monitor class ─────────────────────────────────────────────────

class RolloutMonitor:
    """Background thread that polls k8s deployments and stores rollout issues."""

    def __init__(self, namespaces: Optional[List[str]] = None):
        self._namespaces = namespaces or MONITORED_NAMESPACES
        self._issues: Dict[str, Dict[str, Any]] = {}  # key = "ns/svc"
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_scan: Optional[datetime] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="rollout-monitor")
        self._thread.start()
        logger.info(f"RolloutMonitor started (namespaces={self._namespaces}, interval={POLL_INTERVAL}s)")

    def stop(self) -> None:
        self._stop_event.set()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_all()
            except Exception as e:
                logger.error(f"RolloutMonitor scan error: {e}", exc_info=True)
            self._stop_event.wait(timeout=POLL_INTERVAL)

    def _scan_all(self) -> None:
        now = datetime.now(timezone.utc)
        fresh: Dict[str, Dict[str, Any]] = {}

        for ns in self._namespaces:
            for issue in scan_namespace(ns):
                key = f"{ns}/{issue['service']}"
                # Preserve existing fix_pr_url if set
                with self._lock:
                    existing = self._issues.get(key, {})
                if existing.get("fix_pr_url"):
                    issue["fix_pr_url"] = existing["fix_pr_url"]
                fresh[key] = issue

        # Expire issues no longer seen
        # Critical issues (all pods down / deadline exceeded) use full TTL.
        # Warning issues (normal rolling update completing) clear quickly.
        with self._lock:
            for key, old in list(self._issues.items()):
                if key in fresh:
                    continue
                detected = old.get("detected_at", "")
                try:
                    age = (now - datetime.fromisoformat(detected)).total_seconds()
                except Exception:
                    age = ISSUE_TTL + 1
                ttl = ISSUE_TTL if old.get("severity") == "critical" else 60
                if age < ttl:
                    fresh[key] = old  # keep until TTL

            self._issues = fresh
            self._last_scan = now

        logger.info(
            f"RolloutMonitor scan complete: {len(fresh)} active issues "
            f"across {self._namespaces}"
        )

    def get_issues(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            issues = list(self._issues.values())
        if namespace:
            issues = [i for i in issues if i.get("namespace") == namespace]
        return sorted(issues, key=lambda i: i.get("detected_at", ""), reverse=True)

    def set_fix_pr_url(self, namespace: str, service: str, pr_url: str) -> None:
        key = f"{namespace}/{service}"
        with self._lock:
            if key in self._issues:
                self._issues[key]["fix_pr_url"] = pr_url

    def get_issue(self, namespace: str, service: str) -> Optional[Dict[str, Any]]:
        key = f"{namespace}/{service}"
        with self._lock:
            return self._issues.get(key)

    def last_scan_age_seconds(self) -> Optional[float]:
        if not self._last_scan:
            return None
        return (datetime.now(timezone.utc) - self._last_scan).total_seconds()

    def force_scan(self) -> int:
        """Trigger an immediate scan. Returns number of issues found."""
        try:
            self._scan_all()
        except Exception as e:
            logger.error(f"Force scan failed: {e}")
        return len(self._issues)


# Module-level singleton
_monitor: Optional[RolloutMonitor] = None


def get_monitor() -> RolloutMonitor:
    global _monitor
    if _monitor is None:
        _monitor = RolloutMonitor()
    return _monitor
