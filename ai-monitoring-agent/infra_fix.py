"""
Infra Fix: Detect and repair Kubernetes manifest issues.

Detects: VaultSecretMissing, PortMismatch, YamlSyntax, SelectorMismatch
Repairs: vault config injection, port alignment, YAML re-format
"""

import logging
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger("infra_fix")

BACKEND_TECHS = ["java17", "java8", "nodejs", "python"]

# Suffixes stripped when normalizing service names for fuzzy matching
_NAME_SUFFIXES = [
    "-service", "-api", "-svc", "-server", "-app",
    "-backend", "-worker", "-job", "-consumer", "-producer",
]


def _normalize(name: str) -> str:
    """Lowercase, collapse separators, strip common suffixes."""
    n = name.lower().replace("_", "-").strip()
    for sfx in _NAME_SUFFIXES:
        if n.endswith(sfx):
            n = n[: -len(sfx)]
            break
    return n


def _find_best_match(service: str, candidates: List[str]) -> Optional[str]:
    """
    Match *service* against *candidates* (directory names) using a
    priority cascade:
      1. Exact string match
      2. Case-insensitive exact
      3. Separator-normalised (underscore ↔ hyphen)
      4. Suffix-stripped normalised
      5. Prefix containment (one is a prefix of the other after normalise)
      6. difflib fuzzy ≥ 0.75
    Returns the best candidate name or None.
    """
    from difflib import get_close_matches, SequenceMatcher

    if not candidates:
        return None

    # 1. Exact
    if service in candidates:
        return service

    # 2. Case-insensitive
    sl = service.lower()
    for c in candidates:
        if c.lower() == sl:
            return c

    # 3. Separator normalisation only
    sd = service.lower().replace("_", "-")
    for c in candidates:
        if c.lower().replace("_", "-") == sd:
            return c

    # 4. Full normalisation (suffix strip + sep)
    sn = _normalize(service)
    norm_map: Dict[str, str] = {_normalize(c): c for c in candidates}
    if sn in norm_map:
        return norm_map[sn]

    # 5. Prefix containment
    for cn, c in norm_map.items():
        if sn and cn and (sn.startswith(cn) or cn.startswith(sn)):
            return c

    # 6. difflib fuzzy
    norms = list(norm_map.keys())
    close = get_close_matches(sn, norms, n=1, cutoff=0.75)
    if close:
        return norm_map[close[0]]

    return None


def resolve_service_dir(
    base_dir: str, namespace: str, service: str
) -> Optional[tuple]:
    """
    Scan the cloned k8s-manifest repo for the best matching directory for
    *service* under *namespace*.  Tries backend/{tech}/* then frontend/*.

    Returns ``(absolute_dir, relative_path)`` or ``None`` if nothing matches.
    """
    import os

    search_roots: List[tuple] = []
    for tech in BACKEND_TECHS:
        parent = os.path.join(base_dir, namespace, "backend", tech)
        if os.path.isdir(parent):
            search_roots.append((parent, f"{namespace}/backend/{tech}"))

    frontend_parent = os.path.join(base_dir, namespace, "frontend")
    if os.path.isdir(frontend_parent):
        search_roots.append((frontend_parent, f"{namespace}/frontend"))

    for parent, rel_prefix in search_roots:
        try:
            dirs = [d for d in os.listdir(parent) if os.path.isdir(os.path.join(parent, d))]
        except OSError:
            continue
        match = _find_best_match(service, dirs)
        if match:
            abs_dir = os.path.join(parent, match)
            rel_dir = f"{rel_prefix}/{match}"
            if match != service:
                logger.info(
                    f"Service name '{service}' resolved to directory '{match}' "
                    f"({rel_dir})"
                )
            return abs_dir, rel_dir

    return None

# Required vault env vars with their expected shape
VAULT_ENV_REQUIRED = [
    {"name": "GOOGLE_APPLICATION_CREDENTIALS", "value": "/var/secrets/gcp/gcpKey.json"},
    {"name": "SM_CONFIG_SOURCE", "value": "classpath:application-vault.properties"},
    {
        "name": "VAULT_TOKEN",
        "valueFrom": {
            "secretKeyRef": {"key": "VAULT_TOKEN", "name": "vault-secret"}
        },
    },
]

VAULT_VOLUME_MOUNT = {
    "name": "gcp-credentials",
    "mountPath": "/var/secrets/gcp",
    "readOnly": True,
}

VAULT_VOLUME = {
    "name": "gcp-credentials",
    "secret": {"secretName": "google-application-credentials"},
}

# Error text patterns that indicate vault / secret config problems
_VAULT_PATTERNS = [
    r"(?i)could not locate propertysource",
    r"(?i)application-vault\.properties",
    r"(?i)no vault secrets",
    r"(?i)vault.*token",
    r"(?i)vault.*error",
    r"(?i)vault.*fail",
    r"(?i)vault.*connection",
    r"(?i)vault.*unavailable",
    r"(?i)could not resolve placeholder.*vault",
    r"(?i)sm_config_source",
    r"(?i)gcpkey\.json.*not found",
    r"(?i)google.*application.*credentials.*not found",
    r"(?i)secret.*vault-secret.*not found",
    r"(?i)beancreationexception.*vault",
]

# Patterns indicating a port-related connectivity failure
_PORT_PATTERNS = [
    r"(?i)connection refused.*:\d{4}",
    r"(?i)connect.*refused.*port",
    r"(?i)failed to connect.*:\d{4}",
    r"(?i)no route to host",
    r"(?i)503 service unavailable",
]

# Patterns suggesting YAML/config parse problems
_YAML_PATTERNS = [
    r"(?i)yaml.*error",
    r"(?i)yaml.*parse",
    r"(?i)invalid.*yaml",
    r"(?i)mapping values are not allowed",
    r"(?i)found duplicate key",
    r"(?i)block sequence entries are not allowed",
]


def detect_infra_issue(error_texts: List[str]) -> str:
    """Return the most likely infra issue type from a list of error strings, or ''."""
    combined = " ".join(str(t or "") for t in error_texts)
    for p in _VAULT_PATTERNS:
        if re.search(p, combined):
            return "VaultSecretMissing"
    for p in _YAML_PATTERNS:
        if re.search(p, combined):
            return "YamlSyntax"
    for p in _PORT_PATTERNS:
        if re.search(p, combined):
            return "PortMismatch"
    return ""


def find_k8s_files(base_dir: str, namespace: str, service: str) -> Dict[str, Any]:
    """Locate deployment/service/hpa files for a service in a cloned k8s-manifest repo.

    Uses fuzzy service-name resolution so names like ``invoice-reader-service``
    match the directory ``invoice-reader``.
    """
    import os

    result: Dict[str, Any] = {
        "deployment": None,
        "service": None,
        "hpa": None,
        "relative_dir": None,
    }

    resolved = resolve_service_dir(base_dir, namespace, service)
    if not resolved:
        return result

    service_dir, rel_dir = resolved
    result["relative_dir"] = rel_dir

    for fname in ["deployment.yaml", "service.yaml", "hpa.yaml"]:
        full = os.path.join(service_dir, fname)
        if os.path.exists(full):
            key = fname.replace(".yaml", "")
            result[key] = full

    return result


def _load_yaml(path: str) -> Tuple[Optional[Any], Optional[str]]:
    """Return (doc, error). doc is None on parse failure."""
    try:
        with open(path, "r") as f:
            doc = yaml.safe_load(f)
        return doc, None
    except yaml.YAMLError as e:
        return None, str(e)


def _find_correct_port(
    deploy_doc: Any, svc_doc: Any = None
) -> Tuple[Optional[int], Dict[str, int]]:
    """
    Collect every port declaration from deployment.yaml (and optionally service.yaml),
    then return the consensus correct port via majority vote.

    Sources checked:
      - spec.template.spec.containers[0].ports[0].containerPort
      - readinessProbe.httpGet.port
      - livenessProbe.httpGet.port
      - service spec.ports[0].targetPort  (if svc_doc provided)

    Tie-break: containerPort wins when vote counts are equal.

    Returns (correct_port, {source_label: port}).
    Returns (None, {}) when not enough data.
    """
    port_sources: Dict[str, int] = {}

    containers = (
        (deploy_doc or {})
        .get("spec", {}).get("template", {}).get("spec", {})
        .get("containers") or []
    )
    if not containers:
        return None, {}

    container = containers[0]

    for p in (container.get("ports") or []):
        if isinstance(p, dict) and p.get("containerPort"):
            try:
                port_sources["containerPort"] = int(p["containerPort"])
            except (ValueError, TypeError):
                pass
            break

    for probe_name in ("readinessProbe", "livenessProbe"):
        probe = container.get(probe_name) or {}
        http_get = probe.get("httpGet") or {}
        probe_port = http_get.get("port")
        if probe_port is not None:
            try:
                port_sources[probe_name] = int(probe_port)
            except (ValueError, TypeError):
                pass

    if svc_doc:
        for svc_port in ((svc_doc.get("spec", {}).get("ports") or [])):
            if isinstance(svc_port, dict) and svc_port.get("targetPort") is not None:
                try:
                    port_sources["service.targetPort"] = int(svc_port["targetPort"])
                except (ValueError, TypeError):
                    pass
                break

    if not port_sources:
        return None, {}

    votes = Counter(port_sources.values())
    ranked = votes.most_common()

    if len(ranked) == 1:
        # All declarations agree — no mismatch
        return ranked[0][0], port_sources

    # Tie-break: if top two have equal vote count, prefer containerPort value
    if ranked[0][1] == ranked[1][1]:
        cp = port_sources.get("containerPort")
        correct_port = cp if cp is not None else ranked[0][0]
    else:
        correct_port = ranked[0][0]

    return correct_port, port_sources


def audit_deployment(path: str, namespace: str) -> List[Dict[str, str]]:
    """Return list of {type, description} for every detected problem in deployment.yaml."""
    issues = []
    doc, err = _load_yaml(path)
    if err:
        issues.append({"type": "YamlSyntax", "description": f"YAML parse error: {err}"})
        return issues

    spec = (doc or {}).get("spec", {}).get("template", {}).get("spec", {}) or {}
    containers = spec.get("containers") or []
    if not containers:
        issues.append({"type": "YamlSyntax", "description": "No containers found in spec"})
        return issues

    container = containers[0]
    env = container.get("env") or []
    env_names = {str(e.get("name", "") or "") for e in env if isinstance(e, dict)}

    missing_env = []
    for var in VAULT_ENV_REQUIRED:
        if var["name"] not in env_names:
            missing_env.append(var["name"])
    if missing_env:
        issues.append({
            "type": "VaultSecretMissing",
            "description": f"Missing env: {', '.join(missing_env)}",
        })

    vol_mounts = container.get("volumeMounts") or []
    if not any(str(vm.get("name", "") or "") == "gcp-credentials" for vm in vol_mounts):
        issues.append({
            "type": "VaultSecretMissing",
            "description": "Missing volumeMount: gcp-credentials",
        })

    volumes = spec.get("volumes") or []
    if not any(str(v.get("name", "") or "") == "gcp-credentials" for v in volumes):
        issues.append({
            "type": "VaultSecretMissing",
            "description": "Missing volume: gcp-credentials (google-application-credentials)",
        })

    node_selector = spec.get("nodeSelector") or {}
    if node_selector.get("node-pool") != namespace:
        issues.append({
            "type": "NodeSelectorWrong",
            "description": (
                f"nodeSelector.node-pool is '{node_selector.get('node-pool', 'missing')}' "
                f"but should be '{namespace}'"
            ),
        })

    # Cross-validate probe ports using majority vote (deployment only; full
    # cross-check including service.targetPort happens in audit_service)
    correct_port, port_sources = _find_correct_port(doc)
    if correct_port is not None:
        for src, port in port_sources.items():
            if port != correct_port and src in ("readinessProbe", "livenessProbe"):
                agreed = [s for s, p in port_sources.items() if p == correct_port]
                issues.append({
                    "type": "ProbeMismatch",
                    "description": (
                        f"{src}.httpGet.port={port} — correct port is {correct_port}"
                        f" (agreed by: {', '.join(agreed)})"
                    ),
                })

    return issues


def audit_service(deployment_path: str, service_path: str) -> List[Dict[str, str]]:
    """Full cross-validation of ports and selector between deployment and service."""
    issues = []
    deploy_doc, _ = _load_yaml(deployment_path)
    svc_doc, err = _load_yaml(service_path)
    if err:
        issues.append({"type": "YamlSyntax", "description": f"service.yaml parse error: {err}"})
        return issues

    if not deploy_doc or not svc_doc:
        return issues

    # Full port cross-validation: containerPort + probe ports + service targetPort
    # majority vote determines the correct port — any outlier is flagged
    correct_port, port_sources = _find_correct_port(deploy_doc, svc_doc)
    if correct_port is not None:
        for src, port in port_sources.items():
            if port != correct_port:
                agreed = [s for s, p in port_sources.items() if p == correct_port]
                issue_type = "PortMismatch" if src in ("containerPort", "service.targetPort") else "ProbeMismatch"
                issues.append({
                    "type": issue_type,
                    "description": (
                        f"{src}={port} — correct port is {correct_port}"
                        f" (agreed by: {', '.join(agreed)})"
                    ),
                })

    # Selector mismatch
    pod_labels = (
        deploy_doc.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("labels") or {}
    )
    svc_selector = svc_doc.get("spec", {}).get("selector") or {}
    bad_sel = [
        f"{k}={v}" for k, v in svc_selector.items() if pod_labels.get(k) != v
    ]
    if bad_sel:
        issues.append({
            "type": "SelectorMismatch",
            "description": f"service selector mismatch: {', '.join(bad_sel)}",
        })

    return issues


def apply_vault_fix(deployment_path: str, namespace: str) -> Tuple[bool, List[str]]:
    """
    Inject missing vault config into deployment.yaml.
    Returns (changed, [list of changes made]).
    """
    doc, err = _load_yaml(deployment_path)
    if err:
        return False, [f"Parse error: {err}"]

    spec = doc.get("spec", {}).get("template", {}).get("spec", {})
    containers = spec.get("containers")
    if not containers:
        return False, ["No containers"]

    container = containers[0]
    changes: List[str] = []

    # Env vars
    env = list(container.get("env") or [])
    env_names = {str(e.get("name", "") or "") for e in env if isinstance(e, dict)}
    for var in VAULT_ENV_REQUIRED:
        if var["name"] not in env_names:
            env.append(dict(var))
            changes.append(f"+env.{var['name']}")
    container["env"] = env

    # volumeMounts
    mounts = list(container.get("volumeMounts") or [])
    if not any(str(m.get("name", "") or "") == "gcp-credentials" for m in mounts):
        mounts.append(dict(VAULT_VOLUME_MOUNT))
        changes.append("+volumeMount.gcp-credentials")
    container["volumeMounts"] = mounts

    # volumes
    volumes = list(spec.get("volumes") or [])
    if not any(str(v.get("name", "") or "") == "gcp-credentials" for v in volumes):
        volumes.append(dict(VAULT_VOLUME))
        changes.append("+volume.gcp-credentials")
    spec["volumes"] = volumes

    # nodeSelector
    node_selector = dict(spec.get("nodeSelector") or {})
    if node_selector.get("node-pool") != namespace:
        node_selector["node-pool"] = namespace
        spec["nodeSelector"] = node_selector
        changes.append(f"+nodeSelector.node-pool={namespace}")

    if not changes:
        return False, ["Already fully configured"]

    with open(deployment_path, "w") as f:
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False)

    return True, changes


def apply_port_fix(service_path: str, correct_port: int) -> Tuple[bool, str]:
    """Set service targetPort to match the deployment containerPort."""
    doc, err = _load_yaml(service_path)
    if err:
        return False, f"Parse error: {err}"

    ports = doc.get("spec", {}).get("ports") or []
    changed = False
    for port in ports:
        if isinstance(port, dict) and port.get("targetPort") != correct_port:
            port["targetPort"] = correct_port
            changed = True

    if not changed:
        return False, "No port change needed"

    with open(service_path, "w") as f:
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False)

    return True, f"targetPort → {correct_port}"


def apply_yaml_syntax_fix(path: str) -> Tuple[bool, str]:
    """Re-parse and re-dump a YAML file to fix indentation/syntax issues."""
    try:
        with open(path, "r") as f:
            content = f.read()
        doc = yaml.safe_load(content)
        if doc is None:
            return False, "File parsed as empty"
        with open(path, "w") as f:
            yaml.dump(doc, f, default_flow_style=False, sort_keys=False)
        return True, f"{path} re-formatted"
    except yaml.YAMLError as e:
        return False, f"Cannot fix YAML: {e}"


def apply_all_fixes(
    files: Dict[str, Any], namespace: str, issues: List[Dict[str, str]]
) -> List[str]:
    """
    Apply all fixes for the detected issues.
    Returns a list of human-readable change descriptions.
    """
    all_changes: List[str] = []
    issue_types = {i["type"] for i in issues}

    if "VaultSecretMissing" in issue_types and files.get("deployment"):
        ok, changes = apply_vault_fix(files["deployment"], namespace)
        if ok:
            all_changes.extend(changes)
        else:
            logger.warning(f"Vault fix skipped: {changes}")

    if "NodeSelectorWrong" in issue_types and files.get("deployment"):
        # Vault fix already handles nodeSelector; skip if already handled
        if not any(c.startswith("+nodeSelector") for c in all_changes):
            ok, changes = apply_vault_fix(files["deployment"], namespace)
            if ok:
                all_changes.extend([c for c in changes if "nodeSelector" in c])

    if ("PortMismatch" in issue_types or "ProbeMismatch" in issue_types) and files.get("deployment"):
        dep_doc, _ = _load_yaml(files["deployment"])
        svc_doc, _ = _load_yaml(files["service"]) if files.get("service") else (None, None)
        correct_port, port_sources = _find_correct_port(dep_doc, svc_doc)
        if correct_port:
            for src, port in (port_sources or {}).items():
                if port == correct_port:
                    continue
                if src == "service.targetPort" and files.get("service"):
                    ok, desc = apply_port_fix(files["service"], correct_port)
                    if ok:
                        all_changes.append(f"service.yaml: {desc}")
                elif src in ("readinessProbe", "livenessProbe"):
                    probe_type = src.replace("Probe", "").lower()
                    ok, probe_changes = apply_probe_fix(files["deployment"], probe_type, correct_port)
                    if ok:
                        all_changes.extend(probe_changes)
                elif src == "containerPort" and files.get("deployment"):
                    # containerPort itself is the outlier — patch it in deployment.yaml
                    dep_doc2, _ = _load_yaml(files["deployment"])
                    containers2 = (dep_doc2 or {}).get("spec", {}).get("template", {}).get("spec", {}).get("containers") or []
                    if containers2:
                        for p in (containers2[0].get("ports") or []):
                            if isinstance(p, dict) and p.get("containerPort"):
                                p["containerPort"] = correct_port
                                break
                        with open(files["deployment"], "w") as _f:
                            yaml.dump(dep_doc2, _f, default_flow_style=False, sort_keys=False)
                        all_changes.append(f"deployment.yaml: containerPort → {correct_port}")

    if "YamlSyntax" in issue_types:
        for fname in ["deployment", "service", "hpa"]:
            if files.get(fname):
                ok, desc = apply_yaml_syntax_fix(files[fname])
                if ok:
                    all_changes.append(f"{fname}.yaml re-formatted")

    return all_changes


# ── Rollout / probe fix ──────────────────────────────────────────────────────

def apply_probe_fix(
    deployment_path: str,
    probe_type: str,
    correct_port: int,
    correct_path: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """
    Fix a readiness / liveness probe port (and optionally path) in deployment.yaml.

    probe_type: 'readiness' | 'liveness' | 'startup'
    """
    doc, err = _load_yaml(deployment_path)
    if err:
        return False, [f"Parse error: {err}"]

    containers = (
        doc.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers") or []
    )
    if not containers:
        return False, ["No containers found"]

    changes: List[str] = []
    container = containers[0]
    probe_key = f"{probe_type}Probe"
    probe = container.get(probe_key) or {}
    http_get = probe.get("httpGet") or {}

    old_port = http_get.get("port")
    if old_port != correct_port:
        http_get["port"] = correct_port
        probe["httpGet"] = http_get
        container[probe_key] = probe
        changes.append(f"{probe_key}.httpGet.port: {old_port} → {correct_port}")

    if correct_path is not None:
        old_path = http_get.get("path", "")
        if old_path != correct_path:
            http_get["path"] = correct_path
            changes.append(f"{probe_key}.httpGet.path: {old_path!r} → {correct_path!r}")

    if not changes:
        return False, ["Probe already configured correctly"]

    with open(deployment_path, "w") as f:
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False)

    return True, changes


def apply_rollout_fix(
    files: Dict[str, Any],
    namespace: str,
    fix_details: Dict[str, Any],
) -> List[str]:
    """
    Apply a fix derived from rollout_monitor root-cause analysis.

    fix_details keys (from analyze_deployment_root_cause):
      probe_type, current_probe_port, correct_port, probe_path, files
    """
    all_changes: List[str] = []

    fix_type = fix_details.get("fix_type", "ProbePortMismatch")

    if fix_type in ("ProbePortMismatch", "") and files.get("deployment"):
        probe_type = fix_details.get("probe_type", "readiness")
        correct_port = fix_details.get("correct_port")
        probe_path = fix_details.get("probe_path") or None
        if correct_port:
            ok, chg = apply_probe_fix(
                files["deployment"], probe_type, int(correct_port), probe_path
            )
            if ok:
                all_changes.extend(chg)

    # Also align service targetPort if service.yaml is present
    if files.get("service") and fix_details.get("correct_port"):
        ok, desc = apply_port_fix(files["service"], int(fix_details["correct_port"]))
        if ok:
            all_changes.append(f"service.yaml: {desc}")

    return all_changes
