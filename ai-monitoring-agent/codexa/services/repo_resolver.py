"""Shared repo/branch resolution.

Single source of truth used by the analyzer, verifier and git_ops so the
clone directory, the build-verification path and the PR target all agree.

Mappings come from config.json:
  - pr_automation.repo_overrides   {service|namespace/service: {repo, branch} | "repo"}
  - codexa.repo_mappings           (same shape, takes precedence)
"""

import json
import logging
import os
from typing import Tuple

logger = logging.getLogger("codexa.repo_resolver")

_CONFIG_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config.json"
)

# Writable override file — survives across analyses in the same pod session
_OVERRIDE_FILE = os.environ.get(
    "CODEXA_REPO_MAPPINGS_FILE",
    "/var/otel/codexa_repo_mappings.json",
)

# In-memory live mappings set by the dashboard on every Save
_LIVE_MAPPINGS: dict = {}


def set_live_mappings(mappings: dict) -> None:
    """Called by web_dashboard when user saves repo mappings via UI."""
    global _LIVE_MAPPINGS
    _LIVE_MAPPINGS = dict(mappings or {})
    # Persist to writable file so analyzer/git_ops pick it up even in sub-processes
    try:
        os.makedirs(os.path.dirname(_OVERRIDE_FILE), exist_ok=True)
        with open(_OVERRIDE_FILE, "w") as f:
            json.dump(_LIVE_MAPPINGS, f)
    except Exception as e:
        logger.warning(f"Could not persist repo mappings to {_OVERRIDE_FILE}: {e}")


def _load_mappings() -> dict:
    """Load merged repo mappings. Priority (highest first):
    1. In-memory live mappings (set by UI save in same process)
    2. Writable override file (/var/otel/codexa_repo_mappings.json)
    3. config.json (ConfigMap — may be read-only, baseline only)
    """
    # Base: config.json
    base: dict = {}
    try:
        if os.path.exists(_CONFIG_JSON):
            with open(_CONFIG_JSON, "r") as f:
                cfg = json.load(f)
            pr_auto = cfg.get("pr_automation", {}) or {}
            base.update(pr_auto.get("repo_overrides", {}) or {})
            codexa_cfg = cfg.get("codexa", {}) or {}
            base.update(codexa_cfg.get("repo_mappings", {}) or {})
    except Exception as e:
        logger.warning(f"Failed to load config.json mappings: {e}")

    # Override: writable file
    file_override: dict = {}
    try:
        if os.path.exists(_OVERRIDE_FILE):
            with open(_OVERRIDE_FILE, "r") as f:
                file_override = json.load(f) or {}
    except Exception as e:
        logger.warning(f"Failed to load override mappings from {_OVERRIDE_FILE}: {e}")

    # Merge: live memory wins over file wins over config.json
    return {**base, **file_override, **_LIVE_MAPPINGS}


def _normalize_svc(s: str) -> str:
    """Lowercase + strip dashes/underscores for fuzzy key matching."""
    return s.lower().replace("-", "").replace("_", "").replace(".", "")


def _apply_mapped(mapped, resolved_repo, resolved_branch):
    if isinstance(mapped, dict):
        if mapped.get("repo"):
            resolved_repo = mapped["repo"]
        if mapped.get("branch"):
            resolved_branch = mapped["branch"]
    elif isinstance(mapped, str) and mapped:
        resolved_repo = mapped
    return resolved_repo, resolved_branch


def resolve_repo_and_branch(config, issue) -> Tuple[str, str]:
    """Resolve the actual repo name and branch for an issue.

    Resolution order for the repo name:
      1. config.json mapping — exact key match (namespace/service or service)
      2. config.json mapping — fuzzy/prefix match (dashes stripped)
      3. issue.repo_name (set by the detector)
      4. issue.service_name

    The branch always prefers the mapping; otherwise the configured default.
    """
    service_name = (issue.service_name or "").lower().strip()
    namespace = (getattr(issue, "namespace", "") or "").lower().strip()

    resolved_repo = issue.repo_name or issue.service_name
    resolved_branch = config.github.default_branch

    mappings = _load_mappings()

    # Step 1 — exact match
    for key in (f"{namespace}/{service_name}", service_name):
        if key in mappings:
            resolved_repo, resolved_branch = _apply_mapped(
                mappings[key], resolved_repo, resolved_branch
            )
            logger.info(f"Resolved {service_name} (exact): repo={resolved_repo}, branch={resolved_branch}")
            return resolved_repo, resolved_branch

    # Step 2 — fuzzy/prefix match: strip dashes/underscores from both sides
    # e.g. "otaconsumer" matches "ota-consumer-service" after normalization
    svc_norm = _normalize_svc(service_name)
    best_key = None
    best_mapped = None
    for key, mapped in mappings.items():
        # Strip namespace part from key for comparison
        key_svc = key.split("/")[-1] if "/" in key else key
        key_norm = _normalize_svc(key_svc)
        # Match if one is a prefix of the other (handles otaconsumer ↔ ota-consumer-service)
        if key_norm and svc_norm and (
            key_norm.startswith(svc_norm) or svc_norm.startswith(key_norm)
        ):
            # Prefer longer (more specific) key
            if best_key is None or len(key_norm) > len(_normalize_svc(best_key.split("/")[-1])):
                best_key = key
                best_mapped = mapped

    if best_key is not None:
        resolved_repo, resolved_branch = _apply_mapped(
            best_mapped, resolved_repo, resolved_branch
        )
        logger.info(f"Resolved {service_name} (fuzzy→{best_key}): repo={resolved_repo}, branch={resolved_branch}")
        return resolved_repo, resolved_branch

    logger.info(f"No mapping for {service_name}, using fallback: repo={resolved_repo}, branch={resolved_branch}")
    return resolved_repo, resolved_branch


def clone_dir_for(workspace: str, repo_name: str, issue_id: str) -> str:
    """Deterministic clone path shared by analyzer/verifier/git_ops."""
    return os.path.join(workspace, f"{repo_name}-{issue_id[:8]}")
