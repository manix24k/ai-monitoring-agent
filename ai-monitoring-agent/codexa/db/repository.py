"""Repository for CodeXA data storage."""

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from collections import defaultdict

from ..models import CodeIssue, CodeFix, PullRequest, IssueStatus, PRStatus, LLMStats
from ..config import get_config

logger = logging.getLogger("codexa.repository")

# Retention windows
_PR_HISTORY_RETAIN_DAYS = 30
_ISSUE_HISTORY_RETAIN_HOURS = 168  # 7 days
_FIX_HISTORY_RETAIN_DAYS = 30


class IssueRepository:
    """In-memory repository for issues, fixes, and PRs.

    Issues and PRs are persisted to JSON files so stats survive pod restarts.
    Pass pr_history_file / issue_history_file to enable file persistence.
    """

    def __init__(self, redis_client=None, pr_history_file: str = "", issue_history_file: str = "", fix_history_file: str = ""):
        self._redis = redis_client
        self._issues: Dict[str, CodeIssue] = {}
        self._fixes: Dict[str, CodeFix] = {}
        self._prs: Dict[str, PullRequest] = {}
        self._issue_history: List[Dict[str, Any]] = []
        self._analysis_steps: Dict[str, List[Dict[str, Any]]] = {}
        self._pr_file = (pr_history_file or "").strip()
        self._issue_file = (issue_history_file or "").strip()
        self._fix_file = (fix_history_file or "").strip()
        self._pr_file_lock = threading.Lock()
        self._issue_file_lock = threading.Lock()
        self._fix_file_lock = threading.Lock()
        if self._pr_file:
            self._load_prs_from_file()
        if self._issue_file:
            self._load_issues_from_file()
        if self._fix_file:
            self._load_fixes_from_file()

    # ==================== Issue File Persistence ====================

    @staticmethod
    def _issue_from_dict(d: Dict) -> Optional["CodeIssue"]:
        """Reconstruct a CodeIssue from its to_dict() output."""
        try:
            from ..models import IssueType
            ts = d.get("detected_at")
            detected_at = datetime.fromisoformat(ts) if ts else datetime.utcnow()
            if detected_at.tzinfo is None:
                detected_at = detected_at.replace(tzinfo=timezone.utc)
            ats = d.get("analyzed_at")
            analyzed_at = datetime.fromisoformat(ats) if ats else None
            lsts = d.get("last_seen_at")
            last_seen_at = datetime.fromisoformat(lsts) if lsts else None
            return CodeIssue(
                id=d.get("id", ""),
                service_name=d.get("service_name", ""),
                namespace=d.get("namespace", ""),
                error_signature=d.get("error_signature", ""),
                exception_type=d.get("exception_type", ""),
                exception_message=d.get("exception_message", ""),
                stack_trace=d.get("stack_trace", ""),
                log_evidence=d.get("log_evidence", ""),
                file_path=d.get("file_path", ""),
                line_number=int(d.get("line_number", 0) or 0),
                issue_type=IssueType(d.get("issue_type", "other")),
                confidence=float(d.get("confidence", 0.0) or 0.0),
                status=IssueStatus(d.get("status", "pending")),
                detected_at=detected_at,
                analyzed_at=analyzed_at,
                last_seen_at=last_seen_at,
                occurrence_count=int(d.get("occurrence_count", 1) or 1),
                repo_name=d.get("repo_name", ""),
                branch=d.get("branch", ""),
            )
        except Exception:
            return None

    @staticmethod
    def _allowed_services_set() -> set:
        """Read CODEXA_ALLOWED_SERVICES env var and return normalized set.
        Empty set means all services allowed (normal mode).
        """
        import re as _re
        raw = os.environ.get("CODEXA_ALLOWED_SERVICES", "").strip()
        if not raw:
            return set()
        return {_re.sub(r"[-_.]", "", s.strip().lower()) for s in raw.split(",") if s.strip()}

    def _load_issues_from_file(self) -> None:
        if not self._issue_file or not os.path.exists(self._issue_file):
            return
        try:
            with open(self._issue_file, "r") as f:
                raw = json.load(f)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_ISSUE_HISTORY_RETAIN_HOURS)
            allowed = self._allowed_services_set()
            loaded = skipped = 0
            for item in raw if isinstance(raw, list) else []:
                if not isinstance(item, dict):
                    continue
                issue = self._issue_from_dict(item)
                if issue is None:
                    continue
                # Respect CODEXA_ALLOWED_SERVICES on load so old issues from
                # non-allowed services don't reappear after a pod restart.
                if allowed:
                    import re as _re
                    norm = _re.sub(r"[-_.]", "", (issue.service_name or "").strip().lower())
                    if norm not in allowed:
                        skipped += 1
                        continue
                ts = issue.detected_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    self._issues[issue.id] = issue
                    loaded += 1
            logger.info(f"Loaded {loaded} issues from {self._issue_file}"
                        + (f" (skipped {skipped} from non-allowed services)" if skipped else ""))
        except Exception as e:
            logger.warning(f"Could not load issue history from {self._issue_file}: {e}")

    def _save_issues_to_file(self) -> None:
        if not self._issue_file:
            return
        try:
            os.makedirs(os.path.dirname(self._issue_file), exist_ok=True)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_ISSUE_HISTORY_RETAIN_HOURS)
            records = []
            for issue in self._issues.values():
                ts = issue.detected_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    records.append(issue.to_dict())
            with self._issue_file_lock:
                with open(self._issue_file, "w") as f:
                    json.dump(records, f)
        except Exception as e:
            logger.warning(f"Could not save issue history to {self._issue_file}: {e}")

    # ==================== PR File Persistence ====================

    @staticmethod
    def _pr_from_dict(d: Dict) -> Optional["PullRequest"]:
        """Reconstruct a PullRequest from its to_dict() output."""
        try:
            ts = d.get("created_at")
            created_at = datetime.fromisoformat(ts) if ts else datetime.utcnow()
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            mts = d.get("merged_at")
            merged_at = datetime.fromisoformat(mts) if mts else None
            cts = d.get("closed_at")
            closed_at = datetime.fromisoformat(cts) if cts else None
            return PullRequest(
                id=d.get("id", ""),
                issue_id=d.get("issue_id", ""),
                fix_id=d.get("fix_id", ""),
                service_name=d.get("service_name", ""),
                namespace=d.get("namespace", ""),
                repo_name=d.get("repo_name", ""),
                branch_name=d.get("branch_name", ""),
                pr_number=int(d.get("pr_number", 0) or 0),
                pr_url=d.get("pr_url", ""),
                pr_title=d.get("pr_title", ""),
                pr_body=d.get("pr_body", ""),
                status=PRStatus(d.get("status", "created")),
                created_at=created_at,
                merged_at=merged_at,
                closed_at=closed_at,
            )
        except Exception:
            return None

    def _load_prs_from_file(self) -> None:
        if not self._pr_file or not os.path.exists(self._pr_file):
            return
        try:
            with open(self._pr_file, "r") as f:
                raw = json.load(f)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_PR_HISTORY_RETAIN_DAYS)
            loaded = 0
            for item in raw if isinstance(raw, list) else []:
                if not isinstance(item, dict):
                    continue
                pr = self._pr_from_dict(item)
                if pr is None:
                    continue
                pr_ts = pr.created_at
                if pr_ts.tzinfo is None:
                    pr_ts = pr_ts.replace(tzinfo=timezone.utc)
                if pr_ts >= cutoff:
                    self._prs[pr.id] = pr
                    loaded += 1
            logger.info(f"Loaded {loaded} PRs from {self._pr_file}")
        except Exception as e:
            logger.warning(f"Could not load PR history from {self._pr_file}: {e}")

    def _save_prs_to_file(self) -> None:
        if not self._pr_file:
            return
        try:
            os.makedirs(os.path.dirname(self._pr_file), exist_ok=True)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_PR_HISTORY_RETAIN_DAYS)
            records = []
            for pr in self._prs.values():
                pr_ts = pr.created_at
                if pr_ts.tzinfo is None:
                    pr_ts = pr_ts.replace(tzinfo=timezone.utc)
                if pr_ts >= cutoff:
                    records.append(pr.to_dict())
            with self._pr_file_lock:
                with open(self._pr_file, "w") as f:
                    json.dump(records, f)
        except Exception as e:
            logger.warning(f"Could not save PR history to {self._pr_file}: {e}")

    # ==================== Fix File Persistence ====================

    @staticmethod
    def _fix_from_dict(d: Dict) -> Optional["CodeFix"]:
        """Reconstruct a CodeFix from its to_dict() output."""
        try:
            ts = d.get("created_at")
            created_at = datetime.fromisoformat(ts) if ts else datetime.utcnow()
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            return CodeFix(
                id=d.get("id", ""),
                issue_id=d.get("issue_id", ""),
                file_path=d.get("file_path", ""),
                line_number=int(d.get("line_number", 0) or 0),
                original_code=d.get("original_code", ""),
                fixed_code=d.get("fixed_code", ""),
                diff_patch=d.get("diff_patch", ""),
                fix_description=d.get("fix_description", ""),
                llm_model=d.get("llm_model", ""),
                llm_reasoning=d.get("llm_reasoning", ""),
                confidence=float(d.get("confidence", 0.0) or 0.0),
                verification_status=d.get("verification_status", "pending"),
                verification_output=d.get("verification_output", ""),
                created_at=created_at,
            )
        except Exception:
            return None

    def _load_fixes_from_file(self) -> None:
        if not self._fix_file or not os.path.exists(self._fix_file):
            return
        try:
            with open(self._fix_file, "r") as f:
                raw = json.load(f)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_FIX_HISTORY_RETAIN_DAYS)
            loaded = 0
            for item in raw if isinstance(raw, list) else []:
                if not isinstance(item, dict):
                    continue
                fix = self._fix_from_dict(item)
                if fix is None:
                    continue
                ts = fix.created_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    self._fixes[fix.id] = fix
                    loaded += 1
            logger.info(f"Loaded {loaded} fixes from {self._fix_file}")
        except Exception as e:
            logger.warning(f"Could not load fix history from {self._fix_file}: {e}")

    def _save_fixes_to_file(self) -> None:
        if not self._fix_file:
            return
        try:
            os.makedirs(os.path.dirname(self._fix_file), exist_ok=True)
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_FIX_HISTORY_RETAIN_DAYS)
            records = []
            for fix in self._fixes.values():
                ts = fix.created_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    records.append(fix.to_dict())
            with self._fix_file_lock:
                with open(self._fix_file, "w") as f:
                    json.dump(records, f)
        except Exception as e:
            logger.warning(f"Could not save fix history to {self._fix_file}: {e}")

    # ==================== Analysis Log ====================

    def start_analysis_log(self, issue_id: str) -> None:
        """Reset the step log for a fresh analysis run."""
        self._analysis_steps[issue_id] = []

    def add_analysis_step(self, issue_id: str, name: str, status: str = "ok", detail: str = "") -> None:
        """Append a step. status: running | ok | warn | error."""
        self._analysis_steps.setdefault(issue_id, []).append({
            "name": name,
            "status": status,
            "detail": (detail or "")[:1500],
            "ts": datetime.utcnow().isoformat(),
        })

    def get_analysis_steps(self, issue_id: str) -> List[Dict[str, Any]]:
        return self._analysis_steps.get(issue_id, [])

    # ==================== Issues ====================

    def add_issue(self, issue: CodeIssue) -> None:
        """Add or update an issue and persist to file."""
        is_new = issue.id not in self._issues
        self._issues[issue.id] = issue
        if is_new:
            detected_at = issue.detected_at.isoformat() if getattr(issue, 'detected_at', None) else datetime.utcnow().isoformat()
            self._issue_history.append({
                "timestamp": datetime.utcnow().isoformat(),
                "detected_at": detected_at,
                "issue_id": issue.id,
                "action": "created",
                "service": issue.service_name,
                "namespace": issue.namespace,
                "exception": issue.exception_type,
                "status": issue.status.value if hasattr(issue.status, 'value') else str(issue.status),
            })

        if self._redis:
            try:
                key = f"codexa:issue:{issue.id}"
                self._redis.setex(key, 86400 * 7, json.dumps(issue.to_dict()))
            except Exception as e:
                logger.warning(f"Redis write failed: {e}")

        self._save_issues_to_file()

    def get_issue(self, issue_id: str) -> Optional[CodeIssue]:
        """Get issue by ID."""
        issue = self._issues.get(issue_id)

        # Try Redis if not in memory
        if not issue and self._redis:
            try:
                key = f"codexa:issue:{issue_id}"
                data = self._redis.get(key)
                if data:
                    raw = json.loads(data) if isinstance(data, (str, bytes)) else data
                    issue = self._issue_from_dict(raw) if isinstance(raw, dict) else None
                    if issue:
                        self._issues[issue_id] = issue
            except Exception as e:
                logger.warning(f"Redis read failed: {e}")

        return issue

    def get_issues(
        self,
        status: Optional[IssueStatus] = None,
        service: Optional[str] = None,
        limit: int = 100,
    ) -> List[CodeIssue]:
        """Get issues with optional filters."""
        issues = list(self._issues.values())

        if status:
            issues = [i for i in issues if i.status == status]

        if service:
            issues = [i for i in issues if i.service_name == service]

        # Sort by detected_at descending — strip tzinfo to avoid mixed tz comparison errors.
        def _sort_ts(x: Any) -> datetime:
            ts = getattr(x, 'detected_at', None) or getattr(x, 'created_at', None) or datetime.min
            return ts.replace(tzinfo=None) if getattr(ts, 'tzinfo', None) else ts
        issues.sort(key=_sort_ts, reverse=True)

        return issues[:limit]

    def update_issue_status(self, issue_id: str, status: IssueStatus) -> bool:
        """Update issue status and persist to file."""
        issue = self.get_issue(issue_id)
        if issue:
            issue.status = status
            self._issues[issue_id] = issue
            self._save_issues_to_file()
            return True
        return False

    def issue_exists(self, service: str, exception_type: str, message: str) -> bool:
        """Check if similar issue already exists (deduplication)."""
        for issue in self._issues.values():
            if (
                issue.service_name == service
                and issue.exception_type == exception_type
                and issue.status not in (IssueStatus.FIXED, IssueStatus.DISMISSED)
            ):
                # Check if same error message (fuzzy match)
                if message and issue.exception_message:
                    if message[:100] == issue.exception_message[:100]:
                        return True
        return False

    def get_issue_by_signature(self, signature: str) -> Optional[CodeIssue]:
        """Get issue by error signature (for deduplication)."""
        for issue in self._issues.values():
            if issue.error_signature == signature:
                return issue

        # Try Redis if available
        if self._redis:
            try:
                keys = self._redis.keys("codexa:issue:*")
                for key in keys[:100]:
                    data = self._redis.get(key)
                    if data:
                        try:
                            raw = json.loads(data) if isinstance(data, (str, bytes)) else data
                            issue = self._issue_from_dict(raw) if isinstance(raw, dict) else None
                        except Exception:
                            issue = None
                        if issue and issue.error_signature == signature:
                            self._issues[issue.id] = issue
                            return issue
            except Exception as e:
                logger.warning(f"Redis search failed: {e}")

        return None

    def save_issue(self, issue: CodeIssue) -> None:
        """Save an issue (alias for add_issue)."""
        self.add_issue(issue)

    def increment_occurrence(self, issue_id: str, new_ts: Optional[datetime] = None) -> None:
        """Increment occurrence_count only if new_ts is newer than last recorded time.

        Prevents the same log entry from being counted multiple times across
        repeated scan cycles that overlap the same time window.
        """
        issue = self._issues.get(issue_id)
        if issue is None:
            return
        if new_ts is not None:
            def _naive(dt):
                return dt.replace(tzinfo=None) if dt and dt.tzinfo else (dt or datetime.min)
            existing = _naive(issue.last_seen_at or issue.detected_at)
            if _naive(new_ts) <= existing:
                return  # same or older log entry — skip
        issue.occurrence_count = int(getattr(issue, 'occurrence_count', 1) or 1) + 1
        issue.last_seen_at = new_ts or datetime.utcnow()
        self._save_issues_to_file()

    def get_issue_count_by_status(self) -> Dict[str, int]:
        """Get count of issues by status."""
        counts = defaultdict(int)
        for issue in self._issues.values():
            counts[issue.status.value] += 1
        return dict(counts)

    # ==================== Fixes ====================

    def add_fix(self, fix: CodeFix) -> None:
        """Add a fix."""
        self._fixes[fix.id] = fix

        if self._redis:
            try:
                key = f"codexa:fix:{fix.id}"
                self._redis.setex(key, 86400 * 7, json.dumps(fix.to_dict()))
            except Exception as e:
                logger.warning(f"Redis write failed: {e}")

        self._save_fixes_to_file()

    def get_fix(self, fix_id: str) -> Optional[CodeFix]:
        """Get fix by ID."""
        fix = self._fixes.get(fix_id)

        if not fix and self._redis:
            try:
                key = f"codexa:fix:{fix_id}"
                data = self._redis.get(key)
                if data:
                    raw = json.loads(data) if isinstance(data, (str, bytes)) else data
                    fix = self._fix_from_dict(raw) if isinstance(raw, dict) else None
                    if fix:
                        self._fixes[fix_id] = fix
            except Exception as e:
                logger.warning(f"Redis read failed: {e}")

        return fix

    def get_fixes_for_issue(self, issue_id: str) -> List[CodeFix]:
        """Get all fixes for an issue."""
        return [f for f in self._fixes.values() if f.issue_id == issue_id]

    def save_fix(self, fix: CodeFix) -> None:
        """Save a fix (alias for add_fix)."""
        self.add_fix(fix)

    def get_fix_by_issue_id(self, issue_id: str) -> Optional[CodeFix]:
        """Get the most recent fix for an issue."""
        fixes = self.get_fixes_for_issue(issue_id)
        if fixes:
            # Return most recent fix
            fixes.sort(key=lambda x: getattr(x, 'created_at', datetime.min), reverse=True)
            return fixes[0]
        return None

    def get_pending_fixes(self) -> List[CodeFix]:
        """Get fixes that haven't been applied yet."""
        fixes = []
        for fix in self._fixes.values():
            issue = self.get_issue(fix.issue_id)
            if issue and issue.status == IssueStatus.FIX_READY:
                fixes.append(fix)
        return fixes

    # ==================== Pull Requests ====================

    def add_pr(self, pr: PullRequest) -> None:
        """Add a PR and persist to file."""
        self._prs[pr.id] = pr

        if self._redis:
            try:
                key = f"codexa:pr:{pr.id}"
                self._redis.setex(key, 86400 * 30, json.dumps(pr.to_dict()))
            except Exception as e:
                logger.warning(f"Redis write failed: {e}")

        self._save_prs_to_file()

    def get_pr(self, pr_id: str) -> Optional[PullRequest]:
        """Get PR by ID."""
        pr = self._prs.get(pr_id)

        if not pr and self._redis:
            try:
                key = f"codexa:pr:{pr_id}"
                data = self._redis.get(key)
                if data:
                    raw = json.loads(data) if isinstance(data, (str, bytes)) else data
                    pr = self._pr_from_dict(raw) if isinstance(raw, dict) else None
                    if pr:
                        self._prs[pr_id] = pr
            except Exception as e:
                logger.warning(f"Redis read failed: {e}")

        return pr

    def get_prs(
        self,
        status: Optional[PRStatus] = None,
        service: Optional[str] = None,
        limit: int = 100,
        hours: Optional[int] = None,
    ) -> List[PullRequest]:
        """Get PRs with optional filters. hours=24 returns only last 24h."""
        prs = list(self._prs.values())

        if hours is not None:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
            filtered = []
            for p in prs:
                ts = p.created_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    filtered.append(p)
            prs = filtered

        if status:
            prs = [p for p in prs if p.status == status]

        if service:
            prs = [p for p in prs if p.service_name == service]

        # Strip tzinfo for sort — mixed tz-aware/naive datetimes raise TypeError
        prs.sort(key=lambda x: (x.created_at.replace(tzinfo=None) if x.created_at and x.created_at.tzinfo else x.created_at) or datetime.min, reverse=True)

        return prs[:limit]

    def save_pr(self, pr: PullRequest) -> None:
        """Save a PR (alias for add_pr)."""
        self.add_pr(pr)

    def update_pr_status(self, pr_id: str, status: PRStatus) -> bool:
        """Update PR status."""
        pr = self.get_pr(pr_id)
        if pr:
            pr.status = status
            self._prs[pr_id] = pr
            return True
        return False

    def get_pr_count_by_status(self) -> Dict[str, int]:
        """Get count of PRs by status."""
        counts = defaultdict(int)
        for pr in self._prs.values():
            counts[pr.status.value] += 1
        return dict(counts)

    # ==================== Statistics ====================

    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics."""
        issue_counts = self.get_issue_count_by_status()
        pr_counts = self.get_pr_count_by_status()

        # Calculate success rate
        total_prs = len(self._prs)
        merged_prs = pr_counts.get("merged", 0)
        success_rate = (merged_prs / total_prs * 100) if total_prs > 0 else 0

        return {
            "issues": {
                "total": len(self._issues),
                "detected": issue_counts.get("detected", 0),
                "analyzing": issue_counts.get("analyzing", 0),
                "fix_ready": issue_counts.get("fix_ready", 0),
                "fixed": issue_counts.get("fixed", 0),
                "dismissed": issue_counts.get("dismissed", 0),
            },
            "prs": {
                "total": total_prs,
                "created": pr_counts.get("created", 0),
                "merged": merged_prs,
                "closed": pr_counts.get("closed", 0),
            },
            "success_rate": round(success_rate, 1),
        }

    def get_trend_data(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get issue trend data for the last N days."""
        now = datetime.utcnow()
        trend = []

        for i in range(days - 1, -1, -1):
            date = now - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")

            # Count issues created on this day
            count = sum(
                1 for h in self._issue_history
                if h["timestamp"].startswith(date_str) and h["action"] == "created"
            )

            trend.append({
                "date": date_str,
                "day": date.strftime("%a"),
                "count": count,
            })

        return trend

    def get_issue_type_distribution(self) -> List[Dict[str, Any]]:
        """Get distribution of issue types."""
        type_counts = defaultdict(int)

        for issue in self._issues.values():
            exc_type = issue.exception_type or "Unknown"
            # Simplify exception names
            if "NullPointer" in exc_type:
                exc_type = "NullPointerException"
            elif "SQL" in exc_type:
                exc_type = "SQLException"
            elif "IllegalArgument" in exc_type:
                exc_type = "IllegalArgumentException"
            elif "Config" in exc_type or "Property" in exc_type:
                exc_type = "ConfigError"
            elif "IO" in exc_type or "File" in exc_type:
                exc_type = "IOError"

            type_counts[exc_type] += 1

        total = sum(type_counts.values()) or 1
        distribution = [
            {
                "type": exc_type,
                "count": count,
                "percentage": round(count / total * 100, 1),
            }
            for exc_type, count in sorted(
                type_counts.items(), key=lambda x: x[1], reverse=True
            )
        ]

        return distribution[:10]  # Top 10 types

    def get_service_stats(self) -> List[Dict[str, Any]]:
        """Get stats by service."""
        service_issues = defaultdict(int)
        service_fixes = defaultdict(int)

        for issue in self._issues.values():
            service_issues[issue.service_name] += 1
            if issue.status == IssueStatus.FIXED:
                service_fixes[issue.service_name] += 1

        stats = []
        for service, issue_count in service_issues.items():
            fix_count = service_fixes[service]
            stats.append({
                "service": service,
                "issues": issue_count,
                "fixes": fix_count,
                "fix_rate": round(fix_count / issue_count * 100, 1) if issue_count > 0 else 0,
            })

        return sorted(stats, key=lambda x: x["issues"], reverse=True)

    def get_issue_history(self, hours: int = 24, limit: int = 500) -> List[Dict[str, Any]]:
        """Return detection history entries within the requested lookback window."""
        lookback_hours = max(1, min(int(hours or 24), 24 * 7))
        max_items = max(1, min(int(limit or 500), 2000))
        cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)

        out: List[Dict[str, Any]] = []
        for item in reversed(self._issue_history):
            if not isinstance(item, dict):
                continue
            ts_raw = str(item.get("detected_at", "") or item.get("timestamp", "")).strip()
            if not ts_raw:
                continue
            parsed = None
            try:
                parsed = datetime.fromisoformat(ts_raw.replace('Z', '+00:00')).replace(tzinfo=None)
            except Exception:
                try:
                    p = datetime.fromisoformat(ts_raw)
                    parsed = p.replace(tzinfo=None) if p.tzinfo else p
                except Exception:
                    parsed = None
            if parsed is None or parsed < cutoff:
                continue

            entry = dict(item)
            issue_id = str(item.get("issue_id", "") or "")
            issue = self._issues.get(issue_id)
            if isinstance(issue, CodeIssue):
                entry["issue"] = issue.to_dict()
            out.append(entry)
            if len(out) >= max_items:
                break

        return out

    # ==================== Cleanup ====================

    @staticmethod
    def _issue_ts(issue: CodeIssue) -> datetime:
        ts = getattr(issue, 'detected_at', None) or getattr(issue, 'created_at', None) or datetime.utcnow()
        return ts.replace(tzinfo=None) if ts.tzinfo else ts

    @staticmethod
    def _naive(dt: Optional[datetime]) -> datetime:
        """Return a naive UTC datetime for safe comparison."""
        if dt is None:
            return datetime.utcnow()
        return dt.replace(tzinfo=None) if dt.tzinfo else dt

    def cleanup_old_data(self, days: int = 30) -> int:
        """Remove data older than N days."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        removed = 0

        # Cleanup issues
        for issue_id, issue in list(self._issues.items()):
            issue_ts = self._issue_ts(issue)
            if issue_ts < cutoff and issue.status in (
                IssueStatus.FIXED,
                IssueStatus.DISMISSED,
            ):
                del self._issues[issue_id]
                removed += 1

        # Cleanup PRs
        for pr_id, pr in list(self._prs.items()):
            if self._naive(pr.created_at) < cutoff and pr.status in (
                PRStatus.MERGED,
                PRStatus.CLOSED,
            ):
                del self._prs[pr_id]
                removed += 1

        # Cleanup history
        cutoff_str = cutoff.isoformat()
        self._issue_history = [
            h for h in self._issue_history if h["timestamp"] > cutoff_str
        ]

        logger.info(f"Cleaned up {removed} old records")
        return removed

    # ==================== All Getters ====================

    def get_all_issues(self) -> List[CodeIssue]:
        """Get all issues."""
        return list(self._issues.values())

    def get_all_fixes(self) -> List[CodeFix]:
        """Get all fixes."""
        return list(self._fixes.values())

    def get_all_prs(self) -> List[PullRequest]:
        """Get all PRs."""
        return list(self._prs.values())

    # ==================== LLM Stats ====================

    def get_llm_stats(self) -> LLMStats:
        """Get LLM performance statistics using current config model."""
        config = get_config()
        model_name = f"{config.llm.provider}:{config.llm.model}"

        # Calculate stats from fixes
        total_calls = len(self._fixes)
        successful_calls = sum(1 for f in self._fixes.values() if f.confidence and f.confidence > 0.5)
        failed_calls = total_calls - successful_calls
        success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0.0

        return LLMStats(
            model_name=model_name,
            total_calls=total_calls,
            successful_calls=successful_calls,
            failed_calls=failed_calls,
            avg_response_time_seconds=0.0,  # Not tracked yet
            avg_tokens_per_fix=0,  # Not tracked yet
            success_rate=round(success_rate, 1),
        )
