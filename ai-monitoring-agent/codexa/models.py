"""CodeXA data models."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid


class IssueStatus(str, Enum):
    """Issue processing status."""
    PENDING = "pending"
    ANALYZING = "analyzing"
    FIX_READY = "fix_ready"
    PR_CREATED = "pr_created"
    MERGED = "merged"
    FAILED = "failed"
    DISMISSED = "dismissed"


class IssueType(str, Enum):
    """Type of code issue."""
    NULL_POINTER = "NullPointerException"
    SQL_EXCEPTION = "SQLException"
    ILLEGAL_ARGUMENT = "IllegalArgumentException"
    INDEX_OUT_OF_BOUNDS = "IndexOutOfBoundsException"
    NUMBER_FORMAT = "NumberFormatException"
    CLASS_NOT_FOUND = "ClassNotFoundException"
    NO_SUCH_BEAN = "NoSuchBeanDefinitionException"
    CONFIG_ERROR = "ConfigurationException"
    VALIDATION_ERROR = "ValidationException"
    RUNTIME_ERROR = "RuntimeException"
    OTHER = "Other"


class PRStatus(str, Enum):
    """Pull request status."""
    CREATED = "created"
    OPEN = "open"
    MERGED = "merged"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass
class CodeIssue:
    """Detected code issue."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    service_name: str = ""
    namespace: str = ""
    error_signature: str = ""
    exception_type: str = ""
    exception_message: str = ""
    stack_trace: str = ""
    log_evidence: str = ""
    file_path: str = ""
    line_number: int = 0
    issue_type: IssueType = IssueType.OTHER
    confidence: float = 0.0
    status: IssueStatus = IssueStatus.PENDING
    detected_at: datetime = field(default_factory=datetime.utcnow)
    analyzed_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    occurrence_count: int = 1
    repo_name: str = ""
    branch: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "service_name": self.service_name,
            "namespace": self.namespace,
            "error_signature": self.error_signature,
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "stack_trace": self.stack_trace,
            "log_evidence": self.log_evidence,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "issue_type": self.issue_type.value,
            "confidence": self.confidence,
            "status": self.status.value,
            "detected_at": (self.detected_at.isoformat() + 'Z') if self.detected_at and self.detected_at.tzinfo is None else (self.detected_at.isoformat() if self.detected_at else None),
            "analyzed_at": (self.analyzed_at.isoformat() + 'Z') if self.analyzed_at and self.analyzed_at.tzinfo is None else (self.analyzed_at.isoformat() if self.analyzed_at else None),
            "last_seen_at": (self.last_seen_at.isoformat() + 'Z') if self.last_seen_at and self.last_seen_at.tzinfo is None else (self.last_seen_at.isoformat() if self.last_seen_at else None),
            "occurrence_count": self.occurrence_count,
            "repo_name": self.repo_name,
            "branch": self.branch,
        }


@dataclass
class CodeFix:
    """Generated code fix."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    issue_id: str = ""
    file_path: str = ""
    line_number: int = 0  # analysis-reported line; used as fuzzy-search hint
    original_code: str = ""
    fixed_code: str = ""
    diff_patch: str = ""
    fix_description: str = ""
    llm_model: str = ""
    llm_reasoning: str = ""
    confidence: float = 0.0
    verification_status: str = "pending"
    verification_output: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "issue_id": self.issue_id,
            "file_path": self.file_path,
            "original_code": self.original_code,
            "fixed_code": self.fixed_code,
            "diff_patch": self.diff_patch,
            "fix_description": self.fix_description,
            "llm_model": self.llm_model,
            "llm_reasoning": self.llm_reasoning,
            "confidence": self.confidence,
            "verification_status": self.verification_status,
            "verification_output": self.verification_output,
            "created_at": (self.created_at.isoformat() + 'Z') if self.created_at and self.created_at.tzinfo is None else (self.created_at.isoformat() if self.created_at else None),
        }


@dataclass
class PullRequest:
    """Created pull request."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    issue_id: str = ""
    fix_id: str = ""
    service_name: str = ""
    namespace: str = ""
    repo_name: str = ""
    branch_name: str = ""
    pr_number: int = 0
    pr_url: str = ""
    pr_title: str = ""
    pr_body: str = ""
    status: PRStatus = PRStatus.CREATED
    created_at: datetime = field(default_factory=datetime.utcnow)
    merged_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "issue_id": self.issue_id,
            "fix_id": self.fix_id,
            "service_name": self.service_name,
            "namespace": self.namespace,
            "repo_name": self.repo_name,
            "branch_name": self.branch_name,
            "pr_number": self.pr_number,
            "pr_url": self.pr_url,
            "pr_title": self.pr_title,
            "pr_body": self.pr_body,
            "status": self.status.value,
            "created_at": (self.created_at.isoformat() + 'Z') if self.created_at and self.created_at.tzinfo is None else (self.created_at.isoformat() if self.created_at else None),
            "merged_at": (self.merged_at.isoformat() + 'Z') if self.merged_at and self.merged_at.tzinfo is None else (self.merged_at.isoformat() if self.merged_at else None),
            "closed_at": (self.closed_at.isoformat() + 'Z') if self.closed_at and self.closed_at.tzinfo is None else (self.closed_at.isoformat() if self.closed_at else None),
        }


@dataclass
class DashboardStats:
    """Dashboard statistics."""
    total_detected: int = 0
    analyzing_count: int = 0
    fixes_ready: int = 0
    prs_created: int = 0
    prs_merged: int = 0
    success_rate: float = 0.0
    avg_fix_time_seconds: float = 0.0
    today_detected: int = 0
    issue_type_counts: Dict[str, int] = field(default_factory=dict)
    trend_data: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_detected": self.total_detected,
            "analyzing_count": self.analyzing_count,
            "fixes_ready": self.fixes_ready,
            "prs_created": self.prs_created,
            "prs_merged": self.prs_merged,
            "success_rate": self.success_rate,
            "avg_fix_time_seconds": self.avg_fix_time_seconds,
            "today_detected": self.today_detected,
            "issue_type_counts": self.issue_type_counts,
            "trend_data": self.trend_data,
        }


@dataclass
class LLMStats:
    """LLM performance statistics."""
    model_name: str = ""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    avg_response_time_seconds: float = 0.0
    avg_tokens_per_fix: int = 0
    success_rate: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "avg_response_time_seconds": self.avg_response_time_seconds,
            "avg_tokens_per_fix": self.avg_tokens_per_fix,
            "success_rate": self.success_rate,
        }
