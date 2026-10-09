"""CodeXA FastAPI Application - Autonomous Code Fix Engine."""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .config import get_config, get_available_models, AVAILABLE_MODELS
from .models import (
    CodeIssue, CodeFix, PullRequest, DashboardStats, LLMStats,
    IssueStatus, IssueType, PRStatus
)
from .db.repository import IssueRepository
from .services.detector import IssueDetector
from .services.analyzer import CodeAnalyzer
from .services.fixer import FixGenerator
from .services.git_ops import GitOperations

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("codexa")

# Initialize FastAPI app
codexa_app = FastAPI(
    title="CodeXA",
    description="Autonomous Code Fix Engine",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Templates
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATE_DIR)

# Global instances
_repository: Optional[IssueRepository] = None
_detector: Optional[IssueDetector] = None
_analyzer: Optional[CodeAnalyzer] = None
_fixer: Optional[FixGenerator] = None
_git_ops: Optional[GitOperations] = None
_detection_task: Optional[asyncio.Task] = None


def get_repository() -> IssueRepository:
    global _repository
    if _repository is None:
        _repository = IssueRepository()
    return _repository


def get_detector() -> IssueDetector:
    global _detector
    if _detector is None:
        _detector = IssueDetector(get_config(), get_repository())
    return _detector


def get_analyzer() -> CodeAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = CodeAnalyzer(get_config())
    return _analyzer


def get_fixer() -> FixGenerator:
    global _fixer
    if _fixer is None:
        _fixer = FixGenerator(get_config(), get_analyzer())
    return _fixer


def get_git_ops() -> GitOperations:
    global _git_ops
    if _git_ops is None:
        _git_ops = GitOperations(get_config())
    return _git_ops


# Pydantic models for API
class AnalyzeRequest(BaseModel):
    issue_id: str


class GenerateFixRequest(BaseModel):
    issue_id: str


class DismissRequest(BaseModel):
    issue_id: str
    reason: Optional[str] = None


class MergeRequest(BaseModel):
    pr_id: str


class SetModelRequest(BaseModel):
    model_id: str


# ============================================================================
# Dashboard Route
# ============================================================================

@codexa_app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Render CodeXA dashboard."""
    return templates.TemplateResponse("codexa.html", {"request": request})


# ============================================================================
# Health & Status
# ============================================================================

@codexa_app.get("/health")
async def health():
    """Health check endpoint."""
    config = get_config()
    return {
        "status": "healthy",
        "service": "codexa",
        "version": "1.0.0",
        "llm_model": config.llm.model,
        "llm_host": config.llm.host,
    }


@codexa_app.get("/api/status")
async def status():
    """Get service status."""
    repo = get_repository()
    config = get_config()
    return {
        "status": "running",
        "issues_count": len(repo.get_all_issues()),
        "fixes_count": len(repo.get_all_fixes()),
        "prs_count": len(repo.get_all_prs()),
        "detection_active": _detection_task is not None and not _detection_task.done(),
        "llm_model": config.llm.model,
    }


# ============================================================================
# Stats & Charts
# ============================================================================

@codexa_app.get("/api/stats")
async def get_stats() -> Dict[str, Any]:
    """Get dashboard statistics."""
    repo = get_repository()
    stats = repo.get_stats()
    return stats.to_dict()


@codexa_app.get("/api/chart/trend")
async def get_trend_data() -> Dict[str, Any]:
    """Get 7-day issue trend data."""
    repo = get_repository()
    issues = repo.get_all_issues()

    # Calculate daily counts for last 7 days
    today = datetime.utcnow().date()
    trend = []
    for i in range(6, -1, -1):
        day = today - timedelta(days=i)
        day_start = datetime.combine(day, datetime.min.time())
        day_end = datetime.combine(day, datetime.max.time())
        count = sum(1 for issue in issues
                    if issue.detected_at and day_start <= issue.detected_at <= day_end)
        trend.append({
            "date": day.strftime("%Y-%m-%d"),
            "day": day.strftime("%a"),
            "count": count
        })

    return {"trend": trend}


@codexa_app.get("/api/chart/types")
async def get_issue_types() -> Dict[str, Any]:
    """Get issue type distribution."""
    repo = get_repository()
    issues = repo.get_all_issues()

    type_counts: Dict[str, int] = {}
    for issue in issues:
        issue_type = issue.issue_type.value if issue.issue_type else "Other"
        type_counts[issue_type] = type_counts.get(issue_type, 0) + 1

    # Sort by count descending
    sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)

    return {
        "types": [{"type": t, "count": c} for t, c in sorted_types[:6]]
    }


@codexa_app.get("/api/llm/stats")
async def get_llm_stats() -> Dict[str, Any]:
    """Get LLM performance statistics."""
    repo = get_repository()
    return repo.get_llm_stats().to_dict()


@codexa_app.get("/api/llm/models")
async def list_models() -> Dict[str, Any]:
    """List available LLM models."""
    config = get_config()
    models = get_available_models()
    return {
        "models": models,
        "current_model": config.llm.model,
        "fallback_model": config.llm.fallback_model,
    }


@codexa_app.post("/api/llm/model")
async def set_model(request: SetModelRequest) -> Dict[str, Any]:
    """Set the active LLM model."""
    model_id = request.model_id

    # Validate model exists
    valid_models = [m.id for m in AVAILABLE_MODELS]
    if model_id not in valid_models:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model. Available: {', '.join(valid_models)}"
        )

    config = get_config()
    old_model = config.llm.model
    old_provider = config.llm.provider
    config.llm.model = model_id

    # Update provider based on model prefix
    new_provider = old_provider
    if ":" in model_id:
        prefix = model_id.split(":", 1)[0].lower()
        if prefix in ("gemini", "ollama", "openai", "anthropic"):
            new_provider = prefix
            config.llm.provider = new_provider

    # Re-initialize analyzer with new model
    global _analyzer
    _analyzer = None  # Will be recreated on next use

    logger.info(f"LLM model changed: {old_model} -> {model_id} (provider: {new_provider})")

    return {
        "status": "success",
        "old_model": old_model,
        "new_model": model_id,
        "provider": new_provider,
    }


@codexa_app.get("/api/llm/test")
async def test_llm() -> Dict[str, Any]:
    """Test LLM connectivity and response."""
    config = get_config()
    analyzer = get_analyzer()

    try:
        # Simple test prompt
        start_time = datetime.utcnow()
        response = await analyzer.test_connection()
        end_time = datetime.utcnow()

        return {
            "status": "success",
            "model": config.llm.model,
            "host": config.llm.host,
            "response_time_ms": (end_time - start_time).total_seconds() * 1000,
            "response": response,
        }
    except Exception as e:
        return {
            "status": "error",
            "model": config.llm.model,
            "host": config.llm.host,
            "error": str(e),
        }


# ============================================================================
# Issues
# ============================================================================

@codexa_app.get("/api/issues")
async def list_issues(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """List detected issues."""
    repo = get_repository()
    issues = repo.get_all_issues()

    # Filter by status
    if status:
        try:
            status_enum = IssueStatus(status)
            issues = [i for i in issues if i.status == status_enum]
        except ValueError:
            pass

    # Sort by detected_at descending
    issues.sort(key=lambda x: x.detected_at or datetime.min, reverse=True)

    # Paginate
    total = len(issues)
    issues = issues[offset:offset + limit]

    return {
        "issues": [i.to_dict() for i in issues],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@codexa_app.get("/api/issues/pending")
async def list_pending_issues() -> Dict[str, Any]:
    """List issues pending fix or with fix ready."""
    repo = get_repository()
    issues = repo.get_all_issues()

    pending = [
        i for i in issues
        if i.status in [IssueStatus.PENDING, IssueStatus.ANALYZING, IssueStatus.FIX_READY]
    ]
    pending.sort(key=lambda x: x.detected_at or datetime.min, reverse=True)

    # Include fix info for FIX_READY issues
    result = []
    for issue in pending:
        data = issue.to_dict()
        if issue.status == IssueStatus.FIX_READY:
            fix = repo.get_fix_by_issue_id(issue.id)
            if fix:
                data["fix"] = fix.to_dict()
        result.append(data)

    return {"issues": result, "count": len(result)}


@codexa_app.get("/api/issues/{issue_id}")
async def get_issue(issue_id: str) -> Dict[str, Any]:
    """Get issue details."""
    repo = get_repository()
    issue = repo.get_issue(issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")

    data = issue.to_dict()

    # Include fix if available
    fix = repo.get_fix_by_issue_id(issue_id)
    if fix:
        data["fix"] = fix.to_dict()

    # Include PR if available
    pr = repo.get_pr_by_issue_id(issue_id)
    if pr:
        data["pr"] = pr.to_dict()

    return data


@codexa_app.post("/api/issues/{issue_id}/analyze")
async def analyze_issue(issue_id: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """Trigger LLM analysis for an issue."""
    repo = get_repository()
    issue = repo.get_issue(issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")

    if issue.status not in [IssueStatus.PENDING, IssueStatus.FAILED]:
        return {"status": "already_analyzing", "message": f"Issue is {issue.status.value}"}

    # Update status
    issue.status = IssueStatus.ANALYZING
    repo.save_issue(issue)

    # Run analysis in background
    background_tasks.add_task(_run_analysis, issue_id)

    return {"status": "analyzing", "issue_id": issue_id}


@codexa_app.get("/api/issues/{issue_id}/analysis-log")
async def get_analysis_log(issue_id: str) -> Dict[str, Any]:
    """Live step-by-step log of the most recent analysis run for an issue."""
    repo = get_repository()
    issue = repo.get_issue(issue_id)
    return {
        "issue_id": issue_id,
        "status": issue.status.value if issue else "unknown",
        "steps": repo.get_analysis_steps(issue_id),
    }


async def _run_analysis(issue_id: str):
    """Background task to run code analysis."""
    repo = get_repository()
    analyzer = get_analyzer()
    fixer = get_fixer()

    issue = repo.get_issue(issue_id)
    if not issue:
        return

    # Fresh step log for this run; analyzer reports each stage via on_step.
    repo.start_analysis_log(issue_id)

    def record(name, status, detail=""):
        repo.add_analysis_step(issue_id, name, status, detail)

    try:
        record("Analysis started", "ok",
               f"service={issue.service_name} | exception={issue.exception_type}")

        # Analyze the issue (emits clone/find/LLM/fix steps via record)
        analysis = await analyzer.analyze(issue, on_step=record)

        if analysis and analysis.get("fix"):
            # Generate fix object (diff + verification)
            fix = await fixer.generate_fix(issue, analysis)

            if fix and fix.fixed_code:
                repo.save_fix(fix)
                issue.status = IssueStatus.FIX_READY
                issue.analyzed_at = datetime.utcnow()
                record("Fix ready", "ok", "Click 'Create PR' to open a pull request")
            else:
                issue.status = IssueStatus.FAILED
                record("Save fix", "error", "No applicable fix was produced")
        else:
            issue.status = IssueStatus.FAILED
            record("Result", "error", "No fix generated - see the failing step above")

    except Exception as e:
        logger.error(f"Analysis failed for {issue_id}: {e}")
        issue.status = IssueStatus.FAILED
        record("Analysis crashed", "error", str(e))

    repo.save_issue(issue)


@codexa_app.post("/api/issues/{issue_id}/fix")
async def create_fix_pr(issue_id: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """Generate fix and create PR."""
    repo = get_repository()
    issue = repo.get_issue(issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")

    fix = repo.get_fix_by_issue_id(issue_id)
    if not fix:
        raise HTTPException(status_code=400, detail="No fix available. Run analysis first.")

    # Create PR in background
    background_tasks.add_task(_create_pr, issue_id, fix.id)

    return {"status": "creating_pr", "issue_id": issue_id, "fix_id": fix.id}


async def _create_pr(issue_id: str, fix_id: str):
    """Background task to create PR."""
    repo = get_repository()
    git_ops = get_git_ops()

    issue = repo.get_issue(issue_id)
    fix = repo.get_fix(fix_id)

    if not issue or not fix:
        return

    try:
        pr = await git_ops.create_pr(issue, fix)

        if pr and pr.pr_url:
            repo.save_pr(pr)
            issue.status = IssueStatus.PR_CREATED
        else:
            issue.status = IssueStatus.FAILED

    except Exception as e:
        logger.error(f"PR creation failed for {issue_id}: {e}")
        issue.status = IssueStatus.FAILED

    repo.save_issue(issue)


@codexa_app.post("/api/issues/{issue_id}/dismiss")
async def dismiss_issue(issue_id: str, request: DismissRequest) -> Dict[str, Any]:
    """Dismiss an issue."""
    repo = get_repository()
    issue = repo.get_issue(issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")

    issue.status = IssueStatus.DISMISSED
    repo.save_issue(issue)

    return {"status": "dismissed", "issue_id": issue_id}


# ============================================================================
# Pull Requests
# ============================================================================

@codexa_app.get("/api/prs")
async def list_prs(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> Dict[str, Any]:
    """List created PRs."""
    repo = get_repository()
    prs = repo.get_all_prs()

    # Filter by status
    if status:
        try:
            status_enum = PRStatus(status)
            prs = [p for p in prs if p.status == status_enum]
        except ValueError:
            pass

    # Sort by created_at descending
    prs.sort(key=lambda x: x.created_at or datetime.min, reverse=True)

    # Paginate
    total = len(prs)
    prs = prs[offset:offset + limit]

    return {
        "prs": [p.to_dict() for p in prs],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@codexa_app.get("/api/prs/{pr_id}")
async def get_pr(pr_id: str) -> Dict[str, Any]:
    """Get PR details."""
    repo = get_repository()
    pr = repo.get_pr(pr_id)
    if not pr:
        raise HTTPException(status_code=404, detail="PR not found")
    return pr.to_dict()


@codexa_app.post("/api/prs/{pr_id}/merge")
async def merge_pr(pr_id: str) -> Dict[str, Any]:
    """Merge a PR."""
    repo = get_repository()
    git_ops = get_git_ops()

    pr = repo.get_pr(pr_id)
    if not pr:
        raise HTTPException(status_code=404, detail="PR not found")

    if pr.status != PRStatus.OPEN and pr.status != PRStatus.CREATED:
        return {"status": "error", "message": f"PR is {pr.status.value}"}

    try:
        success = await git_ops.merge_pr(pr)
        if success:
            pr.status = PRStatus.MERGED
            pr.merged_at = datetime.utcnow()
            repo.save_pr(pr)

            # Update issue status
            issue = repo.get_issue(pr.issue_id)
            if issue:
                issue.status = IssueStatus.MERGED
                repo.save_issue(issue)

            return {"status": "merged", "pr_id": pr_id}
        else:
            return {"status": "error", "message": "Merge failed"}

    except Exception as e:
        logger.error(f"Merge failed for PR {pr_id}: {e}")
        return {"status": "error", "message": str(e)}


# ============================================================================
# Detection Control
# ============================================================================

@codexa_app.post("/api/detection/start")
async def start_detection() -> Dict[str, Any]:
    """Start background issue detection."""
    global _detection_task

    if _detection_task and not _detection_task.done():
        return {"status": "already_running"}

    _detection_task = asyncio.create_task(_detection_loop())
    return {"status": "started"}


@codexa_app.post("/api/detection/stop")
async def stop_detection() -> Dict[str, Any]:
    """Stop background issue detection."""
    global _detection_task

    if _detection_task and not _detection_task.done():
        _detection_task.cancel()
        return {"status": "stopped"}

    return {"status": "not_running"}


async def _detection_loop():
    """Background detection loop."""
    config = get_config()
    detector = get_detector()

    while True:
        try:
            await detector.detect_issues()
        except Exception as e:
            logger.error(f"Detection error: {e}")

        await asyncio.sleep(config.monitoring_agent.poll_interval_seconds)


# ============================================================================
# Startup Event
# ============================================================================

@codexa_app.on_event("startup")
async def startup():
    """Initialize on startup."""
    logger.info("CodeXA starting up...")
    config = get_config()
    logger.info(f"LLM Model: {config.llm.model}")
    logger.info(f"LLM Host: {config.llm.host}")

    # Start detection loop
    global _detection_task
    _detection_task = asyncio.create_task(_detection_loop())
    logger.info("Detection loop started")
