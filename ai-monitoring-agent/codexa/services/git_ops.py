"""Git operations service - handles Git/GitHub operations for PR creation."""

import asyncio
import difflib
import json
import logging
import os
import re
import subprocess
from datetime import datetime
from typing import Any, Dict, Optional

import aiohttp

from ..config import CodeXAConfig
from ..models import CodeIssue, CodeFix, PullRequest, PRStatus
from .repo_resolver import resolve_repo_and_branch, clone_dir_for

logger = logging.getLogger("codexa.git_ops")


class GitOperations:
    """Handles Git and GitHub operations."""

    def __init__(self, config: CodeXAConfig):
        self.config = config
        self.workspace = config.analysis.workspace
        self.last_error: str = ""

    async def create_pr(
        self, issue: CodeIssue, fix: CodeFix
    ) -> Optional[PullRequest]:
        """Create a pull request with the fix."""
        logger.info(f"Creating PR for: {issue.service_name}")
        self.last_error = ""

        # Resolve the SAME repo/branch the analyzer used to clone, so we find
        # the existing clone and target the correct base branch.
        repo_name, base_branch = resolve_repo_and_branch(self.config, issue)
        repo_path = clone_dir_for(self.workspace, repo_name, issue.id)

        if not os.path.exists(repo_path):
            self.last_error = f"Repository clone not found at {repo_path} - run analysis first"
            logger.error(self.last_error)
            return None

        try:
            # 1. Create branch
            branch_name = self._generate_branch_name(issue)
            if not await self._create_branch(repo_path, branch_name):
                if not self.last_error:
                    self.last_error = "Failed to create branch"
                return None

            # 2. Apply fix
            if not self._apply_fix_to_repo(repo_path, fix):
                if not self.last_error:
                    self.last_error = "Failed to apply fix to repository"
                return None

            # 3. Commit changes
            commit_message = self._generate_commit_message(issue, fix)
            if not await self._commit_changes(repo_path, commit_message):
                if not self.last_error:
                    self.last_error = "Failed to commit changes"
                return None

            # 4. Push branch
            if not await self._push_branch(repo_path, branch_name):
                if not self.last_error:
                    self.last_error = "Failed to push branch"
                return None

            # 5. Create PR (GitHub REST API, with gh CLI fallback)
            pr_result = await self._create_github_pr(
                repo_name, base_branch, branch_name, issue, fix
            )

            if pr_result:
                return PullRequest(
                    issue_id=issue.id,
                    fix_id=fix.id,
                    service_name=issue.service_name,
                    namespace=issue.namespace,
                    repo_name=repo_name,
                    branch_name=branch_name,
                    pr_number=pr_result.get("number", 0),
                    pr_url=pr_result.get("url", ""),
                    pr_title=pr_result.get("title", ""),
                    pr_body=pr_result.get("body", ""),
                    status=PRStatus.CREATED,
                )

            self.last_error = self.last_error or "GitHub PR creation returned no result"
            return None

        except Exception as e:
            self.last_error = str(e)
            logger.error(f"PR creation failed: {e}")
            return None

    async def merge_pr(self, pr: PullRequest) -> bool:
        """Merge a pull request."""
        logger.info(f"Merging PR: {pr.pr_url}")

        if not pr.pr_url:
            return False

        try:
            cmd = [
                "gh", "pr", "merge", pr.pr_url,
                "--squash",
                "--delete-branch",
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=60,
            )

            if process.returncode == 0:
                logger.info(f"PR merged successfully: {pr.pr_url}")
                return True
            else:
                logger.error(f"Merge failed: {stderr.decode()}")
                return False

        except Exception as e:
            logger.error(f"Merge error: {e}")
            return False

    def _generate_branch_name(self, issue: CodeIssue) -> str:
        """Generate branch name for the fix."""
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        service = issue.service_name.replace("/", "-").replace("_", "-")
        return f"codexa-fix/{service}-{timestamp}"

    def _generate_commit_message(self, issue: CodeIssue, fix: CodeFix) -> str:
        """Generate commit message."""
        exception = issue.exception_type or "issue"
        short_desc = fix.fix_description[:50] if fix.fix_description else "auto-fix"
        return f"fix({issue.service_name}): {short_desc}\n\nFixed {exception}\n\nGenerated by CodeXA"

    @staticmethod
    def _strip_line_numbers(text: str) -> str:
        """Remove '  142 | ' style prefixes the LLM may echo back."""
        return "\n".join(re.sub(r"^\s*\d+\s*\|\s?", "", ln) for ln in text.splitlines())

    _SOURCE_EXTS = {'.java', '.kt', '.py', '.ts', '.js', '.go', '.scala', '.groovy'}
    _SKIP_DIRS = {'.git', 'target', 'build', 'node_modules', '.gradle', '__pycache__', '.mvn'}

    def _find_file(self, repo_path: str, file_path: str) -> Optional[str]:
        """Locate a file in the repo by basename / partial path / Java FQN.

        Handles:
          - Normal paths:  src/main/java/.../Foo.java  → basename Foo.java
          - Java FQN:      com.casa.pkg.Foo.java       → extract Foo.java
          - FQN no ext:    com.casa.pkg.FooService      → try FooService.java etc.
        """
        candidates: list = []

        base = os.path.basename(file_path)
        candidates.append(base)

        # Java FQN uses dots as package separators (no slashes)
        if '.' in file_path and '/' not in file_path and '\\' not in file_path:
            parts = file_path.split('.')
            ext = parts[-1].lower()
            if ext in ('java', 'kt', 'py', 'ts', 'js', 'go', 'scala', 'groovy'):
                # com.casa.pkg.ClassName.java  →  ClassName.java
                candidates.append(f"{parts[-2]}.{parts[-1]}")
            else:
                # com.casa.pkg.ClassName  →  ClassName.java / ClassName.kt
                for e in ('java', 'kt', 'py', 'ts', 'js', 'go'):
                    candidates.append(f"{parts[-1]}.{e}")

        for root, dirs, files in os.walk(repo_path):
            dirs[:] = [d for d in dirs if d not in self._SKIP_DIRS]
            for f in files:
                if f in candidates or file_path in os.path.join(root, f):
                    return os.path.join(root, f)
        return None

    def _find_file_by_content(self, repo_path: str, snippet: str) -> Optional[str]:
        """Search source files for a code snippet; return first file that contains it.

        Used as fallback when the LLM returns a file_path from a dependency (e.g.
        a frame from another service's package that doesn't exist in this repo).
        """
        needle = snippet.strip()
        if not needle:
            return None
        for root, dirs, files in os.walk(repo_path):
            dirs[:] = [d for d in dirs if d not in self._SKIP_DIRS]
            for fname in files:
                if os.path.splitext(fname)[1].lower() not in self._SOURCE_EXTS:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                        content = fh.read()
                    if needle in content:
                        return fpath
                except Exception:
                    continue
        return None

    def _replace_block(self, content: str, original: str, fixed: str,
                       line_hint: int = 0) -> Optional[str]:
        """Replace the original snippet with the fixed snippet.

        Returns the new file content, or None if the original snippet can't be
        located. NEVER returns whole-file replacement - if we can't anchor the
        change we fail safe so we don't delete the rest of the file.
        """
        # 1. Exact substring match
        if original and original in content:
            return content.replace(original, fixed, 1)

        # 2. Line-based match ignoring per-line leading/trailing whitespace
        o_lines = original.splitlines()
        while o_lines and not o_lines[0].strip():
            o_lines.pop(0)
        while o_lines and not o_lines[-1].strip():
            o_lines.pop()
        if not o_lines:
            return None

        o_norm = [ln.strip() for ln in o_lines]
        c_lines = content.splitlines()
        n = len(o_norm)
        for i in range(len(c_lines) - n + 1):
            if [c_lines[i + j].strip() for j in range(n)] == o_norm:
                new_lines = c_lines[:i] + fixed.splitlines() + c_lines[i + n:]
                return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")

        # 3. Aggressive normalization: collapse ALL internal whitespace runs too.
        # Catches cases like "final  String x" vs "final String x", or spacing
        # around operators that the model returns slightly differently.
        def _collapse(ln: str) -> str:
            return re.sub(r"\s+", " ", ln).strip()

        o_agg = [_collapse(ln) for ln in o_lines]
        c_agg = [_collapse(ln) for ln in c_lines]
        for i in range(len(c_agg) - n + 1):
            if c_agg[i:i + n] == o_agg:
                new_lines = c_lines[:i] + fixed.splitlines() + c_lines[i + n:]
                return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")

        # 4a. Fuzzy SequenceMatcher near line_hint (looser: 0.72 — safer because
        # we're already in the right neighbourhood of the file).
        best_ratio = 0.0
        best_i = -1

        def _check_range(start: int, end: int) -> None:
            nonlocal best_ratio, best_i
            for i in range(max(0, start), min(len(c_agg) - n + 1, end)):
                ratio = difflib.SequenceMatcher(None, o_agg, c_agg[i:i + n]).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_i = i

        if line_hint > 0:
            _check_range(line_hint - 65, line_hint + 65)
            if best_i >= 0 and best_ratio >= 0.72:
                logger.warning(
                    f"Fuzzy block match ({best_ratio:.2f}) near hint line {line_hint} "
                    f"→ applying at line {best_i + 1}"
                )
                new_lines = c_lines[:best_i] + fixed.splitlines() + c_lines[best_i + n:]
                return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")

        # 4b. Full-file fuzzy scan — stricter (0.80) to avoid wrong-block matches
        # far from the target.
        _check_range(0, len(c_agg))
        if best_i >= 0 and best_ratio >= 0.80:
            logger.warning(
                f"Full-file fuzzy match ({best_ratio:.2f}) at line {best_i + 1}"
            )
            new_lines = c_lines[:best_i] + fixed.splitlines() + c_lines[best_i + n:]
            return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")

        # 5. Last resort — direct line-number replacement.
        # When the model fabricates original_code that bears little resemblance
        # to the file, trust the line_number from the analysis instead.
        # Scan ±10 lines to align on the line whose first token best matches
        # o_agg[0], then replace n lines from there.
        if line_hint > 0 and n > 0:
            idx = line_hint - 1  # convert to 0-based
            o_first = o_agg[0] if o_agg else ""
            best_offset = 0
            best_first = 0.0
            for off in range(-10, 11):
                ci = idx + off
                if 0 <= ci < len(c_agg):
                    score = difflib.SequenceMatcher(None, o_first, c_agg[ci]).ratio()
                    if score > best_first:
                        best_first = score
                        best_offset = off
            idx += best_offset
            if 0 <= idx and idx + n <= len(c_lines):
                logger.warning(
                    f"Direct line-number replacement at line {idx + 1} "
                    f"(first-line similarity={best_first:.2f}; "
                    f"all fuzzy strategies failed — original_code not found in file)"
                )
                new_lines = c_lines[:idx] + fixed.splitlines() + c_lines[idx + n:]
                return "\n".join(new_lines) + ("\n" if content.endswith("\n") else "")

        return None

    def _apply_fix_to_repo(self, repo_path: str, fix: CodeFix) -> bool:
        """Apply the fix by replacing ONLY the changed block.

        We deliberately do not overwrite the whole file: a small model often
        returns a partial snippet, and a blind overwrite would delete the rest
        of the file (that is exactly what produced the +4/-96 PR). We anchor on
        fix.original_code and replace just that block; if we can't find it, we
        refuse rather than clobber the file.
        """
        try:
            if not fix.file_path or not fix.fixed_code:
                self.last_error = "Fix payload is incomplete (missing file_path or fixed_code)"
                return False

            target_path = self._find_file(repo_path, fix.file_path)

            # Fallback: LLM may return a file from a dependency/other-service package
            # that doesn't exist in this repo. Search for the original_code snippet
            # in all source files — if found, that's the real file to patch.
            if not target_path and (fix.original_code or "").strip():
                target_path = self._find_file_by_content(repo_path, fix.original_code)
                if target_path:
                    logger.warning(
                        f"File {fix.file_path!r} not found in repo; "
                        f"located original_code snippet in {target_path}"
                    )

            if not target_path:
                self.last_error = f"Could not find target file in repo: {fix.file_path}"
                logger.warning(self.last_error)
                return False

            with open(target_path, "r", encoding="utf-8") as f:
                content = f.read()

            original = self._strip_line_numbers(fix.original_code or "")
            fixed = self._strip_line_numbers(fix.fixed_code or "")

            if not original.strip():
                self.last_error = (
                    "No original_code anchor returned by analyzer; refusing full-file overwrite for "
                    f"{os.path.basename(target_path)}"
                )
                logger.error(self.last_error)
                return False

            new_content = self._replace_block(content, original, fixed,
                                               line_hint=getattr(fix, 'line_number', 0))
            if new_content is None:
                self.last_error = (
                    "original_code anchor snippet not found in target file; file likely changed "
                    f"or model selected wrong block ({os.path.basename(target_path)})"
                )
                logger.error(self.last_error)
                return False
            if new_content == content:
                self.last_error = "Generated fix produced no effective change"
                logger.error(self.last_error)
                return False

            with open(target_path, "w", encoding="utf-8") as f:
                f.write(new_content)

            logger.info(f"Applied targeted fix to: {target_path}")
            return True

        except Exception as e:
            self.last_error = f"Failed while applying fix: {e}"
            logger.error(f"Failed to apply fix: {e}")
            return False

    async def _create_branch(self, repo_path: str, branch_name: str) -> bool:
        """Create a new git branch."""
        try:
            cmd = ["git", "checkout", "-b", branch_name]
            result = subprocess.run(
                cmd,
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Branch creation failed: {e}")
            return False

    async def _commit_changes(self, repo_path: str, message: str) -> bool:
        """Commit changes."""
        try:
            # Configure git user
            subprocess.run(
                ["git", "config", "user.name", "CodeXA"],
                cwd=repo_path,
                capture_output=True,
                timeout=10,
            )
            subprocess.run(
                ["git", "config", "user.email", "codexa@local"],
                cwd=repo_path,
                capture_output=True,
                timeout=10,
            )

            # Stage all changes
            subprocess.run(
                ["git", "add", "-A"],
                cwd=repo_path,
                capture_output=True,
                timeout=30,
            )

            # Commit
            result = subprocess.run(
                ["git", "commit", "-m", message],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=30,
            )

            return result.returncode == 0

        except Exception as e:
            logger.error(f"Commit failed: {e}")
            return False

    async def _push_branch(self, repo_path: str, branch_name: str) -> bool:
        """Push branch to remote."""
        try:
            # The repo was cloned with the token already embedded in the origin
            # URL (see analyzer._clone_repo), so we push directly. The previous
            # `git remote set-url ... /*.git` used a literal '*' and broke push.
            token = self.config.github.token

            process = await asyncio.create_subprocess_exec(
                "git", "push", "-u", "origin", branch_name,
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=120,
            )

            if process.returncode == 0:
                return True
            else:
                # Sanitize error
                error = stderr.decode()
                if token:
                    error = error.replace(token, "***")
                self.last_error = error[:500]
                logger.error(f"Push failed: {error}")
                return False

        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Push error: {e}")
            return False

    async def _create_github_pr(
        self,
        repo_name: str,
        base_branch: str,
        branch_name: str,
        issue: CodeIssue,
        fix: CodeFix,
    ) -> Optional[Dict[str, Any]]:
        """Create PR via the GitHub REST API (falls back to gh CLI)."""
        title = f"fix({issue.service_name}): {fix.fix_description[:60]}"
        body = self._generate_pr_body(issue, fix)
        org = self.config.github.org

        # Primary path: REST API with the token (works in any pod, no gh needed)
        token = self.config.github.token
        if token:
            result = await self._create_pr_via_api(
                org, repo_name, base_branch, branch_name, title, body
            )
            if result:
                return result
            logger.warning("REST PR creation failed, trying gh CLI fallback")

        # Fallback: gh CLI (if installed/authenticated)
        return await self._create_pr_via_gh(
            org, repo_name, base_branch, branch_name, title, body
        )

    async def _create_pr_via_api(
        self, org, repo_name, base_branch, branch_name, title, body
    ) -> Optional[Dict[str, Any]]:
        """Create PR using GitHub REST API."""
        url = f"https://api.github.com/repos/{org}/{repo_name}/pulls"
        headers = {
            "Authorization": f"token {self.config.github.token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "codexa-bot",
        }
        payload = {
            "title": title,
            "head": branch_name,
            "base": base_branch,
            "body": body,
        }

        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status in (200, 201):
                        data = json.loads(text)
                        pr_number = data.get("number", 0)
                        pr_url = data.get("html_url", "")
                        await self._add_labels(session, org, repo_name, pr_number, headers)
                        logger.info(f"PR created via API: {pr_url}")
                        return {
                            "url": pr_url,
                            "number": pr_number,
                            "title": title,
                            "body": body,
                        }
                    # 422 usually means the PR already exists for this head
                    self.last_error = f"GitHub API PR creation failed ({resp.status}): {text[:500]}"
                    logger.error(f"GitHub API PR creation failed ({resp.status}): {text[:500]}")
                    return None
        except Exception as e:  # noqa: BLE001
            self.last_error = str(e)
            logger.error(f"GitHub API PR creation error: {e}")
            return None

    async def _add_labels(self, session, org, repo_name, pr_number, headers) -> None:
        """Best-effort label application (PRs are issues for the labels API)."""
        labels = self.config.github.pr_labels
        if not labels or not pr_number:
            return
        url = f"https://api.github.com/repos/{org}/{repo_name}/issues/{pr_number}/labels"
        try:
            async with session.post(url, json={"labels": labels}, headers=headers) as resp:
                if resp.status not in (200, 201):
                    logger.warning(f"Failed to add labels ({resp.status})")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Label add error: {e}")

    async def _create_pr_via_gh(
        self, org, repo_name, base_branch, branch_name, title, body
    ) -> Optional[Dict[str, Any]]:
        """Create PR using gh CLI (fallback)."""
        try:
            cmd = [
                "gh", "pr", "create",
                "--repo", f"{org}/{repo_name}",
                "--base", base_branch,
                "--head", branch_name,
                "--title", title,
                "--body", body,
            ]
            for label in self.config.github.pr_labels:
                cmd.extend(["--label", label])

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
            output = stdout.decode()
            error = stderr.decode()

            if process.returncode == 0:
                url_match = re.search(r"https://github\.com/[^\s]+/pull/\d+", output)
                pr_url = url_match.group(0) if url_match else ""
                num_match = re.search(r"/pull/(\d+)", pr_url)
                pr_number = int(num_match.group(1)) if num_match else 0
                return {"url": pr_url, "number": pr_number, "title": title, "body": body}

            logger.error(f"gh CLI PR creation failed: {error}")
            self.last_error = error[:500]
            return None
        except FileNotFoundError:
            self.last_error = "gh CLI not installed and REST API path unavailable"
            logger.error("gh CLI not installed and REST API path unavailable")
            return None
        except Exception as e:  # noqa: BLE001
            self.last_error = str(e)
            logger.error(f"gh CLI PR creation error: {e}")
            return None

    def _generate_pr_body(self, issue: CodeIssue, fix: CodeFix) -> str:
        """Generate PR body."""
        return f"""## CodeXA Auto-Fix

**Service:** {issue.service_name}
**Namespace:** {issue.namespace}
**Issue Type:** {issue.exception_type}

### Problem
{issue.exception_message[:500] if issue.exception_message else 'See logs for details'}

### Fix Description
{fix.fix_description}

### Confidence
{fix.confidence * 100:.0f}%

### Verification
Status: {fix.verification_status}

---
*This PR was automatically generated by CodeXA - Autonomous Code Fix Engine*
"""
