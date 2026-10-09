"""Code analyzer service - uses LLM to analyze issues and generate fixes."""

import asyncio
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..config import CodeXAConfig
from ..models import CodeIssue, CodeFix
from ..llm.provider import LLMProvider
from ..llm.prompts import ANALYSIS_PROMPT, FIX_GENERATION_PROMPT
from .repo_resolver import resolve_repo_and_branch, clone_dir_for

logger = logging.getLogger("codexa.analyzer")


def _strip_line_numbers(text: str) -> str:
    """Remove '  142 | ' style prefixes added for the analysis prompt."""
    return "\n".join(re.sub(r"^\s*\d+\s*\|\s?", "", ln) for ln in text.splitlines())

# File extensions to analyze
CODE_EXTENSIONS = {
    ".java", ".kt", ".scala",  # JVM
    ".py",  # Python
    ".js", ".ts", ".tsx",  # JavaScript/TypeScript
    ".go",  # Go
    ".rs",  # Rust
    ".cpp", ".c", ".h", ".hpp",  # C/C++
}

# Directories to skip
SKIP_DIRS = {
    ".git", "node_modules", "target", "build", "dist",
    ".idea", ".vscode", "__pycache__", "venv", ".venv",
    "vendor", ".mvn", ".gradle",
}


class CodeAnalyzer:
    """Analyzes code issues and generates fixes using LLM."""

    def __init__(self, config: CodeXAConfig):
        self.config = config
        self.llm = LLMProvider(config.llm)
        self.workspace = config.analysis.workspace
        self._last_clone_error = ""

    async def test_connection(self) -> str:
        """Test LLM connectivity."""
        return await self.llm.test()

    async def analyze(self, issue: CodeIssue, on_step=None) -> Optional[Dict[str, Any]]:
        """Analyze an issue and return analysis with suggested fix.

        on_step(name, status, detail) is an optional callback invoked at each
        stage so callers (the dashboard) can show a live progress/error log.
        """
        logger.info(f"Analyzing issue: {issue.service_name} - {issue.exception_type}")

        def step(name, status="ok", detail=""):
            try:
                if on_step:
                    on_step(name, status, detail)
            except Exception:  # never let logging break analysis
                pass

        try:
            # Step 1: Clone the repository
            repo_name, branch = self._resolve_repo_and_branch(issue)
            step("Resolve repo", "ok", f"{repo_name} @ {branch}")
            step("Clone repository", "running", f"{repo_name} ({branch})")
            repo_path = await self._clone_repo(issue)
            if not repo_path:
                step("Clone repository", "error", self._last_clone_error or "git clone failed")
                logger.error("Failed to clone repository")
                return None
            step("Clone repository", "ok", repo_path)

            # Step 2: Find relevant files
            step("Find relevant files", "running")
            files = await self._find_relevant_files(repo_path, issue)
            if not files:
                step("Find relevant files", "error",
                     "No matching source files found (check stack trace / repo mapping)")
                logger.warning("No relevant files found")
                return None
            step("Find relevant files", "ok",
                 ", ".join(os.path.basename(f) for f, _ in files))
            logger.info(f"Found {len(files)} relevant files")

            # Step 3: Read file contents
            file_contents = await self._read_files(files)

            # Step 4: Analyze with LLM
            step("LLM analyze", "running", f"model={self.config.llm.model}")
            try:
                analysis = await self._analyze_with_llm(issue, file_contents)
            except asyncio.TimeoutError:
                step("LLM analyze", "error",
                     f"LLM timed out after {self.config.llm.timeout_seconds}s — "
                     "model may be loading or overloaded; try again in 1-2 min")
                return None
            except Exception as _llm_err:
                step("LLM analyze", "error",
                     f"LLM error: {str(_llm_err)[:300]}")
                return None
            if not analysis:
                step("LLM analyze", "error",
                     "LLM returned empty response (model may be unavailable)")
                return None

            # Step 5: Generate fix if the model says can_fix, OR if its
            # confidence is high enough (small models often report can_fix=false
            # even at high confidence). Fixes are reviewed in the PR before merge.
            confidence = 0.0
            try:
                confidence = float(analysis.get("confidence", 0) or 0)
            except (TypeError, ValueError):
                confidence = 0.0
            step("LLM analyze", "ok",
                 f"can_fix={analysis.get('can_fix')} confidence={confidence} "
                 f"file={analysis.get('file_path')}")

            if analysis.get("can_fix") or confidence >= self.config.analysis.min_fix_confidence:
                logger.info(f"Generating fix (can_fix={analysis.get('can_fix')}, confidence={confidence})")
                step("Generate fix", "running")
                fix = await self._generate_fix_with_llm(issue, analysis, file_contents)
                analysis["fix"] = fix
                fixed_code = (fix or {}).get("fixed_code", "")
                if fix and fixed_code:
                    step("Generate fix", "ok",
                         f"{len(fixed_code)} chars - {(fix.get('description') or '')[:120]}")
                else:
                    step("Generate fix", "error", "Model did not return fixed_code")
            else:
                step("Generate fix", "warn",
                     f"Skipped: confidence {confidence} < {self.config.analysis.min_fix_confidence} "
                     f"and can_fix is false")

            # Cleanup
            if not self.config.analysis.workspace:
                shutil.rmtree(repo_path, ignore_errors=True)

            return analysis

        except Exception as e:
            step("Analyze", "error", str(e))
            logger.error(f"Analysis failed: {e}")
            return None

    def _resolve_repo_and_branch(self, issue: CodeIssue) -> Tuple[str, str]:
        """Resolve the actual repo name and branch using shared resolver."""
        return resolve_repo_and_branch(self.config, issue)

    async def _clone_repo(self, issue: CodeIssue) -> Optional[str]:
        """Clone the service repository."""
        # Resolve repo name and branch using mappings, then stamp the resolved
        # values back onto the issue so the verifier and git_ops (which run
        # later) look in the SAME clone directory and target the SAME branch.
        repo_name, branch = self._resolve_repo_and_branch(issue)
        issue.repo_name = repo_name
        issue.branch = branch

        # Build repo URL
        github_token = self.config.github.token
        org = self.config.github.org

        if github_token:
            repo_url = f"https://x-access-token:{github_token}@github.com/{org}/{repo_name}.git"
        else:
            repo_url = f"https://github.com/{org}/{repo_name}.git"

        # Create workspace
        os.makedirs(self.workspace, exist_ok=True)
        clone_path = clone_dir_for(self.workspace, repo_name, issue.id)

        # Remove if exists
        if os.path.exists(clone_path):
            shutil.rmtree(clone_path)

        try:
            cmd = ["git", "clone", "--depth", "1", "--branch", branch, repo_url, clone_path]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode == 0:
                logger.info(f"Cloned repo to {clone_path}")
                self._last_clone_error = ""
                return clone_path
            else:
                # Sanitize error (remove token)
                error = result.stderr.replace(github_token, "***") if github_token else result.stderr
                self._last_clone_error = (error or "git clone failed").strip()[:500]
                logger.error(f"Clone failed: {error}")
                return None
        except Exception as e:
            self._last_clone_error = str(e)[:500]
            logger.error(f"Clone error: {e}")
            return None

    async def _find_relevant_files(
        self, repo_path: str, issue: CodeIssue
    ) -> List[Tuple[str, float]]:
        """Find files relevant to the issue."""
        files_with_scores: List[Tuple[str, float]] = []

        # Build search tokens from issue
        tokens = self._extract_search_tokens(issue)
        logger.debug(f"Search tokens: {tokens}")

        # Walk repository
        file_count = 0
        max_files = self.config.analysis.max_files_per_issue * 100  # Scan limit

        for root, dirs, files in os.walk(repo_path):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

            for file in files:
                if file_count >= max_files:
                    break

                file_count += 1
                ext = os.path.splitext(file)[1].lower()
                if ext not in CODE_EXTENSIONS:
                    continue

                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, repo_path)

                # Score file by token matches
                score = self._score_file(file_path, rel_path, tokens)
                if score > 0:
                    files_with_scores.append((file_path, score))

        # Sort by score and take top N
        files_with_scores.sort(key=lambda x: x[1], reverse=True)
        result = files_with_scores[: self.config.analysis.max_files_per_issue]

        # Fallback: if no files found, look for common important files
        if not result:
            logger.info("No token matches found, searching for important files...")
            fallback_patterns = [
                "Application.java", "Main.java", "App.java",
                "Controller.java", "Service.java", "Repository.java",
                "Config.java", "Configuration.java",
                "app.py", "main.py", "application.py",
                "index.ts", "app.ts", "main.ts",
            ]
            for root, dirs, files in os.walk(repo_path):
                dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
                for file in files:
                    if any(file.endswith(p) or p in file for p in fallback_patterns):
                        file_path = os.path.join(root, file)
                        result.append((file_path, 1.0))
                        if len(result) >= self.config.analysis.max_files_per_issue:
                            break
                if len(result) >= self.config.analysis.max_files_per_issue:
                    break

            if result:
                logger.info(f"Fallback found {len(result)} important files")

        return result

    def _extract_search_tokens(self, issue: CodeIssue) -> List[str]:
        """Extract search tokens from issue."""
        tokens = []

        # Add service name (important for matching)
        if issue.service_name:
            # Add both full name and parts
            tokens.append(issue.service_name)
            for part in issue.service_name.replace("-", " ").replace("_", " ").split():
                if len(part) > 2:
                    tokens.append(part)

        # Add exception type
        if issue.exception_type:
            tokens.append(issue.exception_type)
            # Also add parts of the exception name
            # e.g., "IllegalStateException" -> "IllegalState", "State"
            exc_parts = re.findall(r'[A-Z][a-z]+', issue.exception_type)
            tokens.extend(exc_parts)

        # Add file name if known
        if issue.file_path:
            tokens.append(issue.file_path)
            # Also add just the filename without extension
            basename = os.path.basename(issue.file_path)
            name_only = os.path.splitext(basename)[0]
            tokens.append(name_only)

        # Extract class/method names from stack trace
        if issue.stack_trace:
            # Java class names
            class_pattern = re.compile(r"at\s+([\w.$]+)\.([\w<>]+)\(")
            for match in class_pattern.finditer(issue.stack_trace):
                full_class = match.group(1)
                class_name = full_class.split(".")[-1]
                method_name = match.group(2).replace("<", "").replace(">", "")
                if class_name:
                    tokens.append(class_name)
                if method_name and method_name not in ("init", "clinit"):
                    tokens.append(method_name)

            # Also extract file names from stack trace
            file_pattern = re.compile(r"\(([A-Za-z0-9_]+\.java):(\d+)\)")
            for match in file_pattern.finditer(issue.stack_trace):
                file_name = match.group(1).replace(".java", "")
                tokens.append(file_name)

        # Extract from exception message
        if issue.exception_message:
            # Extract quoted values
            quoted = re.findall(r"['\"]([^'\"]+)['\"]", issue.exception_message)
            tokens.extend(quoted[:5])
            # Extract CamelCase words
            camel_words = re.findall(r'[A-Z][a-z]+(?:[A-Z][a-z]+)*', issue.exception_message)
            tokens.extend(camel_words[:5])

        # Deduplicate and filter
        seen = set()
        unique = []
        for t in tokens:
            t = t.strip()
            if t and len(t) > 2 and t.lower() not in seen:
                seen.add(t.lower())
                unique.append(t)

        logger.debug(f"Extracted search tokens: {unique[:20]}")
        return unique[:20]

    def _score_file(self, file_path: str, rel_path: str, tokens: List[str]) -> float:
        """Score file relevance based on tokens."""
        score = 0.0

        # Check filename
        filename = os.path.basename(file_path).lower()
        for token in tokens:
            if token.lower() in filename:
                score += 5.0

        # Check path
        rel_lower = rel_path.lower()
        for token in tokens:
            if token.lower() in rel_lower:
                score += 2.0

        # Check file contents (quick scan)
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read(50000)  # First 50KB
                content_lower = content.lower()
                for token in tokens:
                    if token.lower() in content_lower:
                        score += 3.0
        except Exception:
            pass

        return score

    async def _read_files(
        self, files: List[Tuple[str, float]]
    ) -> Dict[str, str]:
        """Read file contents."""
        contents = {}
        max_size = 100000  # 100KB per file

        for file_path, score in files:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read(max_size)
                    # Add line numbers
                    lines = content.split("\n")
                    numbered = "\n".join(f"{i+1:4d} | {line}" for i, line in enumerate(lines))
                    contents[file_path] = numbered
            except Exception as e:
                logger.warning(f"Failed to read {file_path}: {e}")

        return contents

    async def _analyze_with_llm(
        self, issue: CodeIssue, file_contents: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Use LLM to analyze the issue."""
        # Build context
        files_context = ""
        for path, content in file_contents.items():
            rel_path = os.path.basename(path)
            # 3000 chars per file keeps the total prompt comfortably under 16k tokens
            # (2 files × 3000 + stack + log + template ≈ 10k tokens).
            files_context += f"\n### File: {rel_path}\n```\n{content[:3000]}\n```\n"

        prompt = ANALYSIS_PROMPT.format(
            exception_type=issue.exception_type,
            exception_message=issue.exception_message,
            stack_trace=issue.stack_trace[:3000] if issue.stack_trace else "Not available",
            log_evidence=issue.log_evidence[:2000] if issue.log_evidence else "Not available",
            files_context=files_context,
        )

        try:
            response = await self.llm.generate(prompt)
            return self._parse_analysis_response(response)
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            raise

    def _parse_analysis_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM analysis response."""
        # Try to extract JSON
        import json

        # Look for JSON block
        json_match = re.search(r"```json\s*(.*?)\s*```", response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try direct JSON parse
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # Fallback: extract key info
        return {
            "can_fix": "fix" in response.lower() and "cannot" not in response.lower(),
            "root_cause": response[:500],
            "file_path": "",
            "line_number": 0,
            "confidence": 0.5,
        }

    async def _generate_fix_with_llm(
        self,
        issue: CodeIssue,
        analysis: Dict[str, Any],
        file_contents: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        """Use LLM to generate the actual fix."""
        target_file = analysis.get("file_path", "")
        target_content = ""

        # Find the target file content
        for path, content in file_contents.items():
            if target_file and target_file in path:
                target_content = content
                break
        if not target_content and file_contents:
            # Use first file
            target_content = list(file_contents.values())[0]

        # Strip "142 | " prefixes so model copies exact bytes that exist in the file.
        clean_content = _strip_line_numbers(target_content)

        # Narrow to a window around the target line so the model sees (and can
        # copy verbatim) only the code it needs to change.  A 100-line window is
        # enough for any method-level fix; sending the whole file causes small
        # models (1.5B) to hallucinate slightly different code as original_code,
        # which then can't be found in the file at apply time.
        target_line = int(analysis.get("line_number", 0) or issue.line_number or 0)
        lines = clean_content.splitlines()
        if target_line > 0 and target_line <= len(lines):
            # Tight anchor shown as "exact lines" — model MUST copy these verbatim
            anchor_start = max(0, target_line - 5)
            anchor_end   = min(len(lines), target_line + 8)
            target_lines = "\n".join(lines[anchor_start:anchor_end])

            # Wider context so the model understands the surrounding method
            ctx_start = max(0, target_line - 40)
            ctx_end   = min(len(lines), target_line + 60)
            fix_content = "\n".join(lines[ctx_start:ctx_end])
        else:
            target_lines = "(line number not identified — copy the buggy lines verbatim)"
            fix_content  = clean_content[:5000]

        prompt = FIX_GENERATION_PROMPT.format(
            exception_type=issue.exception_type,
            exception_message=issue.exception_message,
            root_cause=analysis.get("root_cause", "Unknown"),
            target_lines=target_lines,
            file_content=fix_content,
            line_number=analysis.get("line_number", issue.line_number),
        )

        try:
            response = await self.llm.generate(prompt)
            fix_dict = self._parse_fix_response(response)

            # Small models often return fixed_code but leave original_code empty.
            # Auto-extract from the file at the target line so _replace_block
            # has a real anchor and can apply the fix surgically.
            if fix_dict and not (fix_dict.get("original_code") or "").strip():
                fixed = (fix_dict.get("fixed_code") or "").strip()
                if fixed and lines and 0 < target_line <= len(lines):
                    n   = max(len(fixed.splitlines()), 1)
                    idx = target_line - 1  # 0-based
                    extracted = lines[idx : idx + n]
                    if extracted:
                        fix_dict["original_code"] = "\n".join(extracted)
                        logger.info(
                            f"Auto-extracted original_code: {len(extracted)} lines "
                            f"at line {target_line} (model returned empty original_code)"
                        )

            return fix_dict
        except Exception as e:
            logger.error(f"LLM fix generation failed: {e}")
            return None

    def _parse_fix_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM fix response."""
        import json

        # Ollama JSON mode returns a raw JSON object (no fences) - try that first
        try:
            data = json.loads(response)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass

        # Try to extract JSON from a fenced block
        json_match = re.search(r"```json\s*(.*?)\s*```", response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to extract code block
        code_match = re.search(r"```(?:java|python|kotlin|go)?\s*(.*?)\s*```", response, re.DOTALL)
        if code_match:
            return {
                "fixed_code": code_match.group(1),
                "description": "Generated fix",
                "confidence": 0.7,
            }

        return {
            "fixed_code": "",
            "description": response[:500],
            "confidence": 0.3,
        }
