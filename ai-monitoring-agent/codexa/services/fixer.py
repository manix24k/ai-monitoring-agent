"""Fix generator service - creates code fixes from LLM analysis."""

import difflib
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

from ..config import CodeXAConfig
from ..models import CodeIssue, CodeFix
from .analyzer import CodeAnalyzer
from .verifier import CodeVerifier

logger = logging.getLogger("codexa.fixer")


class FixGenerator:
    """Generates code fixes from analysis."""

    def __init__(self, config: CodeXAConfig, analyzer: CodeAnalyzer):
        self.config = config
        self.analyzer = analyzer
        self.verifier = CodeVerifier(config)

    async def generate_fix(
        self, issue: CodeIssue, analysis: Dict[str, Any]
    ) -> Optional[CodeFix]:
        """Generate a code fix from analysis."""
        logger.info(f"Generating fix for: {issue.service_name}")

        fix_data = analysis.get("fix", {})
        if not fix_data:
            logger.warning("No fix data in analysis")
            return None

        fixed_code = fix_data.get("fixed_code", "")
        if not fixed_code:
            logger.warning("No fixed code generated")
            return None

        # Get original code
        original_code = fix_data.get("original_code", "")
        file_path = analysis.get("file_path", issue.file_path)

        # Generate diff
        diff_patch = self._generate_diff(original_code, fixed_code, file_path)

        # Create fix object
        fix = CodeFix(
            issue_id=issue.id,
            file_path=file_path,
            line_number=int(analysis.get("line_number", 0) or 0),
            original_code=original_code,
            fixed_code=fixed_code,
            diff_patch=diff_patch,
            fix_description=fix_data.get("description", "Auto-generated fix"),
            llm_model=self.config.llm.model,
            llm_reasoning=analysis.get("root_cause", ""),
            confidence=float(fix_data.get("confidence", analysis.get("confidence", 0.7))),
            verification_status="pending",
        )

        # Verify the fix (optional but recommended)
        if self.config.analysis.verification_timeout > 0:
            verification = await self.verifier.verify(fix, issue)
            fix.verification_status = verification.get("status", "unknown")
            fix.verification_output = verification.get("output", "")[:2000]

        return fix

    def _generate_diff(
        self, original: str, fixed: str, file_path: str
    ) -> str:
        """Generate unified diff between original and fixed code."""
        if not original or not fixed:
            return ""

        original_lines = original.splitlines(keepends=True)
        fixed_lines = fixed.splitlines(keepends=True)

        diff = difflib.unified_diff(
            original_lines,
            fixed_lines,
            fromfile=f"a/{file_path}",
            tofile=f"b/{file_path}",
            lineterm="",
        )

        return "".join(diff)

    def format_diff_html(self, diff_patch: str) -> str:
        """Format diff for HTML display."""
        if not diff_patch:
            return ""

        lines = []
        for line in diff_patch.split("\n"):
            if line.startswith("+++") or line.startswith("---"):
                lines.append(f'<span class="diff-header">{self._escape_html(line)}</span>')
            elif line.startswith("@@"):
                lines.append(f'<span class="diff-hunk">{self._escape_html(line)}</span>')
            elif line.startswith("+"):
                lines.append(f'<span class="diff-add">{self._escape_html(line)}</span>')
            elif line.startswith("-"):
                lines.append(f'<span class="diff-del">{self._escape_html(line)}</span>')
            else:
                lines.append(f'<span class="diff-context">{self._escape_html(line)}</span>')

        return "\n".join(lines)

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )
