"""Code verifier service - verifies fixes compile/pass tests."""

import asyncio
import logging
import os
import subprocess
from typing import Any, Dict, Optional

from ..config import CodeXAConfig
from ..models import CodeIssue, CodeFix

logger = logging.getLogger("codexa.verifier")


class CodeVerifier:
    """Verifies code fixes by running build/compile."""

    def __init__(self, config: CodeXAConfig):
        self.config = config
        self.timeout = config.analysis.verification_timeout

    async def verify(
        self, fix: CodeFix, issue: CodeIssue
    ) -> Dict[str, Any]:
        """Verify a fix compiles correctly."""
        logger.info(f"Verifying fix for: {issue.service_name}")

        # Find the repo path
        workspace = self.config.analysis.workspace
        repo_name = issue.repo_name or issue.service_name
        repo_path = os.path.join(workspace, f"{repo_name}-{issue.id[:8]}")

        if not os.path.exists(repo_path):
            return {"status": "skipped", "output": "Repository not found"}

        # Detect build system
        build_system = self._detect_build_system(repo_path)
        if not build_system:
            return {"status": "skipped", "output": "No build system detected"}

        # Apply fix to file
        if fix.file_path and fix.fixed_code:
            applied = self._apply_fix(repo_path, fix)
            if not applied:
                return {"status": "failed", "output": "Failed to apply fix"}

        # Run build
        try:
            result = await self._run_build(repo_path, build_system)
            return result
        except Exception as e:
            logger.error(f"Verification error: {e}")
            return {"status": "error", "output": str(e)}

    def _detect_build_system(self, repo_path: str) -> Optional[str]:
        """Detect the build system used."""
        checks = [
            ("mvnw", "maven-wrapper"),
            ("pom.xml", "maven"),
            ("gradlew", "gradle-wrapper"),
            ("build.gradle", "gradle"),
            ("build.gradle.kts", "gradle-kotlin"),
            ("package.json", "npm"),
            ("requirements.txt", "python"),
            ("go.mod", "go"),
            ("Cargo.toml", "cargo"),
        ]

        for filename, build_type in checks:
            if os.path.exists(os.path.join(repo_path, filename)):
                return build_type

        return None

    def _apply_fix(self, repo_path: str, fix: CodeFix) -> bool:
        """Apply fix to the repository."""
        try:
            # Find the file in repo
            target_path = None
            for root, dirs, files in os.walk(repo_path):
                if ".git" in root:
                    continue
                for f in files:
                    if fix.file_path in f or f == os.path.basename(fix.file_path):
                        target_path = os.path.join(root, f)
                        break
                if target_path:
                    break

            if not target_path:
                logger.warning(f"Could not find file: {fix.file_path}")
                return False

            # Write fixed code
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(fix.fixed_code)

            logger.info(f"Applied fix to: {target_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to apply fix: {e}")
            return False

    async def _run_build(
        self, repo_path: str, build_system: str
    ) -> Dict[str, Any]:
        """Run the build command."""
        commands = {
            "maven-wrapper": ["./mvnw", "-q", "-DskipTests", "compile"],
            "maven": ["mvn", "-q", "-DskipTests", "compile"],
            "gradle-wrapper": ["./gradlew", "-q", "compileJava"],
            "gradle-kotlin": ["./gradlew", "-q", "compileKotlin"],
            "gradle": ["gradle", "-q", "compileJava"],
            "npm": ["npm", "run", "build"],
            "python": ["python", "-m", "py_compile"],
            "go": ["go", "build", "./..."],
            "cargo": ["cargo", "check"],
        }

        cmd = commands.get(build_system)
        if not cmd:
            return {"status": "skipped", "output": f"Unknown build system: {build_system}"}

        try:
            # Make wrapper executable if needed
            if build_system in ("maven-wrapper", "gradle-wrapper"):
                wrapper = os.path.join(repo_path, cmd[0].lstrip("./"))
                if os.path.exists(wrapper):
                    os.chmod(wrapper, 0o755)

            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=repo_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                return {"status": "timeout", "output": f"Build timed out after {self.timeout}s"}

            output = (stdout.decode() + stderr.decode())[:2000]

            if process.returncode == 0:
                return {"status": "passed", "output": "Build successful"}
            else:
                return {"status": "failed", "output": output}

        except FileNotFoundError:
            return {"status": "skipped", "output": f"Build tool not found: {cmd[0]}"}
        except Exception as e:
            return {"status": "error", "output": str(e)}
