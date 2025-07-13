"""Automation helper to iteratively fix failing tests using an LLM."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, Iterable


class PatchGenerator(Protocol):
    """Interface for objects capable of generating patches."""

    def propose_patch(self, report: str) -> str:
        """Return a unified diff that tries to fix the failing tests."""
        ...


@dataclass
class DefaultPatchGenerator:
    """LLM-based patch generator using google-generativeai."""

    model: str = "gemini-pro"

    def propose_patch(self, report: str) -> str:  # pragma: no cover - network
        import google.generativeai as genai

        prompt = (
            "Given the following pytest output, propose a unified diff patch that "
            "fixes the failures. Only respond with the diff.\n" + report
        )
        response = genai.generate_text(model=self.model, prompt=prompt)  # type: ignore[attr-defined]
        return response.result


def _ensure_venv(path: Path) -> Path:
    """Create a virtual environment under ``path`` if it does not exist."""
    if not path.exists():
        subprocess.run([sys.executable, "-m", "venv", str(path)], check=True)
        subprocess.run([
            str(path / "bin" / "pip"),
            "install",
            "-r",
            "requirements.txt",
            "-r",
            "requirements-dev.txt",
        ], check=True)
    return path


def _run_pytest(venv: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Execute pytest using the given virtual environment."""
    cmd = [str(venv / "bin" / "pytest"), *args]
    return subprocess.run(cmd, capture_output=True, text=True)


def _iter_changed_files(diff: str) -> Iterable[str]:
    """Yield file paths that appear in ``diff``."""
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            yield line[6:]


def validate_patch(diff: str, *, whitelist: Iterable[str], max_size: int) -> bool:
    """Return ``True`` if ``diff`` touches only whitelisted paths and is small."""
    if len(diff) > max_size:
        return False
    allowed = [Path(w).resolve() for w in whitelist]
    for changed in _iter_changed_files(diff):
        path = Path(changed).resolve()
        ok = False
        for w in allowed:
            try:
                path.relative_to(w)
                ok = True
                break
            except ValueError:
                continue
        if not ok:
            return False
    return True


@dataclass
class AutoFixLoop:
    """Coordinates test execution and patch application."""

    generator: PatchGenerator
    max_iter: int = 5
    diff_limit: int = 8_000
    whitelist: tuple[str, ...] = ("adapter", "bocagrande", "tests")
    artifacts: Path = Path("artifacts")

    def run(self) -> None:  # pragma: no cover - integration
        venv = _ensure_venv(Path(".autoenv"))
        self.artifacts.mkdir(exist_ok=True)

        for i in range(1, self.max_iter + 1):
            test = _run_pytest(venv)
            log_dir = self.artifacts / f"iter_{i}"
            log_dir.mkdir()
            (log_dir / "pytest.log").write_text(test.stdout + "\n" + test.stderr)
            if test.returncode == 0:
                break
            patch = self.generator.propose_patch(test.stdout + test.stderr)
            (log_dir / "patch.diff").write_text(patch)
            if not validate_patch(patch, whitelist=self.whitelist, max_size=self.diff_limit):
                continue
            apply = subprocess.run(["git", "apply", "-"], input=patch, text=True)
            if apply.returncode != 0:
                continue


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI
    parser = argparse.ArgumentParser(description="Iteratively fix tests with an LLM")
    parser.add_argument("--max-iter", type=int, default=5)
    parser.add_argument("--diff-limit", type=int, default=8_000)
    args = parser.parse_args(argv)

    loop = AutoFixLoop(DefaultPatchGenerator(), args.max_iter, args.diff_limit)
    loop.run()


if __name__ == "__main__":  # pragma: no cover - manual entry
    main(sys.argv[1:])
