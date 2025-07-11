from __future__ import annotations

import os
from fastapi import FastAPI


def ensure_directory(path: str) -> None:
    """Create the given directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def register_startup_events(app: FastAPI, upload_dir: str) -> None:
    """Register startup events required for the application."""

    @app.on_event("startup")
    def _ensure_upload_dir() -> None:
        ensure_directory(upload_dir)
