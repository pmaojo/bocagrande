from fastapi import FastAPI
from app.startup import register_startup_events


def test_upload_dir_created(tmp_path) -> None:
    upload_dir = tmp_path / "uploads"
    app = FastAPI()
    register_startup_events(app, str(upload_dir))

    for handler in app.router.on_startup:
        handler()

    assert upload_dir.exists()
