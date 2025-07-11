from fastapi.testclient import TestClient
from unittest.mock import patch

from app.main import app

client = TestClient(app)


def test_google_login_success(db_session_for_test):
    user = {"id": 1, "email": "user@example.com"}
    with patch("app.api.v1.auth.GoogleAuthService") as MockService:
        instance = MockService.return_value
        instance.authenticate.return_value = user
        response = client.post("/api/v1/auth/google-login", json={"token": "t"})
        instance.authenticate.assert_called_once()
    assert response.status_code == 200
    assert response.json()["data"] == user
