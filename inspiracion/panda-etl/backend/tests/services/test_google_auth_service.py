import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.orm import Session

from app.services.google_auth_service import GoogleAuthService

@pytest.fixture
def service():
    return GoogleAuthService(client_id="test-id")

@pytest.fixture
def db_session():
    return MagicMock(spec=Session)


def test_authenticate_existing_user(service, db_session):
    with patch("app.services.google_auth_service.id_token.verify_oauth2_token", return_value={"email": "user@example.com"}), \
         patch("app.services.google_auth_service.user_repository.get_user", return_value="u") as get_user:
        result = service.authenticate(db_session, "token")
    get_user.assert_called_once_with(db_session, "user@example.com")
    assert result == "u"


def test_authenticate_creates_user(service, db_session):
    with patch("app.services.google_auth_service.id_token.verify_oauth2_token", return_value={"email": "user@example.com"}), \
         patch("app.services.google_auth_service.user_repository.get_user", return_value=None), \
         patch("app.services.google_auth_service.user_repository.create_user", return_value="new") as create:
        result = service.authenticate(db_session, "token")
    create.assert_called_once()
    assert result == "new"


def test_authenticate_invalid_token(service, db_session):
    with patch("app.services.google_auth_service.id_token.verify_oauth2_token", side_effect=Exception("bad")):
        with pytest.raises(Exception):
            service.authenticate(db_session, "token")
