import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

os.environ.setdefault("GROQ_API_KEY", "test")
os.environ.setdefault("GOOGLE_CLIENT_ID", "test")

from app.main import app  # FastAPI application instance
from app.models.base import Base  # SQLAlchemy Base for metadata
from app.api.dependencies import get_db  # The dependency to override

# Define the test database URL (in-memory SQLite)
TEST_SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

# Create a new SQLAlchemy engine for testing
test_engine = create_engine(
    TEST_SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}  # Needed for SQLite
)

# Create a sessionmaker for the test engine
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

@pytest.fixture(scope="session", autouse=True)
def create_test_db_tables():
    """
    Session-scoped fixture to create all database tables before any tests run
    and drop them after all tests have run.
    `autouse=True` ensures it's activated for the session.
    """
    Base.metadata.create_all(bind=test_engine)
    yield
    # Optional: Drop all tables after tests are done if necessary,
    # but for in-memory, it's usually not needed as it's ephemeral.
    # Base.metadata.drop_all(bind=test_engine)

@pytest.fixture(scope="function")
def db_session_for_test() -> Session:
    """
    Provides a transactional database session for a single test function.
    Rolls back any changes after the test.
    """
    connection = test_engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture(scope="function", autouse=True)
def override_get_db_dependency(db_session_for_test: Session):
    """
    Overrides the `get_db` dependency for the FastAPI app during tests.
    Ensures that API endpoints use the test database session.
    `autouse=True` ensures it's activated for each test function.
    """
    app.dependency_overrides[get_db] = lambda: db_session_for_test
    yield
    app.dependency_overrides.clear() # Clear overrides after the test

@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Provides a TestClient instance for making API requests.
    This client will use the app with overridden dependencies.
    Scope is module to avoid re-creating client for every test function if not needed,
    but function scope for db_session_override ensures DB isolation.
    """
    # Ensure tables are created before client is used (covered by create_test_db_tables autouse=True)
    return TestClient(app)

# It might also be necessary to override settings if tests rely on specific
# configurations different from the default .env file.
# For now, focusing on the database.
# @pytest.fixture(scope="session", autouse=True)
# def test_settings_override():
#     # Example: Override settings if needed
#     # original_settings = app_settings._settings_cls
#     # app.dependency_overrides[get_settings] = lambda: Settings(sqlalchemy_database_url=TEST_SQLALCHEMY_DATABASE_URL, ...)
#     # yield
#     # app.dependency_overrides.clear()
#     pass
