"""PostgreSQL 16 thật: DQ_TEST_DATABASE_URL trỏ tới server (user quyền CREATE DATABASE). Mỗi session tạo DB mới
rồi áp postgres/init.sql qua psql. Không đặt biến -> skip sạch."""
import os
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
import sqlalchemy as sa

POSTGRES_DIR = Path(__file__).resolve().parents[3] / "postgres"


def _url_with_db(url: str, db: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{db}"))


def _libpq(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def psql(url: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["psql", _libpq(url), "-v", "ON_ERROR_STOP=1", "-X", "-q", *args],
                          capture_output=True, text=True, cwd=POSTGRES_DIR)


@pytest.fixture(scope="session")
def server_url() -> str:
    url = os.environ.get("DQ_TEST_DATABASE_URL")
    if not url:
        pytest.skip("DQ_TEST_DATABASE_URL chưa đặt: bỏ qua test tích hợp PostgreSQL")
    return url


@pytest.fixture(scope="session")
def database_url(server_url):
    name = f"dqtest_{uuid.uuid4().hex[:12]}"
    admin = sa.create_engine(server_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(sa.text(f'CREATE DATABASE "{name}"'))
    url = _url_with_db(server_url, name)
    done = psql(url, "-f", str(POSTGRES_DIR / "init.sql"))
    assert done.returncode == 0, done.stderr
    yield url
    with admin.connect() as c:
        c.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture
def fresh_database_url(server_url):
    """Isolated DB per test: lifecycle tests reuse dataset IDs and grant names."""
    name = f"dqtest_{uuid.uuid4().hex[:12]}"
    admin = sa.create_engine(server_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(sa.text(f'CREATE DATABASE "{name}"'))
    url = _url_with_db(server_url, name)
    done = psql(url, "-f", str(POSTGRES_DIR / "init.sql"))
    assert done.returncode == 0, done.stderr
    yield url
    with admin.connect() as c:
        c.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture(scope="session")
def engine(database_url):
    eng = sa.create_engine(database_url)
    yield eng
    eng.dispose()
