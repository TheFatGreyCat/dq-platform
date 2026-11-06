import hashlib
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
import pytest

from app import create_app
from auth import TokenRegistry
from errors import ApiError


@pytest.fixture
def test_setup():
    store = MagicMock()
    token = "tok-editor-123"
    token_sha = hashlib.sha256(token.encode()).hexdigest()
    reg_data = {
        "principals": [
            {
                "principal": "test-editor",
                "token_sha256": token_sha,
                "grants": [{"dataset_id": "ds1", "role": "editor", "workflow": None, "purpose": None}],
            },
            {
                "principal": "test-viewer",
                "token_sha256": hashlib.sha256(b"tok-viewer").hexdigest(),
                "grants": [{"dataset_id": "ds1", "role": "viewer", "workflow": None, "purpose": None}],
            },
        ]
    }
    registry = TokenRegistry.from_dict(reg_data)
    app = create_app(store, registry, MagicMock())
    return TestClient(app), store, token


def test_validate_endpoint_permissions_and_success(test_setup):
    client, store, token = test_setup
    store.get_suite.return_value = {
        "suite_id": "s1",
        "dataset_id": "ds1",
        "workflow": "WRITE_GATE",
        "name": "Suite 1",
        "description": "test",
    }

    headers_editor = {"Authorization": f"Bearer {token}"}
    headers_viewer = {"Authorization": "Bearer tok-viewer"}

    # Viewer gets 403
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "complete", "column": "col_a"}
        }]
    }}, headers=headers_viewer)
    assert r.status_code == 403

    # Editor gets 200 valid: true, contains_sql: false
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "complete", "column": "col_a"}
        }]
    }, "policy": {
        "rules": [{"rule_id": "r1", "action": "QUARANTINE", "required": True}],
        "max_quarantine_ratio": "0.05",
        "anomaly": {"enabled": False}
    }}, headers=headers_editor)
    assert r.status_code == 200
    assert r.json() == {"valid": True, "contains_sql": False}

    # Contains SQL check
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "sql_predicate", "predicate": "col_a IS NOT NULL", "columns": ["col_a"]}
        }]
    }}, headers=headers_editor)
    assert r.status_code == 200
    assert r.json() == {"valid": True, "contains_sql": True}

    # Workflow mismatch
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "workflow": "AUDIT_TABLE",
        "rules": [{
                "rule_id": "r1", "dimension": "validity", "scope": "TABLE", "stage": "AUDIT_SNAPSHOT",
            "check": {"kind": "complete", "column": "col_a"}
        }]
    }}, headers=headers_editor)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_CONFIG"
    assert "workflow" in r.json()["error"]["field"]

    # Policy rule ID mismatch
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "complete", "column": "col_a"}
        }]
    }, "policy": {
        "rules": [{"rule_id": "r2", "action": "OBSERVE", "required": True}],
        "max_quarantine_ratio": "0.05",
        "anomaly": {"enabled": False}
    }}, headers=headers_editor)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_CONFIG"
    assert r.json()["error"]["field"] == "policy"

    # QUARANTINE on non-row-level check (e.g. size)
    r = client.post("/api/v2/datasets/ds1/suites/s1/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "accuracy", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "size", "assert": {"op": ">=", "value": "10"}}
        }]
    }, "policy": {
        "rules": [{"rule_id": "r1", "action": "QUARANTINE", "required": True}],
        "max_quarantine_ratio": "0.05",
        "anomaly": {"enabled": False}
    }}, headers=headers_editor)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_CONFIG"
    assert r.json()["error"]["field"] == "policy"

    # Suite not found
    store.get_suite.side_effect = ApiError("NOT_FOUND")
    r = client.post("/api/v2/datasets/ds1/suites/nonexistent/validate", json={"suite": {
        "format": "dq-rules/v3",
        "rules": [{
            "rule_id": "r1", "dimension": "validity", "scope": "PARTITION", "stage": "CANDIDATE",
            "check": {"kind": "complete", "column": "col_a"}
        }]
    }}, headers=headers_editor)
    assert r.status_code == 404
