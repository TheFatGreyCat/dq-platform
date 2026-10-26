"""Authorized collection endpoints used by the operations console."""
from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient

from app import create_app
from auth import TokenRegistry
from store import Store
from .helpers import registration, token_registry_dict
from .test_execution_acceptance import DS, DS2, H, NoWorker


@pytest.fixture
def console_env(fresh_database_url):
    data, tokens = token_registry_dict([DS, DS2])
    data["principals"].append({"principal": "job_only", "token_sha256": hashlib.sha256(
        b"job-only-token").hexdigest(), "grants": [{"dataset_id": DS, "role": "job", "workflow": "WRITE_GATE",
                                                       "purpose": "default"}]})
    data["principals"].append({"principal": "unregistered_editor", "token_sha256": hashlib.sha256(
        b"unregistered-token").hexdigest(), "grants": [{"dataset_id": "silver.unregistered", "role": "editor",
                                                          "workflow": None, "purpose": None}]})
    tokens.update(job_only="job-only-token", unregistered_editor="unregistered-token")
    app = create_app(Store(fresh_database_url), TokenRegistry.from_dict(data), NoWorker())
    with TestClient(app) as client:
        yield client, tokens


def test_me_and_dataset_catalog_are_grant_scoped(console_env):
    client, tokens = console_env
    assert client.get("/api/v2/me").status_code == 401
    assert client.get("/api/v2/me", headers=H(tokens, "viewer")).json()["principal"] == "viewer"
    assert client.put(f"/api/v2/datasets/{DS}", json=registration(),
                      headers=H(tokens, "editor")).status_code == 200
    catalog = client.get("/api/v2/datasets", headers=H(tokens, "viewer")).json()
    assert [item["dataset_id"] for item in catalog["items"]] == [DS]
    assert "registration" not in catalog["items"][0]
    assert client.get("/api/v2/datasets", headers=H(tokens, "job_only")).json()["items"] == []
    grants = client.get("/api/v2/me", headers=H(tokens, "unregistered_editor")).json()["grants"]
    assert grants[0]["dataset_id"] == "silver.unregistered"


def test_dataset_collections_require_human_read_grants(console_env):
    client, tokens = console_env
    assert client.put(f"/api/v2/datasets/{DS}", json=registration(), headers=H(tokens, "editor")).status_code == 200
    assert client.get(f"/api/v2/datasets/{DS}/suites", headers=H(tokens, "job_only")).status_code == 403
    assert client.get(f"/api/v2/datasets/{DS}/purposes", headers=H(tokens, "viewer")).json() == {
        "items": [], "next_cursor": None}


def test_suite_collection_uses_keyset_cursor(console_env):
    client, tokens = console_env
    client.put(f"/api/v2/datasets/{DS}", json=registration(), headers=H(tokens, "editor"))
    for name in ("alpha", "beta"):
        response = client.post(f"/api/v2/datasets/{DS}/suites", json={"name": name, "workflow": "WRITE_GATE",
                                  "description": None}, headers=H(tokens, "editor", name))
        assert response.status_code == 201
    first = client.get(f"/api/v2/datasets/{DS}/suites?limit=1", headers=H(tokens, "viewer"))
    assert first.status_code == 200
    page = first.json()
    assert len(page["items"]) == 1 and page["next_cursor"]
    second = client.get(f"/api/v2/datasets/{DS}/suites?limit=1&cursor={page['next_cursor']}",
                        headers=H(tokens, "viewer")).json()
    assert len(second["items"]) == 1 and second["next_cursor"] is None
    assert page["items"][0]["suite_id"] != second["items"][0]["suite_id"]
def test_revision_purpose_and_accepted_manifest_collections(console_env):
    from .test_execution_acceptance import setup_gate

    client, tokens = console_env
    _, manifest_id, suite_id, suite_revision, policy_revision = setup_gate(client, tokens)
    headers = H(tokens, "viewer")
    suites = client.get(f"/api/v2/datasets/{DS}/suites", headers=headers).json()
    assert [item["suite_id"] for item in suites["items"]] == [suite_id]
    revisions = client.get(f"/api/v2/datasets/{DS}/suites/{suite_id}/revisions", headers=headers).json()
    policies = client.get(f"/api/v2/datasets/{DS}/suites/{suite_id}/policy-revisions", headers=headers).json()
    assert [item["revision_id"] for item in revisions["items"]] == [suite_revision["revision_id"]]
    assert [item["revision_id"] for item in policies["items"]] == [policy_revision["revision_id"]]
    assert revisions["items"][0]["payload"]["format"] == "dq-rules/v3"
    purposes = client.get(f"/api/v2/datasets/{DS}/purposes", headers=headers).json()
    assert [(item["workflow"], item["purpose"]) for item in purposes["items"]] == [("WRITE_GATE", "default")]
    accepted = client.get(f"/api/v2/datasets/{DS}/compat-manifests", headers=headers).json()
    assert [item["manifest"]["manifest_id"] for item in accepted["items"]] == [manifest_id]
    assert accepted["items"][0]["acceptance"]["dataset_id"] == DS
    assert client.get(f"/api/v2/datasets/{DS}/suites?cursor=not-base64!", headers=H(tokens, "viewer")).status_code == 422
