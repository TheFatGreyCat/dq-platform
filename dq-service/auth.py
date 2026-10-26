"""
Principal / grant authorization.

Bootstrap source: `DQ_TOKEN_REGISTRY_FILE` (0600 JSON, chỉ chứa SHA-256 digest của token):
  {"principals":[{"principal":"p1","token_sha256":"<64 hex>",
                  "grants":[{"dataset_id":"d","role":"editor","workflow":null,"purpose":null}]}]}
File là nguồn quyết định quyền (thu hồi trong file -> mất quyền ngay cả khi DB còn grant cũ);
`principal_grants` trong DB được đồng bộ từ file cho dataset đã tồn tại để truy vấn/kiểm toán.
Không có wildcard/admin/global token.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path

from errors import ApiError

ROLES = ("viewer", "editor", "approver", "operator", "job")
READ_ROLES = frozenset({"viewer", "editor", "approver", "operator"})
WORKFLOWS = ("WRITE_GATE", "AUDIT_TABLE")


@dataclass(frozen=True)
class Grant:
    dataset_id: str
    role: str
    workflow: str | None
    purpose: str | None


@dataclass(frozen=True)
class Principal:
    name: str
    grants: frozenset[Grant] = field(default_factory=frozenset)

    def roles(self, dataset_id: str) -> set[str]:
        return {g.role for g in self.grants if g.dataset_id == dataset_id}

    def job_grant(self, dataset_id: str, workflow: str, purpose: str) -> bool:
        return Grant(dataset_id, "job", workflow, purpose) in self.grants

    def job_purposes(self, dataset_id: str) -> set[tuple[str, str]]:
        return {(g.workflow, g.purpose) for g in self.grants if g.dataset_id == dataset_id and g.role == "job"}


class TokenRegistry:
    def __init__(self, principals: dict[str, Principal], digests: dict[str, str]):
        self._principals = principals
        self._digests = digests  # sha256 hex -> principal

    @classmethod
    def from_dict(cls, data: dict) -> "TokenRegistry":
        if not isinstance(data, dict) or set(data) != {"principals"} or not isinstance(data["principals"], list):
            raise ValueError("token registry: cần đúng {principals:[...]}")
        principals: dict[str, Principal] = {}
        digests: dict[str, str] = {}
        for p in data["principals"]:
            if not isinstance(p, dict) or set(p) != {"principal", "token_sha256", "grants"}:
                raise ValueError("token registry: principal entry sai shape")
            name, digest = p["principal"], p["token_sha256"]
            if not isinstance(name, str) or not name or len(name) > 128 or name in principals:
                raise ValueError("token registry: principal rỗng/trùng")
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("token registry: token_sha256 phải là 64 hex thường")
            if digest == hashlib.sha256(b"").hexdigest():
                raise ValueError("token registry: token rỗng")
            if digest in digests:
                raise ValueError("token registry: digest trùng")
            grants = set()
            for g in p["grants"]:
                if not isinstance(g, dict) or set(g) != {"dataset_id", "role", "workflow", "purpose"}:
                    raise ValueError("token registry: grant sai shape")
                if g["role"] not in ROLES or not isinstance(g["dataset_id"], str) or not g["dataset_id"]:
                    raise ValueError("token registry: role/dataset sai")
                is_job = g["role"] == "job"
                if is_job != (g["workflow"] is not None) or is_job != (g["purpose"] is not None):
                    raise ValueError("token registry: job cần workflow+purpose, role khác thì null")
                if is_job and (g["workflow"] not in WORKFLOWS or not isinstance(g["purpose"], str) or not g["purpose"]):
                    raise ValueError("token registry: workflow/purpose sai")
                grants.add(Grant(g["dataset_id"], g["role"], g["workflow"], g["purpose"]))
            principals[name] = Principal(name, frozenset(grants))
            digests[digest] = name
        return cls(principals, digests)

    @classmethod
    def from_file(cls, path: str) -> "TokenRegistry":
        st = os.stat(path)
        if stat.S_IMODE(st.st_mode) & 0o077:
            raise ValueError("token registry file phải có mode 0600")
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def authenticate(self, authorization: str | None) -> Principal:
        if not authorization or not authorization.startswith("Bearer "):
            raise ApiError("UNAUTHENTICATED")
        token = authorization[len("Bearer "):]
        if not token:
            raise ApiError("UNAUTHENTICATED")
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        match = None
        for d, name in self._digests.items():  # compare_digest trên mọi entry, không rẽ nhánh sớm
            if hmac.compare_digest(d, digest):
                match = name
        if match is None:
            raise ApiError("UNAUTHENTICATED")
        return self._principals[match]

    def grants_for(self, dataset_id: str) -> list[tuple[str, Grant]]:
        return sorted(((p.name, g) for p in self._principals.values() for g in p.grants if g.dataset_id == dataset_id),
                      key=lambda x: (x[0], x[1].role, x[1].workflow or "", x[1].purpose or ""))


def require_dataset(principal: Principal, dataset_id: str, roles: set[str] | frozenset[str]) -> None:
    """Role không đủ -> 403. (Dataset không tồn tại được xử lý bởi caller sau khi đã kiểm quyền.)"""
    if not (principal.roles(dataset_id) & set(roles)):
        raise ApiError("FORBIDDEN")


def can_read_dataset(principal: Principal, dataset_id: str) -> bool:
    return bool(principal.roles(dataset_id) & READ_ROLES) or bool(principal.job_purposes(dataset_id))


def require_read(principal: Principal, dataset_id: str) -> None:
    if not can_read_dataset(principal, dataset_id):
        raise ApiError("FORBIDDEN")


def require_execution(principal: Principal, execution: dict, operation: str) -> str:
    """operation: read | job | operator | job_or_operator. Trả role được dùng ('job'/'operator'/'viewer').
    Job chỉ chạm execution của đúng purpose được cấp; foreign execution -> 404 (không lộ metadata)."""
    ds, wf, pu = execution["dataset_id"], execution["workflow"], execution["purpose"]
    is_job = principal.job_grant(ds, wf, pu)
    roles = principal.roles(ds)
    if operation == "read":
        if is_job or roles & READ_ROLES:
            return "job" if is_job else "viewer"
    elif operation == "job":
        if is_job:
            return "job"
    elif operation == "operator":
        if "operator" in roles:
            return "operator"
    elif operation == "job_or_operator":
        if is_job:
            return "job"
        if "operator" in roles:
            return "operator"
    if not (roles or principal.job_purposes(ds)):
        raise ApiError("NOT_FOUND")
    raise ApiError("FORBIDDEN")
