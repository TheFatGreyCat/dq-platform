#!/usr/bin/env python3
"""Tạo Secret runtime cho một namespace dq-platform (stdlib + kubectl). Secret không bao giờ nằm trong Git.

  bootstrap-secrets.py --namespace dq-dev --tokens-out /abs/dir --dataset silver.transactions [--dataset ...]

Sinh ngẫu nhiên: mật khẩu Postgres, credential S3, token cho editor/approver/operator/viewer/job.
Token plaintext chỉ ghi vào --tokens-out/<role> (thư mục 0700, file 0600) cho operator; registry trong cluster
chỉ chứa sha256. Secret tạo bằng `kubectl apply` từ stdin (giá trị không lên argv). Đã tồn tại -> từ chối
(xoay vòng là thao tác có chủ đích: xóa Secret cũ trước). Production: thay bằng External Secrets/Sealed Secrets
cùng tên và key (dq-database, dq-token-registry, dq-s3, dq-job-token).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import subprocess
import sys

ROLES = ("editor", "approver", "operator", "viewer", "job")
NAMES = ("dq-database", "dq-token-registry", "dq-s3", "dq-job-token")


def registry(datasets: list[str], tokens: dict[str, str]) -> dict:
    def grants(role: str) -> list[dict]:
        if role == "job":
            return [{"dataset_id": d, "role": "job", "workflow": w, "purpose": p}
                    for d in datasets for w, p in (("WRITE_GATE", "default"), ("AUDIT_TABLE", "nightly"))]
        return [{"dataset_id": d, "role": role, "workflow": None, "purpose": None} for d in datasets]
    return {"principals": [{"principal": f"k8s-{r}", "grants": grants(r),
                            "token_sha256": hashlib.sha256(t.encode()).hexdigest()} for r, t in tokens.items()]}


def secret(name: str, data: dict[str, str]) -> dict:
    return {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "labels": {"app.kubernetes.io/part-of": "dq-platform"}},
            "data": {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}}


def build(datasets: list[str], db_host: str) -> tuple[list[dict], dict[str, str]]:
    tokens = {r: secrets.token_urlsafe(32) for r in ROLES}
    pg = secrets.token_urlsafe(24)
    ak, sk = secrets.token_hex(10), secrets.token_urlsafe(30)
    objs = [
        secret("dq-database", {"postgres_password": pg,
                               "database_url": f"postgresql+psycopg://dq:{pg}@{db_host}:5432/dq",
                               "psql_url": f"postgresql://dq:{pg}@{db_host}:5432/dq"}),
        secret("dq-token-registry", {"token_registry.json": json.dumps(registry(datasets, tokens))}),
        secret("dq-s3", {"access-key": ak, "secret-key": sk}),
        secret("dq-job-token", {"token": tokens["job"]}),
    ]
    return objs, tokens


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--tokens-out", required=True)
    ap.add_argument("--dataset", action="append", required=True)
    ap.add_argument("--db-host", default="postgres")
    a = ap.parse_args(argv)
    if not os.path.isabs(a.tokens_out):
        ap.error("--tokens-out phải là đường dẫn tuyệt đối")
    kc = ["kubectl", "-n", a.namespace]
    have = subprocess.run([*kc, "get", "secret", "-o", "name", *NAMES], capture_output=True, text=True)
    if have.stdout.strip():
        print("bootstrap: secret đã tồn tại, không ghi đè", file=sys.stderr)
        return 2
    objs, tokens = build(a.dataset, a.db_host)
    os.makedirs(a.tokens_out, mode=0o700, exist_ok=True)
    os.chmod(a.tokens_out, 0o700)
    for r, t in tokens.items():
        fd = os.open(os.path.join(a.tokens_out, r), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(t)
    p = subprocess.run([*kc, "apply", "-f", "-"], input=json.dumps({"apiVersion": "v1", "kind": "List", "items": objs}),
                       capture_output=True, text=True)
    if p.returncode:
        print(f"bootstrap: kubectl apply exit={p.returncode}", file=sys.stderr)
        return 2
    print(json.dumps({"namespace": a.namespace, "secrets": list(NAMES), "tokens_out": a.tokens_out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
