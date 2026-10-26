#!/usr/bin/env python3
"""Parity between a Python-job execution and a Scala-job execution of the same Bronze partition.

  parity_check.py --python-execution E1 --scala-execution E2 --service-url URL --output result.json \
                  [--spark --dt YYYY-MM-DD] [--expect-candidate N --expect-quarantined N]

Token: env DQ_API_TOKEN. Compares the stored reports (rules: scope/stage/kind/status/metric name/decimal value/
schema evidence/failed_rows/error; counts, classification, report errors, decision and publication state).
Only identity UUIDs and timestamps are ignored. With --spark also reads each execution's published outputs
versionAsOf the exact publication pointer and compares row multisets (exceptAll both directions).
Exit 0 = parity, 1 = mismatch, 2 = cannot fetch.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal

RULE_FIELDS = ("scope", "stage", "kind", "measurement_status", "metric_name", "schema_evidence", "failed_rows", "error")
REPORT_FIELDS = ("candidate_rows", "valid_rows", "quarantine_rows", "classification_status", "classified_schema_hash",
                 "business_schema_hash", "clean_selector", "quarantine_selector", "errors", "measured_rows")
QUARANTINE_META = ("_dq_execution_id", "_dq_candidate_version", "_dq_quarantined_at")


class FetchError(Exception):
    pass


def api(base: str, token: str, path: str):
    req = urllib.request.Request(base.rstrip("/") + path, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise FetchError(f"GET {path} status={e.code}") from None
    except (urllib.error.URLError, OSError, ValueError):
        raise FetchError(f"GET {path} unreachable") from None


def fetch(base: str, token: str, execution_id: str) -> dict:
    q = urllib.parse.quote(execution_id, safe="")
    doc = api(base, token, f"/api/v2/executions/{q}/report")
    if "report" not in doc:
        raise FetchError(f"execution {execution_id} has no accepted report")
    rules: list[dict] = []
    cursor = None
    while True:
        page = api(base, token, f"/api/v2/executions/{q}/rules?limit=500"
                   + (f"&cursor={urllib.parse.quote(cursor)}" if cursor else ""))
        rules += page["items"]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    ex = api(base, token, f"/api/v2/executions/{q}")
    return {"id": execution_id, "report": doc["report"], "accepted": doc["accepted"], "rules": rules, "execution": ex}


def _value(r: dict):
    v = r.get("metric_value")
    if v is None:
        return None
    return Decimal(str(v))  # 1, "1" and "1.0" are the same measurement


def compare(py: dict, sc: dict) -> list[str]:
    problems: list[str] = []
    rp, rs = {r["rule_id"]: r for r in py["rules"]}, {r["rule_id"]: r for r in sc["rules"]}
    if rp.keys() != rs.keys():
        problems.append(f"rule set differs: only python={sorted(rp.keys() - rs.keys())} only scala={sorted(rs.keys() - rp.keys())}")
    for rid in sorted(rp.keys() & rs.keys()):
        for f in RULE_FIELDS:
            if rp[rid].get(f) != rs[rid].get(f):
                problems.append(f"rule {rid}.{f}: python={rp[rid].get(f)} scala={rs[rid].get(f)}")
        if _value(rp[rid]) != _value(rs[rid]):
            problems.append(f"rule {rid}.metric_value: python={rp[rid].get('metric_value')} scala={rs[rid].get('metric_value')}")
    for f in REPORT_FIELDS:
        if py["report"].get(f) != sc["report"].get(f):
            problems.append(f"report.{f}: python={py['report'].get(f)} scala={sc['report'].get(f)}")
    dp, ds = py["accepted"].get("decision"), sc["accepted"].get("decision")
    if (dp or {}).get("decision") != (ds or {}).get("decision"):
        problems.append(f"decision: python={(dp or {}).get('decision')} scala={(ds or {}).get('decision')}")
    reasons = lambda d: sorted(((x["code"], x["rule_id"]) for x in (d or {}).get("reasons", [])), key=str)
    if reasons(dp) != reasons(ds):
        problems.append("decision reasons differ")
    if py["accepted"].get("assessment") != sc["accepted"].get("assessment"):
        problems.append("assessment differs")
    for f in ("state", "publication_state", "unresolved"):
        if py["execution"].get(f) != sc["execution"].get(f):
            problems.append(f"execution.{f}: python={py['execution'].get(f)} scala={sc['execution'].get(f)}")
    return problems


def check_expected(label: str, doc: dict, a) -> list[str]:
    out = []
    for flag, key in (("expect_candidate", "candidate_rows"), ("expect_quarantined", "quarantine_rows")):
        want = getattr(a, flag)
        if want is not None and doc["report"].get(key) != want:
            out.append(f"{label} {key}={doc['report'].get(key)} expected {want}")
    return out


def compare_outputs(a, token: str, py: dict, sc: dict) -> list[str]:
    from pyspark.sql import SparkSession  # lazy: only --spark mode needs pyspark
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("dq_parity_check").getOrCreate()
    dt = urllib.parse.quote(a.dt)

    def outputs(doc):
        plan = doc["execution"]["plan"]
        ds = urllib.parse.quote(doc["execution"]["dataset_id"], safe="")
        pub = api(a.service_url, token, f"/api/v2/datasets/{ds}/publications?dt={dt}")
        cur = pub["current"]
        if cur is None or cur["execution_id"] != doc["id"]:
            raise FetchError(f"execution {doc['id']} is not the published pointer for dt={a.dt}")
        reg = plan["registration_snapshot"]
        silver = (spark.read.format("delta").option("versionAsOf", cur["silver"]["commit_version"])
                  .load(reg["silver"]["uri"]).where(F.col("dt") == F.lit(a.dt).cast("date")))
        qv = cur["quarantine_commit_version"]
        quarantine = None
        if qv is not None:
            quarantine = (spark.read.format("delta").option("versionAsOf", qv).load(reg["quarantine"]["uri"])
                          .where(F.col("_dq_execution_id") == doc["id"])
                          .withColumn("_dq_errors", F.array_sort("_dq_errors")).drop(*QUARANTINE_META))
        return silver, quarantine

    def same_rows(left, right) -> bool:
        return left.exceptAll(right).isEmpty() and right.exceptAll(left).isEmpty()

    (s_py, q_py), (s_sc, q_sc) = outputs(py), outputs(sc)
    problems = []
    if not same_rows(s_py, s_sc.select(*s_py.columns)):
        problems.append("silver rows differ")
    if (q_py is None) != (q_sc is None):
        problems.append("only one execution has quarantine rows")
    elif q_py is not None and not same_rows(q_py, q_sc.select(*q_py.columns)):
        problems.append("quarantine rows/errors differ")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--python-execution", required=True)
    ap.add_argument("--scala-execution", required=True)
    ap.add_argument("--service-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--spark", action="store_true")
    ap.add_argument("--dt")
    ap.add_argument("--expect-candidate", type=int)
    ap.add_argument("--expect-quarantined", type=int)
    a = ap.parse_args(argv)
    token = os.environ.get("DQ_API_TOKEN", "")
    if not token:
        ap.error("env DQ_API_TOKEN required")
    if a.spark and not a.dt:
        ap.error("--spark needs --dt")
    try:
        py, sc = fetch(a.service_url, token, a.python_execution), fetch(a.service_url, token, a.scala_execution)
        problems = compare(py, sc) + check_expected("python", py, a) + check_expected("scala", sc, a)
        if a.spark:
            problems += compare_outputs(a, token, py, sc)
    except FetchError as e:
        print(f"cannot fetch: {e}", file=sys.stderr)
        return 2
    with open(a.output, "w") as f:
        json.dump({"python_execution": a.python_execution, "scala_execution": a.scala_execution,
                   "rules": len(py["rules"]), "status": "PASS" if not problems else "FAIL", "problems": problems},
                  f, indent=2)
    print("PARITY OK" if not problems else "\n".join(problems) + f"\nPARITY FAILED ({len(problems)})")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
