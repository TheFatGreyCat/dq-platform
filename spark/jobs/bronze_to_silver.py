"""
Bronze -> Silver cho transactions qua DQ gate (API v2, Delta):
  pin Bronze version -> transform -> start_write -> evaluate -> submit -> publish (JVM ghi quarantine rồi Silver).
Python không ghi Delta business output và không tự quyết định gate.
Exit: 0 = Silver+quarantine đã commit (hoặc quarantine không áp dụng); 3 = DENY; 2 = lỗi kỹ thuật/dịch vụ/cấu hình/chưa giải quyết.
Dòng cuối stdout là JSON {execution_id, decision, publication}.
"""
import argparse
import json
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from dq_client import DQClient, DQFailed

EXIT_OK, EXIT_DENY, EXIT_ERROR = 0, 3, 2

# Runtime profile dq-runtime-v1: SDK/server so khớp lại; kiểm sớm để không đếm nguồn trên session sai.
RUNTIME_PROFILE = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.ansi.enabled": "false",
    "spark.sql.caseSensitive": "false",
    "spark.sql.legacy.timeParserPolicy": "EXCEPTION",
    "spark.sql.sources.partitionOverwriteMode": "static",
}


def bronze_root(lake: str) -> str:
    return f"{lake}/bronze/transactions"


def pinned_bronze(spark: SparkSession, root: str, dt: str):
    """Bronze Delta root ở version hiện tại, cố định cho cả transform lẫn source count; lọc đúng partition dt."""
    version = spark.sql(f"DESCRIBE HISTORY delta.`{root}` LIMIT 1").first()["version"]
    raw = spark.read.format("delta").option("versionAsOf", version).load(root)
    return raw.where(F.col("dt") == F.lit(date.fromisoformat(dt))), version


def transform(raw, dt: str):
    """Giống dqplatform.jobs.TransactionsTransform: dedup ingest_time mới nhất, giữ mọi dòng thiếu transaction_id."""
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_time").desc())
    return (raw
            .withColumn("msisdn", F.regexp_replace(F.trim("msisdn"), r"^0", "84"))
            .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1 OR transaction_id IS NULL")
            .drop("_rn", "ingest_time")
            .withColumn("dt", F.lit(date.fromisoformat(dt))))


def source_row_count(raw) -> int:
    """Số giao dịch nguồn, độc lập với transform (đối soát completeness)."""
    r = raw.agg(F.countDistinct("transaction_id").alias("keys"),
                F.count(F.when(F.col("transaction_id").isNull(), 1)).alias("no_key")).first()
    return int(r["keys"]) + int(r["no_key"])


def suite_parameters(dt: str, src_rows: int) -> dict:
    d = date.fromisoformat(dt)
    return {
        "partition_start": f"{d} 00:00:00",
        "partition_end": f"{d + timedelta(days=1)} 00:00:00",
        "source_row_count": str(src_rows),
    }


def _emit(execution_id, decision, publication) -> None:
    print(json.dumps({"execution_id": execution_id, "decision": decision, "publication": publication},
                     sort_keys=True))


def run(spark: SparkSession, dt: str, lake: str) -> int:
    for k, v in RUNTIME_PROFILE.items():
        if spark.conf.get(k, None) != v:
            print("DQ ERROR: RUNTIME_MISMATCH", file=sys.stderr)
            _emit(None, None, None)
            return EXIT_ERROR

    raw, _ = pinned_bronze(spark, bronze_root(lake), dt)
    params = suite_parameters(dt, source_row_count(raw))
    df = transform(raw, dt)

    dq = DQClient(spark)
    execution_id = None
    try:
        handle = dq.start_write({"dt": dt}, params, f"{dt}T00:00:00Z")
        execution_id = handle["execution_id"]
        evaluation = dq.evaluate(handle, df)
        accepted = dq.submit(evaluation)
        decision = (accepted.get("decision") or {}).get("decision")
        if decision == "DENY":
            print(f"DQ DENY: execution {execution_id}", file=sys.stderr)
            _emit(execution_id, "DENY", None)
            return EXIT_DENY
        if decision != "ALLOW":
            print(f"DQ ERROR: no decision for execution {execution_id}", file=sys.stderr)
            _emit(execution_id, decision, None)
            return EXIT_ERROR
        pub = dq.publish(evaluation, accepted)
    except DQFailed as e:
        print(f"DQ ERROR: {e.code}", file=sys.stderr)
        _emit(execution_id, None, None)
        return EXIT_ERROR

    ok = pub["silver"]["state"] == "SUCCEEDED" and pub["quarantine"]["state"] in ("SUCCEEDED", "NOT_APPLICABLE")
    _emit(execution_id, "ALLOW", pub)
    return EXIT_OK if ok else EXIT_ERROR


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True)
    p.add_argument("--lake", default="/data/lake")
    a = p.parse_args(argv)
    date.fromisoformat(a.dt)
    spark = SparkSession.builder.appName("bronze_to_silver_transactions").getOrCreate()
    return run(spark, a.dt, a.lake)


if __name__ == "__main__":
    sys.exit(main())
