"""Sinh dữ liệu Bronze mẫu (Delta, partitionBy dt), cố ý trộn lỗi để thấy DQ gate hoạt động."""

import argparse
import random
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

STATUS = [
    "success",
    "SUCCESS",
    " Failed ",
    "REFUNDED",
]  # lẫn hoa/thường, khoảng trắng -> job chuẩn hóa

# Bronze: mọi cột nguồn là string (job ép kiểu); dt là cột partition DateType.
SCHEMA = StructType([StructField(n, StringType(), True) for n in (
    "transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "ingest_time")])


def generate(dt: str, rows: int, bad_ratio: float, seed: int) -> list[dict]:
    random.seed(seed)
    base = datetime.fromisoformat(dt)
    out = []
    for i in range(rows):
        r = {
            "transaction_id": f"TX{dt.replace('-', '')}{i:07d}",
            "subscriber_id": f"SUB{random.randint(1, 50000):06d}",
            "msisdn": random.choice(["0", "84"]) + f"9{random.randint(10000000, 99999999)}",
            "amount": str(random.choice([10000, 20000, 50000, 100000, 200000, 500000])),
            "status": random.choice(STATUS),
            "province_code": (
                random.choice(["HN", "HCM", "DN", "HP", None]) if random.random() > 0.02 else None
            ),
            "event_time": (base + timedelta(seconds=random.randint(0, 86399))).isoformat(sep=" "),
            "ingest_time": (base + timedelta(days=1, minutes=random.randint(0, 30))).isoformat(sep=" "),
        }
        if random.random() < bad_ratio:  # lỗi cố ý
            kind = random.choice([
                "null_sub",  # completeness
                "bad_amount",  # completeness: cast decimal ra null
                "bad_msisdn",  # validity
                "neg_amount",  # validity
                "bad_status",  # validity
                "wrong_day",  # consistency: event_time không thuộc partition dt
            ])
            if kind == "null_sub":
                r["subscriber_id"] = None
            elif kind == "bad_amount":
                r["amount"] = "50k"
            elif kind == "bad_msisdn":
                r["msisdn"] = "12345"
            elif kind == "neg_amount":
                r["amount"] = "-50000"
            elif kind == "bad_status":
                r["status"] = "PENDING???"
            else:
                r["event_time"] = (base - timedelta(seconds=random.randint(1, 86400))).isoformat(sep=" ")
        out.append(r)
    replay_ingest = (base + timedelta(days=1, hours=2)).isoformat(sep=" ")
    # 50 bản ghi bị gửi lại (Kafka replay), ingest_time mới hơn
    out += [dict(out[i], ingest_time=replay_ingest) for i in range(min(rows, 50))]
    return out


def write_day(spark: SparkSession, out: str, dt: str, rows: list[dict]) -> None:
    """Ngày đã có -> replaceWhere typed (giữ ngày khác); lần đầu -> tạo Delta root partitionBy dt."""
    day = date.fromisoformat(dt)
    df = spark.createDataFrame([tuple(r[f.name] for f in SCHEMA.fields) for r in rows], SCHEMA) \
        .withColumn("dt", F.lit(day)).coalesce(1)
    w = df.write.format("delta")
    log = spark._jvm.org.apache.hadoop.fs.Path(out, "_delta_log")
    if not log.getFileSystem(spark._jsc.hadoopConfiguration()).exists(log):
        w.partitionBy("dt").save(out)
    else:
        w.mode("overwrite").option("replaceWhere", f"dt = DATE '{dt}'").save(out)


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--dt", required=True)
    p.add_argument("--rows", type=int, default=10000)
    p.add_argument("--bad-ratio", type=float, default=0.03)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    rows = generate(a.dt, a.rows, a.bad_ratio, a.seed)
    spark = SparkSession.builder.appName("generate_bronze").getOrCreate()
    write_day(spark, a.out, a.dt, rows)
    print(f"wrote {len(rows)} rows to {a.out} (dt={a.dt})")


if __name__ == "__main__":
    main()
