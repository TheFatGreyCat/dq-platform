"""Sinh dữ liệu Bronze mẫu (Delta, partitionBy dt), cố ý trộn lỗi để thấy DQ gate hoạt động.

Dữ liệu sinh phân tán trên executor từ `spark.range`: mỗi giá trị suy từ hash(id, seed, cột), nên cùng tham số
cho cùng dữ liệu bất kể số partition, và driver không giữ dòng nào (chạy được hàng chục triệu dòng).
"""

import argparse
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

STATUS = ["success", "SUCCESS", " Failed ", "REFUNDED"]  # lẫn hoa/thường, khoảng trắng -> job chuẩn hóa
AMOUNTS = ["10000", "20000", "50000", "100000", "200000", "500000"]
PROVINCES = ["HN", "HCM", "DN", "HP", None]
# completeness, completeness (cast decimal ra null), validity x3, consistency (event_time khác ngày dt)
BAD_KINDS = ["null_sub", "bad_amount", "bad_msisdn", "neg_amount", "bad_status", "wrong_day"]
REPLAYS = 50  # bản ghi đầu tiên bị gửi lại (Kafka replay), ingest_time mới hơn

# Bronze: mọi cột nguồn là string (job ép kiểu); dt là cột partition DateType.
SCHEMA = StructType([StructField(n, StringType(), True) for n in (
    "transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "ingest_time")])


def _pick(values: list, h: Column) -> Column:
    return F.element_at(F.array(*[F.lit(v).cast("string") for v in values]), (h % len(values) + 1).cast("int"))


def generate(spark: SparkSession, dt: str, rows: int, bad_ratio: float, seed: int, partitions: int | None = None) -> DataFrame:
    """`rows` giao dịch của ngày `dt` + min(rows, 50) bản replay; tỉ lệ dòng lỗi xấp xỉ `bad_ratio`."""
    def h(salt: int) -> Column:  # số nguyên không âm, xác định theo (id, seed, salt)
        return F.abs(F.xxhash64(F.col("id"), F.lit(seed), F.lit(salt)))

    def unit(salt: int) -> Column:  # [0, 1)
        return (h(salt) % 1_000_000) / 1_000_000

    base = F.unix_timestamp(F.lit(dt), "yyyy-MM-dd")
    ts = lambda seconds: F.from_unixtime(base + seconds, "yyyy-MM-dd HH:mm:ss")  # noqa: E731
    ingest = ts(86400 + (h(7) % 31) * 60)
    good = {
        "transaction_id": F.format_string("TX%s%07d", F.lit(dt.replace("-", "")), F.col("id")),
        "subscriber_id": F.format_string("SUB%06d", h(1) % 50000 + 1),
        "msisdn": F.concat(_pick(["0", "84"], h(2)), F.lit("9"), (h(3) % 90_000_000 + 10_000_000).cast("string")),
        "amount": _pick(AMOUNTS, h(4)),
        "status": _pick(STATUS, h(5)),
        "province_code": F.when(unit(6) < 0.02, F.lit(None).cast("string")).otherwise(_pick(PROVINCES, h(6))),
        "event_time": ts(h(8) % 86400),
    }
    kind = F.when(unit(9) < bad_ratio, _pick(BAD_KINDS, h(10)))
    bad = {
        "subscriber_id": ("null_sub", F.lit(None).cast("string")),
        "amount": ("bad_amount", F.lit("50k")),
        "msisdn": ("bad_msisdn", F.lit("12345")),
        "status": ("bad_status", F.lit("PENDING???")),
        "event_time": ("wrong_day", ts(-(h(11) % 86400 + 1))),
    }
    cols = {}
    for name in good:
        c = good[name]
        if name in bad:
            k, v = bad[name]
            c = F.when(kind == k, v).otherwise(c)
        if name == "amount":
            c = F.when(kind == "neg_amount", F.lit("-50000")).otherwise(c)
        cols[name] = c
    ids = spark.range(0, rows, numPartitions=partitions or spark.sparkContext.defaultParallelism)
    df = ids.select(*[cols[n].alias(n) for n in cols], ingest.alias("ingest_time"))
    replay = df.limit(0) if rows == 0 else (
        ids.where(F.col("id") < min(rows, REPLAYS))
        .select(*[cols[n].alias(n) for n in cols], ts(86400 + 7200).alias("ingest_time")))
    return df.unionByName(replay).select(*SCHEMA.fieldNames())


def write_day(spark: SparkSession, out: str, dt: str, df: DataFrame) -> None:
    """Ngày đã có -> replaceWhere typed (giữ ngày khác); lần đầu -> tạo Delta root partitionBy dt."""
    day = date.fromisoformat(dt)
    w = df.withColumn("dt", F.lit(day)).write.format("delta")
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
    p.add_argument("--partitions", type=int, default=None, help="số file/partition Spark khi sinh (mặc định: parallelism)")
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("generate_bronze").getOrCreate()
    write_day(spark, a.out, a.dt, generate(spark, a.dt, a.rows, a.bad_ratio, a.seed, a.partitions))
    print(f"wrote {a.rows + min(a.rows, REPLAYS)} rows to {a.out} (dt={a.dt})")


if __name__ == "__main__":
    main()
