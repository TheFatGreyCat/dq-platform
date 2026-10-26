"""Hành vi dữ liệu của job Python: Bronze Delta (generate) -> transform. Chạy trong image Spark, xem docstring cuối file.

  docker run --rm -v $PWD:/w -v /tmp/dqbuild/m2/repository/io/delta:/delta:ro -w /w/spark apache/spark:3.5.1-scala2.12-java17-python3-ubuntu \
    /opt/spark/bin/spark-submit --jars /delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar,/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    --py-files jobs/bronze_to_silver.py,jobs/generate_bronze.py,jobs/dq_client.py tests/test_jobs.py
"""
import datetime as dt_
import os
import sys
import tempfile
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

import bronze_to_silver as b2s  # noqa: E402  (import không được chạy job)
import generate_bronze as gen  # noqa: E402

DT = "2026-10-20"
OTHER = "2026-10-19"


def row(tid, ingest, **kw):
    r = {"transaction_id": tid, "subscriber_id": "SUB000001", "msisdn": "0912345678", "amount": "10000",
         "status": " success ", "province_code": "HN", "event_time": f"{DT} 12:00:00", "ingest_time": ingest}
    r.update(kw)
    return r


def frame(spark, rows):
    return spark.createDataFrame([tuple(r[f.name] for f in gen.SCHEMA.fields) for r in rows], gen.SCHEMA)


class JobsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[1]").appName("test_jobs")
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.caseSensitive", "false")
                     .config("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.root = f"{cls.tmp.name}/bronze/transactions"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp.cleanup()

    def test_1_bronze_days_isolated_and_transform(self):
        s = self.spark
        gen.write_day(s, self.root, OTHER, frame(s, [row("A1", "2026-10-20 01:00:00", event_time=f"{OTHER} 09:00:00")]))
        rows = [
            row("T1", "2026-10-21 00:10:00", amount="10000"),
            row("T1", "2026-10-21 02:00:00", amount="20000"),  # replay mới hơn: thắng
            row(None, "2026-10-21 00:10:00"),
            row(None, "2026-10-21 00:10:00"),                  # null key: giữ cả hai
            row("T2", "2026-10-21 00:10:00", amount="50k"),    # cast hỏng -> null, vẫn giữ dòng
        ]
        gen.write_day(s, self.root, DT, frame(s, rows))
        gen.write_day(s, self.root, DT, frame(s, rows))  # replaceWhere: chạy lại không nhân bản, không đụng ngày khác

        self.assertEqual(s.read.format("delta").load(self.root).where(f"dt = DATE '{OTHER}'").count(), 1)
        raw, version = b2s.pinned_bronze(s, self.root, DT)
        self.assertEqual(raw.count(), len(rows))
        self.assertEqual(b2s.source_row_count(raw), 2 + 2 + 0 + 0 - 0 + 0)  # T1,T2 + 2 null-key

        out = b2s.transform(raw, DT)
        got = out.collect()
        self.assertEqual(len(got), 5 - 1)
        t1 = [r for r in got if r.transaction_id == "T1"]
        self.assertEqual([r.amount for r in t1], [Decimal("20000.00")])
        self.assertEqual(sum(r.transaction_id is None for r in got), 2)
        self.assertIsNone([r for r in got if r.transaction_id == "T2"][0].amount)

        types = {f.name: f.dataType.simpleString() for f in out.schema.fields}
        self.assertEqual(types["dt"], "date")
        self.assertEqual(types["amount"], "decimal(18,2)")
        self.assertEqual(types["event_time"], "timestamp")
        self.assertNotIn("ingest_time", types)
        self.assertEqual({r.dt for r in got}, {dt_.date(2026, 10, 20)})
        self.assertEqual(t1[0].msisdn, "84912345678")
        self.assertEqual(t1[0].status, "SUCCESS")

    def test_2_generated_bronze_replay_and_pin(self):
        s = self.spark
        root = f"{self.tmp.name}/gen"
        rows = gen.generate(s, DT, 10, 0.0, 7, partitions=3)
        self.assertEqual(rows.count(), 10 + 10)  # replay = range(min(rows,50))
        gen.write_day(s, root, DT, rows)
        raw, v0 = b2s.pinned_bronze(s, root, DT)
        gen.write_day(s, root, OTHER, gen.generate(s, OTHER, 3, 0.0, 7))  # version mới sau khi pin
        self.assertEqual(raw.count(), 20)                               # pin không đổi
        self.assertEqual(b2s.transform(raw, DT).count(), 10)            # dedup replay
        self.assertEqual(b2s.source_row_count(raw), 10)

    def test_2b_generator_deterministic_and_bad_ratio(self):
        s = self.spark
        a = gen.generate(s, DT, 20000, 0.1, 42, partitions=1).collect()
        b = gen.generate(s, DT, 20000, 0.1, 42, partitions=4).collect()
        self.assertEqual(sorted(map(tuple, a)), sorted(map(tuple, b)))  # không phụ thuộc số partition
        df = gen.generate(s, DT, 20000, 0.1, 42)
        bad = df.where("subscriber_id IS NULL OR amount IN ('50k', '-50000') OR msisdn = '12345' "
                       f"OR status = 'PENDING???' OR event_time < '{DT} 00:00:00'").count()
        self.assertTrue(0.08 < bad / 20050 < 0.12, bad)
        self.assertEqual(df.where("ingest_time LIKE '2026-10-21 02:00:00'").count(), 50)  # 50 replay rows

    def test_3_suite_parameters(self):
        self.assertEqual(b2s.suite_parameters(DT, 7), {
            "partition_start": "2026-10-20 00:00:00", "partition_end": "2026-10-21 00:00:00", "source_row_count": "7"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
