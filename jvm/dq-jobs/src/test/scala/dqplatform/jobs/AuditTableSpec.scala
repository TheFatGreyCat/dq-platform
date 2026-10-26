package dqplatform.jobs

import dqplatform.client.DeltaRuntime
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class AuditTableSpec extends AnyFunSuite with Matchers with JobsFixture {

  private def silverWith(dir: String, rows: Int): (Service, String) = {
    val reg = BootstrapTables.bootstrap(spark, s"$dir/tables")
    val silver = reg.get("silver").get("uri").asText()
    import spark.implicits._
    (1 to rows).map(i => (s"T$i", "S", "849", BigDecimal(1), "OK", "HN", java.sql.Timestamp.valueOf("2026-10-17 12:00:00"),
      java.sql.Date.valueOf("2026-10-17")))
      .toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "dt").withColumn("amount", org.apache.spark.sql.functions.col("amount").cast("decimal(18,2)"))
      .write.format("delta").mode("append").save(silver)
    (new Service(dir, silver, reg.get("quarantine").get("uri").asText(), "VIOLATION"), silver)
  }

  test("audits the pinned version: a later foreign append does not change measured rows; no table is written") {
    val dir = newDir()
    val (svc, silver) = silverWith(dir, 5)
    val pinned = DeltaRuntime.snapshot(spark, silver)
    import spark.implicits._
    Seq(("Z", "S", "849", BigDecimal(1), "OK", "HN", java.sql.Timestamp.valueOf("2026-10-17 12:00:00"),
      java.sql.Date.valueOf("2026-10-17"))).toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status",
      "province_code", "event_time", "dt").withColumn("amount", org.apache.spark.sql.functions.col("amount").cast("decimal(18,2)")).write.format("delta").mode("append").save(silver)
    val after = DeltaRuntime.snapshot(spark, silver).version
    AuditTable.run(spark, client(svc, dir), pinned, Map.empty, Instant.parse("2026-10-22T00:00:00Z")) shouldBe 0
    val rep = svc.reports.head
    rep.get("measured_rows").asLong() shouldBe 5L
    rep.get("classification_status").asText() shouldBe "NOT_APPLICABLE"
    rep.get("audit_reference").get("version").asLong() shouldBe pinned.version
    rep.has("raw_reference") shouldBe false
    DeltaRuntime.snapshot(spark, silver).version shouldBe after
    svc.executions.head.get("workflow").asText() shouldBe "AUDIT_TABLE"
  }

  test("technical ERROR assessment exits 2") {
    val dir = newDir()
    val (svc0, silver) = silverWith(dir, 1)
    val svc = new Service(dir, silver, svc0.toString, "ERROR")
    AuditTable.run(spark, client(svc, dir), DeltaRuntime.snapshot(spark, silver), Map.empty, Instant.now()) shouldBe 2
  }
}
