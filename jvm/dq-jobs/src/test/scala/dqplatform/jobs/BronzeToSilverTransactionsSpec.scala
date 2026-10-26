package dqplatform.jobs

import dqplatform.client.DeltaRuntime
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import scala.collection.JavaConverters._

class BronzeToSilverTransactionsSpec extends AnyFunSuite with Matchers with JobsFixture {

  private def rawDf: DataFrame = {
    import spark.implicits._
    Seq[(String, String, String, String, String, String, String, String)](
      ("TX1", "SUB1", "0912345678", "10000", " success ", "HN", "2026-10-17 23:30:00", "2026-10-18 00:05:00"),
      ("TX1", "SUB1", "0912345678", "20000", "FAILED", "HN", "2026-10-17 23:30:00", "2026-10-18 02:00:00"),
      ("TX2", "SUB2", "84912345671", "10000", "SUCCESS", "HCM", "2026-10-17 10:00:00", "2026-10-18 00:01:00"),
      (null, "SUB4", "84912345672", "10000", "SUCCESS", "HN", "2026-10-17 12:00:00", "2026-10-18 00:03:00"),
      (null, "SUB5", "84912345673", "10000", "SUCCESS", "HN", "2026-10-17 13:00:00", "2026-10-18 00:04:00")
    ).toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "ingest_time")
  }

  test("transform: normalises, keeps the newest replay, keeps null keys, dt is DateType, amount decimal(18,2)") {
    val out = TransactionsTransform.transform(rawDf, "2026-10-17")
    out.count() shouldBe 4
    val tx1 = out.where("transaction_id = 'TX1'").head()
    tx1.getAs[String]("msisdn") shouldBe "84912345678"
    tx1.getAs[String]("status") shouldBe "FAILED"
    out.where("transaction_id IS NULL").count() shouldBe 2
    out.schema("dt").dataType.typeName shouldBe "date"
    out.schema("amount").dataType.simpleString shouldBe "decimal(18,2)"
    out.columns should not contain "ingest_time"
    TransactionsTransform.sourceRowCount(rawDf) shouldBe 4
  }

  test("args: only --dt and --lake are accepted") {
    BronzeToSilverTransactions.Args.parse(Seq("--dt", "2026-10-17", "--lake", "/x")).dt shouldBe LocalDate.parse("2026-10-17")
    an[IllegalArgumentException] should be thrownBy
      BronzeToSilverTransactions.Args.parse(Seq("--dt", "2026-10-17", "--target-layer", "silver"))
  }

  private final class Run(decision: String = "ALLOW", bad: Int = 3, foreign: Boolean = false) {
    val dir = newDir()
    val lake = s"$dir/lake"
    bronze(s"$lake/bronze/transactions", "2026-10-17", 100, bad, if (foreign) Some("2026-10-16") else None)
    val reg = BootstrapTables.bootstrap(spark, s"$dir/tables")
    val silver = reg.get("silver").get("uri").asText()
    val quarantine = reg.get("quarantine").get("uri").asText()
    val svc = new Service(dir, silver, quarantine, decision)
    def exit: Int = BronzeToSilverTransactions.run(spark,
      BronzeToSilverTransactions.Args(LocalDate.parse("2026-10-17"), lake), client(svc, dir))
  }

  test("100 rows / 3 violations: exit 0, Silver 97 rows, quarantine 3, source count from the pinned Bronze day") {
    val r = new Run(foreign = true)
    r.exit shouldBe BronzeToSilverTransactions.ExitPublished
    ids(r.silver) should have size 97
    ids(r.silver) intersect ids(r.quarantine) shouldBe empty
    ids(r.quarantine) shouldBe Set("TX2026-10-171", "TX2026-10-172", "TX2026-10-173")
    val body = r.svc.executions.head
    body.get("parameters").get("source_row_count").asText() shouldBe "100"
    body.get("parameters").get("partition_end").asText() shouldBe "2026-10-18 00:00:00"
    val rep = r.svc.reports.head
    rep.get("candidate_rows").asLong() shouldBe 100L
    rep.get("valid_rows").asLong() shouldBe 97L
    spark.read.format("delta").load(r.silver).schema("dt").dataType.typeName shouldBe "date"
  }

  test("0 violations: exit 0, quarantine NOT_APPLICABLE and untouched") {
    val r = new Run(bad = 0)
    val qv = DeltaRuntime.snapshot(spark, r.quarantine).version
    r.exit shouldBe BronzeToSilverTransactions.ExitPublished
    ids(r.silver) should have size 100
    DeltaRuntime.snapshot(spark, r.quarantine).version shouldBe qv
  }

  test("DENY: exit 3 and neither target is written") {
    val r = new Run(decision = "DENY")
    val (sv, qv) = (DeltaRuntime.snapshot(spark, r.silver).version, DeltaRuntime.snapshot(spark, r.quarantine).version)
    r.exit shouldBe BronzeToSilverTransactions.ExitDenied
    DeltaRuntime.snapshot(spark, r.silver).version shouldBe sv
    DeltaRuntime.snapshot(spark, r.quarantine).version shouldBe qv
  }

  test("technical failure (missing Bronze) exits 2 without any execution") {
    val dir = newDir()
    val reg = BootstrapTables.bootstrap(spark, s"$dir/tables")
    val svc = new Service(dir, reg.get("silver").get("uri").asText(), reg.get("quarantine").get("uri").asText())
    BronzeToSilverTransactions.run(spark, BronzeToSilverTransactions.Args(LocalDate.parse("2026-10-17"), s"$dir/none"),
      client(svc, dir)) shouldBe BronzeToSilverTransactions.ExitError
    svc.executions shouldBe empty
  }
}
