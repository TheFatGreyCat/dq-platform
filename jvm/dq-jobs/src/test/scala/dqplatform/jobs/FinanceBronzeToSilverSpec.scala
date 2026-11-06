package dqplatform.jobs

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class FinanceBronzeToSilverSpec extends AnyFunSuite with Matchers with JobsFixture {

  private val Dt = "2026-11-01"

  private def txns: DataFrame = {
    import spark.implicits._
    Seq[(String, String, String, String, String, String, String, String, String, String, String, String)](
      // tx1 twice: the newer ingest_time (EUR 20) must win
      ("tx1", " c1 ", "a1", "10.00", "usd", "purchase", " success ", "2026-11-01 10:00:00", "2026-11-02 00:00:00", "retail", "web", "us"),
      ("TX1", "C1", "A1", "20.00", "eur", "PURCHASE", "SUCCESS", "2026-11-01 10:00:00", "2026-11-02 01:00:00", "retail", "web", "us"),
      ("TX2", "C1", "A1", "30.00", "USD", "PURCHASE", "SUCCESS", "2026-11-01 15:00:00", "2026-11-02 00:00:00", "retail", "web", "us"),
      ("TX3", "C2", "A2", "5.00", "USD", "PURCHASE", "SUCCESS", "2026-11-01 09:00:00", "2026-11-02 00:00:00", "retail", "web", "us"),
      ("TX4", "C9", "A9", "7.00", "USD", "PURCHASE", "SUCCESS", "2026-11-01 09:00:00", "2026-11-02 00:00:00", "retail", "web", "us"),
      // malformed rows must survive for the gate to see them
      ("TX5", "C1", "A1", "12x", "usd", "PURCHASE", "SUCCESS", "not-a-time", "2026-11-02 00:00:00", "retail", "web", "us"),
      (null, "C3", "A3", "1.00", "USD", "PURCHASE", "SUCCESS", "2026-11-01 09:00:00", "2026-11-02 00:00:00", "retail", "web", "us"),
      (null, "C3", "A3", "1.00", "USD", "PURCHASE", "SUCCESS", "2026-11-01 09:00:00", "2026-11-02 00:00:00", "retail", "web", "us")
    ).toDF("transaction_id", "customer_id", "account_id", "amount", "currency", "transaction_type", "status",
      "event_time", "ingest_time", "merchant_category", "channel", "country")
  }

  private def customers: DataFrame = {
    import spark.implicits._
    Seq(
      ("C1", " Ann Lee ", "retail", "low", "us", "verified", "2026-01-01 00:00:00", "2026-01-02 00:00:00"),
      ("c1", "Ann Old", "retail", "low", "us", "verified", "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
      ("C2", "Bob Ray", "vip", "high", "vn", "pending", "2026-01-01 00:00:00", "2026-01-02 00:00:00")
    ).toDF("customer_id", "full_name", "segment", "risk_level", "country", "kyc_status", "created_at", "ingest_time")
  }

  private def out = FinanceBronzeToSilver.transform(txns, customers, Dt)
  private def row(id: String) = out.where(s"transaction_id = '$id'").head()

  test("normalises, trims, uppercases and casts") {
    val r = row("TX2")
    r.getAs[String]("currency") shouldBe "USD"
    r.getAs[String]("status") shouldBe "SUCCESS"
    r.getAs[String]("country") shouldBe "US"
    r.getAs[String]("customer_id") shouldBe "C1"
    out.schema("amount").dataType.simpleString shouldBe "decimal(18,2)"
    out.schema("event_time").dataType.typeName shouldBe "timestamp"
    out.schema("dt").dataType.typeName shouldBe "date"
    out.columns should not contain "ingest_time"
  }

  test("deduplicates by latest ingest_time and keeps null transaction ids") {
    out.where("transaction_id = 'TX1'").count() shouldBe 1
    row("TX1").getAs[java.math.BigDecimal]("amount").toPlainString shouldBe "20.00"
    row("TX1").getAs[String]("currency") shouldBe "EUR"
    out.where("transaction_id IS NULL").count() shouldBe 2
    out.count() shouldBe 7
  }

  test("left join: matched, unmatched kept with null customer fields, latest customer version used") {
    row("TX2").getAs[String]("customer_name") shouldBe "ANN LEE"
    row("TX3").getAs[String]("customer_segment") shouldBe "VIP"
    row("TX4").getAs[String]("customer_name") shouldBe null
    out.count() shouldBe 7
  }

  test("malformed amount and timestamp are retained as nulls") {
    val r = row("TX5")
    r.getAs[java.math.BigDecimal]("amount") shouldBe null
    r.getAs[java.sql.Timestamp]("event_time") shouldBe null
    r.getAs[java.math.BigDecimal]("amount_usd") shouldBe null
    r.getAs[String]("transaction_month") shouldBe null
    r.getAs[java.lang.Long]("customer_day_txn_count") shouldBe null
  }

  test("derives amount_usd, transaction_month and customer-day aggregates") {
    val r = row("TX2")
    r.getAs[java.math.BigDecimal]("amount_usd").toPlainString shouldBe "30.00"
    row("TX1").getAs[java.math.BigDecimal]("amount_usd").toPlainString shouldBe "21.60"
    r.getAs[String]("transaction_month") shouldBe "2026-11"
    // C1 on 2026-11-01: TX1 (21.60) + TX2 (30.00)
    r.getAs[Long]("customer_day_txn_count") shouldBe 2L
    r.getAs[java.math.BigDecimal]("customer_day_total").toPlainString shouldBe "51.60"
    r.getAs[java.math.BigDecimal]("customer_day_avg").toPlainString shouldBe "25.80"
    row("TX3").getAs[Long]("customer_day_txn_count") shouldBe 1L
    out.where("transaction_id IS NULL").collect().foreach(_.getAs[java.lang.Long]("customer_day_txn_count") should not be null)
  }

  test("exit constants and argument parsing") {
    FinanceBronzeToSilver.ExitPublished shouldBe 0
    FinanceBronzeToSilver.ExitError shouldBe 2
    FinanceBronzeToSilver.ExitDenied shouldBe 3
    val a = FinanceBronzeToSilver.Args.parse(Seq("--dt", Dt))
    a.dt shouldBe LocalDate.parse(Dt)
    a.transactions shouldBe "s3a://dq-lake/bronze/finance/transactions"
    a.customers shouldBe "s3a://dq-lake/bronze/finance/customers"
    FinanceBronzeToSilver.Args.parse(Seq("--dt", Dt, "--transactions", "/t", "--customers", "/c")).customers shouldBe "/c"
    an[IllegalArgumentException] should be thrownBy FinanceBronzeToSilver.Args.parse(Seq("--dt", Dt, "--lake", "/x"))
    an[IllegalArgumentException] should be thrownBy FinanceBronzeToSilver.Args.parse(Seq("--transactions", "/t"))
  }

  test("technical failure (missing Bronze) exits 2 without any execution") {
    val dir = newDir()
    val reg = BootstrapTables.bootstrap(spark, s"$dir/tables")
    val svc = new Service(dir, reg.get("silver").get("uri").asText(), reg.get("quarantine").get("uri").asText())
    FinanceBronzeToSilver.run(spark,
      FinanceBronzeToSilver.Args(LocalDate.parse(Dt), s"$dir/none-t", s"$dir/none-c"), client(svc, dir)) shouldBe
      FinanceBronzeToSilver.ExitError
    svc.executions shouldBe empty
  }
}
