package dqplatform.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.collection.JavaConverters._

class FinanceBronzeGeneratorSpec extends AnyFunSuite with Matchers with JobsFixture {

  private val Bad = "2026-12-01"

  private def cfg(dir: String, days: Long = 2, rows: Long = 200, target: Long = 1, seed: Long = 7) =
    FinanceBronzeGenerator.Config(s"$dir/transactions", s"$dir/customers", days, rows, target, LocalDate.parse(Bad), seed)

  private def delta(uri: String): DataFrame = spark.read.format("delta").load(uri)

  test("parses explicit args and rejects missing or unknown ones") {
    val argv = Array("--transactions", "s3a://b/t", "--customers", "s3a://b/c", "--days", "30", "--rows-per-day", "1000",
      "--target-bytes", "10737418240", "--bad-date", Bad, "--seed", "42")
    FinanceBronzeGenerator.parse(argv) shouldBe FinanceBronzeGenerator.Config("s3a://b/t", "s3a://b/c", 30, 1000,
      10737418240L, LocalDate.parse(Bad), 42L)
    an[IllegalArgumentException] should be thrownBy FinanceBronzeGenerator.parse(argv.dropRight(2))
    an[IllegalArgumentException] should be thrownBy FinanceBronzeGenerator.parse(argv ++ Array("--x", "1"))
  }

  test("generation is deterministic per seed and has the raw string shape") {
    val c = cfg(newDir())
    val a = FinanceBronzeGenerator.transactions(spark, c, 0, 2)
    a.collect().toSeq shouldBe FinanceBronzeGenerator.transactions(spark, c, 0, 2).collect().toSeq
    a.count() shouldBe 400L
    a.schema.fields.map(_.name).toSeq shouldBe Seq("transaction_id", "customer_id", "account_id", "amount", "currency",
      "transaction_type", "status", "event_time", "ingest_time", "merchant_category", "channel", "country", "dt")
    a.schema.fields.init.map(_.dataType).toSet shouldBe Set(StringType)
    a.schema("dt").dataType shouldBe DateType
    FinanceBronzeGenerator.transactions(spark, c.copy(seed = 8), 0, 2).collect().toSeq should not be a.collect().toSeq
    FinanceBronzeGenerator.customers(spark, c).collect().toSeq shouldBe FinanceBronzeGenerator.customers(spark, c).collect().toSeq
  }

  test("run partitions by dt, skips bad-date in valid days, isolates defects, and measures real bytes") {
    val dir = newDir()
    val c = cfg(dir, days = 2, rows = 500, target = 100000)
    val ev = FinanceBronzeGenerator.run(spark, c)
    val tx = delta(c.transactions)
    val dirs = Files.list(Paths.get(c.transactions)).iterator().asScala.map(_.getFileName.toString).toSet
    dirs should contain("dt=2026-11-01")
    dirs should contain("dt=2026-11-02")
    dirs should contain(s"dt=$Bad")
    dirs should contain("_delta_log")

    val measured = Files.walk(Paths.get(c.transactions)).iterator().asScala
      .filter(p => Files.isRegularFile(p) && !p.getFileName.toString.endsWith(".crc")).map(Files.size(_)).sum
    measured should be >= 100000L
    ev.get("transactions_bytes").asLong() shouldBe measured

    val good = tx.filter(col("dt") =!= java.sql.Date.valueOf(Bad))
    val bad = tx.filter(col("dt") === java.sql.Date.valueOf(Bad))
    bad.count() shouldBe ev.get("bad_rows").asLong()
    good.count() shouldBe ev.get("valid_rows").asLong()

    val customers = delta(c.customers)
    customers.select("customer_id").distinct().count() shouldBe customers.count()
    good.filter(col("transaction_id").isNull || col("amount").isNull || col("customer_id").isNull).count() shouldBe 0L
    good.filter(!col("amount").rlike("^[0-9]+\\.[0-9]{2}$")).count() shouldBe 0L
    good.filter(!col("status").isin("POSTED", "PENDING", "SETTLED", "REVERSED")).count() shouldBe 0L
    good.filter(!col("currency").isin("USD", "EUR", "VND", "GBP", "JPY")).count() shouldBe 0L
    good.filter(col("event_time").substr(1, 10) =!= col("dt").cast("string")).count() shouldBe 0L
    good.select("transaction_id").distinct().count() shouldBe good.count()
    good.join(customers, Seq("customer_id"), "left_anti").count() shouldBe 0L

    bad.filter(col("transaction_id").isNull).count() should be > 0L
    bad.filter(col("amount").isNull).count() should be > 0L
    bad.filter(col("amount").startsWith("-")).count() should be > 0L
    bad.filter(col("status") === "UNKNOWN").count() should be > 0L
    bad.filter(col("currency") === "XXX").count() should be > 0L
    bad.filter(col("event_time") === "not-a-time").count() should be > 0L
    bad.filter(col("event_time").substr(1, 10) =!= col("dt").cast("string") && col("event_time") =!= "not-a-time").count() should be > 0L
    bad.join(customers, Seq("customer_id"), "left_anti").count() should be > 0L
    bad.filter(col("transaction_id").isNotNull).groupBy("transaction_id").count().filter(col("count") > 1).count() should be > 0L
  }

  test("extends valid days past the first batch when below target and never reuses bad-date") {
    val dir = newDir()
    val c = cfg(dir, days = 1, rows = 50, target = 1, seed = 3).copy(badDate = LocalDate.parse("2026-11-02"))
    val tx = FinanceBronzeGenerator.transactions(spark, c, 0, 3)
    tx.select("dt").distinct().collect().map(_.getDate(0).toString).toSet shouldBe Set("2026-11-01", "2026-11-03", "2026-11-04")
  }
}
