package dqplatform.client

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.analyzers.{AnalyzerOptions, FilteredRowOutcome}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Contract test: ghim lại hành vi của Deequ mà dq-client dựa vào
 * (docs/DEEQU_MIGRATION_PLAN.md mục 6). Nâng version Deequ làm đổi hành vi -> spec này đỏ.
 */
class DeequSemanticsSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private def run(df: DataFrame, checks: Check*): VerificationResult =
    VerificationSuite().onData(df).addChecks(checks).run()

  private def constraintOf(r: VerificationResult, c: Check): ConstraintResult =
    r.checkResults(c).constraintResults.head

  private def metricOf(r: VerificationResult, c: Check): Double =
    constraintOf(r, c).metric.flatMap(_.value.toOption).collect { case d: Double => d }.get

  /** id -> giá trị cột row-level của check (None = null). */
  private def rowLevelOf(r: VerificationResult, df: DataFrame, c: Check): Map[String, Option[Boolean]] =
    VerificationResult.rowLevelResultsAsDataFrame(spark, r, df)
      .select(col("id"), col(c.description))
      .collect()
      .map(row => row.getString(0) -> Option(row.get(1)).map(_.asInstanceOf[Boolean]))
      .toMap

  private lazy val msisdn: DataFrame = {
    import spark.implicits._
    Seq(("a", "84912345678"), ("b", null), ("c", "12345")).toDF("id", "msisdn")
  }

  private val Msisdn = "^84[0-9]{9}$".r

  test("hasPattern mặc định: null bị tính là KHÔNG khớp (khác GX)") {
    val chk = Check(CheckLevel.Error, "p").hasPattern("msisdn", Msisdn)
    val r = run(msisdn, chk)
    metricOf(r, chk) shouldBe (1.0 / 3 +- 1e-9)
    rowLevelOf(r, msisdn, chk)("b") should not be Some(true)
  }

  test("hasPattern + where(IS NOT NULL) + FilteredRowOutcome.TRUE: mẫu số bỏ null, dòng null đạt ở row-level") {
    val chk = Check(CheckLevel.Error, "p")
      .hasPattern("msisdn", Msisdn,
        analyzerOptions = Some(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
      .where("msisdn IS NOT NULL")
    val r = run(msisdn, chk)
    metricOf(r, chk) shouldBe 0.5
    rowLevelOf(r, msisdn, chk) shouldBe Map("a" -> Some(true), "b" -> Some(true), "c" -> Some(false))
  }

  test("isContainedIn khoảng số: null đạt, includeUpperBound=false loại biên trên") {
    import spark.implicits._
    val df = Seq(("a", Some(0.0)), ("b", Some(100.0)), ("c", None), ("d", Some(-1.0)))
      .toDF("id", "amount").withColumn("amount", col("amount").cast("decimal(18,2)"))
    val chk = Check(CheckLevel.Error, "r")
      .isContainedIn("amount", 0.0, 100.0, includeLowerBound = true, includeUpperBound = false)
    val r = run(df, chk)
    metricOf(r, chk) shouldBe 0.5
    rowLevelOf(r, df, chk) shouldBe Map("a" -> Some(true), "b" -> Some(false), "c" -> Some(true), "d" -> Some(false))
  }

  test("isContainedIn tập giá trị: null đạt") {
    import spark.implicits._
    val df = Seq(("a", "SUCCESS"), ("b", null), ("c", "PENDING")).toDF("id", "status")
    val chk = Check(CheckLevel.Error, "s").isContainedIn("status", Array("SUCCESS", "FAILED"))
    val r = run(df, chk)
    rowLevelOf(r, df, chk) shouldBe Map("a" -> Some(true), "b" -> Some(true), "c" -> Some(false))
  }

  test("satisfies: predicate ra null không đạt -> rule phải viết null-safe") {
    import spark.implicits._
    val df = Seq(("a", Some(1)), ("b", None), ("c", Some(-1))).toDF("id", "x")
    val unsafe = Check(CheckLevel.Error, "unsafe").satisfies("x > 0", "unsafe")
    val safe = Check(CheckLevel.Error, "safe").satisfies("x IS NULL OR x > 0", "safe")
    val r = run(df, unsafe, safe)
    metricOf(r, unsafe) shouldBe (1.0 / 3 +- 1e-9)
    rowLevelOf(r, df, unsafe)("b") should not be Some(true)
    metricOf(r, safe) shouldBe (2.0 / 3 +- 1e-9)
    rowLevelOf(r, df, safe) shouldBe Map("a" -> Some(true), "b" -> Some(true), "c" -> Some(false))
  }

  test("satisfies với TIMESTAMP literal (rule consistency theo partition)") {
    import spark.implicits._
    val df = Seq(("a", "2026-10-17 01:00:00"), ("b", "2026-10-16 23:00:00"), ("c", "2026-10-18 00:00:00"), ("d", null))
      .toDF("id", "event_time").withColumn("event_time", col("event_time").cast("timestamp"))
    val pred = "event_time IS NULL OR (event_time >= TIMESTAMP '2026-10-17 00:00:00' " +
      "AND event_time < TIMESTAMP '2026-10-18 00:00:00')"
    val chk = Check(CheckLevel.Error, "c").satisfies(pred, "c")
    val r = run(df, chk)
    rowLevelOf(r, df, chk) shouldBe Map("a" -> Some(true), "b" -> Some(false), "c" -> Some(false), "d" -> Some(true))
  }

  test("isUnique: null bị loại khỏi cả tử số lẫn mẫu số (giống GX), dòng trùng đều fail") {
    import spark.implicits._
    val df = Seq(("r1", "t1"), ("r2", "t1"), ("r3", "t2"), ("r4", null)).toDF("id", "tx")
    val chk = Check(CheckLevel.Error, "u").isUnique("tx")
    val r = run(df, chk)
    metricOf(r, chk) shouldBe (1.0 / 3 +- 1e-9)
    val rows = rowLevelOf(r, df, chk)
    rows("r1") shouldBe Some(false)
    rows("r2") shouldBe Some(false)
    rows("r3") shouldBe Some(true)
  }

  test("hasUniqueness nhiều cột") {
    import spark.implicits._
    val df = Seq(("r1", "s1", "m1"), ("r2", "s1", "m1"), ("r3", "s1", "m2")).toDF("id", "sub", "msisdn")
    val chk = Check(CheckLevel.Warning, "u2").hasUniqueness(Seq("sub", "msisdn"), (v: Double) => v == 1.0)
    val r = run(df, chk)
    metricOf(r, chk) shouldBe (1.0 / 3 +- 1e-9)
    constraintOf(r, chk).status shouldBe ConstraintStatus.Failure
  }

  test("customSql chạy trên temp view, biểu thức TIMESTAMP - INTERVAL (rule freshness)") {
    import spark.implicits._
    val df = Seq(("a", "2026-10-17 23:30:00"), ("b", "2026-10-17 10:00:00"))
      .toDF("id", "event_time").withColumn("event_time", col("event_time").cast("timestamp"))
    df.createOrReplaceTempView("spec_freshness")
    try {
      val sql = "SELECT CAST(max(event_time) >= TIMESTAMP '2026-10-18 00:00:00' - INTERVAL 60 MINUTES AS DOUBLE) FROM spec_freshness"
      val ok = Check(CheckLevel.Warning, "fresh").customSql(sql, _ == 1.0)
      val late = Check(CheckLevel.Warning, "late").customSql(sql.replace("60 MINUTES", "10 MINUTES"), _ == 1.0)
      val r = run(df, ok, late)
      metricOf(r, ok) shouldBe 1.0
      metricOf(r, late) shouldBe 0.0
      constraintOf(r, late).status shouldBe ConstraintStatus.Failure
    } finally spark.catalog.dropTempView("spec_freshness")
  }

  test("rowLevelResultsAsDataFrame: chỉ constraint row-level mới sinh cột, tên cột = check.description") {
    import spark.implicits._
    val df = Seq(("a", "x"), ("b", null)).toDF("id", "v")
    val size = Check(CheckLevel.Error, "row_count_min").hasSize(_ >= 1)
    val complete = Check(CheckLevel.Error, "v_not_null").isComplete("v")
    val r = run(df, size, complete)
    VerificationResult.rowLevelResultsAsDataFrame(spark, r, df).columns.toSeq shouldBe Seq("id", "v", "v_not_null")
    rowLevelOf(r, df, complete) shouldBe Map("a" -> Some(true), "b" -> Some(false))
  }

  test("hasMean trên decimal, hasSize") {
    import spark.implicits._
    val df = Seq(("a", "10"), ("b", "20")).toDF("id", "amount").withColumn("amount", col("amount").cast("decimal(18,2)"))
    val mean = Check(CheckLevel.Warning, "m").hasMean("amount", v => v >= 10 && v <= 20)
    val size = Check(CheckLevel.Error, "s").hasSize(_ == 2)
    val r = run(df, mean, size)
    metricOf(r, mean) shouldBe 15.0
    metricOf(r, size) shouldBe 2.0
    constraintOf(r, size).status shouldBe ConstraintStatus.Success
  }

  test("cột không tồn tại -> constraint Failure (không ném exception)") {
    val chk = Check(CheckLevel.Error, "missing").isComplete("nope")
    val r = run(msisdn, chk)
    val c = constraintOf(r, chk)
    c.status shouldBe ConstraintStatus.Failure
  }
}
