package dqplatform.client

import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class CandidateSnapshotSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private def rule(id: String, check: String): CompiledRule = {
    val c = Canonical.parse(check)
    new CompiledRule(id, "PARTITION", "CANDIDATE", c.get("kind").asText(), Some("Compliance"), "DECIMAL",
      "sha256:" + "0" * 64, c, "QUARANTINE", true)
  }

  private val rules = Seq(
    rule("value_complete", """{"kind":"complete","column":"value"}"""),
    rule("amount_range", """{"kind":"range","column":"amount","min":"0","max":"100"}"""),
    rule("event_in_partition", """{"kind":"sql_predicate","columns":["event_time"],"predicate":"event_time IS NOT NULL AND event_time >= TIMESTAMP '2026-10-20 00:00:00' AND event_time < TIMESTAMP '2026-10-21 00:00:00'"}"""))

  private def source = {
    import spark.implicits._
    (1 to 100).map { i =>
      val bad = i <= 3
      (i.toLong,
        if (i == 1 || i == 2) null else "ok",
        if (i == 1 || i == 3) BigDecimal(-1) else BigDecimal(10),
        if (i == 1) "2026-10-19 23:00:00" else "2026-10-20 12:00:00")
    }.toDF("id", "value", "amount", "ts")
      .select(col("id"), col("value"), col("amount").cast("decimal(18,2)").as("amount"),
        to_timestamp(col("ts")).as("event_time"), to_date(lit("2026-10-20")).as("dt"))
  }

  test("classification is pinned, exact and union-sorted") {
    val dir = Files.createTempDirectory("dqcand").toString
    val raw = s"$dir/raw"; val cls = s"$dir/classified"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source.write.format("delta").mode("errorifexists").save(raw)
    val pinned = spark.read.format("delta").option("versionAsOf", 0).load(raw)

    val tagged = Classify.tag(spark, rules, pinned)
    tagged.write.format("delta").mode("errorifexists").save(cls)
    val classified = spark.read.format("delta").option("versionAsOf", 0).load(cls)

    val c = Classify.counts(classified, rules.map(_.ruleId).sorted)
    (c.candidate, c.valid, c.quarantine) shouldBe ((100L, 97L, 3L))
    c.perRule("value_complete") shouldBe 2L
    classified.columns.exists(_.startsWith("_dq_rule_")) shouldBe false
    classified.schema("_dq_errors").dataType.simpleString shouldBe "array<string>"

    val errs = classified.filter("id = 1").select("_dq_errors").head().getSeq[String](0)
    errs shouldBe Seq("amount_range", "event_in_partition", "value_complete")
    classified.filter("size(_dq_errors) > 0").select("id").collect().map(_.getLong(0)).toSet shouldBe Set(1L, 2L, 3L)
    Classify.clean(classified).columns.toSeq shouldBe Seq("id", "value", "amount", "event_time", "dt")
    Classify.clean(classified).count() shouldBe 97L

    source.write.format("delta").mode("append").save(raw)
    spark.read.format("delta").option("versionAsOf", 0).load(raw).count() shouldBe 100L
    Classify.counts(spark.read.format("delta").option("versionAsOf", 0).load(cls), Nil).candidate shouldBe 100L
  }

  test("no quarantine rules -> empty typed _dq_errors") {
    val t = Classify.tag(spark, Nil, source)
    t.schema("_dq_errors").dataType.simpleString shouldBe "array<string>"
    t.filter("size(_dq_errors) > 0").count() shouldBe 0L
  }
}
