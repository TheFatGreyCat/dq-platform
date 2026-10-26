package dqplatform.client

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MeasureSpec extends AnyFunSuite with Matchers with SparkTestSession {

  def rule(id: String, check: String, stage: String = "RAW", action: String = "OBSERVE", metric: Option[String] = Some("Compliance")): CompiledRule = {
    val c = Canonical.parse(check)
    new CompiledRule(id, "PARTITION", stage, c.get("kind").asText(), metric, "DECIMAL", "sha256:" + "0" * 64, c, action, true)
  }

  private def df = {
    import spark.implicits._
    (1 to 100).map(i => (i.toLong, if (i <= 2) null else "ok", BigDecimal(10))).toDF("id", "value", "amount")
      .withColumn("amount", col("amount").cast("decimal(18,2)"))
  }

  private def run(rules: CompiledRule*): Map[String, ObjectNode] = {
    val d = df
    d.createOrReplaceTempView("mv_batch")
    Measure.run(spark, rules, d, "mv_batch").map(n => n.get("rule_id").asText() -> n).toMap
  }

  test("size is an exact integer; completeness a decimal string; all keys present") {
    val r = run(rule("n", """{"kind":"size"}""", metric = Some("Size")),
      rule("c", """{"kind":"completeness","column":"value"}""", metric = Some("Completeness")))
    r("n").get("metric_value").isIntegralNumber shouldBe true
    r("n").get("metric_value").asLong() shouldBe 100L
    r("c").get("metric_value").asText() shouldBe "0.98"
    r("c").get("measurement_status").asText() shouldBe "OK"
    r("c").fieldNames().hasNext shouldBe true
    r("c").size() shouldBe 11
  }

  test("sql_metric reads the ${batch} view") {
    val r = run(rule("s", """{"kind":"sql_metric","sql":"SELECT COUNT(*) FROM ${batch} WHERE value IS NULL"}""", metric = Some("SqlMetric")))
    r("s").get("measurement_status").asText() shouldBe "OK"
    r("s").get("metric_value").asText() shouldBe "2"
  }

  test("missing column -> COLUMN_NOT_FOUND without message") {
    val r = run(rule("m", """{"kind":"complete","column":"nope"}"""))
    r("m").get("measurement_status").asText() shouldBe "ERROR"
    r("m").get("error").get("code").asText() shouldBe "COLUMN_NOT_FOUND"
    r("m").get("error").get("params").has("message") shouldBe false
  }

  test("schema_type reports persisted spark type; pattern/range/in_set ignore nulls") {
    val r = run(
      rule("t", """{"kind":"schema_type","column":"amount","type":"decimal"}""", metric = None),
      rule("pat", """{"kind":"pattern","column":"value","regex":"^ok$"}"""),
      rule("rng", """{"kind":"range","column":"amount","min":"0","max":"100"}"""),
      rule("ins", """{"kind":"in_set","column":"value","values":["ok"]}"""))
    val ev = r("t").get("schema_evidence")
    ev.get("actual_type").asText() shouldBe "decimal(18,2)"
    ev.get("matches").asBoolean() shouldBe true
    Seq("pat", "rng", "ins").foreach(k => r(k).get("metric_value").asText() shouldBe "1")
  }
}
