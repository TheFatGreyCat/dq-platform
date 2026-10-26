package dqplatform.worker

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.funsuite.AnyFunSuite

class AnomalyEvaluatorSpec extends AnyFunSuite {
  private val m = new ObjectMapper()
  private def eval(prev: Option[String], cur: String, lower: String = "0.8", upper: String = "1.2"): JsonNode = {
    val base = prev.map(p => s"""{"execution_id":"e","event_time":"2026-01-01T00:00:00.000000Z","value":"$p"}""").mkString(",")
    AnomalyEvaluator.evaluate(m.readTree(
      s"""{"metric_definition_hash":"h","scope":"PARTITION","stage":"CLEAN","lower":"$lower","upper":"$upper","baseline":[$base],"current":"$cur"}"""))
  }

  test("ratio 1.3 above 1.2 is ANOMALY with decimal evidence") {
    val r = eval(Some("100"), "130")
    assert(r.get("status").asText == "ANOMALY")
    assert(r.get("evidence").get("ratio").asText == "1.3")
  }
  test("ratio 1.2 is NORMAL (inclusive bound)") { assert(eval(Some("100"), "120").get("status").asText == "NORMAL") }
  test("zero predecessor is ERROR ZERO_DENOMINATOR") {
    val r = eval(Some("0"), "5")
    assert(r.get("status").asText == "ERROR" && r.get("error").get("code").asText == "ZERO_DENOMINATOR")
  }
  test("no predecessor is INSUFFICIENT_HISTORY") {
    val r = eval(None, "5")
    assert(r.get("status").asText == "INSUFFICIENT_HISTORY")
    assert(r.get("evidence").get("reason").asText == "NO_FINITE_PREDECESSOR")
  }
  test("overflowing value is ERROR INVALID_NUMBER") {
    assert(eval(Some("100"), "1e999").get("error").get("code").asText == "INVALID_NUMBER")
  }
  test("malformed value is ERROR INVALID_REQUEST") {
    assert(eval(Some("100"), "abc").get("error").get("code").asText == "INVALID_REQUEST")
  }
}
