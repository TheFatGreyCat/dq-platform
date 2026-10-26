package dqplatform.worker

import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}

import java.math.{MathContext, BigDecimal => JBigDecimal}
import scala.util.Try

/** Advisory detector: ratio of current to previous finite baseline value against [lower, upper]. */
object AnomalyEvaluator {
  val DetectorVersion = "deequ-2.0.21-spark-3.5:RelativeRateOfChange:order1"
  val MaxPoints = 200
  val MaxScalarChars = 384

  private val f = JsonNodeFactory.instance
  private object Invalid extends Exception

  private def decimal(n: JsonNode): JBigDecimal = {
    if (n == null || !n.isTextual || n.asText.length > MaxScalarChars) throw Invalid
    Try(new JBigDecimal(n.asText)).getOrElse(throw Invalid)
  }

  private def plain(d: JBigDecimal): String =
    if (d.signum == 0) "0" else d.stripTrailingZeros.toPlainString

  private def finite(d: JBigDecimal): Option[Double] =
    Some(d.doubleValue).filter(x => !x.isNaN && !x.isInfinite)

  private def error(base: ObjectNode, code: String): ObjectNode = {
    base.put("status", "ERROR")
    base.putObject("error").put("code", code)
    base
  }

  def evaluate(item: JsonNode): ObjectNode = {
    val out = f.objectNode()
    def text(k: String) = Option(item.get(k)).filter(_.isTextual).map(_.asText)
    out.put("metric_definition_hash", text("metric_definition_hash").orNull)
    out.put("scope", text("scope").orNull)
    out.put("stage", text("stage").orNull)
    out.put("detector_version", DetectorVersion)
    try run(item, out) catch {
      case Invalid => error(out, "INVALID_REQUEST")
      case _: ArithmeticException => error(out, "INVALID_NUMBER")
    }
  }

  private def run(item: JsonNode, out: ObjectNode): ObjectNode = {
    val lowerD = decimal(item.get("lower"))
    val upperD = decimal(item.get("upper"))
    val baseline = item.get("baseline")
    if (baseline == null || !baseline.isArray || baseline.size > MaxPoints) throw Invalid
    val values = (0 until baseline.size).map(i => decimal(baseline.get(i).get("value")))
    val cur = item.get("current")
    if (cur == null || cur.isNull) throw Invalid
    val currentD = decimal(cur)
    val (lower, upper) = (finite(lowerD), finite(upperD)) match {
      case (Some(l), Some(u)) if l <= u => (l, u)
      case _ => throw Invalid
    }
    val prior = values.filter(v => finite(v).isDefined)
    if (prior.isEmpty) {
      out.put("status", "INSUFFICIENT_HISTORY")
      out.putObject("evidence").put("reason", "NO_FINITE_PREDECESSOR")
      return out
    }
    val previousD = prior.last
    if (previousD.signum == 0) return error(out, "ZERO_DENOMINATOR")
    val series = (prior :+ currentD).map(v => finite(v).getOrElse(throw new ArithmeticException))
      .toVector
    val last = series.length - 1
    val anomalies = RelativeRateOfChangeStrategy(Some(lower), Some(upper), 1)
      .detect(series, (last, last + 1))
    val ratio = currentD.divide(previousD, MathContext.DECIMAL128)
    out.put("status", if (anomalies.nonEmpty) "ANOMALY" else "NORMAL")
    val ev = out.putObject("evidence")
    ev.put("previous", plain(previousD)).put("current", plain(currentD)).put("ratio", plain(ratio))
    ev.put("lower", plain(lowerD)).put("upper", plain(upperD))
    out
  }

  def evaluateBatch(items: JsonNode): ArrayNode = {
    val arr = f.arrayNode()
    (0 until items.size).foreach(i => arr.add(evaluate(items.get(i))))
    arr
  }
}
