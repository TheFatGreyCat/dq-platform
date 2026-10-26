package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import scala.collection.JavaConverters._

/** JSON-string facade for Py4J: references and handles travel as JSON, DataFrames as `df._jdf`. */
object PyBridge {
  private def client(spark: SparkSession) = DQClient.fromSession(spark)

  private def strings(n: JsonNode, field: String): Map[String, String] = {
    val m = n.get(field)
    if (m == null || !m.isObject) Json.fail(field)
    m.fields().asScala.map { e =>
      if (!e.getValue.isTextual) Json.fail(s"$field.${e.getKey}")
      e.getKey -> e.getValue.textValue()
    }.toMap
  }

  /** request: `{partition_values, parameters, event_time[, event_origin, previous_execution_id]}` -> handle JSON. */
  def startWrite(spark: SparkSession, requestJson: String): String = {
    val r = Canonical.parse(requestJson)
    val origin = Option(r.get("event_origin")).map(_.textValue()).getOrElse("SCHEDULED")
    val previous = Option(r.get("previous_execution_id")).filterNot(_.isNull).map(_.textValue())
    val h = client(spark).startWrite(strings(r, "partition_values"), strings(r, "parameters"),
      Instant.parse(Json.str(r, "event_time", "request")), origin, previous)
    Canonical.string(h.toJson)
  }

  /** -> evaluation JSON `{handle, report_file}`. */
  def evaluate(spark: SparkSession, handleJson: String, df: DataFrame): String =
    Canonical.string(client(spark).evaluate(ExecutionHandle.fromJson(Canonical.parse(handleJson)), df).toJson)

  /** -> AcceptedResponse JSON. */
  def submit(spark: SparkSession, evaluationJson: String): String =
    Canonical.string(client(spark).submit(CandidateEvaluation.fromJson(Canonical.parse(evaluationJson), spark.sessionState.newHadoopConf())))

  /** -> `{silver:{state,evidence,commit_version}, quarantine:{...}}`. */
  def publish(spark: SparkSession, evaluationJson: String, acceptedJson: String): String = {
    val r = client(spark).publish(CandidateEvaluation.fromJson(Canonical.parse(evaluationJson), spark.sessionState.newHadoopConf()), Canonical.parse(acceptedJson))
    val n = Json.node()
    Seq("silver" -> r.silver, "quarantine" -> r.quarantine).foreach { case (k, d) =>
      val o = n.putObject(k)
      o.put("state", d.state)
      d.evidence.fold(o.putNull("evidence"))(o.put("evidence", _))
      d.commitVersion.fold(o.putNull("commit_version"))(o.put("commit_version", _))
    }
    Canonical.string(n)
  }
}
