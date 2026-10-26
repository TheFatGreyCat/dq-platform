package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date}

import java.time.{LocalDate, ZoneOffset}
import scala.util.control.NonFatal

/**
 * Bronze (Delta root, pinned versionAsOf) -> Silver transactions through the DQ gate:
 * startWrite -> evaluate -> submit -> publish (Delta publisher). The job never writes Silver/quarantine itself.
 *
 * Exit: 0 = both destinations published (quarantine NOT_APPLICABLE counts), 3 = gate DENY,
 * 2 = technical / service / config / unresolved. Last stdout line: `{execution_id, decision, publication}` (no rows).
 */
object BronzeToSilverTransactions {

  val AppName = "bronze_to_silver_transactions_scala"
  val ExitPublished = 0
  val ExitError = 2
  val ExitDenied = 3

  final case class Args(dt: LocalDate, lake: String = "/data/lake")

  object Args {
    def parse(argv: Seq[String]): Args = {
      val kv = argv.grouped(2).collect { case Seq(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
      require(argv.size % 2 == 0 && (kv.keySet -- Set("dt", "lake")).isEmpty, "invalid arguments: only --dt and --lake")
      val dt = kv.getOrElse("dt", throw new IllegalArgumentException("missing --dt yyyy-MM-dd"))
      Args(LocalDate.parse(dt), kv.getOrElse("lake", "/data/lake"))
    }
  }

  def bronzeRoot(lake: String): String = s"${lake.stripSuffix("/")}/bronze/transactions"

  /** Stable code only: exception messages may carry data values and are never forwarded. */
  private[jobs] def errorCode(e: Throwable): String = e match {
    case d: DQFailedException => d.code
    case s: ServiceException => s.code
    case p: ProtocolException => p.code
    case o: OutcomeConflict => o.code
    case _: IllegalArgumentException => "INVALID_CONFIG"
    case _ => "UNCLASSIFIED_ENGINE_ERROR"
  }

  private[jobs] def emit(executionId: Option[String], decision: Option[String], publication: Option[ObjectNode],
                         assessment: Option[String] = None): Unit = {
    val n = Json.node()
    n.put("execution_id", executionId.orNull)
    n.put("decision", decision.orNull)
    n.set[JsonNode]("publication", publication.getOrElse(com.fasterxml.jackson.databind.node.NullNode.instance))
    assessment.foreach(n.put("assessment", _))
    println(Canonical.string(n))
  }

  def main(argv: Array[String]): Unit = {
    val args = Args.parse(argv.toSeq)
    val spark = SparkSession.builder().appName(AppName).getOrCreate()
    val code =
      try run(spark, args, DQClient.fromSession(spark))
      catch { case NonFatal(e) => System.err.println(s"DQ ERROR: ${errorCode(e)}"); emit(None, None, None); ExitError }
      finally spark.stop()
    sys.exit(code)
  }

  private def dest(d: DestResult): ObjectNode = {
    val n = Json.node()
    n.put("state", d.state)
    d.evidence.fold(n.putNull("evidence"))(n.put("evidence", _))
    d.commitVersion.fold(n.putNull("commit_version"))(n.put("commit_version", _))
    n
  }

  def run(spark: SparkSession, args: Args, dq: DQClient): Int = {
    var executionId: Option[String] = None
    try {
      val dt = args.dt.toString
      RuntimeProbe.compareConf(spark.conf.getOption).foreach(e => throw new DQFailedException(e.code))
      val root = bronzeRoot(args.lake)
      val version = DeltaRuntime.snapshot(spark, root).version
      val raw = spark.read.format("delta").option("versionAsOf", version).load(root)
        .where(col("dt") === to_date(lit(dt)))
      val params = Map(
        "partition_start" -> s"$dt 00:00:00",
        "partition_end" -> s"${args.dt.plusDays(1)} 00:00:00",
        "source_row_count" -> TransactionsTransform.sourceRowCount(raw).toString)

      val handle = dq.startWrite(Map("dt" -> dt), params, args.dt.atStartOfDay(ZoneOffset.UTC).toInstant)
      executionId = Some(handle.executionId)
      val evaluation = dq.evaluate(handle, TransactionsTransform.transform(raw, dt))
      val accepted = dq.submit(evaluation)
      accepted.path("decision").path("decision").asText("") match {
        case "DENY" =>
          System.err.println(s"DQ DENY: execution ${handle.executionId}")
          emit(executionId, Some("DENY"), None)
          ExitDenied
        case "ALLOW" =>
          val pub = dq.publish(evaluation, accepted)
          val node = Json.node()
          node.set[JsonNode]("silver", dest(pub.silver))
          node.set[JsonNode]("quarantine", dest(pub.quarantine))
          emit(executionId, Some("ALLOW"), Some(node))
          if (pub.silver.state == "SUCCEEDED" && Set("SUCCEEDED", "NOT_APPLICABLE")(pub.quarantine.state)) ExitPublished
          else ExitError
        case _ =>
          System.err.println(s"DQ ERROR: no decision for execution ${handle.executionId}")
          emit(executionId, None, None)
          ExitError
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"DQ ERROR: ${errorCode(e)}")
        emit(executionId, None, None)
        ExitError
    }
  }
}
