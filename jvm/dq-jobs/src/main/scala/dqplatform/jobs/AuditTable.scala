package dqplatform.jobs

import dqplatform.client._
import org.apache.spark.sql.SparkSession

import java.time.Instant
import scala.util.control.NonFatal

/**
 * Read-only audit of the registered Silver table at one pinned version. Writes nothing to any table.
 * Exit: 0 = assessment PASS or VIOLATION, 2 = technical ERROR / service / config.
 * CLI: `--param name=value` (repeatable, declared suite parameters).
 */
object AuditTable {

  val AppName = "audit_table"

  final case class Args(params: Map[String, String])

  object Args {
    def parse(argv: Seq[String]): Args = {
      require(argv.size % 2 == 0 && argv.grouped(2).forall(p => p.head == "--param" && p(1).contains("=")),
        "invalid arguments: only repeated --param name=value")
      Args(argv.grouped(2).map { p => val Array(k, v) = p(1).split("=", 2); k -> v }.toMap)
    }
  }

  def main(argv: Array[String]): Unit = {
    val args = Args.parse(argv.toSeq)
    val spark = SparkSession.builder().appName(AppName).getOrCreate()
    val code =
      try {
        val cfg = DQConfig.fromSpark(spark)
        val api = new HttpServiceApi(cfg.serviceUrl, sys.env.getOrElse("DQ_API_TOKEN", ""), java.time.Duration.ofSeconds(cfg.timeoutSec.toLong))
        val uri = api.request("GET", DQClient.V2 ++ Seq("datasets", cfg.dataset)).path("registration").path("silver")
          .path("uri").asText()
        run(spark, new DQClient(spark, cfg, api), DeltaRuntime.snapshot(spark, uri), args.params, Instant.now())
      } catch {
        case NonFatal(e) =>
          System.err.println(s"DQ ERROR: ${BronzeToSilverTransactions.errorCode(e)}")
          BronzeToSilverTransactions.emit(None, None, None)
          2
      } finally spark.stop()
    sys.exit(code)
  }

  def run(spark: SparkSession, dq: DQClient, input: SnapshotRef, params: Map[String, String], eventTime: Instant): Int = {
    var executionId: Option[String] = None
    try {
      RuntimeProbe.compareConf(spark.conf.getOption).foreach(e => throw new DQFailedException(e.code))
      val handle = dq.startAudit(input, params, eventTime)
      executionId = Some(handle.executionId)
      val df = spark.read.format("delta").option("versionAsOf", input.version).load(input.uri)
      val accepted = dq.submit(dq.evaluateAudit(handle, df))
      val assessment = accepted.path("assessment").asText("")
      BronzeToSilverTransactions.emit(executionId, None, None, Some(assessment))
      if (assessment == "PASS" || assessment == "VIOLATION") 0 else 2
    } catch {
      case NonFatal(e) =>
        System.err.println(s"DQ ERROR: ${BronzeToSilverTransactions.errorCode(e)}")
        BronzeToSilverTransactions.emit(executionId, None, None)
        2
    }
  }
}
