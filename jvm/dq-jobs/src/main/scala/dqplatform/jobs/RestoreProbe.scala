package dqplatform.jobs

import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.util.UUID
import scala.util.Try

/**
 * Read-only scan of both target tables after a PostgreSQL restore: every retained `dq:` SetTransaction token with the
 * partitions it touched, as `RestoreFindings` pages (contiguous, max tokens/bytes per page, `final_page` on the last).
 * Touches no Delta data. A table that changed while scanning aborts the scan (caller restarts with a new scan_id).
 */
object RestoreProbe {

  val MaxTokens = 2000
  /** Page byte budget, under the 1 MiB HTTP body limit with headroom for the envelope. */
  val MaxPageBytes = 900 * 1024

  private val Destinations = Seq("silver", "quarantine")

  private def date(s: String): Option[String] = Try(LocalDate.parse(s).toString).toOption.filter(_ == s)

  /** Typed partitions of one token commit: add/remove actions first, else canonical userMetadata.partition_values. */
  private[jobs] def partitionsOf(c: DeltaRuntime.TokenCommit): Seq[String] = {
    val fromActions = c.partitions.toSeq.map(p => if (p.keySet == Set("dt")) date(p("dt")) else None)
    val fromMeta = c.userMetadata.flatMap(m => Try(Canonical.parse(m).path("partition_values").path("dt").asText("")).toOption)
      .flatMap(date)
    if (fromActions.nonEmpty && fromActions.forall(_.isDefined)) fromActions.flatten.distinct.sorted
    else if (fromActions.isEmpty) fromMeta.toSeq
    else Nil // partial/invalid evidence: never fabricate a partition
  }

  private def tokenRow(dest: String, c: DeltaRuntime.TokenCommit, dt: Option[String]): ObjectNode = {
    val n = Json.node()
    n.put("destination", dest).put("txn_app_id", c.appId).put("txn_version", c.txnVersion).put("commit_version", c.version)
    dt.fold(n.putNull("partition_values"))(d => n.putObject("partition_values").put("dt", d))
    n
  }

  def scan(spark: SparkSession, uris: Map[String, String], stopReference: String, scanId: String = UUID.randomUUID().toString)
      : Seq[ObjectNode] = {
    val scanned = Destinations.map { d =>
      val uri = uris(d)
      val state = DeltaRuntime.state(spark, uri) // TARGET_NOT_INITIALIZED when absent
      if (state.properties.contains(DeltaRuntime.RetentionProperty))
        throw new ProtocolException("INVALID_CONFIG", s"$d.set_transaction_retention")
      val (ref, _, tokens) = DeltaRuntime.dqTokens(spark, uri)
      if (ref.version != state.ref.version) throw new ProtocolException("SCAN_CHANGED", d)
      (d, ref, tokens)
    }
    scanned.foreach { case (d, ref, _) =>
      if (DeltaRuntime.snapshot(spark, ref.uri).version != ref.version) throw new ProtocolException("SCAN_CHANGED", d)
    }
    val rows = scanned.flatMap { case (d, _, tokens) =>
      tokens.flatMap { c =>
        val parts = partitionsOf(c)
        if (parts.isEmpty) Seq(tokenRow(d, c, None)) else parts.map(p => tokenRow(d, c, Some(p)))
      }
    }.sortBy(r => (r.get("destination").asText, r.get("commit_version").asLong, r.get("txn_app_id").asText,
      r.path("partition_values").path("dt").asText("")))

    def page(tokens: Seq[ObjectNode], index: Int, last: Boolean): ObjectNode = {
      val p = Json.node()
      p.put("scan_id", scanId).put("page", index).put("final_page", last).put("scheduler_stop_reference", stopReference)
      val sc = p.putArray("scans")
      scanned.foreach { case (d, ref, _) =>
        sc.addObject().put("destination", d).put("table_id", ref.tableId).put("snapshot_version", ref.version)
      }
      val arr = p.putArray("tokens")
      tokens.foreach(arr.add)
      p
    }

    val groups = scala.collection.mutable.ArrayBuffer[Seq[ObjectNode]]()
    var cur = Vector.empty[ObjectNode]
    rows.foreach { r =>
      val tentative = cur :+ r
      if (cur.nonEmpty && (tentative.size > MaxTokens || Canonical.bytes(page(tentative, 0, true)).length > MaxPageBytes)) {
        groups += cur
        cur = Vector(r)
      } else cur = tentative
    }
    groups += cur
    groups.zipWithIndex.map { case (g, i) => page(g, i, i == groups.size - 1) }.toSeq
  }

  def main(argv: Array[String]): Unit = {
    val kv = argv.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    require(argv.length % 2 == 0 && kv.keySet == Set("silver", "quarantine", "scheduler-stop-reference", "output"),
      "usage: --silver URI --quarantine URI --scheduler-stop-reference REF --output FILE")
    val spark = SparkSession.builder().appName("restore_probe").getOrCreate()
    val code =
      try {
        val pages = scan(spark, Map("silver" -> kv("silver"), "quarantine" -> kv("quarantine")), kv("scheduler-stop-reference"))
        val out = Canonical.mapper.createArrayNode()
        pages.foreach(out.add)
        Files.write(Paths.get(kv("output")), Canonical.bytes(out))
        println(Canonical.string(out))
        0
      } catch {
        case e: ProtocolException => System.err.println(s"RESTORE PROBE ERROR: ${e.code}"); 2
        case _: Exception => System.err.println("RESTORE PROBE ERROR: UNCLASSIFIED_ENGINE_ERROR"); 2
      } finally spark.stop()
    sys.exit(code)
  }
}
