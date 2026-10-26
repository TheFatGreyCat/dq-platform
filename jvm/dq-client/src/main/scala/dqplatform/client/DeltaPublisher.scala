package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import scala.util.control.NonFatal

/** Service transport for outcomes; implementations throw OutcomeConflict on HTTP 409. */
trait OutcomeApi {
  def getExecution(executionId: String): JsonNode
  def putOutcome(executionId: String, destination: String, body: ObjectNode): JsonNode
}

final class OutcomeConflict(val code: String) extends RuntimeException(code)

/** state is the service outcome state after this call; evidence EXACT, or TXN_ONLY when a bare token was seen (not reported). */
final case class DestResult(state: String, evidence: Option[String], commitVersion: Option[Long])
final case class PublicationResult(silver: DestResult, quarantine: DestResult)

/**
 * Writes the approved classified candidate to the quarantine then silver Delta tables, one idempotent
 * transaction token per destination. Never reruns the transform; reads only the pinned classified snapshot.
 */
final class DeltaPublisher(spark: SparkSession, api: OutcomeApi) {
  import DeltaPublisher._

  private val Destinations = Seq("quarantine", "silver")

  /** Everything immutable about one accepted execution. */
  private final class Ctx(val plan: JsonNode, val accepted: JsonNode) {
    val executionId: String = plan.get("execution_id").textValue()
    val decision: JsonNode = accepted.get("decision")
    val classified: SnapshotRef = SnapshotRef.fromJson(decision.get("classified_reference"), "classified_reference")
    val partitionValues: JsonNode = plan.get("partition_values")
    def appId(dest: String) = s"dq:$executionId:$dest"
    def base(dest: String): SnapshotRef = SnapshotRef.fromJson(plan.get("target_bases").get(dest), s"target_bases.$dest")
    def metaFor(dest: String): String = {
      val n = Json.node()
      n.put("classified_table_id", classified.tableId).put("classified_version", classified.version)
        .put("decision_id", decision.get("decision_id").textValue()).put("destination", dest)
        .put("execution_id", executionId).put("plan_hash", plan.get("plan_hash").textValue())
        .put("report_hash", accepted.get("report_hash").textValue())
      n.set[JsonNode]("partition_values", partitionValues.deepCopy())
      Canonical.string(n)
    }
  }

  private def prepare(plan: JsonNode, accepted: JsonNode): (Ctx, JsonNode) = {
    val ctx = new Ctx(plan, accepted)
    val d = accepted.get("decision")
    if (d == null || d.isNull || d.get("decision").textValue() != "ALLOW") throw new ProtocolException("NOT_ALLOWED", "decision")
    if (accepted.get("execution_id").textValue() != ctx.executionId) throw new ProtocolException("PLAN_MISMATCH", "execution_id")
    if (d.get("plan_hash").textValue() != plan.get("plan_hash").textValue()) throw new ProtocolException("PLAN_MISMATCH", "plan_hash")
    val exec = api.getExecution(ctx.executionId)
    if (exec.get("plan").get("plan_hash").textValue() != plan.get("plan_hash").textValue())
      throw new ProtocolException("PLAN_MISMATCH", "plan_hash")
    (ctx, exec)
  }

  private def outcomeOf(exec: JsonNode, dest: String): JsonNode = {
    val o = exec.path("outcomes").path(dest)
    if (!o.isObject) throw new OutcomeConflict("STATE_CONFLICT")
    o
  }

  private def stateOf(o: JsonNode) = o.get("state").textValue()

  private def read(ref: SnapshotRef): DataFrame =
    spark.read.format("delta").option("versionAsOf", ref.version).load(ref.uri)

  private def isQuarantine(df: DataFrame) = df.filter("size(_dq_errors) > 0")

  private def frame(ctx: Ctx, dest: String): DataFrame = {
    val c = read(ctx.classified)
    if (dest == "quarantine") {
      val ts = java.sql.Timestamp.from(java.time.Instant.parse(ctx.plan.get("execution_timestamp").textValue()))
      isQuarantine(c).withColumn("_dq_execution_id", lit(ctx.executionId))
        .withColumn("_dq_candidate_version", lit(ctx.classified.version))
        .withColumn("_dq_quarantined_at", lit(ts))
    } else {
      val clean = c.filter("size(_dq_errors) = 0")
      clean.select(clean.columns.filterNot(_.startsWith("_dq_")).map(col): _*)
    }
  }

  /**
   * Verified publication: quarantine then silver. One write attempt per destination per call; an unresolved
   * (UNKNOWN/IN_PROGRESS without proof) outcome is never retried here.
   */
  def publish(plan: JsonNode, accepted: JsonNode): PublicationResult = {
    val (ctx, exec) = prepare(plan, accepted)
    val complete = Destinations.forall(d => Set("SUCCEEDED", "NOT_APPLICABLE")(stateOf(outcomeOf(exec, d))))
    if (exec.get("state").textValue() != "REPORTED" && !complete) throw new OutcomeConflict("STATE_CONFLICT")
    val results = Destinations.map { d => d -> publishOne(ctx, d, outcomeOf(exec, d)) }.toMap
    PublicationResult(results("silver"), results("quarantine"))
  }

  /**
   * Reads the Delta log for every unresolved destination, in write order (stops at the first one that stays
   * unresolved so silver never goes before quarantine). Exact own commit -> PUT SUCCEEDED EXACT. Bare token without
   * the exact commit -> returned as TXN_ONLY (operator path, never PUT here). Proven absence plus `writerStop`
   * (scheduler-confirmed reference that the old writer exited) or a FAILED pre-write outcome -> resumes the same
   * intent once.
   */
  def reconcile(plan: JsonNode, accepted: JsonNode, writerStop: Option[String] = None): ReconciliationResult = {
    val (ctx, exec) = prepare(plan, accepted)
    var blocked = false
    val results = Destinations.map { d =>
      val o = outcomeOf(exec, d)
      val r =
        if (!blocked && Set("IN_PROGRESS", "UNKNOWN", "FAILED")(stateOf(o))) resolve(ctx, d, o, writerStop) else settled(o)
      if (!Set("SUCCEEDED", "NOT_APPLICABLE")(r.state)) blocked = true
      d -> r
    }.toMap
    PublicationResult(results("silver"), results("quarantine"))
  }

  private def settled(o: JsonNode): DestResult = {
    val c = o.path("commit")
    DestResult(stateOf(o), if (c.isObject) Some(c.get("evidence").textValue()) else None,
      if (c.isObject && c.get("version").isIntegralNumber) Some(c.get("version").longValue()) else None)
  }

  private def publishOne(ctx: Ctx, dest: String, o: JsonNode): DestResult = stateOf(o) match {
    case "SUCCEEDED" | "NOT_APPLICABLE" => settled(o)
    case "IN_PROGRESS" | "UNKNOWN" | "FAILED" =>
      val r = resolve(ctx, dest, o, None)
      if (r.state != "SUCCEEDED") throw new OutcomeConflict("OUTCOME_UNRESOLVED")
      r
    case "NOT_STARTED" => start(ctx, dest, o.get("outcome_version").longValue())
    case _ => throw new OutcomeConflict("STATE_CONFLICT")
  }

  private def intentPut(ctx: Ctx, dest: String, expected: Long, recovery: Option[ObjectNode] = None): Long =
    put(ctx, dest, "IN_PROGRESS", expected, intent = true, recovery = recovery)
      .get("outcome").get("outcome_version").longValue()

  /** First write of a destination. Definite pre-write failures are recorded FAILED (no commit claimed). */
  private def start(ctx: Ctx, dest: String, outcomeVersion: Long): DestResult = {
    val uri = ctx.base(dest).uri
    val ev = DeltaRuntime.findTransaction(spark, uri, ctx.appId(dest))
    // base check before any intent: changed base leaves NOT_STARTED so the execution can be abandoned
    if (ev.commits.isEmpty && ev.snapshotTxnVersion.isEmpty && ev.snapshotVersion != ctx.base(dest).version)
      throw new OutcomeConflict("BASE_CHANGED")
    val version = intentPut(ctx, dest, outcomeVersion)
    if (ev.commits.nonEmpty || ev.snapshotTxnVersion.nonEmpty) {
      // token already on the table: never write again, only verify
      return resolve(ctx, dest, inProgress(version), None)
    }
    try checkWritable(ctx, dest, ev)
    catch {
      case e: ProtocolException if e.code == "CANDIDATE_UNAVAILABLE" =>
        failPrewrite(ctx, dest, version, ev, "CANDIDATE_UNAVAILABLE", ClientError("CANDIDATE_UNAVAILABLE", Map.empty)); throw e
    }
    write(ctx, dest, version)
  }

  private def checkWritable(ctx: Ctx, dest: String, ev: TransactionEvidence): Unit = {
    if (ev.snapshotVersion != ctx.base(dest).version) throw new OutcomeConflict("BASE_CHANGED")
    DeltaRuntime.verify(spark, ctx.classified)
  }

  /** FAILED only with log-proven absence; otherwise UNKNOWN so nothing is claimed about the table. */
  private def failPrewrite(ctx: Ctx, dest: String, version: Long, ev: TransactionEvidence, reason: String,
                           err: ClientError): Unit =
    if (ev.absenceProven)
      put(ctx, dest, "FAILED", version, reason = Some(reason), error = Some(err),
        recovery = Some(recoveryBody(ev, ctx.base(dest).version, None)))
    else put(ctx, dest, "UNKNOWN", version, reason = Some(reason), error = Some(err))

  private def recoveryBody(ev: TransactionEvidence, baseVersion: Long, stop: Option[String]): ObjectNode = {
    val r = Json.node()
    r.putObject("absence").put("table_id", ev.tableId).put("checked_through_version", ev.checkedThrough)
    r.put("current_base_version", ev.snapshotVersion)
    stop match {
      case Some(ref) => r.putObject("writer_stop").put("confirmed_stopped", true).put("reference", ref)
      case None => r.putNull("writer_stop")
    }
    r.putNull("txn_evidence")
    r
  }

  private def inProgress(version: Long): JsonNode = {
    val n = Json.node(); n.put("state", "IN_PROGRESS").put("outcome_version", version); n
  }

  private def resolve(ctx: Ctx, dest: String, o: JsonNode, stop: Option[String]): DestResult = {
    val meta = ctx.metaFor(dest)
    val ev = DeltaRuntime.findTransaction(spark, ctx.base(dest).uri, ctx.appId(dest))
    val state = stateOf(o)
    val version = o.get("outcome_version").longValue()
    if (exact(ev, meta).isDefined) {
      if (state == "FAILED") throw new OutcomeConflict("OUTCOME_UNRESOLVED")
      DeltaRuntime.verify(spark, ctx.classified)
      settle(ctx, dest, version, ev, meta, frame(ctx, dest).count())
    } else if (ev.commits.nonEmpty) {
      if (state == "IN_PROGRESS") put(ctx, dest, "UNKNOWN", version, reason = Some("CONFLICTING_EVIDENCE"))
      throw new ProtocolException("PLAN_MISMATCH", dest)
    } else if (ev.snapshotTxnVersion.nonEmpty) DestResult(state, Some("TXN_ONLY"), None)
    else if (ev.absenceProven && (state == "FAILED" || stop.isDefined)) resume(ctx, dest, state, version, ev, stop)
    else DestResult(state, None, None)
  }

  /** Same intent, same content, one write: base must be exactly the plan base and the candidate intact. */
  private def resume(ctx: Ctx, dest: String, state: String, outcomeVersion: Long, ev: TransactionEvidence,
                     stop: Option[String]): DestResult = {
    checkWritable(ctx, dest, ev)
    val rec = Some(recoveryBody(ev, ctx.base(dest).version, stop))
    val unknownVersion =
      if (state == "IN_PROGRESS") put(ctx, dest, "UNKNOWN", outcomeVersion, recovery = rec)
        .get("outcome").get("outcome_version").longValue()
      else outcomeVersion
    write(ctx, dest, intentPut(ctx, dest, unknownVersion, rec))
  }

  private def write(ctx: Ctx, dest: String, version: Long): DestResult = {
    val meta = ctx.metaFor(dest)
    val uri = ctx.base(dest).uri
    val df = frame(ctx, dest)
    val rows = df.count()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
    try {
      val w = df.write.format("delta").option("txnAppId", ctx.appId(dest)).option("txnVersion", 1L)
        .option("userMetadata", meta)
      if (dest == "quarantine") w.mode("append").partitionBy("dt").save(uri)
      else w.mode("overwrite").option("replaceWhere", ctx.plan.get("partition_predicate").textValue()).save(uri)
    } catch {
      case NonFatal(e) =>
        // an exception around .write proves nothing: look once for the exact commit, else UNKNOWN (never FAILED)
        val found = try Some(DeltaRuntime.findTransaction(spark, uri, ctx.appId(dest))).filter(exact(_, meta).isDefined)
        catch { case NonFatal(_) => None }
        found match {
          case Some(ev) => return settle(ctx, dest, version, ev, meta, rows)
          case None =>
            put(ctx, dest, "UNKNOWN", version, error = Some(ClientError.withClass("STORAGE_UNAVAILABLE", e)))
            throw e
        }
    }
    val ev = DeltaRuntime.findTransaction(spark, uri, ctx.appId(dest))
    settle(ctx, dest, version, ev, meta, rows)
  }

  private def exact(ev: TransactionEvidence, meta: String): Option[CommitMatch] =
    ev.commits.find(c => c.txnVersion == 1L && c.userMetadata.contains(meta))

  /** PUT SUCCEEDED EXACT for the matching commit, else UNKNOWN/PLAN_MISMATCH. */
  private def settle(ctx: Ctx, dest: String, version: Long, ev: TransactionEvidence, meta: String, rows: Long): DestResult =
    exact(ev, meta) match {
      case Some(c) =>
        put(ctx, dest, "SUCCEEDED", version, commit = Some((ev.tableId, c.version, c.metadataHash.get)), rows = Some(rows))
        DestResult("SUCCEEDED", Some("EXACT"), Some(c.version))
      case None =>
        put(ctx, dest, "UNKNOWN", version, reason = Some("CONFLICTING_EVIDENCE"))
        throw new ProtocolException("PLAN_MISMATCH", dest)
    }

  private def put(ctx: Ctx, dest: String, state: String, expected: Long, intent: Boolean = false,
                  commit: Option[(String, Long, String)] = None, rows: Option[Long] = None,
                  reason: Option[String] = None, error: Option[ClientError] = None,
                  recovery: Option[ObjectNode] = None): JsonNode = {
    val b = Json.node()
    b.put("state", state).put("expected_outcome_version", expected)
    if (intent) b.putObject("intent").put("txn_app_id", ctx.appId(dest)).put("txn_version", 1L) else b.putNull("intent")
    commit match {
      case Some((t, v, h)) => b.putObject("commit").put("table_id", t).put("version", v).put("metadata_hash", h).put("evidence", "EXACT")
      case None => b.putNull("commit")
    }
    rows.fold(b.putNull("rows"))(r => b.put("rows", r))
    reason.fold(b.putNull("reason"))(r => b.put("reason", r))
    error match { case Some(e) => b.set[JsonNode]("error", e.toJson); case None => b.putNull("error") }
    recovery match { case Some(r) => b.set[JsonNode]("recovery", r); case None => b.putNull("recovery") }
    api.putOutcome(ctx.executionId, dest, b)
  }
}

object DeltaPublisher {
  type ReconciliationResult = PublicationResult
}
