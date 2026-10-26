package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** The batch cannot be gated (runtime mismatch, service refusal). Only a stable code; job should exit 2. */
final class DQFailedException(val code: String) extends RuntimeException(code)

/**
 * Spark conf `spark.dq.service.url`, `.service.timeout.sec` (default 10), `.dataset`, `.purpose` (default "default"),
 * `.compat.manifest.id`, `.state.dir`; env `DQ_RUN_KEY` (required, stable per logical invocation), `DQ_IMAGE_DIGEST`.
 * The token is read from env DQ_API_TOKEN only (never Spark conf, it would show in the Spark UI).
 */
final case class DQConfig(serviceUrl: String, timeoutSec: Int, dataset: String, purpose: String,
                          compatManifestId: String, runKey: String, imageDigest: Option[String], stateDir: String)

object DQConfig {
  def fromSpark(spark: SparkSession, env: Map[String, String] = sys.env): DQConfig = {
    def conf(key: String) = spark.conf.getOption(key).map(_.trim).filter(_.nonEmpty)
    def required(key: String) = conf(key).getOrElse(throw new IllegalArgumentException(s"missing spark conf $key"))
    DQConfig(
      serviceUrl = required("spark.dq.service.url"),
      timeoutSec = conf("spark.dq.service.timeout.sec").map(_.toInt).getOrElse(10),
      dataset = required("spark.dq.dataset"),
      purpose = conf("spark.dq.purpose").getOrElse("default"),
      compatManifestId = required("spark.dq.compat.manifest.id"),
      runKey = env.get("DQ_RUN_KEY").map(_.trim).filter(_.nonEmpty)
        .getOrElse(throw new IllegalArgumentException("missing env DQ_RUN_KEY")),
      imageDigest = env.get("DQ_IMAGE_DIGEST").map(_.trim).filter(_.nonEmpty),
      stateDir = required("spark.dq.state.dir"))
  }
}

final case class ExecutionHandle(executionId: String, attemptId: String, plan: JsonNode) {
  def toJson: ObjectNode = {
    val n = Json.node()
    n.put("execution_id", executionId).put("attempt_id", attemptId).set[JsonNode]("plan", plan)
    n
  }
}

object ExecutionHandle {
  def fromJson(n: JsonNode): ExecutionHandle = {
    Json.exactKeys(n, "handle", Set("execution_id", "attempt_id", "plan"))
    ExecutionHandle(Json.str(n, "execution_id", "handle"), Json.str(n, "attempt_id", "handle"), Json.obj(n, "plan", "handle"))
  }
}

/** `report` is the full WRITE report; `reportFile` holds its exact canonical bytes (retries resend those bytes). */
final case class CandidateEvaluation(handle: ExecutionHandle, report: ObjectNode, reportFile: String) {
  def toJson: ObjectNode = {
    val n = Json.node()
    n.set[JsonNode]("handle", handle.toJson)
    n.put("report_file", reportFile)
    n
  }
}

object CandidateEvaluation {
  def fromJson(n: JsonNode, conf: Configuration): CandidateEvaluation = {
    Json.exactKeys(n, "evaluation", Set("handle", "report_file"))
    val handle = ExecutionHandle.fromJson(n.get("handle"))
    val file = Json.str(n, "report_file", "evaluation")
    val report = Canonical.parse(new String(StateFiles.read(new Path(file), conf), UTF_8))
    if (!report.isObject || report.path("attempt_id").asText() != handle.attemptId) Json.fail("evaluation.report_file")
    CandidateEvaluation(handle, report.asInstanceOf[ObjectNode], file)
  }
}

/**
 * Embedded in every Spark job. The service compiles the rules and decides; this client only measures the
 * pinned candidate snapshots and reports (no suite parsing, no verdict, no fail-open).
 */
final class DQClient(spark: SparkSession, val config: DQConfig, api: ServiceApi,
                     versions: () => RuntimeVersions = () => RuntimeProbe.readVersions()) {
  import DQClient._

  private def exec(eid: String, rest: String*): Seq[String] = V2 ++ Seq("executions", eid) ++ rest
  private def key(op: String) = Canonical.sha256((config.runKey + op).getBytes(UTF_8))
  private def hadoopConf = spark.sessionState.newHadoopConf()
  private def stateFile(eid: String, name: String): Path = StateFiles.path(config.stateDir, eid, name)

  def startWrite(partitionValues: Map[String, String], parameters: Map[String, String], eventTime: Instant,
                 origin: String = "SCHEDULED", previousExecutionId: Option[String] = None): ExecutionHandle = {
    val dt = partitionValues.get("dt").filter(_ => partitionValues.size == 1)
      .map(d => LocalDate.parse(d).toString).getOrElse(Json.fail("partition_values"))
    val reg = api.request("GET", V2 ++ Seq("datasets", config.dataset)).path("registration")
    val bases = Json.node()
    Seq("silver", "quarantine").foreach { t =>
      bases.set[JsonNode](t, DeltaRuntime.snapshot(spark, Json.str(reg.path(t), "uri", s"registration.$t")).toJson)
    }
    val body = Json.node()
    body.put("dataset_id", config.dataset).put("workflow", "WRITE_GATE").put("purpose", config.purpose)
      .put("event_time", Timestamp.format(eventTime))
    val params = body.putObject("parameters")
    parameters.toSeq.sortBy(_._1).foreach { case (k, v) => params.put(k, v) }
    body.put("history_context", Json.str(reg, "history_context", "registration"))
    body.putObject("partition_values").put("dt", dt)
    body.putNull("input_snapshot")
    body.set[JsonNode]("target_bases", bases)
    body.put("compat_manifest_id", config.compatManifestId).put("event_origin", origin)
    previousExecutionId.foreach(body.put("previous_execution_id", _))

    begin(body)
  }

  /** AUDIT_TABLE: pins the registered Silver snapshot server-side checks; no candidates, no target writes. */
  def startAudit(inputSnapshot: SnapshotRef, parameters: Map[String, String], eventTime: Instant): ExecutionHandle = {
    val reg = api.request("GET", V2 ++ Seq("datasets", config.dataset)).path("registration")
    val body = Json.node()
    body.put("dataset_id", config.dataset).put("workflow", "AUDIT_TABLE").put("purpose", config.purpose)
      .put("event_time", Timestamp.format(eventTime))
    val params = body.putObject("parameters")
    parameters.toSeq.sortBy(_._1).foreach { case (k, v) => params.put(k, v) }
    body.put("history_context", Json.str(reg, "history_context", "registration"))
    body.putNull("partition_values")
    body.set[JsonNode]("input_snapshot", inputSnapshot.toJson)
    body.putNull("target_bases")
    body.put("compat_manifest_id", config.compatManifestId).put("event_origin", "SCHEDULED")
    begin(body)
  }

  private def begin(body: ObjectNode): ExecutionHandle = {
    val plan = api.request("POST", V2 :+ "executions", body = Some(body), idempotencyKey = Some(key(":start"))).path("plan")
    val eid = Json.str(plan, "execution_id", "plan")
    val attempt = Json.node()
    attempt.put("runner_identity", s"scala:${config.runKey}".take(128))
    config.imageDigest.fold(attempt.putNull("image_digest"))(attempt.put("image_digest", _))
    attempt.putNull("previous_attempt_id")
    attempt.putNull("previous_stop")
    val created = api.request("POST", exec(eid, "attempts"), body = Some(attempt))
    val handle = ExecutionHandle(eid, Json.str(created.path("attempt"), "attempt_id", "attempt"), plan)
    persist(stateFile(eid, s"${handle.attemptId}.handle.json"), Canonical.bytes(handle.toJson))
    announce(handle)
    checkRuntime(handle)
    handle
  }

  /** Launcher contract: when DQ_RUN_STATE_FILE is set, the child records its execution/attempt ids there (0600). */
  private def announce(handle: ExecutionHandle): Unit = sys.env.get("DQ_RUN_STATE_FILE").filter(_.nonEmpty).foreach { f =>
    val file = Paths.get(f)
    Files.deleteIfExists(file)
    Files.createFile(file, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
      java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")))
    val n = Json.node()
    n.put("execution_id", handle.executionId).put("attempt_id", handle.attemptId)
    Files.write(file, Canonical.bytes(n))
  }

  def evaluateAudit(handle: ExecutionHandle, audited: DataFrame): CandidateEvaluation = {
    val plan = handle.plan
    if (Json.str(plan, "workflow", "plan") != "AUDIT_TABLE") Json.fail("plan.workflow")
    val file = stateFile(handle.executionId, s"${handle.attemptId}.report.json")
    if (StateFiles.exists(file, hadoopConf)) return CandidateEvaluation.fromJson({
      val n = Json.node()
      n.set[JsonNode]("handle", handle.toJson)
      n.put("report_file", file.toString)
      n
    }, hadoopConf)
    val ref = SnapshotRef.fromJson(plan.get("input_snapshot"), "plan.input_snapshot")
    val rules = PlanRules.fromPlan(plan).filter(_.stage == "AUDIT_SNAPSHOT")
    val results = measure(rules, audited, handle, "audit")

    val report = Json.node()
    report.put("attempt_id", handle.attemptId).put("dataset_id", Json.str(plan, "dataset_id", "plan"))
      .put("plan_hash", Json.str(plan, "plan_hash", "plan"))
      .put("suite_revision_id", Json.str(plan.path("suite_revision"), "revision_id", "plan.suite_revision"))
      .put("policy_revision_id", Json.str(plan.path("policy_revision"), "revision_id", "plan.policy_revision"))
      .put("engine", Engine).put("interpreter_version", InterpreterVersion)
    val conf = report.putObject("runtime_conf")
    RuntimeProfile.Conf.toSeq.sortBy(_._1).foreach { case (k, v) => conf.put(k, v) }
    report.set[JsonNode]("runtime_versions", versions().toJson)
    report.set[JsonNode]("parameters", plan.path("parameters").deepCopy[JsonNode]())
    report.set[JsonNode]("audit_reference", ref.toJson)
    report.put("measured_rows", audited.count()).put("classification_status", "NOT_APPLICABLE")
    val resultsNode = report.putArray("results")
    results.sortBy(_.get("rule_id").textValue()).foreach(resultsNode.add)
    persist(file, Canonical.bytes(report))
    CandidateEvaluation(handle, report, file.toString)
  }

  /** Runs before any transform/materialization; a mismatch retires the attempt (process still alive: UNCONFIRMED). */
  private def checkRuntime(handle: ExecutionHandle): Unit = {
    val manifest = handle.plan.path("compat_manifest").path("payload").deepCopy[ObjectNode]()
    manifest.remove("format")
    val err = RuntimeProbe.compareConf(spark.conf.getOption)
      .orElse(RuntimeProbe.compare(RuntimeVersions.fromJson(manifest, "compat_manifest"), versions()))
    err.foreach { e =>
      failAttempt(handle, e)
      throw new DQFailedException(e.code)
    }
  }

  def failAttempt(handle: ExecutionHandle, error: ClientError): Unit = {
    val body = Json.node()
    body.put("error_category", error.code).set[JsonNode]("error", error.toJson)
    val stop = body.putObject("process_stop")
    stop.put("confirmed_stopped", false).putNull("reference")
    api.request("POST", exec(handle.executionId, "attempts", handle.attemptId, "fail"), body = Some(body))
  }

  def evaluate(handle: ExecutionHandle, candidate: DataFrame): CandidateEvaluation = {
    val plan = handle.plan
    if (Json.str(plan, "workflow", "plan") != "WRITE_GATE") Json.fail("plan.workflow")
    val file = stateFile(handle.executionId, s"${handle.attemptId}.report.json")
    if (StateFiles.exists(file, hadoopConf)) return resume(handle, file)

    val dt = Json.str(plan.path("partition_values"), "dt", "plan.partition_values")
    if (candidate.columns.exists(_.startsWith(Classify.Prefix))) Json.fail("candidate.columns")
    if (!candidate.columns.exists(_.equalsIgnoreCase("dt"))) Json.fail("candidate.dt")
    if (!candidate.where(col("dt").isNull || col("dt") =!= lit(dt).cast("date")).isEmpty)
      throw new ProtocolException("PARTITION_MISMATCH", "dt")

    val root = s"${Json.str(plan.path("candidate_locations"), "execution_root", "plan.candidate_locations")}/${handle.attemptId}"
    val rawUri = s"$root/raw"
    try candidate.write.format("delta").mode("errorifexists").save(rawUri)
    catch { case _: AnalysisException => throw new ProtocolException("CANDIDATE_EXISTS", "raw") }
    val rawRef = DeltaRuntime.snapshot(spark, rawUri)
    if (rawRef.version != 0L) throw new ProtocolException("CANDIDATE_EXISTS", "raw")
    val raw = pinned(rawRef)
    val businessSchema = StructType(raw.schema.fields.filterNot(_.name.startsWith(Classify.Prefix)))

    val rules = PlanRules.fromPlan(plan)
    def stage(s: String) = rules.filter(_.stage == s)
    val quarantineIds = rules.filter(r => r.action == "QUARANTINE" && r.stage == "CANDIDATE").map(_.ruleId).sorted
    val results = scala.collection.mutable.ArrayBuffer[ObjectNode]()
    results ++= measure(stage("RAW"), raw, handle, "raw")

    val classifiedRef = withView(raw, handle, "tag") { view =>
      Try(Classify.tag(spark, rules, raw, view)).map { tagged =>
        tagged.write.format("delta").mode("errorifexists").save(s"$root/classified")
        DeltaRuntime.snapshot(spark, s"$root/classified")
      }
    }

    val report = Json.node()
    report.put("attempt_id", handle.attemptId).put("dataset_id", Json.str(plan, "dataset_id", "plan"))
      .put("plan_hash", Json.str(plan, "plan_hash", "plan"))
      .put("suite_revision_id", Json.str(plan.path("suite_revision"), "revision_id", "plan.suite_revision"))
      .put("policy_revision_id", Json.str(plan.path("policy_revision"), "revision_id", "plan.policy_revision"))
      .put("engine", Engine).put("interpreter_version", InterpreterVersion)
    val conf = report.putObject("runtime_conf")
    RuntimeProfile.Conf.toSeq.sortBy(_._1).foreach { case (k, v) => conf.put(k, v) }
    report.set[JsonNode]("runtime_versions", versions().toJson)
    report.set[JsonNode]("parameters", plan.path("parameters").deepCopy[JsonNode]())
    report.set[JsonNode]("raw_reference", rawRef.toJson)
    report.put("business_schema_hash", SchemaHash.of(businessSchema))

    val errors = Canonical.mapper.createArrayNode()
    classifiedRef match {
      case Success(ref) =>
        if (ref.version != 0L) throw new ProtocolException("CANDIDATE_EXISTS", "classified")
        val classified = pinned(ref)
        val counts = Classify.counts(classified, quarantineIds)
        results ++= measure(stage("CANDIDATE"), Classify.business(classified), handle, "candidate")
          .map(r => withFailedRows(r, counts.perRule))
        results ++= measure(stage("CLEAN"), Classify.clean(classified), handle, "clean")
        report.set[JsonNode]("classified_reference", ref.toJson)
        report.put("classified_schema_hash", SchemaHash.of(classified.schema))
        report.put("classification_status", "COMPLETE").put("candidate_rows", counts.candidate)
          .put("valid_rows", counts.valid).put("quarantine_rows", counts.quarantine)
          .put("clean_selector", CleanSelector).put("quarantine_selector", QuarantineSelector)
      case Failure(e) if NonFatal(e) =>
        // no classification: candidate-stage rules still measure the raw rows, clean-stage rules cannot be measured
        results ++= measure(stage("CANDIDATE"), raw, handle, "candidate")
        stage("CLEAN").foreach(r => results += Measure.error(r, ClientError("CANDIDATE_UNAVAILABLE", Map.empty)))
        errors.add(classificationError(e).toJson)
        report.putNull("classified_reference")
        report.putNull("classified_schema_hash")
        report.put("classification_status", "INCOMPLETE").put("candidate_rows", raw.count())
        report.putNull("valid_rows")
        report.putNull("quarantine_rows")
        report.putNull("clean_selector")
        report.putNull("quarantine_selector")
      case Failure(e) => throw e
    }
    report.set[JsonNode]("errors", errors)
    val resultsNode = report.putArray("results")
    results.sortBy(_.get("rule_id").textValue()).foreach(resultsNode.add)

    persist(file, Canonical.bytes(report))
    CandidateEvaluation(handle, report, file.toString)
  }

  /** Same attempt retried after materialization: reuse the stored evaluation, never rewrite candidates. */
  private def resume(handle: ExecutionHandle, file: Path): CandidateEvaluation = {
    val ev = CandidateEvaluation.fromJson({
      val n = Json.node()
      n.set[JsonNode]("handle", handle.toJson)
      n.put("report_file", file.toString)
      n
    }, hadoopConf)
    DeltaRuntime.verify(spark, SnapshotRef.fromJson(ev.report.get("raw_reference"), "raw_reference"))
    Option(ev.report.get("classified_reference")).filterNot(_.isNull)
      .foreach(r => DeltaRuntime.verify(spark, SnapshotRef.fromJson(r, "classified_reference")))
    ev
  }

  def submit(evaluation: CandidateEvaluation): JsonNode = {
    val eid = evaluation.handle.executionId
    val body = Canonical.parse(new String(StateFiles.read(new Path(evaluation.reportFile), hadoopConf), UTF_8))
    def post() = api.request("POST", exec(eid, "reports"), body = Some(body))
    try post()
    catch {
      case e: ServiceException if e.code == "SERVICE_UNAVAILABLE" =>
        // response may have been lost after acceptance: the stored report decides whether to resend
        val hash = Canonical.hash(body)
        val stored = try Some(api.request("GET", exec(eid, "report"))) catch {
          case s: ServiceException if s.code == "SERVICE_UNAVAILABLE" => None
        }
        stored.map(_.path("accepted")).filter(a => a.path("report_hash").asText() == hash).getOrElse(post())
    }
  }

  def publish(evaluation: CandidateEvaluation, accepted: JsonNode): PublicationResult =
    new DeltaPublisher(spark, outcomeApiOf(api)).publish(evaluation.handle.plan, accepted)

  private def pinned(ref: SnapshotRef): DataFrame =
    spark.read.format("delta").option("versionAsOf", ref.version).load(ref.uri)

  /** Registers `df` as the `${batch}` temp view; dropped via the catalog so the user's cached plan stays intact. */
  private def withView[T](df: DataFrame, handle: ExecutionHandle, tag: String)(f: String => T): T = {
    val name = s"dq_batch_${handle.attemptId.replace("-", "")}_$tag"
    df.createOrReplaceTempView(name)
    try f(name) finally spark.sessionState.catalog.dropTempView(name)
  }

  private def measure(rules: Seq[CompiledRule], data: DataFrame, handle: ExecutionHandle, tag: String): Seq[ObjectNode] =
    if (rules.isEmpty) Nil else withView(data, handle, tag)(view => Measure.run(spark, rules, data, view))

  private def withFailedRows(r: ObjectNode, perRule: Map[String, Long]): ObjectNode = {
    val id = r.get("rule_id").textValue()
    if (r.get("measurement_status").textValue() == "OK" && FailedRowKinds(r.get("kind").textValue()))
      perRule.get(id).foreach(r.put("failed_rows", _))
    r
  }

  private def classificationError(t: Throwable): ClientError = t match {
    case p: ProtocolException if p.code == "CLASSIFICATION_INCOMPLETE" =>
      ClientError.unclassified(p, Some(p.field))
    case a: AnalysisException => ClientError.withClass("SQL_ANALYSIS_ERROR", a)
    case other                => ClientError.unclassified(other)
  }

  private def persist(file: Path, bytes: Array[Byte]): Unit = StateFiles.write(file, bytes, hadoopConf)
}

object DQClient {
  val V2: Seq[String] = Seq("api", "v2")
  val InterpreterVersion = "dq-client-0.1.0"
  val CleanSelector = "size(_dq_errors)=0"
  val QuarantineSelector = "size(_dq_errors)>0"
  private val FailedRowKinds = Set("complete", "completeness", "unique", "pattern", "range", "in_set", "sql_predicate")
  private val Timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC)

  /** e.g. deequ-2.0.21-spark-3.5, from dq-client.properties (filled by Maven at build time). */
  lazy val Engine: String = {
    val props = new Properties()
    Option(getClass.getResourceAsStream("/dq-client.properties")).foreach { in =>
      try props.load(in) finally in.close()
    }
    s"deequ-${props.getProperty("deequ.version", "unknown")}"
  }

  /** Adapts the HTTP service to the publisher's transport: HTTP 409 becomes OutcomeConflict(code). */
  def outcomeApiOf(api: ServiceApi): OutcomeApi = new OutcomeApi {
    override def getExecution(executionId: String): JsonNode =
      api.request("GET", V2 ++ Seq("executions", executionId))

    override def putOutcome(executionId: String, destination: String, body: ObjectNode): JsonNode =
      try api.request("PUT", V2 ++ Seq("executions", executionId, "write-outcomes", destination), body = Some(body))
      catch { case e: ServiceException if e.status == 409 => throw new OutcomeConflict(e.code) }
  }

  def fromSession(spark: SparkSession): DQClient = {
    val config = DQConfig.fromSpark(spark)
    val token = sys.env.getOrElse("DQ_API_TOKEN", "")
    new DQClient(spark, config, new HttpServiceApi(config.serviceUrl, token, Duration.ofSeconds(config.timeoutSec.toLong)))
  }
}
