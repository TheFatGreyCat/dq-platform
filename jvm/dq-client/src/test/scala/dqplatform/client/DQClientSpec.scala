package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable

/** DQClient against an in-memory service double: real Delta tables, real Spark, consumer-visible results. */
class DQClientSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val Hash0 = "sha256:" + "0" * 64
  private val ExecId = UUID.randomUUID().toString
  private val AttemptId = UUID.randomUUID().toString

  private final class Fixture(failPostOnce: Boolean = false, acceptedBeforeLoss: Boolean = true) {
    val dir = Files.createTempDirectory("dqclient").toString
    val posted = mutable.Buffer[JsonNode]()
    val calls = mutable.Buffer[String]()
    var stored: Option[(JsonNode, JsonNode)] = None
    var failPost = failPostOnce

    private def table(name: String): String = {
      val uri = s"$dir/$name"
      import spark.implicits._
      Seq(("x", 1L)).toDF("v", "id").write.format("delta").save(uri)
      uri
    }
    val silver = table("silver")
    val quarantine = table("quarantine")

    def rule(id: String, check: String, stage: String, action: String, name: String, vt: String): JsonNode = {
      val r = Json.node()
      r.put("rule_id", id).put("metric_definition_hash", Hash0).put("measurement_definition_hash", Hash0)
      r.put("metric_name", name).put("value_type", vt)
      val c = Canonical.parse(check)
      r.put("kind", c.get("kind").asText()).set[JsonNode]("resolved_check", c)
      r.put("action", action).put("required", true)
      r
    }

    def binding(stage: String, role: String, rules: JsonNode*): JsonNode = {
      val b = Json.node()
      b.put("scope", "PARTITION").put("stage", stage).put("reference_role", role)
      val a = b.putArray("rules"); rules.foreach(a.add)
      b
    }

    def plan(manifest: RuntimeVersions): JsonNode = {
      val p = Json.node()
      p.put("execution_id", ExecId).put("dataset_id", "ds").put("workflow", "WRITE_GATE").put("plan_hash", Hash0)
      p.putObject("suite_revision").put("revision_id", UUID.randomUUID().toString)
      p.putObject("policy_revision").put("revision_id", UUID.randomUUID().toString)
      p.putObject("partition_values").put("dt", "2026-10-20")
      p.putObject("parameters").put("k", "v")
      p.putObject("candidate_locations").put("execution_root", s"$dir/cand/$ExecId")
      val m = manifest.toJson; m.put("format", "dq-compat-v1")
      p.putObject("compat_manifest").set[JsonNode]("payload", m)
      val sb = p.putArray("stage_bindings")
      sb.add(binding("RAW", "raw", rule("raw_rows", """{"kind":"size"}""", "RAW", "OBSERVE", "Size", "INTEGER")))
      sb.add(binding("CANDIDATE", "candidate",
        rule("amount_range", """{"kind":"range","column":"amount","min":"0","max":"100"}""", "CANDIDATE", "QUARANTINE", "Compliance", "DECIMAL"),
        rule("event_in_partition", """{"kind":"sql_predicate","columns":["event_time"],"predicate":"event_time IS NOT NULL AND event_time >= TIMESTAMP '2026-10-20 00:00:00' AND event_time < TIMESTAMP '2026-10-21 00:00:00'"}""", "CANDIDATE", "QUARANTINE", "Compliance", "DECIMAL"),
        rule("value_complete", """{"kind":"complete","column":"value"}""", "CANDIDATE", "QUARANTINE", "Completeness", "DECIMAL")))
      sb.add(binding("CLEAN", "classified_clean", rule("clean_rows", """{"kind":"size"}""", "CLEAN", "BLOCK", "Size", "INTEGER")))
      p
    }

    // test classpath runs from class directories, so the real jar probe is replaced by a fixed attestation
    var manifest: RuntimeVersions = RuntimeVersions("3.5.1", "2.12.18", 17L, "3.3.4",
      RuntimeVersions.JarKeys.map(k => k -> ("sha256:" + "a" * 64)).toMap)

    val api: ServiceApi = new ServiceApi {
      def request(method: String, path: Seq[String], query: Map[String, String], body: Option[JsonNode],
                  idem: Option[String]): JsonNode = {
        calls += s"$method /${path.drop(2).mkString("/")}"
        val n = Json.node()
        (method, path.drop(2)) match {
          case ("GET", Seq("datasets", _)) =>
            val reg = n.putObject("registration")
            reg.putObject("silver").put("uri", silver)
            reg.putObject("quarantine").put("uri", quarantine)
            reg.put("history_context", "ctx")
            n
          case ("POST", Seq("executions")) =>
            body.get.get("partition_values").get("dt").asText() shouldBe "2026-10-20"
            body.get.get("target_bases").get("silver").get("version").asLong() shouldBe 0L
            n.set[JsonNode]("plan", plan(manifest)); n
          case ("POST", Seq("executions", _, "attempts")) =>
            n.putObject("attempt").put("attempt_id", AttemptId); n
          case ("POST", Seq("executions", _, "attempts", _, "fail")) => posted += body.get; n
          case ("POST", Seq("executions", _, "reports")) =>
            val b = Canonical.parse(Canonical.string(body.get))
            if (failPost) { failPost = false; if (acceptedBeforeLoss) stored = Some(b -> accepted(b)); throw new ServiceException("SERVICE_UNAVAILABLE", 0) }
            posted += b
            accepted(b)
          case ("GET", Seq("executions", _, "report")) =>
            stored match {
              case Some((_, a)) => n.set[JsonNode]("accepted", a); n
              case None => throw new ServiceException("SERVICE_UNAVAILABLE", 0)
            }
        }
      }
      private def accepted(b: JsonNode): JsonNode = {
        val a = Json.node(); a.put("report_hash", Canonical.hash(b)); a.putNull("decision"); a
      }
    }

    def client(versions: () => RuntimeVersions = () => manifest) =
      new DQClient(spark, DQConfig("http://unused", 5, "ds", "default", UUID.randomUUID().toString, "run-1", None, s"$dir/state"),
        api, versions)
  }

  private def source: DataFrame = {
    import spark.implicits._
    (1 to 100).map { i =>
      (i.toLong, if (i == 1 || i == 2) null else "ok", if (i == 1 || i == 3) BigDecimal(-1) else BigDecimal(10),
        if (i == 1) "2026-10-19 23:00:00" else "2026-10-20 12:00:00")
    }.toDF("id", "value", "amount", "ts")
      .select(col("id"), col("value"), col("amount").cast("decimal(18,2)").as("amount"),
        to_timestamp(col("ts")).as("event_time"), to_date(lit("2026-10-20")).as("dt"))
  }

  private def results(report: JsonNode): Map[String, JsonNode] =
    report.get("results").elements().asScala.map(r => r.get("rule_id").asText() -> r).toMap

  private def pinned(ref: JsonNode): DataFrame =
    spark.read.format("delta").option("versionAsOf", ref.get("version").asLong()).load(ref.get("uri").asText())

  test("evaluate pins raw/classified candidates and reports exact counts; submit sends the stored bytes") {
    val f = new Fixture()
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val ev = c.evaluate(h, source)
    val r = ev.report

    r.get("candidate_rows").asLong() shouldBe 100L
    r.get("valid_rows").asLong() shouldBe 97L
    r.get("quarantine_rows").asLong() shouldBe 3L
    r.get("classification_status").asText() shouldBe "COMPLETE"
    r.get("clean_selector").asText() shouldBe "size(_dq_errors)=0"
    r.get("raw_reference").get("uri").asText() shouldBe s"${f.dir}/cand/$ExecId/$AttemptId/raw"
    r.get("classified_reference").get("uri").asText() shouldBe s"${f.dir}/cand/$ExecId/$AttemptId/classified"
    r.get("raw_reference").get("version").asLong() shouldBe 0L

    val res = results(r)
    res("raw_rows").get("metric_value").asLong() shouldBe 100L
    res("clean_rows").get("metric_value").asLong() shouldBe 97L
    res("amount_range").get("failed_rows").asLong() shouldBe 2L
    res("value_complete").get("failed_rows").asLong() shouldBe 2L
    res("event_in_partition").get("failed_rows").asLong() shouldBe 1L

    val classified = pinned(r.get("classified_reference"))
    classified.filter("size(_dq_errors) > 0").select("id").collect().map(_.getLong(0)).toSet shouldBe Set(1L, 2L, 3L)
    classified.filter("id = 1").head().getSeq[String](classified.columns.indexOf("_dq_errors")) shouldBe
      Seq("amount_range", "event_in_partition", "value_complete")

    val accepted = c.submit(ev)
    f.posted.last.toString shouldBe Canonical.parse(new String(Files.readAllBytes(Paths.get(ev.reportFile)), "UTF-8")).toString
    accepted.get("report_hash").asText() shouldBe Canonical.hash(r)
    spark.sessionState.catalog.listLocalTempViews("*").map(_.table).filter(_.startsWith("dq_batch_")) shouldBe empty
  }

  test("retrying evaluate for the same attempt reuses the stored candidate; it never rewrites") {
    val f = new Fixture()
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val first = c.evaluate(h, source)
    val again = c.evaluate(h, source.limit(5))
    again.report.get("candidate_rows").asLong() shouldBe 100L
    Canonical.bytes(again.report) shouldBe Canonical.bytes(first.report)
    pinned(again.report.get("raw_reference")).count() shouldBe 100L
  }

  test("lost response after acceptance: submit reads the stored report instead of resending") {
    val f = new Fixture(failPostOnce = true)
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val ev = c.evaluate(h, source)
    val accepted = c.submit(ev)
    accepted.get("report_hash").asText() shouldBe Canonical.hash(ev.report)
    f.posted.filter(_.has("results")) shouldBe empty
    f.calls.count(_ == "POST /executions/" + ExecId + "/reports") shouldBe 1
  }

  test("service unreachable and nothing stored: identical bytes are resent once") {
    val f = new Fixture(failPostOnce = true, acceptedBeforeLoss = false)
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val ev = c.evaluate(h, source)
    c.submit(ev).get("report_hash").asText() shouldBe Canonical.hash(ev.report)
    f.posted.size shouldBe 1
  }

  test("runtime mismatch retires the attempt before any candidate is materialized") {
    val f = new Fixture()
    val bad = f.manifest.copy(jars = f.manifest.jars.updated("deequ", "sha256:" + "f" * 64))
    val c = f.client(() => bad)
    val e = intercept[DQFailedException](
      c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z")))
    e.code shouldBe "RUNTIME_MISMATCH"
    val fail = f.posted.last
    fail.get("error").get("params").get("component").asText() shouldBe "jars.deequ"
    fail.get("process_stop").get("confirmed_stopped").asBoolean() shouldBe false
    Files.exists(Paths.get(s"${f.dir}/cand")) shouldBe false
  }

  test("candidate with another partition is refused before writing") {
    val f = new Fixture()
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val wrong = source.withColumn("dt", to_date(lit("2026-10-21")))
    intercept[ProtocolException](c.evaluate(h, wrong)).code shouldBe "PARTITION_MISMATCH"
    Files.exists(Paths.get(s"${f.dir}/cand")) shouldBe false
  }

  test("PyBridge round-trips handle and evaluation through JSON") {
    val f = new Fixture()
    val c = f.client()
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val ev = c.evaluate(h, source)
    val again = CandidateEvaluation.fromJson(Canonical.parse(Canonical.string(ev.toJson)), spark.sessionState.newHadoopConf())
    Canonical.bytes(again.handle.toJson) shouldBe Canonical.bytes(ev.handle.toJson)
    Canonical.bytes(again.report) shouldBe Canonical.bytes(ev.report)
  }

  test("file:// state dir: evaluate resumes the stored report and submit posts its exact bytes") {
    val f = new Fixture()
    val stateDir = Paths.get(f.dir, "uri-state").toUri.toString.stripSuffix("/")
    val c = new DQClient(spark, DQConfig("http://unused", 5, "ds", "default", UUID.randomUUID().toString, "run-1", None, stateDir),
      f.api, () => f.manifest)
    val h = c.startWrite(Map("dt" -> "2026-10-20"), Map("k" -> "v"), java.time.Instant.parse("2026-10-20T12:00:00Z"))
    val first = c.evaluate(h, source)
    first.reportFile should startWith (new org.apache.hadoop.fs.Path(stateDir).toString)
    val again = c.evaluate(h, source.limit(5))
    Canonical.bytes(again.report) shouldBe Canonical.bytes(first.report)
    c.submit(again)
    val stored = Files.readAllBytes(Paths.get(new java.net.URI(first.reportFile)))
    Canonical.bytes(f.posted.last) shouldBe stored
    Files.list(Paths.get(new java.net.URI(first.reportFile)).getParent).iterator().asScala
      .map(_.getFileName.toString).filter(_.startsWith(".tmp")).toList shouldBe empty
  }
}
