package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import scala.collection.mutable

class DeltaPublisherSpec extends AnyFunSuite with SparkTestSession {

  /** In-memory service applying the same transition rules as dq-service/store.py `_check_transition` (job role). */
  private final class FakeApi(plan: JsonNode) extends OutcomeApi {
    val outcomes = mutable.Map("quarantine" -> ("NOT_STARTED", 0L), "silver" -> ("NOT_STARTED", 0L))
    val commits = mutable.Map[String, JsonNode]()
    val history = mutable.Buffer[(String, String)]()
    var fault: Option[(String, String)] = None
    private def conflict(code: String) = throw new OutcomeConflict(code)
    def getExecution(id: String): JsonNode = {
      val n = Json.node()
      n.put("execution_id", id).put("state", "REPORTED")
      n.set[JsonNode]("plan", plan)
      val o = n.putObject("outcomes")
      outcomes.foreach { case (d, (s, v)) =>
        val x = o.putObject(d); x.put("state", s).put("outcome_version", v)
        commits.get(d) match { case Some(c) => x.set[JsonNode]("commit", c); case None => x.putNull("commit") }
      }
      n
    }
    def putOutcome(id: String, dest: String, body: ObjectNode): JsonNode = {
      val to = body.get("state").textValue()
      if (fault.contains((dest, to))) { fault = None; throw new RuntimeException("lost response") }
      val (from, v) = outcomes(dest)
      if (body.get("expected_outcome_version").longValue() != v) conflict("CONTENT_CONFLICT")
      val rec = body.path("recovery")
      val baseVersion = plan.get("target_bases").get(dest).get("version").longValue()
      val tableId = plan.get("target_bases").get(dest).get("table_id").textValue()
      def absence() = if (!rec.path("absence").isObject || rec.path("absence").get("table_id").textValue() != tableId ||
        rec.path("absence").get("checked_through_version").longValue() < baseVersion) conflict("INVALID_CONFIG")
      val stopped = rec.path("writer_stop").path("confirmed_stopped").asBoolean(false)
      to match {
        case "IN_PROGRESS" =>
          if (body.path("intent").path("txn_app_id").asText() != s"dq:$id:$dest" || body.path("intent").path("txn_version").asLong() != 1L)
            conflict("PLAN_MISMATCH")
          if (dest == "silver" && !Set("SUCCEEDED", "NOT_APPLICABLE")(outcomes("quarantine")._1)) conflict("STATE_CONFLICT")
          if (from != "NOT_STARTED") {
            if (!Set("FAILED", "UNKNOWN")(from)) conflict("STATE_CONFLICT")
            if (from == "UNKNOWN" && !stopped) conflict("WRITER_UNCONFIRMED")
            absence()
            if (rec.path("current_base_version").longValue() != baseVersion) conflict("BASE_CHANGED")
          }
        case "SUCCEEDED" =>
          if (!Set("IN_PROGRESS", "UNKNOWN")(from)) conflict("STATE_CONFLICT")
          val c = body.get("commit")
          if (c.get("table_id").textValue() != tableId || c.get("evidence").textValue() != "EXACT" || !body.get("rows").isIntegralNumber)
            conflict("PLAN_MISMATCH")
          commits(dest) = c
        case "FAILED" =>
          if (!Set("IN_PROGRESS", "UNKNOWN")(from)) conflict("STATE_CONFLICT")
          absence()
          if (from == "UNKNOWN" && !stopped) conflict("WRITER_UNCONFIRMED")
        case "UNKNOWN" => if (from != "IN_PROGRESS") conflict("STATE_CONFLICT")
      }
      outcomes(dest) = (to, v + 1)
      history += ((dest, to))
      val r = Json.node(); r.putObject("outcome").put("outcome_version", v + 1); r
    }
  }

  private final class Fixture {
    val root = Files.createTempDirectory("pub").toString
    val s = spark; import s.implicits._
    val d19 = java.sql.Date.valueOf("2026-10-19"); val d20 = java.sql.Date.valueOf("2026-10-20")
    val clsUri = s"$root/classified"; val silUri = s"$root/silver"; val quaUri = s"$root/quarantine"
    Seq((1L, "a", d20, Seq("r1")), (2L, "b", d20, Seq("r2")), (3L, "c", d20, Seq.empty[String]),
      (4L, "d", d20, Seq.empty[String]), (5L, "e", d20, Seq.empty[String]))
      .toDF("id", "value", "dt", "_dq_errors").write.format("delta").save(clsUri)
    Seq((9001L, "old", d19), (9002L, "old", d20)).toDF("id", "value", "dt")
      .write.format("delta").partitionBy("dt").save(silUri)
    Seq((9003L, "old", d19, Seq("r1"), "x", 1L, new java.sql.Timestamp(0L)))
      .toDF("id", "value", "dt", "_dq_errors", "_dq_execution_id", "_dq_candidate_version", "_dq_quarantined_at")
      .write.format("delta").partitionBy("dt").save(quaUri)

    def plan: ObjectNode = {
      val p = Json.node()
      p.put("execution_id", "11111111-1111-1111-1111-111111111111").put("plan_hash", "sha256:" + "a" * 64)
        .put("execution_timestamp", "2026-10-20T12:00:00.000000Z").put("partition_predicate", "dt = DATE '2026-10-20'")
      p.putObject("partition_values").put("dt", "2026-10-20")
      val b = p.putObject("target_bases")
      b.set[JsonNode]("silver", DeltaRuntime.snapshot(spark, silUri).toJson)
      b.set[JsonNode]("quarantine", DeltaRuntime.snapshot(spark, quaUri).toJson)
      p
    }
    def accepted(p: JsonNode, decision: String = "ALLOW"): ObjectNode = {
      val a = Json.node()
      a.put("execution_id", p.get("execution_id").textValue()).put("report_hash", "sha256:" + "b" * 64)
      val d = a.putObject("decision")
      d.put("decision_id", "22222222-2222-2222-2222-222222222222").put("decision", decision)
        .put("plan_hash", p.get("plan_hash").textValue())
      d.set[JsonNode]("classified_reference", DeltaRuntime.snapshot(spark, clsUri).toJson)
      a
    }
    def ids(uri: String): Set[Long] = spark.read.format("delta").load(uri).select("id").as[Long].collect().toSet
    def ids(uri: String, dt: java.sql.Date): Set[Long] =
      spark.read.format("delta").load(uri).filter($"dt" === dt).select("id").as[Long].collect().toSet
    def version(uri: String): Long = DeltaRuntime.snapshot(spark, uri).version
  }

  test("publishes quarantine then silver exactly once; rerun is a no-op; other partition untouched") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    val pub = new DeltaPublisher(spark, api)
    val r = pub.publish(p, f.accepted(p))
    assert(r.quarantine.evidence.contains("EXACT") && r.silver.evidence.contains("EXACT"))
    assert(f.ids(f.silUri, f.d20) == Set(3L, 4L, 5L))
    assert(f.ids(f.silUri, f.d19) == Set(9001L))
    assert(f.ids(f.quaUri) == Set(9003L, 1L, 2L))
    val (sv, qv) = (f.version(f.silUri), f.version(f.quaUri))
    val again = pub.publish(p, f.accepted(p))
    assert(again == r && f.version(f.silUri) == sv && f.version(f.quaUri) == qv)
  }

  test("base changed on silver fails closed without writing silver") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    import f.s.implicits._
    Seq((7L, "foreign", f.d20)).toDF("id", "value", "dt").write.format("delta").mode("append").save(f.silUri)
    val sv = f.version(f.silUri)
    val e = intercept[OutcomeConflict](new DeltaPublisher(spark, api).publish(p, f.accepted(p)))
    assert(e.code == "BASE_CHANGED")
    assert(f.version(f.silUri) == sv)
    assert(api.outcomes("quarantine")._1 == "SUCCEEDED" && api.outcomes("silver")._1 == "NOT_STARTED")
  }

  test("lost SUCCEEDED callback: next publish finds the own exact commit and does not append again") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    api.fault = Some(("quarantine", "SUCCEEDED"))
    val pub = new DeltaPublisher(spark, api)
    intercept[RuntimeException](pub.publish(p, f.accepted(p)))
    val qv = f.version(f.quaUri)
    assert(api.outcomes("quarantine")._1 == "IN_PROGRESS")
    val r = pub.publish(p, f.accepted(p))
    assert(r.quarantine.evidence.contains("EXACT") && r.silver.evidence.contains("EXACT"))
    assert(f.version(f.quaUri) == qv)
    assert(f.ids(f.quaUri) == Set(9003L, 1L, 2L))
  }

  test("DENY decision refuses and writes nothing") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    val (sv, qv) = (f.version(f.silUri), f.version(f.quaUri))
    val e = intercept[ProtocolException](new DeltaPublisher(spark, api).publish(p, f.accepted(p, "DENY")))
    assert(e.code == "NOT_ALLOWED")
    assert(f.version(f.silUri) == sv && f.version(f.quaUri) == qv)
  }

  test("fault after silver commit before PUT: reconcile records the EXACT existing version, no new commit") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    api.fault = Some(("silver", "SUCCEEDED"))
    val pub = new DeltaPublisher(spark, api)
    intercept[RuntimeException](pub.publish(p, f.accepted(p)))
    val sv = f.version(f.silUri); val qv = f.version(f.quaUri)
    assert(sv == 1L && api.outcomes("silver")._1 == "IN_PROGRESS" && api.outcomes("quarantine")._1 == "SUCCEEDED")
    val r = pub.reconcile(p, f.accepted(p))
    assert(r.silver.evidence.contains("EXACT") && r.silver.commitVersion.contains(sv))
    assert(f.version(f.silUri) == sv && f.version(f.quaUri) == qv)
    assert(f.ids(f.silUri, f.d20) == Set(3L, 4L, 5L))
    val tx = DeltaRuntime.findTransaction(spark, f.silUri, s"dq:${p.get("execution_id").textValue()}:silver")
    assert(tx.commits.map(_.version) == Seq(sv) && tx.snapshotTxnVersion.contains(1L))
  }

  test("candidate files deleted before publish: CANDIDATE_UNAVAILABLE, zero target commits") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p); val acc = f.accepted(p)
    new java.io.File(f.clsUri).listFiles().filter(_.getName.endsWith(".parquet")).foreach(_.delete())
    val (sv, qv) = (f.version(f.silUri), f.version(f.quaUri))
    val e = intercept[ProtocolException](new DeltaPublisher(spark, api).publish(p, acc))
    assert(e.code == "CANDIDATE_UNAVAILABLE")
    assert(f.version(f.silUri) == sv && f.version(f.quaUri) == qv)
    assert(api.outcomes("quarantine")._1 == "FAILED" && api.outcomes("silver")._1 == "NOT_STARTED")
  }

  test("exception around the write is UNKNOWN, never FAILED, and is not retried automatically") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    p.put("partition_predicate", "no_such_column = 1")
    val pub = new DeltaPublisher(spark, api)
    intercept[Exception](pub.publish(p, f.accepted(p)))
    assert(api.outcomes("silver")._1 == "UNKNOWN" && api.outcomes("quarantine")._1 == "SUCCEEDED")
    val (sv, qv) = (f.version(f.silUri), f.version(f.quaUri))
    val r = pub.reconcile(p, f.accepted(p))
    assert(r.silver.state == "UNKNOWN" && r.silver.evidence.isEmpty)
    assert(f.version(f.silUri) == sv && f.version(f.quaUri) == qv)
  }

  test("proven absence + confirmed writer stop resumes the same intent once; without stop nothing is written") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    api.outcomes("quarantine") = ("IN_PROGRESS", 1L)
    val pub = new DeltaPublisher(spark, api)
    val qv = f.version(f.quaUri)
    assert(pub.reconcile(p, f.accepted(p)).quarantine.state == "IN_PROGRESS")
    assert(f.version(f.quaUri) == qv)
    val r = pub.reconcile(p, f.accepted(p), Some("local-process:run:1:exit=137"))
    assert(r.quarantine.evidence.contains("EXACT") && f.version(f.quaUri) == qv + 1)
    assert(f.ids(f.quaUri) == Set(9003L, 1L, 2L))
    assert(pub.reconcile(p, f.accepted(p), Some("x")).quarantine.commitVersion.contains(qv + 1))
    assert(f.version(f.quaUri) == qv + 1)
  }

  test("own token with foreign content is PLAN_MISMATCH and never PUT SUCCEEDED") {
    val f = new Fixture; val p = f.plan; val api = new FakeApi(p)
    val id = p.get("execution_id").textValue()
    import f.s.implicits._
    Seq((9010L, "x", f.d20, Seq("r"), "x", 1L, new java.sql.Timestamp(0L)))
      .toDF("id", "value", "dt", "_dq_errors", "_dq_execution_id", "_dq_candidate_version", "_dq_quarantined_at")
      .write.format("delta").mode("append").option("txnAppId", s"dq:$id:quarantine").option("txnVersion", 1L)
      .option("userMetadata", "other").save(f.quaUri)
    api.outcomes("quarantine") = ("UNKNOWN", 1L)
    val qv = f.version(f.quaUri)
    val e = intercept[ProtocolException](new DeltaPublisher(spark, api).reconcile(p, f.accepted(p)))
    assert(e.code == "PLAN_MISMATCH" && f.version(f.quaUri) == qv)
    assert(api.history.forall(_._2 != "SUCCEEDED"))
  }
}
