package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}

import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable

/** Real Spark + Delta + DQClient against an in-process service double (gate decision injected, outcomes CAS-free). */
trait JobsFixture {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("jobs-spec").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(RuntimeProfile.Conf.toSeq.foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) })
    .getOrCreate()

  val Hash0 = "sha256:" + "0" * 64
  val manifest: RuntimeVersions = RuntimeVersions("3.5.1", "2.12.18", 17L, "3.3.4",
    RuntimeVersions.JarKeys.map(k => k -> ("sha256:" + "a" * 64)).toMap)

  /** Bronze Delta root partitioned by dt (DateType): ids 1..n, `badMsisdn` leading rows have a null msisdn. */
  def bronze(root: String, dt: String, n: Int, badMsisdn: Int, foreignDt: Option[String] = None): Unit = {
    import spark.implicits._
    def rows(day: String, count: Int, bad: Int) = (1 to count).map { i =>
      (s"TX$day$i", s"SUB$i", if (i <= bad) null else "0912345678", "10000", " success ", "HN",
        s"$day 12:00:00", s"$day 23:00:00", day)
    }
    val all = rows(dt, n, badMsisdn) ++ foreignDt.toSeq.flatMap(rows(_, 7, 0))
    all.toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time",
      "ingest_time", "d").withColumn("dt", to_date(col("d"))).drop("d")
      .write.format("delta").partitionBy("dt").save(root)
  }

  def rule(id: String, check: String, stage: String, action: String, name: String, vt: String): JsonNode = {
    val r = Json.node()
    r.put("rule_id", id).put("metric_definition_hash", Hash0).put("measurement_definition_hash", Hash0)
    r.put("metric_name", name).put("value_type", vt)
    val c = Canonical.parse(check)
    r.put("kind", c.get("kind").asText()).set[JsonNode]("resolved_check", c)
    r.put("action", action).put("required", true)
    r
  }

  private def binding(scope: String, stage: String, rules: JsonNode*): JsonNode = {
    val b = Json.node()
    b.put("scope", scope).put("stage", stage).put("reference_role", "raw")
    val a = b.putArray("rules"); rules.foreach(a.add)
    b
  }

  final class Service(val dir: String, silver: String, quarantine: String, val decision: String = "ALLOW") extends ServiceApi {
    val executions = mutable.Buffer[JsonNode]()
    val reports = mutable.Buffer[JsonNode]()
    val outcomes = mutable.Map[String, ObjectNode]()
    val execId = UUID.randomUUID().toString
    val attemptId = UUID.randomUUID().toString
    private var plan: ObjectNode = _
    private var accepted: ObjectNode = _

    private def mkPlan(body: JsonNode): ObjectNode = {
      val p = Json.node()
      val audit = body.get("workflow").asText() == "AUDIT_TABLE"
      p.put("execution_id", execId).put("dataset_id", "ds").put("workflow", body.get("workflow").asText())
        .put("plan_hash", Hash0).put("execution_timestamp", "2026-10-22T00:00:00.000000Z")
      p.putObject("suite_revision").put("revision_id", UUID.randomUUID().toString)
      p.putObject("policy_revision").put("revision_id", UUID.randomUUID().toString)
      p.set[JsonNode]("parameters", body.get("parameters"))
      val m = manifest.toJson; m.put("format", "dq-compat-v1")
      p.putObject("compat_manifest").set[JsonNode]("payload", m)
      val sb = p.putArray("stage_bindings")
      if (audit) {
        p.set[JsonNode]("input_snapshot", body.get("input_snapshot"))
        sb.add(binding("TABLE", "AUDIT_SNAPSHOT",
          rule("audit_rows", """{"kind":"size"}""", "AUDIT_SNAPSHOT", "OBSERVE", "Size", "INTEGER")))
      } else {
        val dt = body.get("partition_values").get("dt").asText()
        p.set[JsonNode]("partition_values", body.get("partition_values"))
        p.put("partition_predicate", s"dt = DATE '$dt'")
        p.set[JsonNode]("target_bases", body.get("target_bases"))
        p.putObject("candidate_locations").put("execution_root", s"$dir/cand/$execId")
        sb.add(binding("PARTITION", "RAW", rule("raw_rows", """{"kind":"size"}""", "RAW", "OBSERVE", "Size", "INTEGER")))
        sb.add(binding("PARTITION", "CANDIDATE", rule("msisdn_complete", """{"kind":"complete","column":"msisdn"}""",
          "CANDIDATE", "QUARANTINE", "Completeness", "DECIMAL")))
        sb.add(binding("PARTITION", "CLEAN", rule("clean_rows", """{"kind":"size"}""", "CLEAN", "BLOCK", "Size", "INTEGER")))
      }
      p
    }

    override def request(method: String, path: Seq[String], query: Map[String, String], body: Option[JsonNode],
                         idem: Option[String]): JsonNode = {
      val n = Json.node()
      (method, path.drop(2)) match {
        case ("GET", Seq("datasets", _)) =>
          val reg = n.putObject("registration")
          reg.putObject("silver").put("uri", silver)
          reg.putObject("quarantine").put("uri", quarantine)
          reg.put("history_context", "ctx")
          n
        case ("POST", Seq("executions")) =>
          executions += body.get
          plan = mkPlan(body.get)
          n.set[JsonNode]("plan", plan); n
        case ("POST", Seq("executions", _, "attempts")) => n.putObject("attempt").put("attempt_id", attemptId); n
        case ("POST", Seq("executions", _, "reports")) =>
          val b = body.get
          reports += b
          val a = Json.node()
          a.put("execution_id", execId).put("report_hash", Canonical.hash(b))
          if (plan.get("workflow").asText() == "AUDIT_TABLE") {
            a.putNull("decision")
            a.put("assessment", decision)
          } else {
            a.putNull("assessment")
            val d = a.putObject("decision")
            d.put("decision_id", UUID.randomUUID().toString).put("decision", decision).put("plan_hash", Hash0)
            if (b.get("classified_reference").isNull) d.putNull("classified_reference")
            else d.set[JsonNode]("classified_reference", b.get("classified_reference"))
            if (decision == "ALLOW") {
              for (dest <- Seq("silver", "quarantine")) {
                val o = Json.node()
                val na = dest == "quarantine" && b.get("quarantine_rows").asLong() == 0L
                o.put("state", if (na) "NOT_APPLICABLE" else "NOT_STARTED").put("outcome_version", 0L).putNull("commit")
                outcomes(dest) = o
              }
            }
          }
          accepted = a
          a
        case ("GET", Seq("executions", _)) =>
          n.put("execution_id", execId).put("state", "REPORTED")
          n.set[JsonNode]("plan", plan)
          val o = n.putObject("outcomes")
          outcomes.foreach { case (d, v) => o.set[JsonNode](d, v) }
          n
        case ("PUT", Seq("executions", _, "write-outcomes", dest)) =>
          val b = body.get
          val o = outcomes(dest)
          val v = o.get("outcome_version").asLong() + 1
          o.put("state", b.get("state").asText()).put("outcome_version", v)
          if (b.get("commit").isNull) o.putNull("commit") else o.set[JsonNode]("commit", b.get("commit"))
          n.putObject("outcome").put("outcome_version", v); n
      }
    }
  }

  def client(svc: Service, dir: String): DQClient =
    new DQClient(spark, DQConfig("http://unused", 5, "ds", "default", UUID.randomUUID().toString, "run-1", None,
      s"$dir/state"), svc, () => manifest)

  def ids(uri: String): Set[String] =
    spark.read.format("delta").load(uri).select("transaction_id").collect().map(_.getString(0)).toSet

  def newDir(): String = Files.createTempDirectory("dq-jobs").toString
}
