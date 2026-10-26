package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, to_date}
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Release-acceptance fixture (plan V1), synthetic data only. Three modes:
 *  - `bootstrap --root URI --dataset ID --registration-output FILE`: precreates Silver/quarantine with the acceptance
 *    schema, seeds the untouched partition 2026-10-19 (ids 9001/9002) into Silver, emits the registration.
 *  - `run --dt D --variant good|deny [--rows N]`: builds the deterministic candidate in-process and runs it through
 *    startWrite -> evaluate -> submit -> publish. Exit 0 published, 3 DENY, 2 technical.
 *  - `verify --root URI --dt D [--silver-version N] [--quarantine-version N]`: read-only facts as one JSON line.
 * Every mode prints its result as the last stdout line.
 */
object AcceptanceFixture {

  val Business: StructType = StructType(Seq(
    StructField("id", LongType), StructField("value", StringType), StructField("amount", DecimalType(18, 2)),
    StructField("event_time", TimestampType)))
  val Partition: StructType = StructType(Seq(StructField("dt", DateType)))
  val Silver: StructType = StructType(Business.fields ++ Partition.fields)
  val Quarantine: StructType = StructType(Silver.fields ++ Seq(
    StructField("_dq_execution_id", StringType), StructField("_dq_errors", ArrayType(StringType, containsNull = false)),
    StructField("_dq_candidate_version", LongType), StructField("_dq_quarantined_at", TimestampType)))

  val OtherDt = "2026-10-19"
  val Variants = Set("good", "deny")

  final case class RunArgs(dt: LocalDate, variant: String, rows: Int)

  private def options(argv: Seq[String], allowed: Set[String], required: Set[String]): Map[String, String] = {
    val kv = argv.grouped(2).collect { case Seq(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    require(argv.size % 2 == 0 && kv.keySet.subsetOf(allowed) && required.subsetOf(kv.keySet),
      s"invalid arguments; allowed ${allowed.mkString(",")}")
    kv
  }

  def parseRun(argv: Seq[String]): RunArgs = {
    val kv = options(argv, Set("dt", "variant", "rows"), Set("dt", "variant"))
    require(Variants(kv("variant")), "variant must be good|deny")
    val rows = kv.get("rows").map(_.toInt).getOrElse(100)
    require(rows >= 6, "rows must be >= 6")
    RunArgs(LocalDate.parse(kv("dt")), kv("variant"), rows)
  }

  /** Candidate for `dt`: ids 1..rows. good: id1 value=null+amount=-1+previous-day event_time, id2 value=null, id3 amount=-1. deny: ids 1..6 value=null. */
  def candidate(spark: SparkSession, dt: LocalDate, variant: String, rows: Int): DataFrame = {
    val noon = dt.atTime(12, 0).toInstant(ZoneOffset.UTC)
    val prev = dt.minusDays(1).atTime(12, 0).toInstant(ZoneOffset.UTC)
    val day = java.sql.Date.valueOf(dt)
    val data = (1 to rows).map { i =>
      val nullValue = if (variant == "deny") i <= 6 else i <= 2
      val negative = variant == "good" && (i == 1 || i == 3)
      Row(i.toLong, if (nullValue) null else "ok", new java.math.BigDecimal(if (negative) "-1" else "10"),
        Timestamp.from(if (variant == "good" && i == 1) prev else noon), day)
    }
    spark.createDataFrame(spark.sparkContext.parallelize(data, 1), Silver)
  }

  def run(spark: SparkSession, args: RunArgs, dq: DQClient): Int = {
    var executionId: Option[String] = None
    try {
      RuntimeProbe.compareConf(spark.conf.getOption).foreach(e => throw new DQFailedException(e.code))
      val dt = args.dt.toString
      val params = Map("partition_start" -> s"$dt 00:00:00", "partition_end" -> s"${args.dt.plusDays(1)} 00:00:00")
      val handle = dq.startWrite(Map("dt" -> dt), params, args.dt.atStartOfDay(ZoneOffset.UTC).toInstant)
      executionId = Some(handle.executionId)
      val evaluation = dq.evaluate(handle, candidate(spark, args.dt, args.variant, args.rows))
      val accepted = dq.submit(evaluation)
      accepted.path("decision").path("decision").asText("") match {
        case "DENY" =>
          BronzeToSilverTransactions.emit(executionId, Some("DENY"), None)
          BronzeToSilverTransactions.ExitDenied
        case "ALLOW" =>
          val pub = dq.publish(evaluation, accepted)
          val node = Json.node()
          Seq("silver" -> pub.silver, "quarantine" -> pub.quarantine).foreach { case (name, d) =>
            val n = node.putObject(name)
            n.put("state", d.state)
            d.evidence.fold(n.putNull("evidence"))(n.put("evidence", _))
            d.commitVersion.fold(n.putNull("commit_version"))(n.put("commit_version", _))
          }
          BronzeToSilverTransactions.emit(executionId, Some("ALLOW"), Some(node))
          if (pub.silver.state == "SUCCEEDED" && Set("SUCCEEDED", "NOT_APPLICABLE")(pub.quarantine.state))
            BronzeToSilverTransactions.ExitPublished
          else BronzeToSilverTransactions.ExitError
        case _ =>
          BronzeToSilverTransactions.emit(executionId, None, None)
          BronzeToSilverTransactions.ExitError
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"DQ ERROR: ${BronzeToSilverTransactions.errorCode(e)}")
        BronzeToSilverTransactions.emit(executionId, None, None)
        BronzeToSilverTransactions.ExitError
    }
  }

  // ---------- bootstrap ----------
  private def create(spark: SparkSession, uri: String, schema: StructType): Unit =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write.format("delta").mode("errorifexists").partitionBy("dt").save(uri)

  private def tableRef(ref: SnapshotRef): ObjectNode = {
    val n = Json.node()
    n.put("uri", ref.uri).put("table_id", ref.tableId).put("schema_hash", ref.schemaHash)
      .put("partition_schema_hash", SchemaHash.of(Partition))
    n.putNull("set_transaction_retention")
    n
  }

  private def columns(n: ObjectNode, name: String, schema: StructType): Unit = {
    val a = n.putArray(name)
    schema.fields.foreach(f => a.addObject().put("name", f.name).put("type", f.dataType.simpleString)
      .put("nullable", f.nullable))
  }

  def silverUri(root: String): String = s"${root.stripSuffix("/")}/silver"
  def quarantineUri(root: String): String = s"${root.stripSuffix("/")}/quarantine"

  def bootstrap(spark: SparkSession, root: String): ObjectNode = {
    val base = root.stripSuffix("/")
    create(spark, silverUri(base), Silver)
    create(spark, quarantineUri(base), Quarantine)
    val other = Seq(Row(9001L, "other", new java.math.BigDecimal("5"), Timestamp.valueOf(s"$OtherDt 08:00:00"), Date.valueOf(OtherDt)),
      Row(9002L, "other", new java.math.BigDecimal("6"), Timestamp.valueOf(s"$OtherDt 09:00:00"), Date.valueOf(OtherDt)))
    spark.createDataFrame(spark.sparkContext.parallelize(other, 1), Silver)
      .write.format("delta").mode("append").save(silverUri(base))
    val reg = Json.node()
    reg.put("owner", "dq-acceptance")
    reg.set[ObjectNode]("silver", tableRef(DeltaRuntime.snapshot(spark, silverUri(base))))
    reg.set[ObjectNode]("quarantine", tableRef(DeltaRuntime.snapshot(spark, quarantineUri(base))))
    reg.put("candidate_root", s"$base/candidates")
    reg.putArray("allowed_storage_roots").add(base)
    columns(reg, "business_schema", Business)
    columns(reg, "partition_schema", Partition)
    reg.put("history_context", "default")
    val r = reg.putObject("retention")
    r.putNull("candidate_duration_seconds").putNull("report_duration_seconds")
    r.put("retry_window_seconds", 86400L).putNull("transaction_metadata_duration_seconds")
    r.put("cleanup_enabled", false)
    reg
  }

  // ---------- verify ----------
  private def read(spark: SparkSession, uri: String, version: Option[Long]): DataFrame = {
    val r = spark.read.format("delta")
    version.fold(r)(v => r.option("versionAsOf", v)).load(uri)
  }

  /** Read-only facts about both tables (optionally pinned versions) for partition `dt` and the untouched partition. */
  def verify(spark: SparkSession, root: String, dt: String, silverVersion: Option[Long],
             quarantineVersion: Option[Long]): ObjectNode = {
    val silver = read(spark, silverUri(root), silverVersion)
    val quarantine = read(spark, quarantineUri(root), quarantineVersion)
    val n = Json.node()
    val isDt = col("dt") === to_date(lit(dt))
    val ids = n.putArray("silver_ids")
    silver.where(isDt).select("id").orderBy("id").collect().foreach(r => ids.add(r.getLong(0)))
    val other = n.putArray("silver_other")
    silver.where(!isDt).select(concat_ws("|", col("id"), col("value"), col("amount"), col("event_time"), col("dt")).as("r"))
      .orderBy("r").collect().foreach(r => other.add(r.getString(0)))
    n.put("silver_total", silver.count())
    val q = n.putArray("quarantine")
    quarantine.where(isDt).select("id", "_dq_errors").orderBy("id").collect().foreach { r =>
      val o = q.addObject()
      o.put("id", r.getLong(0))
      val e = o.putArray("errors")
      r.getSeq[String](1).foreach(e.add)
    }
    n.put("silver_latest_version", DeltaRuntime.snapshot(spark, silverUri(root)).version)
    n.put("quarantine_latest_version", DeltaRuntime.snapshot(spark, quarantineUri(root)).version)
    n
  }

  def main(argv: Array[String]): Unit = {
    val rest = argv.drop(1).toSeq
    val spark = SparkSession.builder().appName("acceptance_fixture").getOrCreate()
    val code =
      try argv.headOption match {
        case Some("bootstrap") =>
          val kv = options(rest, Set("root", "dataset", "registration-output"), Set("root", "dataset", "registration-output"))
          val out = Json.node()
          out.put("dataset", kv("dataset")).set[JsonNode]("registration", bootstrap(spark, kv("root")))
          Files.write(Paths.get(kv("registration-output")), Canonical.bytes(out))
          println(Canonical.string(out)); 0
        case Some("run") => run(spark, parseRun(rest), DQClient.fromSession(spark))
        case Some("verify") =>
          val kv = options(rest, Set("root", "dt", "silver-version", "quarantine-version"), Set("root", "dt"))
          println(Canonical.string(verify(spark, kv("root"), LocalDate.parse(kv("dt")).toString,
            kv.get("silver-version").map(_.toLong), kv.get("quarantine-version").map(_.toLong)))); 0
        case _ => throw new IllegalArgumentException("usage: bootstrap|run|verify")
      } catch {
        case NonFatal(e) =>
          System.err.println(s"DQ ERROR: ${BronzeToSilverTransactions.errorCode(e)}")
          if (argv.headOption.contains("run")) BronzeToSilverTransactions.emit(None, None, None)
          2
      } finally spark.stop()
    sys.exit(code)
  }
}
