package dqplatform.jobs

import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.{Failure, Try}

/**
 * Compatibility probe: asserts the pinned Spark/Delta/Deequ stack behaves as the publisher relies on
 * (replaceWhere isolation, empty overwrite, txn idempotency, exact txn lookup, schema/UUID/property
 * attestation) on synthetic tables under `<root>/<uuid>/`, then attests the loaded jars.
 */
object CompatibilityProbe {

  private val D19 = "2026-10-19"
  private val D20 = "2026-10-20"
  private val Where20 = s"dt = DATE '$D20'"

  type Row3 = (Long, String, String)

  private def frame(spark: SparkSession, rows: Seq[Row3]): DataFrame = {
    import spark.implicits._
    rows.toDF("id", "value", "d").withColumn("dt", to_date(col("d"))).drop("d")
  }

  private def rows(spark: SparkSession, path: String, version: Option[Long] = None): Seq[Row3] = {
    val r = spark.read.format("delta")
    version.fold(r)(v => r.option("versionAsOf", v)).load(path)
      .selectExpr("id", "value", "cast(dt as string)").collect()
      .map(x => (x.getLong(0), x.getString(1), x.getString(2))).toSeq.sorted
  }

  private def create(spark: SparkSession, path: String, data: Seq[Row3]): Unit =
    frame(spark, data).write.format("delta").partitionBy("dt").save(path)

  private def overwrite(df: DataFrame, path: String, extra: Map[String, String] = Map.empty): Unit =
    df.write.format("delta").mode("overwrite").option("replaceWhere", Where20).options(extra).save(path)

  private def version(spark: SparkSession, path: String): Long = DeltaRuntime.snapshot(spark, path).version

  private val Base: Seq[Row3] = Seq((1L, "a", D19), (2L, "b", D19), (3L, "c", D20), (4L, "d", D20))
  private def p19(s: Seq[Row3]) = s.filter(_._3 == D19)

  def runChecks(spark: SparkSession, root: String): Seq[(String, Try[Unit])] = {
    val base = s"${root.stripSuffix("/")}/${UUID.randomUUID()}"
    def t(n: String) = s"$base/$n"
    def check(name: String)(body: => Unit): (String, Try[Unit]) = name -> Try {
      body
    }
    def need(c: Boolean, what: String): Unit = if (!c) throw new AssertionError(what)

    Seq(
      check("runtime_profile_conf") {
        need(RuntimeProbe.compareConf(spark.conf.getOption).isEmpty, "session conf differs from dq-runtime-v1")
      },
      check("two_partitions_version_as_of") {
        val p = t("t1")
        create(spark, p, Base)
        need(spark.read.format("delta").load(p).schema("dt").dataType.typeName == "date", "dt not DateType")
        overwrite(frame(spark, Seq((5L, "e", D20))), p)
        need(rows(spark, p, Some(0)) == Base.sorted, "versionAsOf 0 changed")
      },
      check("replace_where_isolates_partition") {
        val p = t("t2")
        create(spark, p, Base)
        overwrite(frame(spark, Seq((5L, "e", D20), (6L, "f", D20))), p)
        need(rows(spark, p) == (p19(Base) ++ Seq((5L, "e", D20), (6L, "f", D20))).sorted, "partition rows wrong")
        need(rows(spark, p, Some(0)).filter(_._3 == D19) == p19(Base), "other partition changed")
      },
      check("replace_where_rejects_outside_rows") {
        val p = t("t3")
        create(spark, p, Base)
        val v = version(spark, p)
        val failed = Try(overwrite(frame(spark, Seq((9L, "x", D19))), p)).isFailure
        need(failed, "outside-predicate rows were accepted")
        need(version(spark, p) == v && rows(spark, p) == Base.sorted, "failed write changed table")
      },
      check("empty_replace_where_removes_partition") {
        val p = t("t4")
        create(spark, p, Base)
        overwrite(frame(spark, Base).limit(0), p)
        need(rows(spark, p) == p19(Base), "empty overwrite did not remove exactly the target partition")
      },
      check("txn_idempotent_with_exact_lookup") {
        val p = t("t5")
        val app = s"dq:${UUID.randomUUID()}:silver"
        val meta = Canonical.string(Canonical.parse(s"""{"destination":"silver","execution_id":"$app"}"""))
        val opts = Map("txnAppId" -> app, "txnVersion" -> "1", "userMetadata" -> meta)
        create(spark, p, Base)
        overwrite(frame(spark, Seq((5L, "e", D20))), p, opts)
        val v = version(spark, p)
        val after = rows(spark, p)
        overwrite(frame(spark, Seq((7L, "g", D20), (8L, "h", D20))), p, opts)
        need(version(spark, p) == v, "repeat txn added a version")
        need(rows(spark, p) == after, "repeat txn changed data")
        val ev = DeltaRuntime.findTransaction(spark, p, app)
        need(ev.commits.size == 1, "expected exactly one matching commit")
        val c = ev.commits.head
        need(c.version == v && c.txnVersion == 1L && c.userMetadata.contains(meta), "commit does not match token")
        need(c.metadataHash.contains(Canonical.sha256(meta.getBytes(StandardCharsets.UTF_8))), "metadata hash")
        need(ev.snapshotTxnVersion.contains(1L) && !ev.absenceProven, "snapshot txn map")
        need(DeltaRuntime.findTransaction(spark, p, "dq:other:silver").absenceProven, "absence not proven")
      },
      check("snapshot_uuid_schema_retention") {
        val p = t("t6")
        create(spark, p, Base)
        val st = DeltaRuntime.state(spark, p)
        UUID.fromString(st.ref.tableId)
        need(st.ref.schemaHash == SchemaHash.of(spark.read.format("delta").load(p).schema), "schema hash")
        need(st.ref.version == 0L, "version")
        need(st.partitionColumns == Seq("dt"), "partition columns")
        need(!st.properties.contains(DeltaRuntime.RetentionProperty), "retention property present")
        val q = t("t6b")
        create(spark, q, Base)
        spark.sql(s"ALTER TABLE delta.`$q` SET TBLPROPERTIES ('${DeltaRuntime.RetentionProperty}' = 'interval 1 days')")
        need(DeltaRuntime.state(spark, q).properties.contains(DeltaRuntime.RetentionProperty), "property not attested")
        DeltaRuntime.verify(spark, st.ref)
        Try(DeltaRuntime.verify(spark, DeltaRuntime.snapshot(spark, q))) match {
          case Failure(e: ProtocolException) if e.code == "CANDIDATE_UNAVAILABLE" => ()
          case other => throw new AssertionError(s"verify accepted retention property: $other")
        }
      },
      check("missing_table_not_initialized") {
        Try(DeltaRuntime.snapshot(spark, t("absent"))) match {
          case Failure(e: ProtocolException) if e.code == "TARGET_NOT_INITIALIZED" => ()
          case other => throw new AssertionError(s"expected TARGET_NOT_INITIALIZED: $other")
        }
      })
  }

  /** (output json, manifest if all passed). */
  def run(spark: SparkSession, root: String, versions: () => RuntimeVersions): (ObjectNode, Option[ObjectNode]) = {
    val results = runChecks(spark, root) :+ ("runtime_versions", Try(versions()).map(_ => ()))
    val out = Json.node()
    val arr = out.putArray("checks")
    results.foreach { case (n, r) =>
      val c = arr.addObject().put("name", n).put("status", if (r.isSuccess) "PASS" else "FAIL")
      r.failed.foreach(e => c.put("error", e.getClass.getName))
    }
    if (results.exists(_._2.isFailure)) {
      out.put("status", "FAIL")
      (out, None)
    } else {
      val v = versions().toJson
      val manifest = Json.node()
      manifest.put("format", "dq-compat-v1")
      v.fields().forEachRemaining(e => manifest.set[com.fasterxml.jackson.databind.JsonNode](e.getKey, e.getValue))
      val result = Json.node()
      result.put("status", "PASS").set[com.fasterxml.jackson.databind.JsonNode]("runtime_versions", v)
      result.set[com.fasterxml.jackson.databind.JsonNode]("checks", arr)
      result.put("manifest_hash", Canonical.hash(manifest))
      (result, Some(manifest))
    }
  }

  def main(args: Array[String]): Unit = {
    val opts = args.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    val Seq(root, output, manifestOutput) = Seq("root", "output", "manifest-output").map(k =>
      opts.getOrElse(k, { System.err.println(s"missing --$k"); sys.exit(2) }))
    val spark = SparkSession.builder().appName("dq-compat-probe").getOrCreate()
    val (out, manifest) = try run(spark, root, () => RuntimeProbe.readVersions()) finally spark.stop()
    def write(path: String, n: ObjectNode): Unit =
      Files.write(Paths.get(path), Canonical.bytes(n))
    write(output, out)
    manifest.foreach(write(manifestOutput, _))
    println(Canonical.string(out))
    if (manifest.isEmpty) sys.exit(1)
  }
}
