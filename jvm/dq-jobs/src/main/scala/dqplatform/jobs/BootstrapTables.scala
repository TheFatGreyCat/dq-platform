package dqplatform.jobs

import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client.{Canonical, DeltaRuntime, Json, SchemaHash, SnapshotRef}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

/**
 * Explicit, isolated bootstrap: precreates the Silver and quarantine Delta tables (ordinary writes never create
 * targets) and emits the registration JSON for `PUT /datasets/{id}`. Fails if either table path already exists.
 * `delta.setTransactionRetentionDuration` is deliberately never set (unbounded SetTransaction).
 */
object BootstrapTables {

  val Business: StructType = StructType(Seq(
    StructField("transaction_id", StringType), StructField("subscriber_id", StringType),
    StructField("msisdn", StringType), StructField("amount", DecimalType(18, 2)),
    StructField("status", StringType), StructField("province_code", StringType),
    StructField("event_time", TimestampType)))
  val Partition: StructType = StructType(Seq(StructField("dt", DateType, nullable = false)))
  val Silver: StructType = StructType(Business.fields ++ Partition.fields)
  val Quarantine: StructType = StructType(Silver.fields ++ Seq(
    StructField("_dq_execution_id", StringType), StructField("_dq_errors", ArrayType(StringType, containsNull = false)),
    StructField("_dq_candidate_version", LongType), StructField("_dq_quarantined_at", TimestampType)))

  private def create(spark: SparkSession, uri: String, schema: StructType): SnapshotRef = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write.format("delta").mode("errorifexists").partitionBy("dt").save(uri)
    DeltaRuntime.snapshot(spark, uri)
  }

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

  /** Creates both tables under `<root>/silver_transactions` and `<root>/quarantine_transactions`; returns the registration. */
  def bootstrap(spark: SparkSession, root: String): ObjectNode = {
    val base = root.stripSuffix("/")
    val silver = create(spark, s"$base/silver_transactions", Silver)
    val quarantine = create(spark, s"$base/quarantine_transactions", Quarantine)
    val reg = Json.node()
    reg.put("owner", "dq-demo")
    reg.set[ObjectNode]("silver", tableRef(silver))
    reg.set[ObjectNode]("quarantine", tableRef(quarantine))
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

  def main(argv: Array[String]): Unit = {
    val kv = argv.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    require(argv.length % 2 == 0 && kv.keySet == Set("root", "dataset", "registration-output"),
      "usage: --root URI --dataset ID --registration-output FILE")
    val spark = SparkSession.builder().appName("bootstrap_tables").getOrCreate()
    try {
      val reg = bootstrap(spark, kv("root"))
      val out = Json.node()
      out.put("dataset", kv("dataset")).set[ObjectNode]("registration", reg)
      Files.write(Paths.get(kv("registration-output")), Canonical.bytes(out))
      println(Canonical.string(out))
    } finally spark.stop()
  }
}
