package dqplatform.jobs

import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client.{Canonical, DeltaRuntime, Json, SchemaHash, SnapshotRef}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

/**
 * Finance variant of [[BootstrapTables]]: precreates the empty Silver and quarantine Delta tables for the enriched
 * finance transaction schema and emits the registration JSON for `PUT /datasets/{id}`. Both targets are checked
 * before anything is written and creation uses `errorifexists`, so an existing table is never touched.
 */
object BootstrapFinanceTables {

  val DefaultAllowedRoot = "s3a://dq-lake/"

  val Business: StructType = StructType(Seq(
    StructField("transaction_id", StringType), StructField("customer_id", StringType),
    StructField("account_id", StringType), StructField("amount", DecimalType(18, 2)),
    StructField("currency", StringType), StructField("transaction_type", StringType),
    StructField("status", StringType), StructField("event_time", TimestampType),
    StructField("merchant_category", StringType), StructField("channel", StringType),
    StructField("country", StringType), StructField("customer_name", StringType),
    StructField("customer_segment", StringType), StructField("risk_level", StringType),
    StructField("kyc_status", StringType), StructField("customer_country", StringType),
    StructField("amount_usd", DecimalType(18, 2)), StructField("transaction_month", StringType),
    StructField("customer_day_txn_count", LongType), StructField("customer_day_total", DecimalType(18, 2)),
    StructField("customer_day_avg", DecimalType(18, 2))))
  val Silver: StructType = StructType(Business.fields ++ BootstrapTables.Partition.fields)
  val Quarantine: StructType = StructType(Silver.fields ++ BootstrapTables.Quarantine.fields.drop(BootstrapTables.Silver.length))

  private def exists(spark: SparkSession, uri: String): Boolean = {
    val p = new Path(uri)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
  }

  private def create(spark: SparkSession, uri: String, schema: StructType): SnapshotRef = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write.format("delta").mode("errorifexists").partitionBy("dt").save(uri)
    DeltaRuntime.snapshot(spark, uri)
  }

  private def tableRef(ref: SnapshotRef): ObjectNode = {
    val n = Json.node()
    n.put("uri", ref.uri).put("table_id", ref.tableId).put("schema_hash", ref.schemaHash)
      .put("partition_schema_hash", SchemaHash.of(BootstrapTables.Partition))
    n.putNull("set_transaction_retention")
    n
  }

  private def columns(n: ObjectNode, name: String, schema: StructType): Unit = {
    val a = n.putArray(name)
    schema.fields.foreach(f => a.addObject().put("name", f.name).put("type", f.dataType.simpleString)
      .put("nullable", f.nullable))
  }

  /** Creates `<root>/silver_finance_transactions` and `<root>/quarantine_finance_transactions`; returns the registration. */
  def bootstrap(spark: SparkSession, root: String, allowedRoot: String = DefaultAllowedRoot): ObjectNode = {
    val base = root.stripSuffix("/")
    require(base.startsWith(allowedRoot), s"root $root is outside the allowed storage root $allowedRoot")
    val (silverUri, quarantineUri) = (s"$base/silver_finance_transactions", s"$base/quarantine_finance_transactions")
    Seq(silverUri, quarantineUri).foreach(u => require(!exists(spark, u), s"target already exists: $u"))
    val silver = create(spark, silverUri, Silver)
    val quarantine = create(spark, quarantineUri, Quarantine)
    val reg = Json.node()
    reg.put("owner", "dq-demo")
    reg.set[ObjectNode]("silver", tableRef(silver))
    reg.set[ObjectNode]("quarantine", tableRef(quarantine))
    reg.put("candidate_root", s"$base/candidates")
    reg.putArray("allowed_storage_roots").add(allowedRoot)
    columns(reg, "business_schema", Business)
    columns(reg, "partition_schema", BootstrapTables.Partition)
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
    val spark = SparkSession.builder().appName("bootstrap_finance_tables").getOrCreate()
    try {
      val reg = bootstrap(spark, kv("root"))
      val out = Json.node()
      out.put("dataset", kv("dataset")).set[ObjectNode]("registration", reg)
      Files.write(Paths.get(kv("registration-output")), Canonical.bytes(out))
      println(Canonical.string(out))
    } finally spark.stop()
  }
}
