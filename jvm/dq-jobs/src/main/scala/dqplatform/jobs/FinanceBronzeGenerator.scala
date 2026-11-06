package dqplatform.jobs

import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client.{Canonical, Json}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.temporal.ChronoUnit

/**
 * Deterministic synthetic finance Bronze generator. Writes a customer Delta table and a raw-string transaction Delta
 * table partitioned by `dt`. Valid days start at [[Start]] (skipping `bad-date`) and are appended in batches of `days`
 * until the bytes measured under the transaction prefix reach `target-bytes`. Defects exist only in the `bad-date`
 * partition. Rows are pure functions of (seed, row id); nothing is collected on the driver.
 */
object FinanceBronzeGenerator {

  final case class Config(transactions: String, customers: String, days: Long, rowsPerDay: Long, targetBytes: Long,
                          badDate: LocalDate, seed: Long)

  val Start: LocalDate = LocalDate.of(2026, 11, 1)
  val CustomerCount = 100000L
  val BadRows = 120L
  private val Currencies = Seq("USD", "EUR", "VND", "GBP", "JPY")
  private val Statuses = Seq("POSTED", "PENDING", "SETTLED", "REVERSED")
  private val Types = Seq("PURCHASE", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "REFUND")
  private val Merchants = Seq("GROCERY", "TRAVEL", "UTILITIES", "DINING", "RETAIL", "HEALTH")
  private val Channels = Seq("POS", "ONLINE", "ATM", "MOBILE", "BRANCH")
  private val Countries = Seq("US", "DE", "VN", "GB", "JP", "SG")
  private val Segments = Seq("RETAIL", "PREMIUM", "SMB", "CORPORATE")
  private val Risks = Seq("LOW", "MEDIUM", "HIGH")
  private val Kyc = Seq("VERIFIED", "PENDING", "REJECTED")
  private val Keys = Set("transactions", "customers", "days", "rows-per-day", "target-bytes", "bad-date", "seed")

  def parse(argv: Array[String]): Config = {
    val kv = argv.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    require(argv.length % 2 == 0 && kv.keySet == Keys,
      "usage: --transactions URI --customers URI --days N --rows-per-day N --target-bytes N --bad-date YYYY-MM-DD --seed N")
    val c = Config(kv("transactions"), kv("customers"), kv("days").toLong, kv("rows-per-day").toLong,
      kv("target-bytes").toLong, LocalDate.parse(kv("bad-date")), kv("seed").toLong)
    require(c.days > 0 && c.rowsPerDay > 0 && c.targetBytes >= 0, "days and rows-per-day must be positive")
    c
  }

  private def pick(h: Column, options: Seq[String]): Column =
    element_at(array(options.map(lit): _*), (pmod(h, lit(options.size.toLong)) + 1).cast("int"))

  private def hash(c: Config, salt: String): Column = xxhash64(col("id"), lit(c.seed), lit(salt))

  def customers(spark: SparkSession, c: Config): DataFrame = {
    val created = date_add(lit(java.sql.Date.valueOf("2020-01-01")), pmod(hash(c, "created"), lit(1800L)).cast("int"))
    spark.range(CustomerCount).select(
      format_string("C%08d", col("id")).as("customer_id"),
      format_string("Synthetic Customer %d", col("id")).as("full_name"),
      pick(hash(c, "segment"), Segments).as("segment"),
      pick(hash(c, "risk"), Risks).as("risk_level"),
      pick(hash(c, "country"), Countries).as("country"),
      pick(hash(c, "kyc"), Kyc).as("kyc_status"),
      concat(created.cast("string"), lit(" 00:00:00")).as("created_at"),
      lit("2026-10-31 00:00:00").as("ingest_time"))
  }

  /** Well-formed rows from `id`, `dt` and the transaction id expression. */
  private def rows(ids: DataFrame, c: Config, dt: Column, txnId: Column): DataFrame = {
    val whole = pmod(hash(c, "amount"), lit(100000L)) + 1
    val cents = pmod(hash(c, "cents"), lit(100L))
    val time = format_string("%s %02d:%02d:%02d", dt.cast("string"),
      pmod(hash(c, "hh"), lit(24L)).cast("int"), pmod(hash(c, "mm"), lit(60L)).cast("int"),
      pmod(hash(c, "ss"), lit(60L)).cast("int"))
    ids.select(
      txnId.as("transaction_id"),
      format_string("C%08d", pmod(hash(c, "customer"), lit(CustomerCount))).as("customer_id"),
      format_string("A%010d", pmod(hash(c, "account"), lit(10000000000L))).as("account_id"),
      format_string("%d.%02d", whole, cents).as("amount"),
      pick(hash(c, "currency"), Currencies).as("currency"),
      pick(hash(c, "type"), Types).as("transaction_type"),
      pick(hash(c, "status"), Statuses).as("status"),
      time.as("event_time"),
      concat(date_add(dt, 1).cast("string"), lit(" 00:05:00")).as("ingest_time"),
      pick(hash(c, "merchant"), Merchants).as("merchant_category"),
      pick(hash(c, "channel"), Channels).as("channel"),
      pick(hash(c, "tcountry"), Countries).as("country"),
      dt.as("dt"))
  }

  /** Valid transactions for valid-day indexes [fromDay, toDay); the calendar skips `bad-date`. */
  def transactions(spark: SparkSession, c: Config, fromDay: Long, toDay: Long): DataFrame = {
    val ids = spark.range(fromDay * c.rowsPerDay, toDay * c.rowsPerDay).toDF("id")
    val dayIdx = expr(s"id div ${c.rowsPerDay}")
    val skipFrom = ChronoUnit.DAYS.between(Start, c.badDate)
    val shifted = if (skipFrom >= 0) dayIdx + when(dayIdx >= skipFrom, 1L).otherwise(0L) else dayIdx
    val dt = date_add(lit(java.sql.Date.valueOf(Start)), shifted.cast("int"))
    rows(ids, c, dt, format_string("TX-%012d", col("id")))
  }

  /** The only defective partition: row id modulo 12 selects the defect; remainder 11 stays clean. */
  def badTransactions(spark: SparkSession, c: Config): DataFrame = {
    val dt = lit(java.sql.Date.valueOf(c.badDate))
    val withK = rows(spark.range(BadRows).toDF("id"), c, dt, format_string("TXB-%d", col("id")))
      .withColumn("_k", pmod(regexp_extract(col("transaction_id"), "TXB-(\\d+)", 1).cast("long"), lit(12L)))
    val k = col("_k")
    val out = withK
      .withColumn("transaction_id", when(k === 0, lit(null)).when(k === 10, lit("TXB-1")).otherwise(col("transaction_id")))
      .withColumn("amount", when(k === 1, lit(null)).when(k === 2, lit("12.3x")).when(k === 5, lit("-10.00"))
        .when(k === 6, lit("99999999999.00")).otherwise(col("amount")))
      .withColumn("status", when(k === 3, lit("UNKNOWN")).otherwise(col("status")))
      .withColumn("currency", when(k === 4, lit("XXX")).otherwise(col("currency")))
      .withColumn("customer_id", when(k === 7, lit("C_MISSING")).otherwise(col("customer_id")))
      .withColumn("event_time", when(k === 8, lit("not-a-time"))
        .when(k === 9, lit(s"${c.badDate.minusDays(5)} 12:00:00")).otherwise(col("event_time")))
    out.drop("_k")
  }

  /** Total bytes of every regular file under `uri`, listed from the Hadoop FileSystem. */
  def prefixBytes(spark: SparkSession, uri: String): Long = {
    val path = new org.apache.hadoop.fs.Path(uri)
    val it = path.getFileSystem(spark.sparkContext.hadoopConfiguration).listFiles(path, true)
    var total = 0L
    while (it.hasNext) total += it.next().getLen
    total
  }

  def run(spark: SparkSession, c: Config): ObjectNode = {
    customers(spark, c).write.format("delta").mode("errorifexists").save(c.customers)
    var done = 0L
    var batches = 0
    var bytes = 0L
    do {
      transactions(spark, c, done, done + c.days).write.format("delta").partitionBy("dt")
        .mode(if (batches == 0) "errorifexists" else "append").save(c.transactions)
      done += c.days
      batches += 1
      bytes = prefixBytes(spark, c.transactions)
    } while (bytes < c.targetBytes)
    badTransactions(spark, c).write.format("delta").partitionBy("dt").mode("append").save(c.transactions)
    val ev = Json.node()
    ev.put("transactions_uri", c.transactions).put("customers_uri", c.customers)
      .put("seed", c.seed).put("bad_date", c.badDate.toString).put("valid_days", done)
      .put("valid_rows", done * c.rowsPerDay).put("bad_rows", BadRows).put("customer_rows", CustomerCount)
      .put("batches", batches).put("target_bytes", c.targetBytes)
      .put("transactions_bytes", prefixBytes(spark, c.transactions))
    ev
  }

  def main(argv: Array[String]): Unit = {
    val c = parse(argv)
    val spark = SparkSession.builder().appName("finance_bronze_generator").getOrCreate()
    try println(Canonical.string(run(spark, c))) finally spark.stop()
  }
}
