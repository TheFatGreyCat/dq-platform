package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.{LocalDate, ZoneOffset}
import scala.util.control.NonFatal

/**
 * Finance Bronze (transactions + customers Delta roots, both pinned versionAsOf) -> enriched Silver transactions
 * through the DQ gate: startWrite -> evaluate -> submit -> publish. The job never writes Silver/quarantine itself.
 *
 * Exit: 0 = both destinations published (quarantine NOT_APPLICABLE counts), 3 = gate DENY,
 * 2 = technical / service / config / unresolved. Last stdout line: `{execution_id, decision, publication}` (no rows).
 */
object FinanceBronzeToSilver {

  val AppName = "finance_bronze_to_silver"
  val ExitPublished = 0
  val ExitError = 2
  val ExitDenied = 3

  final case class Args(
      dt: LocalDate,
      transactions: String = "s3a://dq-lake/bronze/finance/transactions",
      customers: String = "s3a://dq-lake/bronze/finance/customers")

  object Args {
    def parse(argv: Seq[String]): Args = {
      val kv = argv.grouped(2).collect { case Seq(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
      require(argv.size % 2 == 0 && (kv.keySet -- Set("dt", "transactions", "customers")).isEmpty,
        "invalid arguments: only --dt, --transactions and --customers")
      val dt = kv.getOrElse("dt", throw new IllegalArgumentException("missing --dt yyyy-MM-dd"))
      val d = Args(LocalDate.parse(dt))
      Args(d.dt, kv.getOrElse("transactions", d.transactions), kv.getOrElse("customers", d.customers))
    }
  }

  /** Fixed synthetic FX table to USD; an unknown currency yields a null amount_usd (the gate sees it). */
  private val UsdRates = Seq("USD" -> "1.00", "EUR" -> "1.08", "GBP" -> "1.27", "JPY" -> "0.0067", "VND" -> "0.00004",
    "SGD" -> "0.74", "CAD" -> "0.73", "AUD" -> "0.65")

  private def clean(c: String): Column = upper(trim(col(c)))

  private def usdRate: Column =
    UsdRates.foldLeft(lit(null).cast("decimal(18,8)")) { case (acc, (cur, rate)) =>
      when(col("currency") === cur, lit(rate).cast("decimal(18,8)")).otherwise(acc)
    }

  /** Newest customer row per normalised customer_id; rows without an id cannot be joined and are dropped. */
  private def latestCustomers(customers: DataFrame): DataFrame = {
    val latest = Window.partitionBy("customer_id").orderBy(col("ingest_time").desc)
    customers
      .withColumn("customer_id", clean("customer_id"))
      .withColumn("ingest_time", to_timestamp(col("ingest_time")))
      .where(col("customer_id").isNotNull)
      .withColumn("_rn", row_number().over(latest))
      .where(col("_rn") === 1)
      .select(
        col("customer_id"),
        clean("full_name").as("customer_name"),
        clean("segment").as("customer_segment"),
        clean("risk_level").as("risk_level"),
        clean("country").as("customer_country"),
        clean("kyc_status").as("kyc_status"),
        to_timestamp(col("created_at")).as("customer_created_at"))
  }

  /** Normalises transaction fields; malformed values become null (never dropped) so the gate can quarantine them. */
  private def normalise(raw: DataFrame): DataFrame =
    raw
      .withColumn("transaction_id", clean("transaction_id"))
      .withColumn("customer_id", clean("customer_id"))
      .withColumn("account_id", clean("account_id"))
      .withColumn("amount", trim(col("amount")).cast("decimal(18,2)"))
      .withColumn("currency", clean("currency"))
      .withColumn("transaction_type", clean("transaction_type"))
      .withColumn("status", clean("status"))
      .withColumn("event_time", to_timestamp(trim(col("event_time"))))
      .withColumn("ingest_time", to_timestamp(trim(col("ingest_time"))))
      .withColumn("merchant_category", clean("merchant_category"))
      .withColumn("channel", clean("channel"))
      .withColumn("country", clean("country"))

  def transform(rawTransactions: DataFrame, rawCustomers: DataFrame, dt: String): DataFrame = {
    val latest = Window.partitionBy("transaction_id").orderBy(col("ingest_time").desc)
    val day = Window.partitionBy("customer_id", "_event_day")
    normalise(rawTransactions)
      .withColumn("_rn", row_number().over(latest))
      // null transaction_id rows are kept (not merged into one) so completeness sends them to quarantine
      .where(col("_rn") === 1 || col("transaction_id").isNull)
      .join(latestCustomers(rawCustomers), Seq("customer_id"), "left")
      .withColumn("amount_usd", (col("amount") * usdRate).cast("decimal(18,2)"))
      .withColumn("transaction_month", date_format(col("event_time"), "yyyy-MM"))
      .withColumn("_event_day", to_date(col("event_time")))
      .withColumn("_has_day", col("customer_id").isNotNull && col("_event_day").isNotNull)
      .withColumn("customer_day_txn_count", when(col("_has_day"), count(lit(1)).over(day)))
      .withColumn("customer_day_total",
        when(col("_has_day"), sum("amount_usd").over(day)).cast("decimal(18,2)"))
      .withColumn("customer_day_avg",
        when(col("_has_day"), avg("amount_usd").over(day)).cast("decimal(18,2)"))
      .drop("_rn", "_event_day", "_has_day", "ingest_time")
      .withColumn("dt", to_date(lit(dt)))
  }

  /** Distinct normalised transaction ids plus keyless rows, computed from Bronze independently of the transform. */
  def sourceRowCount(raw: DataFrame): Long = {
    val r = normalise(raw).agg(
      countDistinct(col("transaction_id")).as("keys"),
      count(when(col("transaction_id").isNull, 1)).as("no_key")).head()
    r.getLong(0) + r.getLong(1)
  }

  private def dest(d: DestResult): ObjectNode = {
    val n = Json.node()
    n.put("state", d.state)
    d.evidence.fold(n.putNull("evidence"))(n.put("evidence", _))
    d.commitVersion.fold(n.putNull("commit_version"))(n.put("commit_version", _))
    n
  }

  def main(argv: Array[String]): Unit = {
    val args = Args.parse(argv.toSeq)
    val spark = SparkSession.builder().appName(AppName).getOrCreate()
    val code =
      try run(spark, args, DQClient.fromSession(spark))
      catch {
        case NonFatal(e) =>
          System.err.println(s"DQ ERROR: ${BronzeToSilverTransactions.errorCode(e)}")
          BronzeToSilverTransactions.emit(None, None, None)
          ExitError
      } finally spark.stop()
    sys.exit(code)
  }

  def run(spark: SparkSession, args: Args, dq: DQClient): Int = {
    import BronzeToSilverTransactions.{emit, errorCode}
    var executionId: Option[String] = None
    try {
      val dt = args.dt.toString
      RuntimeProbe.compareConf(spark.conf.getOption).foreach(e => throw new DQFailedException(e.code))
      // Both snapshots are pinned before any transformation so the whole run reads one consistent input.
      val txVersion = DeltaRuntime.snapshot(spark, args.transactions).version
      val custVersion = DeltaRuntime.snapshot(spark, args.customers).version
      val raw = spark.read.format("delta").option("versionAsOf", txVersion).load(args.transactions)
        .where(col("dt") === to_date(lit(dt)))
      val customers = spark.read.format("delta").option("versionAsOf", custVersion).load(args.customers)
      val params = Map(
        "partition_start" -> s"$dt 00:00:00",
        "partition_end" -> s"${args.dt.plusDays(1)} 00:00:00",
        "source_row_count" -> sourceRowCount(raw).toString)

      val handle = dq.startWrite(Map("dt" -> dt), params, args.dt.atStartOfDay(ZoneOffset.UTC).toInstant)
      executionId = Some(handle.executionId)
      val evaluation = dq.evaluate(handle, transform(raw, customers, dt))
      val accepted = dq.submit(evaluation)
      accepted.path("decision").path("decision").asText("") match {
        case "DENY" =>
          System.err.println(s"DQ DENY: execution ${handle.executionId}")
          emit(executionId, Some("DENY"), None)
          ExitDenied
        case "ALLOW" =>
          val pub = dq.publish(evaluation, accepted)
          val node = Json.node()
          node.set[JsonNode]("silver", dest(pub.silver))
          node.set[JsonNode]("quarantine", dest(pub.quarantine))
          emit(executionId, Some("ALLOW"), Some(node))
          if (pub.silver.state == "SUCCEEDED" && Set("SUCCEEDED", "NOT_APPLICABLE")(pub.quarantine.state)) ExitPublished
          else ExitError
        case _ =>
          System.err.println(s"DQ ERROR: no decision for execution ${handle.executionId}")
          emit(executionId, None, None)
          ExitError
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"DQ ERROR: ${errorCode(e)}")
        emit(executionId, None, None)
        ExitError
    }
  }
}
