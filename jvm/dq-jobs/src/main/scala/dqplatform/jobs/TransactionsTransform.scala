package dqplatform.jobs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/** Bronze -> Silver cho transactions. Giữ y hệt spark/jobs/bronze_to_silver.py để hai job so sánh được. */
object TransactionsTransform {

  def transform(raw: DataFrame, dt: String): DataFrame = {
    val latestFirst = Window.partitionBy("transaction_id").orderBy(col("ingest_time").desc)
    raw
      .withColumn("msisdn", regexp_replace(trim(col("msisdn")), "^0", "84"))
      .withColumn("amount", col("amount").cast("decimal(18,2)"))
      .withColumn("status", upper(trim(col("status"))))
      .withColumn("event_time", to_timestamp(col("event_time")))
      .withColumn("_rn", row_number().over(latestFirst))
      // dedup Kafka replay. Dòng thiếu transaction_id giữ nguyên (không gộp làm một)
      // để rule completeness đưa vào quarantine thay vì mất im lặng.
      .where(col("_rn") === 1 || col("transaction_id").isNull)
      .drop("_rn", "ingest_time")
      .withColumn("dt", to_date(lit(dt)))
  }

  /** Số giao dịch nguồn, tính thẳng từ Bronze, độc lập với transform -> rule đối soát completeness. */
  def sourceRowCount(raw: DataFrame): Long = {
    val r = raw.agg(
      countDistinct(col("transaction_id")).as("keys"),
      count(when(col("transaction_id").isNull, 1)).as("no_key")).head()
    r.getLong(0) + r.getLong(1)
  }
}
