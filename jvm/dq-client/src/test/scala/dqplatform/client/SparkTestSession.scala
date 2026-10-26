package dqplatform.client

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite => TestSuite}

/** SparkSession local dùng chung cho các spec (mỗi JVM test một session). */
trait SparkTestSession extends BeforeAndAfterAll { self: TestSuite =>

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(RuntimeProfile.Conf.toSeq.foldLeft(new org.apache.spark.SparkConf()) { case (c, (k, v)) => c.set(k, v) })
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }
}
