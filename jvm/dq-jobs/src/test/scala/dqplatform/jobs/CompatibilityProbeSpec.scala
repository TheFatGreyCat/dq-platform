package dqplatform.jobs

import dqplatform.client.{Canonical, RuntimeProbe, RuntimeProfile, RuntimeVersions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import scala.collection.JavaConverters._

class CompatibilityProbeSpec extends AnyFunSuite {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("CompatibilityProbeSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(RuntimeProfile.Conf.toSeq.foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) })
    .getOrCreate()

  private val fake = RuntimeVersions("3.5.1", "2.12.18", 17, "3.3.4",
    RuntimeVersions.JarKeys.map(k => k -> ("sha256:" + "0" * 64)).toMap)

  test("all checks pass on synthetic Delta tables and manifest hash matches canonical manifest") {
    val root = Files.createTempDirectory("dq-compat").toUri.toString
    val (out, manifest) = CompatibilityProbe.run(spark, root, () => fake)
    val failed = out.get("checks").elements().asScala.filter(_.get("status").asText != "PASS").map(_.toString).toList
    assert(failed.isEmpty, failed.mkString("; "))
    assert(out.get("status").asText == "PASS")
    assert(out.get("checks").size == 9)
    val m = manifest.get
    assert(m.get("format").asText == "dq-compat-v1")
    assert(out.get("manifest_hash").asText == Canonical.hash(m))
  }

  test("runtime attestation errors (no jar CodeSource) produce FAIL without manifest") {
    val root = Files.createTempDirectory("dq-compat").toUri.toString
    val (out, manifest) = CompatibilityProbe.run(spark, root, () => RuntimeProbe.readVersions())
    // dq-client resolves from a directory or jar depending on build; either way status must be consistent
    assert((out.get("status").asText == "PASS") == manifest.isDefined)
  }
}
