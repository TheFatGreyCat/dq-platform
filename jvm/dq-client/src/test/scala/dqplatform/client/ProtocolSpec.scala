package dqplatform.client

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class ProtocolSpec extends AnyFunSuite {

  private val golden = Canonical.parse(
    new String(getClass.getResourceAsStream("/protocol-v3.json").readAllBytes(), "UTF-8"))

  test("canonical bytes and hashes equal the Python golden vectors") {
    golden.get("canonical_vectors").elements().asScala.foreach { v =>
      val n = Canonical.parse(v.get("json").textValue())
      assert(Canonical.string(n) == v.get("canonical").textValue(), v.get("name").textValue())
      assert(Canonical.hash(n) == v.get("hash").textValue(), v.get("name").textValue())
    }
  }

  test("decimal normalization matches Python") {
    golden.get("decimals").fields().asScala.foreach { e =>
      assert(Canonical.decimal(new java.math.BigDecimal(e.getKey)) == e.getValue.textValue())
    }
  }

  test("duplicate keys and non-finite numbers are rejected") {
    golden.get("rejected_json").elements().asScala.foreach { j =>
      assertThrows[Exception](Canonical.parse(j.textValue()))
    }
  }

  test("Spark StructType schema hash equals Python schema_hash for the same ordered fields") {
    val v = golden.get("schema_vectors").get(0)
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("amount", DecimalType(18, 2), nullable = true,
        new MetadataBuilder().putString("comment", "x").build()),
      StructField("dt", DateType, nullable = true)))
    assert(SchemaHash.of(schema) == v.get("schema_hash").textValue())
    // field order is part of the identity
    assert(SchemaHash.of(StructType(schema.fields.reverse)) != v.get("schema_hash").textValue())
  }

  test("SnapshotRef parse is strict and round-trips") {
    val ref = SnapshotRef("s3a://b/t", "0b7c", 3, "sha256:" + "a" * 64)
    assert(SnapshotRef.fromJson(ref.toJson) == ref)
    val extra = ref.toJson.put("x", 1)
    assertThrows[ProtocolException](SnapshotRef.fromJson(extra))
    val retained = ref.toJson.put("set_transaction_retention", "interval 1 day")
    assertThrows[ProtocolException](SnapshotRef.fromJson(retained))
  }

  test("runtime mismatch reports first differing component without free text") {
    val a = RuntimeVersions("3.5.1", "2.12.18", 17, "3.3.4", RuntimeVersions.JarKeys.map(_ -> "sha256:1").toMap)
    val b = a.copy(jars = a.jars.updated("deequ", "sha256:2"))
    assert(RuntimeProbe.compare(a, a).isEmpty)
    val err = RuntimeProbe.compare(a, b).get
    assert(err.code == "RUNTIME_MISMATCH" && err.params("component") == "jars.deequ")
    val conf = RuntimeProfile.Conf.updated("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    assert(RuntimeProbe.compareConf(conf.get).get.params("component") == "spark.sql.session.timeZone")
  }
}
