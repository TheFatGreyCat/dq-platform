package dqplatform.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Typed error carrying only a stable code; message never contains data values or raw exception text. */
final class ProtocolException(val code: String, val field: String) extends RuntimeException(s"$code at $field")

/** Strict JSON field access for protocol objects (unknown keys rejected). */
object Json {
  def fail(field: String): Nothing = throw new ProtocolException("INVALID_PROTOCOL", field)

  def exactKeys(n: JsonNode, path: String, keys: Set[String]): Unit = {
    if (n == null || !n.isObject) fail(path)
    val actual = n.fieldNames().asScala.toSet
    if (actual != keys) fail(path + "." + (actual.diff(keys) ++ keys.diff(actual)).toSeq.sorted.headOption.getOrElse(""))
  }

  def str(n: JsonNode, f: String, path: String): String = {
    val v = n.get(f)
    if (v == null || !v.isTextual) fail(s"$path.$f")
    v.textValue()
  }

  def optStr(n: JsonNode, f: String, path: String): Option[String] = {
    val v = n.get(f)
    if (v == null) fail(s"$path.$f")
    if (v.isNull) None else if (v.isTextual) Some(v.textValue()) else fail(s"$path.$f")
  }

  def long(n: JsonNode, f: String, path: String): Long = {
    val v = n.get(f)
    if (v == null || !v.isIntegralNumber || !v.canConvertToLong) fail(s"$path.$f")
    v.longValue()
  }

  def obj(n: JsonNode, f: String, path: String): JsonNode = {
    val v = n.get(f)
    if (v == null || !v.isObject) fail(s"$path.$f")
    v
  }

  def node(): ObjectNode = Canonical.mapper.createObjectNode()
}

/** Pinned Delta snapshot. set_transaction_retention must be absent on the table (null on the wire). */
final case class SnapshotRef(uri: String, tableId: String, version: Long, schemaHash: String) {
  def toJson: ObjectNode = {
    val n = Json.node()
    n.put("uri", uri).put("table_id", tableId).put("version", version).put("schema_hash", schemaHash)
    n.putNull("set_transaction_retention")
    n
  }
}

object SnapshotRef {
  private val Keys = Set("uri", "table_id", "version", "schema_hash", "set_transaction_retention")

  def fromJson(n: JsonNode, path: String = "snapshot"): SnapshotRef = {
    Json.exactKeys(n, path, Keys)
    if (!n.get("set_transaction_retention").isNull) Json.fail(s"$path.set_transaction_retention")
    val v = Json.long(n, "version", path)
    if (v < 0) Json.fail(s"$path.version")
    SnapshotRef(Json.str(n, "uri", path), Json.str(n, "table_id", path), v, Json.str(n, "schema_hash", path))
  }
}

object SchemaHash {
  /** Full ordered StructType JSON (names, types, nullability, metadata) canonical hash. */
  def of(schema: StructType): String = Canonical.hash(Canonical.parse(schema.json))
}

/** Manifest-shaped runtime attestation (dq-compat-v1). */
final case class RuntimeVersions(
    sparkVersion: String,
    scalaVersion: String,
    javaMajor: Long,
    hadoopVersion: String,
    jars: Map[String, String]) {

  def toJson: ObjectNode = {
    val n = Json.node()
    n.put("spark_version", sparkVersion).put("scala_version", scalaVersion)
      .put("java_major", javaMajor).put("hadoop_version", hadoopVersion)
    val j = n.putObject("jars")
    jars.toSeq.sortBy(_._1).foreach { case (k, v) => j.put(k, v) }
    n
  }
}

object RuntimeVersions {
  val JarKeys: Seq[String] = Seq("delta_spark", "delta_storage", "hadoop_aws", "deequ", "dq_client")
  private val Keys = Set("spark_version", "scala_version", "java_major", "hadoop_version", "jars")

  def fromJson(n: JsonNode, path: String = "runtime_versions"): RuntimeVersions = {
    Json.exactKeys(n, path, Keys)
    val jars = Json.obj(n, "jars", path)
    Json.exactKeys(jars, s"$path.jars", JarKeys.toSet)
    RuntimeVersions(
      Json.str(n, "spark_version", path), Json.str(n, "scala_version", path), Json.long(n, "java_major", path),
      Json.str(n, "hadoop_version", path), JarKeys.map(k => k -> Json.str(jars, k, s"$path.jars")).toMap)
  }
}

/** Per-code tagged structured error (no free text). params values are bounded identifiers. */
final case class ClientError(code: String, params: Map[String, String]) {
  def toJson: ObjectNode = {
    val n = Json.node()
    n.put("code", code)
    val p = n.putObject("params")
    params.toSeq.sortBy(_._1).foreach { case (k, v) => p.put(k, v) }
    n
  }
}

object ClientError {
  private val Fqcn = "^[A-Za-z_$][A-Za-z0-9_$.]*$".r

  private def cls(t: Throwable): String = {
    val name = t.getClass.getName
    if (name.length <= 256 && Fqcn.findFirstIn(name).isDefined) name else "java.lang.Throwable"
  }

  def unclassified(t: Throwable, ruleId: Option[String] = None): ClientError =
    ClientError("UNCLASSIFIED_ENGINE_ERROR", Map("exception_class" -> cls(t)) ++ ruleId.map("rule_id" -> _))

  def withClass(code: String, t: Throwable): ClientError = ClientError(code, Map("exception_class" -> cls(t)))

  def runtimeMismatch(component: String, expected: String, actual: String): ClientError =
    ClientError("RUNTIME_MISMATCH", Map("component" -> component, "expected" -> expected, "actual" -> actual))
}
