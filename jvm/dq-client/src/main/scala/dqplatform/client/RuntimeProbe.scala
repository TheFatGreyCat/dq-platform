package dqplatform.client

import java.io.{File, FileInputStream}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/** Exact runtime profile the server pins in every plan (`dq-runtime-v1`). */
object RuntimeProfile {
  val Version = "dq-runtime-v1"
  val Conf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "UTC",
    "spark.sql.ansi.enabled" -> "false",
    "spark.sql.caseSensitive" -> "false",
    "spark.sql.legacy.timeParserPolicy" -> "EXCEPTION",
    "spark.sql.sources.partitionOverwriteMode" -> "static")
}

/**
 * Reads versions and SHA-256 of the jars that actually loaded the five attested components.
 * A component loaded from a directory / missing / unreadable CodeSource is a probe ERROR.
 */
object RuntimeProbe {

  /** Representative class per manifest jar key. */
  val Anchors: Seq[(String, String)] = Seq(
    "delta_spark" -> "org.apache.spark.sql.delta.DeltaLog",
    "delta_storage" -> "io.delta.storage.LogStore",
    "hadoop_aws" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "deequ" -> "com.amazon.deequ.VerificationSuite",
    "dq_client" -> "dqplatform.client.RuntimeProbe")

  private val cache = new ConcurrentHashMap[String, String]()

  def jarOf(className: String): File = {
    val cls = try Class.forName(className, false, getClass.getClassLoader) catch {
      case _: ClassNotFoundException => throw new ProtocolException("RUNTIME_PROBE_ERROR", className)
    }
    val src = Option(cls.getProtectionDomain.getCodeSource).flatMap(cs => Option(cs.getLocation))
      .getOrElse(throw new ProtocolException("RUNTIME_PROBE_ERROR", className))
    val f = new File(src.toURI).getCanonicalFile
    if (!f.isFile || !f.canRead) throw new ProtocolException("RUNTIME_PROBE_ERROR", className)
    f
  }

  def sha256(file: File): String = cache.computeIfAbsent(file.getPath, _ => {
    val md = MessageDigest.getInstance("SHA-256")
    val in = new FileInputStream(file)
    try {
      val buf = new Array[Byte](1 << 16)
      var n = in.read(buf)
      while (n >= 0) { md.update(buf, 0, n); n = in.read(buf) }
    } finally in.close()
    "sha256:" + md.digest().map(b => f"${b & 0xff}%02x").mkString
  })

  def readVersions(): RuntimeVersions = {
    val jars = Anchors.map { case (k, c) => k -> sha256(jarOf(c)) }.toMap
    val javaMajor = Runtime.version().feature().toLong
    RuntimeVersions(org.apache.spark.SPARK_VERSION, scala.util.Properties.versionNumberString, javaMajor,
      org.apache.hadoop.util.VersionInfo.getVersion, jars)
  }

  /** First mismatching component against an accepted manifest payload, if any. */
  def compare(expected: RuntimeVersions, actual: RuntimeVersions): Option[ClientError] = {
    val scalar = Seq(
      ("spark_version", expected.sparkVersion, actual.sparkVersion),
      ("scala_version", expected.scalaVersion, actual.scalaVersion),
      ("java_major", expected.javaMajor.toString, actual.javaMajor.toString),
      ("hadoop_version", expected.hadoopVersion, actual.hadoopVersion))
    val jarDiffs = RuntimeVersions.JarKeys.map(k => (s"jars.$k", expected.jars(k), actual.jars(k)))
    (scalar ++ jarDiffs).collectFirst { case (c, e, a) if e != a => ClientError.runtimeMismatch(c, e, a) }
  }

  /** Session conf must equal the pinned profile exactly. */
  def compareConf(get: String => Option[String]): Option[ClientError] =
    RuntimeProfile.Conf.toSeq.sortBy(_._1).collectFirst {
      case (k, v) if !get(k).contains(v) =>
        ClientError.runtimeMismatch(k, v, get(k).filter(_.matches("^[A-Za-z0-9_.:+/-]{1,128}$")).getOrElse("unset"))
    }
}
