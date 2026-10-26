package dqplatform.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dqplatform.client.{Canonical, Json}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/**
 * Candidate cleanup. Input is the operator's `GET /datasets/{id}/retention` response (items with `pinned`), run while
 * holding the dataset slot. Deletes only unpinned `<candidate_root>/<execution>/<attempt>/{raw|classified}` directories
 * (never a shared target table, never anything outside `--candidate-root`) and outputs the removed items as JSON so the
 * operator can POST the cleanup records. Metadata, reports and target histories are not touched; no VACUUM.
 */
object Maintenance {

  private val Kinds = Set("raw", "classified")

  private def under(uri: String, root: String): Boolean = {
    val r = root.stripSuffix("/")
    uri.startsWith(r + "/") && !uri.split("/").exists(s => s == ".." || s == ".")
  }

  /** Items that may be deleted: unpinned, a candidate table directory, inside the candidate root. */
  def deletable(items: Seq[JsonNode], candidateRoot: String): Seq[JsonNode] = items.filter { i =>
    val uri = i.path("uri").asText("")
    !i.path("pinned").asBoolean(true) && under(uri, candidateRoot) && Kinds(uri.substring(uri.lastIndexOf('/') + 1))
  }

  def run(spark: SparkSession, items: Seq[JsonNode], candidateRoot: String): Seq[ObjectNode] =
    deletable(items, candidateRoot).flatMap { i =>
      val p = new Path(i.get("uri").asText())
      val fs = p.getFileSystem(spark.sessionState.newHadoopConf())
      // already-gone directories are reported too so a cleanup record can be written after a crash
      if (!fs.exists(p) || fs.delete(p, true)) {
        val n = Json.node()
        n.put("execution_id", i.get("execution_id").asText()).put("attempt_id", i.get("attempt_id").asText())
          .put("uri", i.get("uri").asText())
        Some(n)
      } else None
    }

  def main(argv: Array[String]): Unit = {
    val kv = argv.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    require(argv.length % 2 == 0 && kv.keySet == Set("retention-file", "candidate-root", "output"),
      "usage: --retention-file FILE --candidate-root URI --output FILE")
    val spark = SparkSession.builder().appName("maintenance").getOrCreate()
    try {
      val items = Canonical.parse(new String(Files.readAllBytes(Paths.get(kv("retention-file"))), "UTF-8"))
        .path("items").elements().asScala.toSeq
      val out = Canonical.mapper.createArrayNode()
      run(spark, items, kv("candidate-root")).foreach(out.add)
      Files.write(Paths.get(kv("output")), Canonical.bytes(out))
      println(Canonical.string(out))
    } finally spark.stop()
  }
}
