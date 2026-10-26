package dqplatform.client

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.{CommitInfo, SetTransaction}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}

/** Matching commit for a DQ transaction token in the retained JSON log. */
final case class CommitMatch(version: Long, txnVersion: Long, userMetadata: Option[String]) {
  def metadataHash: Option[String] =
    userMetadata.map(m => Canonical.sha256(m.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
}

/**
 * Evidence for one appId on one table. `retainedFrom..checkedThrough` is the contiguous JSON
 * commit range inspected; absence is only proven if `retainedFrom == 0` or the token is still in
 * the snapshot transaction map (otherwise expired commits might have held it).
 */
final case class TransactionEvidence(
    tableId: String,
    snapshotVersion: Long,
    snapshotTxnVersion: Option[Long],
    snapshotTxnLastUpdated: Option[Long],
    retainedFrom: Long,
    checkedThrough: Long,
    commits: Seq[CommitMatch]) {
  def absenceProven: Boolean = commits.isEmpty && snapshotTxnVersion.isEmpty && retainedFrom == 0L
}

final case class TableState(ref: SnapshotRef, properties: Map[String, String], partitionColumns: Seq[String],
                            schemaJson: String)

/** Only file that touches Delta 3.2.0 log internals; no DeltaLog types leak out. */
object DeltaRuntime {

  val RetentionProperty = "delta.setTransactionRetentionDuration"

  private def log(spark: SparkSession, uri: String): DeltaLog = DeltaLog.forTable(spark, new Path(uri))

  private def latest(spark: SparkSession, uri: String): (DeltaLog, Snapshot) = {
    val dl = log(spark, uri)
    val snap = dl.update()
    if (snap.version < 0) throw new ProtocolException("TARGET_NOT_INITIALIZED", uri)
    (dl, snap)
  }

  private def refOf(uri: String, s: Snapshot): SnapshotRef =
    SnapshotRef(uri, s.metadata.id, s.version, SchemaHash.of(s.schema))

  def snapshot(spark: SparkSession, uri: String): SnapshotRef = refOf(uri, latest(spark, uri)._2)

  def state(spark: SparkSession, uri: String): TableState = {
    val s = latest(spark, uri)._2
    TableState(refOf(uri, s), s.metadata.configuration, s.metadata.partitionColumns, s.schema.json)
  }

  /** Pinned ref still resolvable: same table UUID, schema hash and every data file present. */
  def verify(spark: SparkSession, ref: SnapshotRef): Unit = {
    val dl = log(spark, ref.uri)
    val cur = dl.update()
    if (cur.version < 0) throw new ProtocolException("CANDIDATE_UNAVAILABLE", "table")
    val s = try dl.getSnapshotAt(ref.version) catch {
      case scala.util.control.NonFatal(_) => throw new ProtocolException("CANDIDATE_UNAVAILABLE", "version")
    }
    if (s.metadata.id != ref.tableId) throw new ProtocolException("CANDIDATE_UNAVAILABLE", "table_id")
    if (SchemaHash.of(s.schema) != ref.schemaHash) throw new ProtocolException("CANDIDATE_UNAVAILABLE", "schema_hash")
    if (s.metadata.configuration.contains(RetentionProperty))
      throw new ProtocolException("CANDIDATE_UNAVAILABLE", "set_transaction_retention")
    val fs = dl.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
    val missing = s.allFiles.collect().exists { f =>
      !fs.exists(new Path(dl.dataPath, new Path(new java.net.URI(f.path))))
    }
    if (missing) throw new ProtocolException("CANDIDATE_UNAVAILABLE", "files")
  }

  /** Retained JSON commit versions (ascending) in _delta_log. */
  private def retainedJsonVersions(spark: SparkSession, dl: DeltaLog): Seq[Long] = {
    val fs = dl.logPath.getFileSystem(spark.sessionState.newHadoopConf())
    val Json = "^(\\d{20})\\.json$".r
    fs.listStatus(dl.logPath).toSeq.map(_.getPath.getName).collect { case Json(v) => v.toLong }.sorted
  }

  def findTransaction(spark: SparkSession, uri: String, appId: String): TransactionEvidence = {
    val (dl, snap) = latest(spark, uri)
    val versions = retainedJsonVersions(spark, dl).filter(_ <= snap.version)
    val from = versions.headOption.getOrElse(snap.version)
    // contiguous range only: a gap means a commit we cannot inspect
    val contiguous = versions.zipWithIndex.forall { case (v, i) => v == from + i }
    if (!contiguous) throw new ProtocolException("CONFLICTING_EVIDENCE", "delta_log")
    val matches = dl.getChanges(from, failOnDataLoss = true).takeWhile(_._1 <= snap.version).flatMap {
      case (v, actions) =>
        val txn = actions.collectFirst { case t: SetTransaction if t.appId == appId => t.version }
        txn.map { tv =>
          val meta = actions.collectFirst { case c: CommitInfo => c.userMetadata }.flatten
          CommitMatch(v, tv, meta)
        }
    }.toVector
    val inSnap = snap.setTransactions.find(_.appId == appId)
    TransactionEvidence(snap.metadata.id, snap.version, inSnap.map(_.version), inSnap.flatMap(_.lastUpdated),
      from, snap.version, matches)
  }

  /** All DQ tokens (`dq:` prefix) in retained commits, with touched partition values. */
  final case class TokenCommit(version: Long, appId: String, txnVersion: Long, userMetadata: Option[String],
                               partitions: Set[Map[String, String]])

  def dqTokens(spark: SparkSession, uri: String): (SnapshotRef, Long, Seq[TokenCommit]) = {
    val (dl, snap) = latest(spark, uri)
    val versions = retainedJsonVersions(spark, dl).filter(_ <= snap.version)
    val from = versions.headOption.getOrElse(snap.version)
    val out = dl.getChanges(from, failOnDataLoss = true).takeWhile(_._1 <= snap.version).flatMap {
      case (v, actions) =>
        actions.collect { case t: SetTransaction if t.appId.startsWith("dq:") => t }.map { t =>
          val meta = actions.collectFirst { case c: CommitInfo => c.userMetadata }.flatten
          val parts = actions.collect {
            case a: org.apache.spark.sql.delta.actions.AddFile => a.partitionValues
            case r: org.apache.spark.sql.delta.actions.RemoveFile => r.partitionValues
          }.toSet
          TokenCommit(v, t.appId, t.version, meta, parts)
        }
    }.toVector
    (refOf(uri, snap), from, out)
  }
}
