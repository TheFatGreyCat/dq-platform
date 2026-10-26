package dqplatform.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}

import java.io.IOException
import java.util.UUID

/** Per-attempt state files on any Hadoop filesystem (local path, file://, s3a://); resolved from the path itself. */
object StateFiles {
  private val OwnerOnly = new FsPermission("600")

  def path(stateDir: String, eid: String, name: String): Path = new Path(new Path(stateDir), s"$eid/$name")

  private def fs(p: Path, conf: Configuration): FileSystem = p.getFileSystem(conf) match {
    case l: LocalFileSystem => l.getRawFileSystem // no .crc sidecars next to the state files
    case other              => other
  }

  def exists(p: Path, conf: Configuration): Boolean = fs(p, conf).exists(p)

  def read(p: Path, conf: Configuration): Array[Byte] = {
    val in = fs(p, conf).open(p)
    try in.readAllBytes() finally in.close()
  }

  /** Temp file in the same directory (0600), then rename: the final name never holds a partial file. */
  def write(p: Path, bytes: Array[Byte], conf: Configuration): Unit = {
    val f = fs(p, conf)
    f.mkdirs(p.getParent)
    val tmp = new Path(p.getParent, s".tmp-${UUID.randomUUID()}.json")
    val out = FileSystem.create(f, tmp, OwnerOnly)
    try out.write(bytes) finally out.close()
    if (!f.rename(tmp, p)) {
      f.delete(tmp, false)
      throw new IOException(s"cannot rename state file into place: $p")
    }
  }
}
