package spark_etl.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, RemoteIterator}
import spark_etl.ConfigError

import scalaz.Scalaz._
import scalaz.ValidationNel

object PathValidator {
  def validate(paths: String*): ValidationNel[ConfigError, Seq[String]] = {
    val validated = paths.map {
      p =>
        if (p.toLowerCase.startsWith("hdfs://")) {
          val hadoopConf = new Configuration()
          val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
          val fsPath = new org.apache.hadoop.fs.Path(p)
          if (!fs.exists(fsPath))
            ConfigError(s"hdfs path doesn't exist: $p").failureNel[String]
          else {
            val children = hdfsList(fs, fsPath)
            if (areValid(children))
              p.successNel[ConfigError]
            else
              ConfigError(s"Unexpected hdfs children for $p: ${children.mkString(", ")} ").failureNel[String]
          }
        } else {
          val ioPath = new File(p)
          if (! ioPath.exists)
            ConfigError(s"Local path doesn't exist: $p").failureNel[String]
          else {
            val children = ioPath.list().toSeq
            if (areValid(children))
              p.successNel[ConfigError]
            else
              ConfigError(s"Unexpected local children for $p: ${children.mkString(", ")}").failureNel[String]
          }
        }
    }
    val res = validated.map(_.map(List(_))).reduce(_ +++ _)
    res
  }

  def hdfsList(fs: FileSystem, parent: org.apache.hadoop.fs.Path): Seq[String] = {
    // as per: https://issues.apache.org/jira/browse/HDFS-7921
    // listing recursively, including all files, then filtering distinct immediate children
    val parentStr = parent.toUri.toString
    toIterator(fs.listFiles(parent, true))
      .map {
        f =>
          val descendant = f.getPath.toUri.toString
          val relativeDescendant = descendant.substring(parentStr.length + 1)
          val immediateChild = relativeDescendant.split("/").head
          s"$parent/$immediateChild"
      }.toSeq.distinct
  }

  private def toIterator[T](iter: RemoteIterator[T]) =
    new Iterator[T] {
      def hasNext = iter.hasNext
      def next = iter.next
    }

  private def areValid(paths: Seq[String]) = {
    lazy val hasValidChildren = paths.forall {
      path =>
        val lastElem = path.split("/").last
        lastElem == "_temporary" || lastElem.contains("=")
    }
    paths.nonEmpty && hasValidChildren
  }
}
