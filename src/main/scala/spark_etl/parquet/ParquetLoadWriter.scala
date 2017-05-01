package spark_etl.parquet

import org.apache.spark.sql.DataFrame
import spark_etl.model.Transform
import spark_etl.{ConfigError, LoadWriter}

import scalaz.Scalaz._
import scalaz._

class ParquetLoadWriter(params: Map[String, String]) extends LoadWriter(params) {
  override def write(transformsAndDfs: Seq[(Transform, DataFrame)]): Unit = {
    transformsAndDfs.foreach {
      case (t, df) => t.output.partition_by match {
        case Some(partitionBy) => df.write.partitionBy(partitionBy:_*).parquet(t.output.uri)
        case None              => df.write.parquet(t.output.uri)
      }
    }
  }

  // nothing to validate
  override def checkLocal(transforms: Seq[Transform]): ValidationNel[ConfigError, Unit] =
    ().successNel[ConfigError]

  override def checkRemote(transforms: Seq[Transform]): ValidationNel[ConfigError, Unit] =
    ().successNel[ConfigError]
}
