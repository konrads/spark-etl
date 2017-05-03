package spark_etl.parquet

import org.apache.spark.sql.DataFrame
import spark_etl.model.Load
import spark_etl.{ConfigError, LoadWriter}

import scalaz.Scalaz._
import scalaz._

class ParquetLoadWriter(params: Map[String, String]) extends LoadWriter(params) {
  override def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit = {
    loadsAndDfs.foreach {
      case (Load(_, _, uri, Some(partitionBy)), df) => df.write.partitionBy(partitionBy:_*).parquet(uri)
      case (Load(_, _, uri, None), df)              => df.write.parquet(uri)
    }
  }

  // nothing to validate
  override def checkLocal(loads: Seq[Load]): ValidationNel[ConfigError, Unit] =
    ().successNel[ConfigError]

  override def checkRemote(loads: Seq[Load]): ValidationNel[ConfigError, Unit] =
    ().successNel[ConfigError]
}
