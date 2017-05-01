package spark_etl.parquet

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark_etl.model.Extract
import spark_etl.{ConfigError, ExtractReader}

import scalaz.Scalaz._
import scalaz._

class ParquetExtractReader(params: Map[String, String]) extends ExtractReader(params) {
  // nothing to validate
  override def checkLocal(extracts: Seq[Extract]): ValidationNel[ConfigError, Unit] =
    ().successNel[ConfigError]

  override def checkRemote(extracts: Seq[Extract]): ValidationNel[ConfigError, Unit] = {
    val parquetUris = extracts.map(_.uri)
    PathValidator.validate(parquetUris: _*).map(_ => ())
  }

  override def read(extracts: Seq[Extract])(implicit spark: SparkSession): Seq[(Extract, DataFrame)] = {
    extracts.map {
      e =>
        val df = spark.read.parquet(e.uri)
        (e, df)
    }
  }
}
