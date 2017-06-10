package spark_etl.parquet

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark_etl.model.Extract
import spark_etl.util.Validation
import spark_etl.util.Validation._
import spark_etl.{ConfigError, ExtractReader}

import scala.util.Try

class ParquetExtractReader(params: Map[String, Any]) extends ExtractReader(params) {
  val checkChildren = Try(params("check-children").asInstanceOf[Boolean]).getOrElse(false)
  val expectPartition = Try(params("expect-partition").asInstanceOf[Boolean]).getOrElse(false)

  // nothing to validate
  override def checkLocal(extracts: Seq[Extract]): Validation[ConfigError, Unit] =
    ().success[ConfigError]

  override def checkRemote(extracts: Seq[Extract]): Validation[ConfigError, Unit] = {
    val parquetUris = extracts.map(_.uri)
    PathValidator.validate(checkChildren, expectPartition, parquetUris: _*).map(_ => ())
  }

  override def read(extracts: Seq[Extract])(implicit spark: SparkSession): Seq[(Extract, DataFrame)] = {
    extracts.map {
      e =>
        val df = spark.read.parquet(e.uri)
        (e, df)
    }
  }
}
