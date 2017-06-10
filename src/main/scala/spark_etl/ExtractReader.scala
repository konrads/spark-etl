package spark_etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark_etl.model.Extract
import spark_etl.util.Validation

abstract class ExtractReader(params: Map[String, Any]) {
  def checkLocal(extracts: Seq[Extract]): Validation[ConfigError, Unit]
  def checkRemote(extracts: Seq[Extract]): Validation[ConfigError, Unit]
  def read(extracts: Seq[Extract])(implicit spark: SparkSession): Seq[(Extract, DataFrame)]
}
