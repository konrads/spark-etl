package spark_etl

import org.apache.spark.sql.DataFrame
import spark_etl.model.Transform

import scalaz._

abstract class LoadWriter(params: Map[String, Any]) {
  def write(transformsAndDfs: Seq[(Transform, DataFrame)]): Unit
  def check(transforms: Seq[Transform]): ValidationNel[ConfigError, Unit]
}
