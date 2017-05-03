package spark_etl

import org.apache.spark.sql.DataFrame
import spark_etl.model.Load

import scalaz._

abstract class LoadWriter(params: Map[String, Any]) {
  def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit
  def checkLocal(loads: Seq[Load]): ValidationNel[ConfigError, Unit]
  def checkRemote(loads: Seq[Load]): ValidationNel[ConfigError, Unit]
}
