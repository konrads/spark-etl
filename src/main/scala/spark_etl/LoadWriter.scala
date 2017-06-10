package spark_etl

import org.apache.spark.sql.DataFrame
import spark_etl.model.Load
import spark_etl.util.Validation

abstract class LoadWriter(params: Map[String, Any]) {
  def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit
  def checkLocal(loads: Seq[Load]): Validation[ConfigError, Unit]
  def checkRemote(loads: Seq[Load]): Validation[ConfigError, Unit]
}
