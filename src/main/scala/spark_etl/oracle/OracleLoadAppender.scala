package spark_etl.oracle

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import spark_etl.model.Load
import spark_etl.util.Validation
import spark_etl.util.Validation._
import spark_etl.{ConfigError, LoadWriter}

import scala.util.Try

/**
  * Sample Oracle appender.
  */
class OracleLoadAppender(params: Map[String, Any]) extends LoadWriter(params) {
  private val log = Logger.getLogger(getClass)

  override def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit = {
    val oracleUri = params("oracle_uri").toString
    val username  = params("oracle_user").toString
    val password  = params("oracle_password").toString
    val driver    = params.get("oracle_driver").map(_.toString).getOrElse("oracle.jdbc.driver.OracleDriver")
    val batchSize = Try(params("oracle_batch").asInstanceOf[Int]).getOrElse(1000)
    val props = {
      val p = new java.util.Properties()
      p.setProperty("user",           username)
      p.setProperty("password",       password)
      p.setProperty("fetchsize",      batchSize.toString)
      p.setProperty("batchsize",      batchSize.toString)
      p.setProperty("isolationLevel", "READ_COMMITTED")
      p.setProperty("driver",         driver)
      p
    }

    log.info(s"Load writing to $oracleUri, with jdbc driver: $driver")
    loadsAndDfs.foreach {
      case (Load(_, _, tableName, _), df) =>
        df.write.mode(SaveMode.Append).jdbc(oracleUri, tableName, props)
    }
  }

  override def checkLocal(loads: Seq[Load]): Validation[ConfigError, Unit] = {
    merge(toVal[String]("oracle_uri"),
      toVal[String]("oracle_user"),
      toVal[String]("oracle_password"),
      toVal[Int]("oracle_batch")) { (_, _, _, _) => () }
  }

  override def checkRemote(loads: Seq[Load]): Validation[ConfigError, Unit] = {
    val oracleUri = params("oracle_uri").toString
    val username  = params("oracle_user").toString
    val password  = params("oracle_password").toString
    val requiredTables = loads.map(_.uri)
    OracleValidator.validateOracle(oracleUri, username, password, requiredTables)
  }

  private def toVal[T](key: String): Validation[ConfigError, T] =
    params.get(key) match {
      case Some(v) =>
        Try(v.asInstanceOf[T]) match {
          case scala.util.Success(v2) => v2.success[ConfigError]
          case scala.util.Failure(_) => ConfigError(s"Invalid type of the env_var: $key").failure[T]
        }
      case None =>
        ConfigError(s"Missing ${getClass.getSimpleName} param: $key").failure[T]
    }
}
