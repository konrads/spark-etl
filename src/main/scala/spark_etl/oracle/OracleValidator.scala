package spark_etl.oracle

import java.sql.Connection

import org.apache.log4j.Logger
import spark_etl.ConfigError

import scala.util.Try

import spark_etl.util.Validation
import spark_etl.util.Validation._

/**
  * Oracle Validator, checks connectivity and existance of tables.
  */
// FIXME: needs tests!!!
object OracleValidator {
  private val log = Logger.getLogger(getClass)

  def validateOracle(connStr: String, user: String, pwd: String, requiredTables: Seq[String]): Validation[ConfigError, Unit] =
    for {
    // open connection
      conn <- Try {
        java.sql.DriverManager.getConnection(connStr, user, pwd)
      } match {
        case scala.util.Success(conn) => conn.success[ConfigError]
        case scala.util.Failure(e) => ConfigError(s"Failed to access Oracle @ $connStr, as user $user", Some(e)).failure[Connection]
      }
      // check all tables
      _ <- {
        val tableVs = requiredTables.map {
          tableName =>
            Try {
              val ps = conn.prepareStatement("SELECT COUNT(1) FROM USER_TABLES WHERE LOWER(TABLE_NAME) = ?")
              ps.setString(1, tableName.toLowerCase)
              val rs = ps.executeQuery()
              rs.next()
              val tableCount = rs.getInt(1)
              if (tableCount == 0)
                throw new Exception(s"Failed to find table $tableName")
              else
                log.info(s"Validated Oracle table: $tableName")
            } match {
              case scala.util.Success(_) => ().success[ConfigError]
              case scala.util.Failure(e) => ConfigError(s"Failed to access Oracle table: $tableName", Some(e)).failure[Unit]
            }
        }
        // validate all tables (allowing for empty set)
        tableVs.foldLeft(().success[ConfigError]) {
          case (v1, v2) => v1 +++ v2
        }
      }
      // close the connection
      _ <- Try(conn.close()) match {
        case scala.util.Success(_) =>
          log.info(s"Validated Oracle connection: $connStr as user: $user")
          ().success[ConfigError]
        case scala.util.Failure(e) =>
          ConfigError(s"Failed to close the Oracle connection", Some(e)).failure[Unit]
      }
    } yield ()
}
