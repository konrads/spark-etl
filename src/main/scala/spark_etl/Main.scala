package spark_etl

import java.io.{File, PrintWriter, StringWriter}

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.rogach.scallop._
import spark_etl.util.Validation._
import spark_etl.util.{BAHelper, Failure, Files, Success}

import scala.util.{Random, Try}

object Main {
  val log = Logger.getLogger(getClass)

  sealed trait CliCommand
  object LineageDot extends CliCommand
  object ValidateLocal extends CliCommand
  object ValidateRemote extends CliCommand
  object TransformLoad extends CliCommand
  object ExtractCheck extends CliCommand
  object TransformCheck extends CliCommand
  object StripPrefixes extends CliCommand
  object CliCommand {
    implicit val cliCommandConverter = singleArgConverter[CliCommand] {
      case "lineage-dot"     => LineageDot
      case "validate-local"  => ValidateLocal
      case "validate-remote" => ValidateRemote
      case "transform-load"  => TransformLoad
      case "extract-check"   => ExtractCheck
      case "transform-check" => TransformCheck
      case "strip-prefixes"  => StripPrefixes
    }
  }

  val className = getClass.getSimpleName
  class CliConf(args: Seq[String]) extends ScallopConf(args) {
    banner(s"""Usage: $className [OPTIONS] (all options required unless otherwise indicated)\n\tOptions:""")
    val extraProps  = props[String]()
    val confUri     = opt[String](name = "conf-uri", descr = "configuration resource uri", default = Some("/app.yaml"))
    val lineageFile = opt[String](name = "lineage-file", descr = "target lineage dot file", default = Some("lineage.dot"))
    val baSqlDir    = opt[File](name = "ba-sql-dir", descr = "dir with BA sql", default = Some(new File("src/main/resources/spark")))
    val devSqlDir   = opt[File](name = "dev-sql-dir", descr = "dir with DEV sql", default = Some(new File("src/main/resources/spark")))
    val rmDevSqlDir = opt[Boolean](name = "rm-dev-sql-dir", descr = "should remove dev sql dir?", default = Some(false))
    val count       = toggle(name = "count", descrYes = "enable transform counts", default = Some(false))
    val command     = trailArg[CliCommand](name = "command", descr = "command")
    verify()
  }

  type ErrorHandler = List[ConfigError] => Unit

  def main(args: Array[String]): Unit = {
    val conf = new CliConf(args)
    main(conf.command(), conf.confUri(), conf.extraProps, conf.count(), conf.lineageFile(), conf.baSqlDir(), conf.devSqlDir(), conf.rmDevSqlDir())
  }

  def main(command: CliCommand, confUri: String, extraProps: Map[String, String], shouldCount: Boolean, lineageFile: String, baSqlDir: File, devSqlDir: File, rmDevSqlDir: Boolean, errorHandler: ErrorHandler = die): Unit = {
    def createSpark(name: String, props: Map[String, String], isMaster: Boolean): SparkSession = {
      val builder = if (isMaster)
          SparkSession.builder.appName(name).master("local[1]").config("spark.ui.port", random(4041, 4999)).config("spark.history.ui.port", random(18080, 19000))
        else
          SparkSession.builder.appName(name)
      if (isMaster)
      props.collect { case (k, v) if k.startsWith("spark.") => builder.config(k, v) }
      builder.getOrCreate
    }

    val pwd = Files.pwd
    val env = extraProps.collect { case (k, v) if k.startsWith("env.") => k.substring("env.".length) -> v }
    val res = command match {
      case LineageDot =>
        MainUtils.dotLineage(confUri, pwd, env, lineageFile)
      case ValidateLocal =>
        MainUtils.validateLocal(confUri, pwd, env)
      case ValidateRemote =>
        implicit val spark = createSpark(className, extraProps, true)
        try {
          MainUtils.validateRemote(confUri, pwd, env)
        } finally {
          spark.stop()
        }
      case TransformLoad =>
        implicit val spark = createSpark(className, extraProps, false)
        try {
          MainUtils.transformAndLoad(confUri, pwd, env, extraProps, shouldCount)
        } finally {
          spark.stop()
        }
      case ExtractCheck =>
        implicit val spark = createSpark(className, extraProps, false)
        try {
          MainUtils.extractCheck(confUri, pwd, env)
        } finally {
          spark.stop()
        }
      case TransformCheck =>
        implicit val spark = createSpark(className, extraProps, false)
        try {
          MainUtils.transformCheck(confUri, pwd, env, shouldCount)
        } finally {
          spark.stop()
        }
      case StripPrefixes =>
        Try(BAHelper.copySqls(baSqlDir, devSqlDir, rmDevSqlDir)) match {
          case scala.util.Success(descs) =>
            val desc = descs.map { case (source, target) => s"• $source -> $target" }
            log.info(s"""Copied BA sql to DEV:\n${desc.mkString("\n")}""").success[ConfigError]
          case scala.util.Failure(e) =>
            ConfigError(s"Failed to copy SQL from $baSqlDir to $devSqlDir", Some(e)).failure[Unit]
        }
    }

    res match {
      case Success(_) =>
        log.info("Success!")
      case Failure(errors) =>
        val errorStr = errors.map(e => e.exc.map(exc => s"• ${e.msg}, exception: $exc\n${stacktrace(exc)}").getOrElse(s"• ${e.msg}")).toList.mkString("\n")
        log.error(s"Failed due to:\n$errorStr")
        errorHandler(errors.toList)
    }
  }

  private def die(errors: List[ConfigError]): Unit =
    System.exit(1)

  private def stacktrace(t: Throwable) = {
    val w = new StringWriter
    t.printStackTrace(new PrintWriter(w))
    w.toString
  }

  private val rand = new Random(System.currentTimeMillis)
  private def random(min: Int, max: Int) = min + rand.nextInt(max - min)
}
