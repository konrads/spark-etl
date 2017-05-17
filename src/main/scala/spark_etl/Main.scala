package spark_etl

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql._
import org.rogach.scallop._
import spark_etl.util.Files

import scalaz.Scalaz._
import scalaz._

object Main {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  sealed trait CliCommand
  object ValidateLocal extends CliCommand
  object ValidateRemote extends CliCommand
  object TransformLoad extends CliCommand
  object ExtractCheck extends CliCommand
  object TransformCheck extends CliCommand
  object CliCommand {
    implicit val cliCommandConverter = singleArgConverter[CliCommand] {
      case "validate-local"  => ValidateLocal
      case "validate-remote" => ValidateRemote
      case "transform-load"  => TransformLoad
      case "extract-check"   => ExtractCheck
      case "transform-check" => TransformCheck
    }
  }

  val className = getClass.getSimpleName
  class CliConf(args: Seq[String]) extends ScallopConf(args) {
    banner(s"""Usage: $className [OPTIONS] (all options required unless otherwise indicated)\n\tOptions:""")
    val extraProps = props[String]()
    val confUri    = opt[String](name = "conf-uri", descr = "configuration resource uri", default = Some("/app.yaml"))
    val count      = toggle(name = "count", descrYes = "enable transform counts", default = Some(false))
    val command    = trailArg[CliCommand](name = "command", descr = "command")
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new CliConf(args)
    main(conf.command(), conf.confUri(), conf.extraProps, conf.count())
  }

  def main(command: CliCommand, confUri: String, extraProps: Map[String, String], shouldCount: Boolean): Unit = {
    def createSpark(name: String, props: Map[String, String], isMaster: Boolean): SparkSession = {
      val builder = if (isMaster)
        SparkSession.builder.master("local[1]").appName(name)
      else
        SparkSession.builder.appName(name)
      props.collect { case (k, v) if k.startsWith("spark.") => builder.config(k, v) }
      builder.getOrCreate
    }

    val pwd = Files.pwd
    val env = extraProps.collect { case (k, v) if k.startsWith("env.") => k.substring("env.".length) -> v }
    val res = command match {
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
    }

    res match {
      case Success(_) =>
        log.info("Success!")
      case Failure(errors) =>
        val errorStr = errors.map(e => e.exc.map(exc => s"• ${e.msg}, exception: $exc\n${stacktrace(exc)}").getOrElse(s"• ${e.msg}")).toList.mkString("\n")
        log.error(s"Failed due to:\n$errorStr")
        System.exit(1)
    }
  }

  private def stacktrace(t: Throwable) = {
    val w = new StringWriter
    t.printStackTrace(new PrintWriter(w))
    w.toString
  }
}
