package spark_etl

import org.apache.spark.sql._
import org.rogach.scallop._

import scala.collection.JavaConverters._

object Main {
  sealed trait CliCommand
  object ValidateLocal extends CliCommand
  object ValidateRemote extends CliCommand
  object Transform extends CliCommand
  object ExtractCheck extends CliCommand
  object TransformCheck extends CliCommand
  object CliCommand {
    implicit val cliCommandConverter = singleArgConverter[CliCommand] {
      case "validate-local"  => ValidateLocal
      case "validate-remote" => ValidateRemote
      case "transform"       => Transform
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
    def createSpark(name: String, props: Map[String, String]): SparkSession = {
      val builder = SparkSession.builder.appName(name)
      props.foreach { case (k, v) if k.startsWith("spark.") => builder.config(k, v) }
      builder.getOrCreate
    }

    val env = extraProps.collect { case (k, v) if k.startsWith("env.") => k.substring("env.".length) -> v }
    command match {
      case ValidateLocal =>
        MainUtils.validateLocal(confUri, env)
      case ValidateRemote =>
        MainUtils.validateRemote(confUri, env)
      case Transform =>
        implicit val spark = createSpark(className, extraProps)
        try {
          MainUtils.transform(confUri, env, extraProps, shouldCount)
        } finally {
          spark.stop()
        }
      case ExtractCheck =>
        implicit val spark = createSpark(className, extraProps)
        try {
          MainUtils.extractCheck(confUri, env)
        } finally {
          spark.stop()
        }
      case TransformCheck =>
        implicit val spark = createSpark(className, extraProps)
        try {
          MainUtils.transformCheck(confUri, env, shouldCount)
        } finally {
          spark.stop()
        }
    }
  }
}
