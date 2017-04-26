package spark_etl

import org.rogach.scallop.ScallopConf

import scala.collection.JavaConverters._

// For sbt purpose only
object MainSbt extends App {
  class CliConf(args: Seq[String]) extends ScallopConf(args) {
    val confUri    = opt[String](name = "confUri", descr = "configuration resource uri", default = Some("/app.yaml"))
    verify()
  }

  val conf = new CliConf(args)
  val env = Map(System.getenv.asScala.toList:_*)
  MainUtils.validateConf(conf.confUri(), env)
}
