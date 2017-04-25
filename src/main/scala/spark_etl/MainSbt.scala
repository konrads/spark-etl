package spark_etl

import org.rogach.scallop.ScallopConf

// For sbt purpose only
object MainSbt extends App {
  class CliConf(args: Seq[String]) extends ScallopConf(args) {
    val confUri    = opt[String](name = "confUri", descr = "configuration resource uri", default = Some("/app.yaml"))
    verify()
  }

  val conf = new CliConf(args)
  MainUtils.validateConf(conf.confUri())
}
