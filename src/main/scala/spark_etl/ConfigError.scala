package spark_etl

case class ConfigError(msg: String, exc: Option[Throwable] = None)
