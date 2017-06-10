package spark_etl.model

import net.jcazevedo.moultingyaml._
import spark_etl.ConfigError
import spark_etl.parquet.{ParquetExtractReader, ParquetLoadWriter}
import spark_etl.util.Validation._
import spark_etl.util.{UriLoader, Validation}

import scala.util.{Failure, Success, Try}

case class Config(
  extracts: List[Extract],
  transforms: List[Transform],
  loads: List[Load],
  extract_reader: Option[ParametrizedConstructor] = Some(ParametrizedConstructor(classOf[ParquetExtractReader].getName, Some(Map.empty))),
  load_writer: Option[ParametrizedConstructor] = Some(ParametrizedConstructor(classOf[ParquetLoadWriter].getName, Some(Map.empty))))

object Config extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat5(Config.apply)

  /**
    * Load Config from resource/file Uri
    */
  def load(resourceUri: String, filePathRoot: String, env: Map[String, String]): Validation[ConfigError, Config] =
    UriLoader.load(resourceUri, filePathRoot, env).flatMap(parse(_, env))

  def parse(configStr: String, env: Map[String, String] = Map.empty): Validation[ConfigError, Config] =
    Try(configStr.parseYaml.convertTo[Config]) match {
      case Success(conf) =>
        // yaml parser does not populate with defaults - force them
        val defaultExtractReader = Config(Nil, Nil, Nil).extract_reader
        val defaultLoadWriter = Config(Nil, Nil, Nil).load_writer
        val conf2 = conf.copy(
          extract_reader = conf.extract_reader.orElse(defaultExtractReader),
          load_writer = conf.load_writer.orElse(defaultLoadWriter)
        )
        conf2.success[ConfigError]
      case Failure(e: DeserializationException) =>
        ConfigError(s"Failed to deserialize config body, exception: ${e.getMessage}").failure[Config]
      case Failure(e) =>
        ConfigError(s"Failed to parse config body", Some(e)).failure[Config]
    }
}
