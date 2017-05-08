package spark_etl.model

import net.jcazevedo.moultingyaml._
import spark_etl.ConfigError
import spark_etl.util.UriLoader

import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class Config(
                   extracts: List[Extract],
                   transforms: List[Transform],
                   loads: List[Load],
                   extract_reader: Option[ParametrizedConstructor] = Some(ParametrizedConstructor("spark_etl.parquet.ParquetExtractReader", Some(Map.empty))),
                   load_writer: Option[ParametrizedConstructor] = Some(ParametrizedConstructor("spark_etl.parquet.ParquetLoadWriter", Some(Map.empty))))

object Config extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat5(Config.apply)

  /**
    * Load Config from resource/file Uri
    */
  def load(resourceUri: String, env: Map[String, String]): ValidationNel[ConfigError, Config] =
    UriLoader.load(resourceUri, env).flatMap(parse(_, env))

  def parse(configStr: String, env: Map[String, String] = Map.empty): ValidationNel[ConfigError, Config] =
    Try(configStr.parseYaml.convertTo[Config]) match {
      case Success(conf) =>
        // yaml parser does not populate with defaults - force them
        val defaultExtractReader = Config(Nil, Nil, Nil).extract_reader
        val defaultLoadWriter = Config(Nil, Nil, Nil).load_writer
        val conf2 = conf.copy(
          extract_reader = conf.extract_reader.orElse(defaultExtractReader),
          load_writer = conf.load_writer.orElse(defaultLoadWriter)
        )
        conf2.successNel[ConfigError]
      case Failure(e: DeserializationException) =>
        ConfigError(s"Failed to deserialize config body, exception: ${e.getMessage}").failureNel[Config]
      case Failure(e) =>
        ConfigError(s"Failed to parse config body", Some(e)).failureNel[Config]
    }
}
