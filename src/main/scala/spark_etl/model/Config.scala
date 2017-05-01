package spark_etl.model

import net.jcazevedo.moultingyaml._
import spark_etl.ConfigError

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class Config(
                   extracts: List[Extract],
                   transforms: List[Transform],
                   extract_reader: Option[ParametrizedConstructor] = Some(ParametrizedConstructor("spark_etl.parquet.ParquetExtractReader", Some(Map.empty))),
                   load_writer: Option[ParametrizedConstructor] = Some(ParametrizedConstructor("spark_etl.parquet.ParquetLoadWriter", Some(Map.empty))))

object Config extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat4(Config.apply)

  /**
    * Load Config from resource Uri
    */
  def load(resourceUri: String, env: Map[String, String]): ValidationNel[ConfigError, Config] =
    loadResource(resourceUri).flatMap(parse(_, env))

  def parse(configStr: String, env: Map[String, String] = Map.empty): ValidationNel[ConfigError, Config] = {
    replaceEnv(configStr, env).flatMap {
      tokenReplaced =>
        Try(tokenReplaced.parseYaml.convertTo[Config]) match {
          case Success(conf) =>
            // yaml parser does not populate with defaults - force them
            val defaultExtractReader = Config(Nil, Nil).extract_reader
            val defaultLoadWriter = Config(Nil, Nil).load_writer
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
  }

  private def loadResource(resourceUri: String): ValidationNel[ConfigError, String] = {
    val res = getClass.getResource(resourceUri)
    if (res == null)
      ConfigError(s"Failed to read resource $resourceUri").failureNel[String]
    else
      Source.fromURL(res).mkString.successNel[ConfigError]
  }

  private def replaceEnv(s: String, env: Map[String, String]): ValidationNel[ConfigError, String] = {
    val s2 = env.foldLeft(s) {
      case (soFar, (key, value)) => soFar.replaceAll("\\$\\{" + key + "\\}", value)
    }
    if ("""\$\{.*\}""".r.findFirstIn(s2).isDefined)
      ConfigError("Config contains ${var} post env substitutions").failureNel[String]
    else
      s2.successNel[ConfigError]
  }
}
