package spark_etl.model

import net.jcazevedo.moultingyaml._
import spark_etl.ConfigError

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz._

case class Config(extracts: List[Extract], transforms: List[Transform])

object Config extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat2(Config.apply)

  /**
    * Load Config from resource Uri
    */
  def load(resourceUri: String): ValidationNel[ConfigError, Config] = {
    loadResource(resourceUri).flatMap {
      configStr =>
        Try(configStr.parseYaml.convertTo[Config]) match {
          case Success(conf) =>
            conf.successNel[ConfigError]
          case Failure(e: DeserializationException) =>
            ConfigError(s"Failed to deserialize config body of $resourceUri, exception: ${e.getMessage}").failureNel[Config]
          case Failure(e) =>
            ConfigError(s"Failed to parse config body of $resourceUri", Some(e)).failureNel[Config]
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
}
