package spark_etl.util

import spark_etl.ConfigError

import scala.io.Source
import scalaz.Scalaz._
import scalaz.Validation.FlatMap._
import scalaz.ValidationNel

object UriLoader {
  private val fileProtocol = "file:"
  private val resourceProtocol = "resource:"

  def load(uri: String, filePathRoot: String, env: Map[String, String]): ValidationNel[ConfigError, String] =
    for {
      contents <-
      if (uri.startsWith(fileProtocol)) {
        val filePath = uri.substring(fileProtocol.length)
        val fqFilePath = if (filePath.startsWith("/"))
          filePath
        else if (filePathRoot.endsWith("/"))
          s"$filePathRoot$filePath"
        else
          s"$filePathRoot/$filePath"
        loadFile(fqFilePath, env)
      } else if (uri.startsWith(resourceProtocol))
        loadResource(uri.substring(resourceProtocol.length), env)
      else
        loadResource(uri, env)
      contents2 <- envVarSub(uri, contents, env)
    } yield contents2

  private def loadResource(uri: String, env: Map[String, String]): ValidationNel[ConfigError, String] = {
    val fqUri = getClass.getResource(uri)
    if (fqUri != null)
      Source.fromURL(fqUri).mkString.successNel[ConfigError]
    else
      ConfigError(s"Failed to read resource $uri").failureNel[String]
  }

  private def loadFile(uri: String, env: Map[String, String]): ValidationNel[ConfigError, String] = {
    val file = new java.io.File(uri)
    if (file.canRead)
      scala.io.Source.fromFile(file).mkString.successNel[ConfigError]
    else
      ConfigError(s"Failed to read file $uri").failureNel[String]
  }

  private def envVarSub(uri: String, contents: String, env: Map[String, String]): ValidationNel[ConfigError, String] = {
    // replace all ${k}, with v, ensuring v can contain '$'
    val contents2 = env.foldLeft(contents) { case (soFar, (k, v)) => soFar.replaceAll("\\$\\{" + k + "\\}", v.replaceAll("\\$", "\\\\\\$")) }
    val remainingVars = "\\$\\{.*?\\}".r.findAllIn(contents2)
    if (remainingVars.isEmpty)
      contents2.successNel[ConfigError]
    else {
      val varNames = remainingVars.toList.distinct
      ConfigError(s"Unresolved env vars in $uri: ${varNames.mkString(", ")}").failureNel[String]
    }
  }
}
