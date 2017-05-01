package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Extract(name: String, uri: String, check: Option[String] = None)

object Extract extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat3(Extract.apply)
}
