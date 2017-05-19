package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Extract(name: String, uri: String, cache: Option[Boolean] = None, persist: Option[Persist] = None, check: Option[String] = None)

object Extract extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat5(Extract.apply)
}
