package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Extract(name: String, `type`: InType, uri: String)

object Extract extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat3(Extract.apply)
}
