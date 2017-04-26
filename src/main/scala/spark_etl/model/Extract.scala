package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Extract(name: String, `type`: InType, uri: String, check: Option[Resource])

object Extract extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat4(Extract.apply)
}
