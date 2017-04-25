package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Transform(name: String, sql: Resource, output: Output, pre_check: Option[Resource], post_check: Option[Resource])

object Transform extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat5(Transform.apply)
}
