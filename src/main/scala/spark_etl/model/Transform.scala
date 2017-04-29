package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Transform(name: String, sql: Resource, output: Output, check: Option[Resource] = None)

object Transform extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat4(Transform.apply)
}
