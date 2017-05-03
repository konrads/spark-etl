package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Transform(name: String, sql: String, check: Option[String] = None)

object Transform extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat3(Transform.apply)
}
