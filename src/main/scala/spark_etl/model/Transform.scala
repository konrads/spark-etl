package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Transform(name: String, sql: String, cache: Option[Boolean] = None, persist: Option[Persist] = None, check: Option[String] = None)

object Transform extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat5(Transform.apply)
}
