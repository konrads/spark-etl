package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Load(name: String, source: String, uri: String, partition_by: Option[List[String]] = None)

object Load extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat4(Load.apply)
}
