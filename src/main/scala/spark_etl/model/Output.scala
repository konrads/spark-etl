package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Output(uri: String, `type`: OutType, partition_by: Option[List[String]] = None, params: Option[Map[String, String]] = None)

object Output extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat4(Output.apply)
}
