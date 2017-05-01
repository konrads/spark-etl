package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Output(uri: String, partition_by: Option[List[String]] = None)

object Output extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat2(Output.apply)
}
