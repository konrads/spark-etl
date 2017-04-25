package spark_etl.model

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

case class Resource(uri: Option[String] = None, contents: Option[String] = None) {
  def shortDesc: String = uri.map(u => s"uri:$u").getOrElse(contents.map(toShort(20)).getOrElse("¡No uri/contents!"))

  private def toShort(maxLength: Int)(s: String) =
    if (s.length > maxLength)
      s.slice(0, maxLength) + "..."
    else
      s
}

object Resource extends DefaultYamlProtocol {
  implicit val yamlFormat = yamlFormat2(Resource.apply)
}
