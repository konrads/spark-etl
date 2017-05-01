package spark_etl.model

import net.jcazevedo.moultingyaml._

case class ParametrizedConstructor(`class`: String, params: Option[Map[String, Any]] = Some(Map.empty))

object ParametrizedConstructor extends DefaultYamlProtocol {
  implicit val mapFormat: YamlFormat[Map[String, Any]] = new YamlFormat[Map[String, Any]] {
    override def read(v: YamlValue): Map[String, Any] = v match {
      case o: YamlObject => readValue(o).asInstanceOf[Map[String, Any]]
      case other => deserializationError(s"Map like object expected, got $other")
    }

    def readValue: (YamlValue) => Any = {
      case x: YamlBoolean  => x.boolean
      case x: YamlDate     => x.date.toDate
      case x: YamlNumber   => x.value.toInt
      case x: YamlString   => x.value
      case x: YamlObject   => x.fields.map { case (k, v) => asStr(k) -> readValue(v)}
      case x: YamlArray    => x.elements.map(readValue)
      case x: YamlSet      => x.set.map(readValue)
      case YamlNull        => null
      case YamlNaN         => Double.NaN
      case YamlNegativeInf => Double.NegativeInfinity
      case YamlPositiveInf => Double.PositiveInfinity
    }

    def asStr: (YamlValue) => String = {
      case x: YamlBoolean => x.boolean.toString
      case x: YamlDate    => x.date.toString
      case x: YamlNumber  => x.value.toString
      case x: YamlString  => x.value
      case YamlNull        => null
      case YamlNaN         => "nan"
      case YamlNegativeInf => "-∞"
      case YamlPositiveInf => "∞"
      case other => deserializationError(s"Failed to stringify map key: $other")
    }

    override def write(obj: Map[String, Any]): YamlValue = ???
  }

  implicit val yamlFormat = yamlFormat2(ParametrizedConstructor.apply)
}
