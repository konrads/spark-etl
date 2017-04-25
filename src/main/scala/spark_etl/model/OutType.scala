package spark_etl.model

import net.jcazevedo.moultingyaml._

sealed trait OutType
object OutParquet extends OutType
object OutOracle extends OutType

object OutType {
  implicit val yamlFormat = new YamlFormat[OutType] {
    def read(value: YamlValue): OutType = value match {
      case YamlString(x) if x.toLowerCase == "parquet" => OutParquet
      case YamlString(x) if x.toLowerCase == "oracle" => OutOracle
      case _ => deserializationError("InType expected")
    }
    def write(t: OutType) = t match {
      case OutParquet => YamlString("parquet")
      case OutOracle => YamlString("oracle")
    }
  }
}
