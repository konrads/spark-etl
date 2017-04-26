package spark_etl.model

import net.jcazevedo.moultingyaml._

sealed trait InType
object InParquet extends InType
object InCSV extends InType

object InType {
  implicit val yamlFormat = new YamlFormat[InType] {
    def read(value: YamlValue): InType = value match {
      case YamlString(x) if x.toLowerCase == "parquet" => InParquet
      case YamlString(x) if x.toLowerCase == "csv" => InCSV
      case _ => deserializationError("InType expected")
    }
    def write(t: InType) = t match {
      case InParquet => YamlString("parquet")
      case InCSV => YamlString("csv")
    }
  }
}
