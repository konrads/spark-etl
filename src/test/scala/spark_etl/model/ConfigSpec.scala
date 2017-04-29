package spark_etl.model

import org.scalatest.{FlatSpec, Inside, Matchers}

import scalaz.{Failure, INil, NonEmptyList, Success}

class ConfigSpec extends FlatSpec with Matchers with Inside {
  "Config" should "fail to parse" in {
    val bogusConfig = "NOT A CONFIG"
    inside(Config.parse(bogusConfig)) {
      case Failure(NonEmptyList(err, INil())) =>
        err.msg should startWith("Failed to deserialize")
    }
  }

  it should "read simple config" in {
    val simpleConfig =
      s"""extracts:
         |  - name: e1
         |    type: parquet
         |    uri:  uri1
         |
         |transforms:
         |  - name: s2
         |    sql:
         |      uri: sql_uri2
         |    output:
         |      type: parquet
         |      uri:  out_uri2
       """.stripMargin
    Config.parse(simpleConfig) shouldBe Success(Config(
      List(Extract("e1", InParquet, "uri1")),
      List(Transform("s2", Resource(Some("sql_uri2")), Output("out_uri2", OutParquet)))
    ))
  }

  it should "substitute tokens" in {
    val simpleConfig =
       """extracts:
         |  - name: e1
         |    type: parquet
         |    uri:  ${var1}
         |
         |transforms:
         |  - name: s2
         |    sql:
         |      uri: ${var1}
         |    output:
         |      type: parquet
         |      uri:  ${var2}
       """.stripMargin
    Config.parse(simpleConfig, Map("var1" -> "XXX", "var2" -> "YYY")) shouldBe Success(Config(
      List(Extract("e1", InParquet, "XXX")),
      List(Transform("s2", Resource(Some("XXX")), Output("YYY", OutParquet)))
    ))
  }

  it should "fail on unaccounted for ${bogus_var}" in {
    val bogusConfig = "extracts: ${bogus_var}"
    inside(Config.parse(bogusConfig)) {
      case Failure(NonEmptyList(err, INil())) =>
        err.msg should startWith("Config contains ${var}")
    }

  }
}
