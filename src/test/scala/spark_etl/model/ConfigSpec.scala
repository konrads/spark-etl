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

  it should "fail on unaccounted for ${bogus_var}" in {
    val bogusConfig = "extracts: ${bogus_var}"
    inside(Config.parse(bogusConfig)) {
      case Failure(NonEmptyList(err, INil())) =>
        err.msg should startWith("Config contains ${vars}")
    }
  }

  it should "read simple config" in {
    val simpleConfig =
      s"""extracts:
         |  - name: e1
         |    uri: uri1
         |
         |transforms:
         |  - name: s2
         |    sql: sql_uri2
         |    output:
         |      uri: out_uri2
       """.stripMargin
    Config.parse(simpleConfig) shouldBe Success(Config(
      List(Extract("e1", "uri1")),
      List(Transform("s2", "sql_uri2", Output("out_uri2")))
    ))
  }

  it should "substitute tokens" in {
    val simpleConfig =
       """extracts:
         |  - name: e1
         |    uri: ${var1}
         |
         |transforms:
         |  - name: s2
         |    sql: ${var1}
         |    output:
         |      uri: ${var2}
         |      partition_by: [col1, col2]
       """.stripMargin
    Config.parse(simpleConfig, Map("var1" -> "XXX", "var2" -> "YYY")) shouldBe Success(Config(
      List(Extract("e1", "XXX")),
      List(Transform("s2", "XXX", Output("YYY", Some(List("col1", "col2")))))
    ))
  }

  it should "read reader/writer constructors" in {
    val simpleConfig =
      s"""extracts:
         |  - name: e1
         |    uri: uri1
         |
         |transforms:
         |  - name: s2
         |    sql: sql_uri2
         |    output:
         |      uri: out_uri2
         |
         |extract_reader:
         |  class: DummyExtractReader
         |  params:
         |    x: 11
         |    y: aa
         |
         |load_writer:
         |  class: DummyLoadWriter
         |  params:
         |    b: false
         |    a: [1, xxx]
       """.stripMargin
    Config.parse(simpleConfig) shouldBe Success(Config(
      List(Extract("e1", "uri1")),
      List(Transform("s2", "sql_uri2", Output("out_uri2"))),
      Some(ParametrizedConstructor("DummyExtractReader", Some(Map("x" -> 11d, "y" -> "aa")))),
      Some(ParametrizedConstructor("DummyLoadWriter", Some(Map("b" -> false, "a" -> List(1d, "xxx")))))
    ))
  }
}
