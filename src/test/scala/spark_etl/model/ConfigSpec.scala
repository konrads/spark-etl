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
         |    uri: e1_uri
         |
         |transforms:
         |  - name: t1
         |    sql: t1_uri
         |
         |loads:
         |  - name: l1
         |    source: t1
         |    uri: l1_uri
       """.stripMargin
    Config.parse(simpleConfig) shouldBe Success(Config(
      List(Extract("e1", "e1_uri")),
      List(Transform("t1", "t1_uri")),
      List(Load("l1", "t1", "l1_uri"))
    ))
  }

  it should "read reader/writer constructors" in {
    val simpleConfig =
      s"""extracts:
         |  - name: e1
         |    uri: e1_uri
         |
         |transforms:
         |  - name: t1
         |    sql: t1_uri
         |
         |loads:
         |  - name: l1
         |    source: t1
         |    uri: l1_uri
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
      List(Extract("e1", "e1_uri")),
      List(Transform("t1", "t1_uri")),
      List(Load("l1", "t1", "l1_uri")),
      Some(ParametrizedConstructor("DummyExtractReader", Some(Map("x" -> 11d, "y" -> "aa")))),
      Some(ParametrizedConstructor("DummyLoadWriter", Some(Map("b" -> false, "a" -> List(1d, "xxx")))))
    ))
  }
}
