package spark_etl.util

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.ConfigError
import spark_etl.util.Validation._

class UriLoaderSpec extends FlatSpec with Matchers with Inside {
  "UriLoader" should "read resource without tokens" in {
    validateWithoutVars("/uri-loader/without_env_vars", "__ignore__")
    validateWithoutVars("resource:/uri-loader/without_env_vars", "__ignore__")
  }

  it should "read resource with tokens" in {
    validateWithVars("/uri-loader/with_env_vars", "__ignore__")
    validateWithVars("resource:/uri-loader/with_env_vars", "__ignore__")
  }

  it should "read file without tokens" in {
    val asFilename = getClass.getResource("/uri-loader/without_env_vars").toString
    asFilename should startWith("file:")
    validateWithoutVars(asFilename, "__ignore__")
  }

  it should "read file with tokens" in {
    val asFilename = getClass.getResource("/uri-loader/with_env_vars").toString
    asFilename should startWith("file:")
    validateWithVars(asFilename, "__ignore__")
    validateWithVars("file:uri-loader/with_env_vars", getClass.getResource("/").getFile)
  }

  it should "fail to read bogus files/resources" in {
    validateNotExists("/bogus_uri", "__ignore__")
    validateNotExists("/bogus_uri", Files.pwd)
    validateNotExists("resource:/bogus_uri", "__ignore__")
    validateNotExists("resource:/bogus_uri", Files.pwd)
    validateNotExists("file:/bogus_uri", "__ignore__")
    validateNotExists("file:/bogus_uri", Files.pwd)
  }

  it should "read #includes" in {
    val expected =
      """|===
         |hello there
         |123
         |
         |---
         |111 val1 222 val2 333 val1 444
         |
         |+++""".stripMargin
    UriLoader.load("/uri-loader/with_includes", "__ignore__", Map("var1" -> "val1", "var2" -> "val2")) shouldBe expected.success[ConfigError]
  }

  it should "read fail on bogus #include" in {
    inside(UriLoader.load("/uri-loader/with_bogus_includes", "__ignore__", Map("var1" -> "val1", "var2" -> "val2"))) {
      case Failure(err) =>
        err.toList shouldBe List(ConfigError("Failed to read resource __bogus_include__"))
    }
  }

  private def validateWithoutVars(uri: String, fileRoot: String) = {
    UriLoader.load(uri, fileRoot, Map("var1" -> "val1")) shouldBe "hello there\n123\n".success[ConfigError]
  }

  private def validateWithVars(uri: String, fileRoot: String) = {
    UriLoader.load(uri, fileRoot, Map("var1" -> "val1", "var2" -> "val2")) shouldBe "111 val1 222 val2 333 val1 444\n".success[ConfigError]
    inside(UriLoader.load(uri, fileRoot, Map.empty)) {
      case Failure(err) =>
        err.toList.length shouldBe 1
        err.head.msg should startWith("Unresolved env vars in")
        err.head.msg should endWith("${var1}, ${var2}")
    }
  }

  private def validateNotExists(uri: String, fileRoot: String) = {
    inside(UriLoader.load(uri, fileRoot, Map.empty)) {
      case Failure(err) =>
        err.toList.length shouldBe 1
        err.head.msg should startWith("Failed to read")
    }
  }
}
