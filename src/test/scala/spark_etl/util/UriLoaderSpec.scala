package spark_etl.util

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.ConfigError

import scalaz.Failure
import scalaz.Scalaz._

class UriLoaderSpec extends FlatSpec with Matchers with Inside {
  "UriLoader" should "read resource without tokens" in {
    validateWithoutVars("/uri-loader/without_env_vars")
    validateWithoutVars("resource:/uri-loader/without_env_vars")
  }

  it should "read resource with tokens" in {
    validateWithVars("/uri-loader/with_env_vars")
    validateWithVars("resource:/uri-loader/with_env_vars")
  }

  it should "read file without tokens" in {
    val asFilename = getClass.getResource("/uri-loader/without_env_vars").toString
    asFilename should startWith("file:")
    validateWithoutVars(asFilename)
  }

  it should "read file with tokens" in {
    val asFilename = getClass.getResource("/uri-loader/with_env_vars").toString
    asFilename should startWith("file:")
    validateWithVars(asFilename)
  }

  private def validateWithoutVars(uri: String) = {
    UriLoader.load(uri, Map("var1" -> "val1")) shouldBe "hello there\n123\n".successNel[ConfigError]
  }

  private def validateWithVars(uri: String) = {
    UriLoader.load(uri, Map("var1" -> "val1", "var2" -> "val2")) shouldBe "111 val1 222 val2 333 val1 444\n".successNel[ConfigError]
    inside(UriLoader.load(uri, Map.empty)) {
      case Failure(err) =>
        err.toList.length shouldBe 1
        err.head.msg should startWith("Unresolved env vars in")
        err.head.msg should endWith("${var1}, ${var2}")
    }
  }
}
