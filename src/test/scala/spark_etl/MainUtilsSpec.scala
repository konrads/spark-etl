package spark_etl

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.util.Files

import scalaz.Scalaz._
import scalaz._

class MainUtilsSpec extends FlatSpec with Matchers with Inside {
  val root = Files.rootResource

  "Main" should "validate-local complex file specs" in {
    val envVars = Map("engine" -> "spark", "length_fun" -> "length", "count_fun" -> "count", "join_type" -> "LEFT OUTER JOIN")
    Main.main(Array("-Denv.engine=spark", "-Denv.length_fun=length", "-Denv.count_fun=count", "-Denv.join_type=LEFT OUTER JOIN", s"--conf-uri=file:$root/main-utils/config/app.yaml", "validate-local"))
  }

  "MainUtils" should "validate-local complex file specs" in {
    val envVars = Map("engine" -> "spark", "length_fun" -> "length", "count_fun" -> "count", "join_type" -> "LEFT OUTER JOIN")
    MainUtils.validateLocal("file:main-utils/config/app.yaml", root, envVars) shouldBe Success(())
  }

  it should "fail on missing app.yaml env vars" in {
    val envVars = Map("length_fun" -> "length", "count_fun" -> "count", "join_type" -> "LEFT OUTER JOIN")
    inside(MainUtils.validateLocal("file:main-utils/config/app.yaml", root, envVars)) {
      case Failure(errs) =>
        errs.toList.length shouldBe 1
        errs.toList.head.msg shouldBe "Unresolved env vars in file:main-utils/config/app.yaml: ${engine}"
    }
  }

  it should "fail on missing SQL env vars" in {
    val envVars = Map("engine" -> "spark")
    inside(MainUtils.validateLocal("file:main-utils/config/app.yaml", root, envVars)) {
      case Failure(errs) =>
        errs.toList.map(_.msg).sorted shouldBe List(
          "Unresolved env vars in file:../spark/extract-check/client.sql: ${count_fun}",
          "Unresolved env vars in file:../spark/transform-check/item_purchase.sql: ${length_fun}",
          "Unresolved env vars in file:../spark/transform/item_purchase.sql: ${join_type}"
        )
    }
  }
}
