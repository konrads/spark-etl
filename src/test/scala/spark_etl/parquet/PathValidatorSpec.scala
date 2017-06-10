package spark_etl.parquet

import java.io.File

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.util._

class PathValidatorSpec extends FlatSpec with Matchers with Inside {
  val root = Files.rootResource

  // create an empty dir, without a placeholder
  val emptyDir = new File(s"$root/parquet/empty")
  if (! emptyDir.exists)
    emptyDir.mkdir()

  "PathValidator" should "validate local `good` path" in {
    PathValidator.validate(
      true,
      true,
      s"$root/parquet/good"
    ) shouldBe Success(List(s"$root/parquet/good"))
  }

  it should "validate local `bad` path" in {
    val res = PathValidator.validate(
      true,
      true,
      s"$root/parquet/empty",
      s"$root/parquet/with_backup_dir",
      s"$root/parquet/__bogus_dir__"
    )
    inside(res) {
      case Failure(errs) =>
        val errMsgs = errs.toList.map(_.msg).sorted
        errMsgs.length shouldBe 3
        errMsgs(0) should startWith("Local path doesn't exist")
        errMsgs(1) should startWith("Local path is empty for")
        errMsgs(2) should startWith("Unexpected local children for")
    }
  }
}
