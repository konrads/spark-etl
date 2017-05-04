package spark_etl.parser

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.parser.Parser._

class ParserSpec extends FlatSpec with Matchers with Inside {
  "Parser" should "resolve all UnresolvedRelations" in {
    getDsos("SELECT a from b.b").toSet shouldBe Set("b.b")
    getDsos("SELECT a from b.c").toSet shouldBe Set("b.c")
    getDsos("SELECT a from b"  ).toSet shouldBe Set("b")
    getDsos("SELECT a.x from b").toSet shouldBe Set("b")
    getDsos("SELECT z.x from b").toSet shouldBe Set("b")
  }
}
