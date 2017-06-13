package spark_etl.util

import spark_etl.util.SparkParser.QfDep
import org.scalatest._

class SparkParserSpec extends FlatSpec with Matchers {
  val complexSql =
    """
      |-- select all from client, transaction, item
      |SELECT *
      |  FROM namespace1.client c,
      |       namespace2.transaction t,
      |       item i
      | WHERE c.id = t.c_id AND t.i_id = i.id""".stripMargin

  "SparkParser" should "fetch deps" in {
    SparkParser.getDeps(complexSql) should contain allOf(
      QfDep("client",      Some("namespace1")),
      QfDep("transaction", Some("namespace2")),
      QfDep("item")
    )
  }

  it should "strip db prefixes" in {
    SparkParser.stripDbs(complexSql) shouldBe
      """
        |-- select all from client, transaction, item
        |SELECT *
        |  FROM client c,
        |       transaction t,
        |       item i
        | WHERE c.id = t.c_id AND t.i_id = i.id""".stripMargin
  }
}