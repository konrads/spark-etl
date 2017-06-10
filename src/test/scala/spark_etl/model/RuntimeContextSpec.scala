package spark_etl.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.util.Validation._
import spark_etl.util._
import spark_etl.{ConfigError, ExtractReader, LoadWriter}

class RuntimeContextSpec extends FlatSpec with Matchers with Inside {
  val extractsAndTransformsStr =
    """
      |extracts:
      |  - name:  client
      |    uri:   "data/dev/client_2017"
      |    check: "/runtime-ctx/spark/extract-check/client.sql"
      |  - name:  item
      |    uri:   "data/dev/item_2017"
      |    check: "/runtime-ctx/spark/extract-check/item.sql"
      |  - name:  transaction
      |    uri:   "data/dev/transaction_2017"
      |    check: "/runtime-ctx/spark/extract-check/transaction.sql"
      |  # unused extract
      |  - name:  ____bogus_extract_not_loaded____
      |    uri:   "hdfs://aaa.bbb"
      |
      |transforms:
      |  - name:  client_spending
      |    check: "/runtime-ctx/spark/transform-check/client_spending.sql"
      |    sql:   "/runtime-ctx/spark/transform/client_spending.sql"
      |  - name:  item_purchase
      |    check: "/runtime-ctx/spark/transform-check/item_purchase.sql"
      |    sql:   "/runtime-ctx/spark/transform/item_purchase.sql"
      |  - name:  minor_purchase
      |    check: "/runtime-ctx/spark/transform-check/minor_purchase.sql"
      |    sql:   "/runtime-ctx/spark/transform/minor_purchase.sql"
      |
      |loads:
      |  - name:   client_spending_out
      |    source: client_spending
      |    uri:    "/tmp/out/client_spending"
      |    # no partition_by
      |  - name:   item_purchase_out
      |    source: item_purchase
      |    uri:    "/tmp/out/item_purchase"
      |    # no partition_by
      |  - name:   minor_purchase_out
      |    source: minor_purchase
      |    uri:    "/tmp/out/minor_purchase"
      |    # no partition_by
      |    """.stripMargin

  "RuntimeContext" should "validate ok extract_reader/load_writer" in {
    val confStr = extractsAndTransformsStr +
      """
        |extract_reader:
        |  class: spark_etl.model.OkExtractReader
        |  params:
        |    x: 11
        |    y: aa
        |
        |load_writer:
        |  class: spark_etl.model.OkLoadWriter
        |  params:
        |    b: false
        |    a: [1, xxx]
      """.stripMargin
    Config.parse(confStr) match {
      case Success(conf) =>
        RuntimeContext.load(conf, ".", Map.empty) match {
          case Success(ctx) =>
            ctx.extractReader.asInstanceOf[OkExtractReader].params shouldBe Map("x" -> 11d, "y" -> "aa")
            ctx.loadWriter.asInstanceOf[OkLoadWriter].params shouldBe Map("b" -> false, "a" -> List(1d, "xxx"))
        }
    }
  }

  it should "fail on incorrect inheritance of extract_reader/load_writer" in {
    val confStr = extractsAndTransformsStr +
      """
        |extract_reader:
        |  class: spark_etl.model.BogusExtractReader1
        |
        |load_writer:
        |  class: spark_etl.model.BogusLoadWriter1
      """.stripMargin
    Config.parse(confStr) match {
      case Success(conf) =>
        RuntimeContext.load(conf, ".", Map.empty) match {
          case Failure(errs) =>
            errs.toList.length shouldBe 2
            errs.toList.forall(_.msg.startsWith("Failed to cast class")) shouldBe true
        }
    }
  }

  it should "fail on parameterless constructors extract_reader/load_writer" in {
    val confStr = extractsAndTransformsStr +
      """
        |extract_reader:
        |  class: spark_etl.model.BogusExtractReader2
        |
        |load_writer:
        |  class: spark_etl.model.BogusLoadWriter2
      """.stripMargin
    Config.parse(confStr) match {
      case Success(conf) =>
        RuntimeContext.load(conf, ".", Map.empty) match {
          case Failure(errs) =>
            errs.toList.length shouldBe 2
            errs.toList.forall(_.msg.startsWith("Failed to instantiate class")) shouldBe true
        }
    }
  }

  it should "fail on duplicates" in {
    val confStr =
      """
      |extracts:
      |  - name:  client
      |    uri:   "data/dev/client_2017"
      |    check: "/runtime-ctx/spark/extract-check/client.sql"
      |  - name:  client
      |    uri:   "data/dev/client_2017"
      |    check: "/runtime-ctx/spark/extract-check/client.sql"
      |
      |transforms:
      |  - name:  client_spending
      |    check: "/runtime-ctx/spark/transform-check/client_spending.sql"
      |    sql:   "/runtime-ctx/spark/transform/client_all.sql"
      |  - name:  client_spending
      |    check: "/runtime-ctx/spark/transform-check/client_spending.sql"
      |    sql:   "/runtime-ctx/spark/transform/client_all.sql"
      |
      |loads:
      |  - name:   client_spending_out
      |    source: client_spending
      |    uri:    "/tmp/out/client_spending"
      |  - name:   client_spending_out
      |    source: client_spending
      |    uri:    "/tmp/out/client_spending"
      |""".stripMargin

    Config.parse(confStr) match {
      case Success(conf) =>
        RuntimeContext.load(conf, ".", Map.empty) match {
          case Failure(errs) =>
            val errList = errs.toList
            errList.length shouldBe 8
            errList.forall(_.msg.startsWith("Duplicates found for")) shouldBe true
        }
    }
  }

  it should "produce dot file" in {
    Config.parse(extractsAndTransformsStr) match {
      case Success(conf) =>
        RuntimeContext.load(conf, ".", Map.empty) match {
          case Success(ctx) =>
            val v = ctx.asDot
            ctx.asDot shouldBe
            """digraph Lineage {
              |  rankdir=LR
              |  node [fontsize=12]
              |
              |  # vertices
              |  client
              |  item
              |  transaction
              |  client_spending [shape=component]
              |  item_purchase [shape=component]
              |  minor_purchase [shape=component]
              |  client_spending_out [shape=cylinder]
              |  item_purchase_out [shape=cylinder]
              |  minor_purchase_out [shape=cylinder]
              |
              |  # edges
              |  item -> client_spending [style=dotted]
              |  transaction -> client_spending [style=dotted]
              |  client -> client_spending [style=dotted]
              |  item -> item_purchase [style=dotted]
              |  transaction -> item_purchase [style=dotted]
              |  client -> item_purchase [style=dotted]
              |  item -> minor_purchase [style=dotted]
              |  transaction -> minor_purchase [style=dotted]
              |  client -> minor_purchase [style=dotted]
              |  client_spending -> client_spending_out
              |  item_purchase -> item_purchase_out
              |  minor_purchase -> minor_purchase_out
              |
              |  # ranks
              |  { rank=same; client item transaction }
              |  { rank=same; client_spending_out item_purchase_out minor_purchase_out }
              |}""".stripMargin
        }
    }

  }
}

class OkExtractReader(val params: Map[String, Any]) extends ExtractReader(params) {
  override def checkLocal(extracts: Seq[Extract]): Validation[ConfigError, Unit] = ().success[ConfigError]
  override def checkRemote(extracts: Seq[Extract]): Validation[ConfigError, Unit] = ???
  override def read(extracts: Seq[Extract])(implicit spark: SparkSession): Seq[(Extract, DataFrame)] = ???
}

class OkLoadWriter(val params: Map[String, Any]) extends LoadWriter(params) {
  override def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit = ???
  override def checkLocal(loads: Seq[Load]): Validation[ConfigError, Unit] = ().success[ConfigError]
  override def checkRemote(loads: Seq[Load]): Validation[ConfigError, Unit] = ???
}

class BogusExtractReader1(params: Map[String, Any])

class BogusLoadWriter1(params: Map[String, Any])

class BogusExtractReader2 extends ExtractReader(Map.empty) {
  override def checkLocal(extracts: Seq[Extract]): Validation[ConfigError, Unit] = ().success[ConfigError]
  override def checkRemote(extracts: Seq[Extract]): Validation[ConfigError, Unit] = ???
  override def read(extracts: Seq[Extract])(implicit spark: SparkSession): Seq[(Extract, DataFrame)] = ???
}

class BogusLoadWriter2 extends LoadWriter(Map.empty) {
  override def write(loadsAndDfs: Seq[(Load, DataFrame)]): Unit = ???
  override def checkLocal(loads: Seq[Load]): Validation[ConfigError, Unit] = ().success[ConfigError]
  override def checkRemote(loads: Seq[Load]): Validation[ConfigError, Unit] = ???
}
