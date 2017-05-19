package spark_etl.parquet

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.Main
import spark_etl.model.{Extract, Load}
import spark_etl.util.Files

class WriteReadRoundtripSpec extends FlatSpec with Matchers with Inside {
  val root = Files.rootResource

  "Reader and Writer" should "roundtrip" in {
    testRoundtrip(None)
  }

  it should "roundtrip partitioned" in {
    testRoundtrip(Some(List("s")))
  }

  "Main" should "transform" in {
    // cleanup dir if exists
    FileUtils.deleteDirectory(new File(s"$root/parquet-roundtrip/x"))

    implicit val spark = SparkSession.builder.appName("test")
      .master("local[1]")
      .config("spark.ui.port", 4046).config("spark.history.ui.port", 18086)
      .getOrCreate
    try {
      import spark.implicits._
      val in = Seq(ParquetTestLoad("a", 1), ParquetTestLoad("b", 2))
      val dfIn = in.toDF()
      val writer = new ParquetLoadWriter(Map.empty)
      writer.write(Seq(
        (Load("x", "x", s"$root/parquet-roundtrip/x", Some(List("s"))), dfIn)
      ))
    } finally {
      spark.stop
    }

    Main.main(Array(s"-Denv.path=$root/parquet-roundtrip", "--conf-uri", s"/parquet-roundtrip/app.yaml", "validate-remote"))
  }

  private def testRoundtrip(partitionBy: Option[List[String]]) = {
    // cleanup dir if exists
    FileUtils.deleteDirectory(new File(s"$root/x"))

    // run the test
    implicit val spark = SparkSession.builder.appName("test")
      .master("local[1]")
      .config("spark.ui.port", 4045).config("spark.history.ui.port", 18085)
      .getOrCreate
    try {
      import spark.implicits._
      val in = Seq(ParquetTestLoad("a", 1), ParquetTestLoad("b", 2))
      val dfIn = in.toDF()
      val writer = new ParquetLoadWriter(Map.empty)
      writer.write(Seq(
        (Load("x", "x", s"$root/x", partitionBy), dfIn)
      ))
      val reader = new ParquetExtractReader(Map.empty)
      val (_, dfOut) :: Nil = reader.read(Seq(Extract("x", s"$root/x")))

      dfOut.as[ParquetTestLoad].collect() should contain allElementsOf(in)
    } finally {
      spark.stop
    }
  }
}

case class ParquetTestLoad(s: String, i: Int)
