package spark_etl.oracle

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.scalatest._
import spark_etl.model.Load

class OracleLoadAppenderSpec extends FlatSpec with Matchers with Inside with BeforeAndAfterAll {
  val dbUri = "jdbc:h2:mem:testDB;user=user;password=pwd"
  val props = new Properties()
  val conn = DriverManager.getConnection(s"$dbUri;create=true", props)

  conn.createStatement().execute("CREATE TABLE user_tables(table_name VARCHAR(20))")
  conn.createStatement().execute("CREATE TABLE test_table(loadid VARCHAR(36), name VARCHAR(20), age INT)")
  conn.prepareStatement("INSERT INTO user_tables(table_name) values ('test_table')").executeUpdate()
  conn.commit()

  val loads = Seq(Load("test_load", "test_transform", "test_table"))

  "OracleLoadAppender" should "validate-local" in {
    new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_user" -> "user", "oracle_password" -> "pwd", "oracle_batch" -> 100)).checkLocal(loads).isSuccess shouldBe true
    new OracleLoadAppender(Map("oracle_user" -> "user", "oracle_password" -> "pwd", "oracle_batch" -> 100)).checkLocal(loads).isSuccess shouldBe false
    new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_password" -> "pwd", "oracle_batch" -> 100)).checkLocal(loads).isSuccess shouldBe false
    new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_user" -> "user", "oracle_batch" -> 100)).checkLocal(loads).isSuccess shouldBe false
    new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_user" -> "user", "oracle_password" -> "pwd")).checkLocal(loads).isSuccess shouldBe false
  }

  it should "validate-remote" in {
    new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_user" -> "user", "oracle_password" -> "pwd", "oracle_batch" -> 100)).checkRemote(loads).isSuccess shouldBe true
    new OracleLoadAppender(Map("oracle_uri" -> "jdbc:h2:mem:__bogus_db__", "oracle_user" -> "user", "oracle_password" -> "pwd", "oracle_batch" -> 100)).checkRemote(loads).isSuccess shouldBe false
  }

  it should "write" in {
    implicit val spark = SparkSession.builder.appName("test")
      .master("local[1]")
      .config("spark.ui.port", 4050).config("spark.history.ui.port", 18090)
      .getOrCreate
    try {
      import spark.implicits._
      val inDF = Seq(PersonRow("Joe", 50), PersonRow("Jane", 23)).toDF()
      // mocking driver for db
      new OracleLoadAppender(Map("oracle_uri" -> dbUri, "oracle_user" -> "user", "oracle_password" -> "pwd", "oracle_driver" -> "org.h2.Driver")).write(Seq(
        (Load("test_load", "test_transform", "test_table"), inDF)
      ))
    } finally {
      spark.stop()
    }

    conn.commit()

    // validate load write
    val rs = conn.prepareStatement("SELECT count(1) FROM test_table").executeQuery()
    rs.next()
    rs.getInt(1) shouldBe 2
  }

  override def afterAll: Unit = conn.createStatement().execute("SHUTDOWN")
}

case class PersonRow(NAME: String, AGE: Int)  // Note, fields need to be uppercase, otherwise it confuses spark.jdbc
