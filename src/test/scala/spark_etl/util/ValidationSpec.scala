package spark_etl.util

import org.scalatest.{FlatSpec, Inside, Matchers}
import spark_etl.util.Validation._

class ValidationSpec extends FlatSpec with Matchers with Inside {
  val unitSuccesses = (0 to 9).map(_ => ().success[String])
  val errorStrs = (0 to 9).map(i => s"error:$i")
  val failures = errorStrs.map(i => i.failure[Unit])

  "Validation" should "merge up to 10 successes" in {
    (2 to 9).foreach(i => unitSuccesses.take(i).reduce(_ +++ _) shouldBe Success(()))
    merge("00".success[String], 11.success[String]) {
      (r0, r1) => s"$r0:$r1"
    } shouldBe Success("00:11")
    merge("00".success[String], 11.success[String], "22".success[String]) {
      (r0, r1, r2) => s"$r0:$r1:$r2"
    } shouldBe Success("00:11:22")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String]) {
      (r0, r1, r2, r3) => s"$r0:$r1:$r2:$r3"
    } shouldBe Success("00:11:22:33")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String]) {
      (r0, r1, r2, r3, r4) => s"$r0:$r1:$r2:$r3:$r4"
    } shouldBe Success("00:11:22:33:44")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String]) {
      (r0, r1, r2, r3, r4, r5) => s"$r0:$r1:$r2:$r3:$r4:$r5"
    } shouldBe Success("00:11:22:33:44:55")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String]) {
      (r0, r1, r2, r3, r4, r5, r6) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6"
    } shouldBe Success("00:11:22:33:44:55:66")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], 77.success[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7"
    } shouldBe Success("00:11:22:33:44:55:66:77")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], 77.success[String], "88".success[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7, r8) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7:$r8"
    } shouldBe Success("00:11:22:33:44:55:66:77:88")
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], 77.success[String], "88".success[String], 99.success[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7, r8, r9) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7:$r8:$r9"
    } shouldBe Success("00:11:22:33:44:55:66:77:88:99")
  }

  it should "merge up to 10 failures" in {
    (2 to 9).foreach(i => failures.take(i).reduce(_ +++ _) shouldBe Failure(errorStrs.take(i)))
    merge("00".success[String], "err".failure[String]) {
      (r0, r1) => s"$r0:$r1"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "err".failure[String]) {
      (r0, r1, r2) => s"$r0:$r1:$r2"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], "err".failure[String]) {
      (r0, r1, r2, r3) => s"$r0:$r1:$r2:$r3"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4) => s"$r0:$r1:$r2:$r3:$r4"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4, r5) => s"$r0:$r1:$r2:$r3:$r4:$r5"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4, r5, r6) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], 77.success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7, r8) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7:$r8"
    } shouldBe Failure(List("err"))
    merge("00".success[String], 11.success[String], "22".success[String], 33.success[String], "44".success[String], 55.success[String], "66".success[String], 77.success[String], "88".success[String], "err".failure[String]) {
      (r0, r1, r2, r3, r4, r5, r6, r7, r8, r9) => s"$r0:$r1:$r2:$r3:$r4:$r5:$r6:$r7:$r8:$r9"
    } shouldBe Failure(List("err"))
  }

  it should "merge with success: unit, list, seq, set, map" in {
    ().success[String] +++ ().success[String] shouldBe Success(())
    List(1, 2).success[String] +++ List(1, 3).success[String] shouldBe Success(List(1, 2, 1, 3))
    Seq(1, 2).success[String] +++ Seq(1, 3).success[String] shouldBe Success(Seq(1, 2, 1, 3))
    Set(1, 2).success[String] +++ Set(1, 3).success[String] shouldBe Success(Set(1, 2, 3))
    Map("a" -> 1, "b" -> 2).success[String] +++ Map("a" -> 11, "c" -> 33).success[String] shouldBe Success(Map("a" -> 11, "b" -> 2, "c" -> 33))
  }

  it should "map" in {
    11.success[Int].map(_ * 2) shouldBe Success(22)
    "err".failure[Int].map(_ * 2) shouldBe Failure(List("err"))
  }

  it should "flatMap" in {
    (for {
      x1 <- 11.success[Int]
      x2 <- 22.success[Int]
    } yield x1 + x2) shouldBe Success(33)
    (for {
      x1 <- "err".failure[Int]
      x2 <- 22.success[String]
    } yield x1 + x2) shouldBe Failure(List("err"))
  }
}
