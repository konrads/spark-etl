package spark_etl.util

import scala.language.implicitConversions

/**
  * Limited scalaz's Validation. Supports map(), flatMap() and +++ (for reduce())
  */
trait Validation[Err, S] {
  def +++[S2, Out](v: Validation[Err, S2])(implicit mapper: ValidationMapper[S, S2, Out]): Validation[Err, Out] = {
    (this, v) match {
      case (Failure(ls1), Failure(ls2)) => Failure(ls1 ++ ls2)
      case (Failure(ls),  Success(_))  => Failure(ls)
      case (Success(_),   Failure(ls))  => Failure(ls)
      case (Success(r1),  Success(r2)) => Success(mapper.map(r1, r2))
    }
  }

  def map[Out](f: S => Out): Validation[Err, Out] = this match {
    case Success(r) => Success(f(r))
    case Failure(ls) => Failure(ls)
  }

  def flatMap[Out](f: S => Validation[Err, Out]): Validation[Err, Out] = this match {
    case Success(r) => f(r)
    case Failure(ls) => Failure(ls)
  }

  protected def getErrs: Seq[Err]
}

object Validation {
  implicit val unitMapper = new ValidationMapper[Unit, Unit, Unit] { def map(in1: Unit, in2: Unit) = () }
  implicit def listMapper[X] = new ValidationMapper[List[X], List[X], List[X]] { def map(in1: List[X], in2: List[X]) = in1 ++ in2 }
  implicit def seqMapper[X] = new ValidationMapper[Seq[X], Seq[X], Seq[X]] { def map(in1: Seq[X], in2: Seq[X]) = in1 ++ in2 }
  implicit def setMapper[X] = new ValidationMapper[Set[X], Set[X], Set[X]] { def map(in1: Set[X], in2: Set[X]) = in1 ++ in2 }
  implicit def mapMapper[K, V] = new ValidationMapper[Map[K, V], Map[K, V], Map[K, V]] { def map(in1: Map[K, V], in2: Map[K, V]) = in1 ++ in2 }
  implicit def validationOps[A](a: A): ValidationOps[A] = new ValidationOps(a)

  def merge[Err, S0, S1, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1])(map: (S0, S1) => Out): Validation[Err, Out] = {
    (v0, v1) match {
      case (Success(s0), Success(s1)) =>
        val out = map(s0, s1)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2])(map: (S0, S1, S2) => Out): Validation[Err, Out] = {
    (v0, v1, v2) match {
      case (Success(s0), Success(s1), Success(s2)) =>
        val out = map(s0, s1, s2)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3])(map: (S0, S1, S2, S3) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3)) =>
        val out = map(s0, s1, s2, s3)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4])(map: (S0, S1, S2, S3, S4) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4)) =>
        val out = map(s0, s1, s2, s3, s4)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, S5, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5])(map: (S0, S1, S2, S3, S4, S5) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4, v5) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5)) =>
        val out = map(s0, s1, s2, s3, s4, s5)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs ++ v5.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6])(map: (S0, S1, S2, S3, S4, S5, S6) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4, v5, v6) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6)) =>
        val out = map(s0, s1, s2, s3, s4, s5, s6)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs ++ v5.getErrs ++ v6.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, S7, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6],
    v7: Validation[Err, S7])(map: (S0, S1, S2, S3, S4, S5, S6, S7) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4, v5, v6, v7) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7)) =>
        val out = map(s0, s1, s2, s3, s4, s5, s6, s7)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs ++ v5.getErrs ++ v6.getErrs ++ v7.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, S7, S8, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6],
    v7: Validation[Err, S7],
    v8: Validation[Err, S8])(map: (S0, S1, S2, S3, S4, S5, S6, S7, S8) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4, v5, v6, v7, v8) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7), Success(s8)) =>
        val out = map(s0, s1, s2, s3, s4, s5, s6, s7, s8)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs ++ v5.getErrs ++ v6.getErrs ++ v7.getErrs ++ v8.getErrs)
    }
  }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, S7, S8, S9, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6],
    v7: Validation[Err, S7],
    v8: Validation[Err, S8],
    v9: Validation[Err, S9])(map: (S0, S1, S2, S3, S4, S5, S6, S7, S8, S9) => Out): Validation[Err, Out] = {
    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7), Success(s8), Success(s9)) =>
        val out = map(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.getErrs ++ v1.getErrs ++ v2.getErrs ++ v3.getErrs ++ v4.getErrs ++ v5.getErrs ++ v6.getErrs ++ v7.getErrs ++ v8.getErrs ++ v9.getErrs)
    }
  }
}

case class Failure[Err, R](errs: Seq[Err]) extends Validation[Err, R] { protected def getErrs = errs  }
case class Success[Err, R](r: R)           extends Validation[Err, R] { protected def getErrs = Nil }

trait ValidationMapper[In1, In2, Out] {
  def map(in1: In1, in2: In2): Out
}

final class ValidationOps[A](val self: A) extends AnyVal {
  def success[X]: Validation[X, A] = Success[X, A](self)
  def failure[X]: Validation[A, X] = Failure[A, X](List(self))
}
