package spark_etl.util

/**
  * Limited scalaz's Validation. Supports map(), flatMap() and +++ (eg. for reduce())
  */
trait Validation[Err, S] {
  def +++[S2, Out](v: Validation[Err, S2])(implicit merger: ValidationMerger[S, S2, Out]): Validation[Err, Out] = {
    (this, v) match {
      case (Failure(ls1), Failure(ls2)) => Failure(ls1 ++ ls2)
      case (Failure(ls),  Success(_))   => Failure(ls)
      case (Success(_),   Failure(ls))  => Failure(ls)
      case (Success(r1),  Success(r2))  => Success(merger.merge(r1, r2))
    }
  }

  def map[Out](f: S => Out): Validation[Err, Out] = this match {
    case Success(r)  => Success(f(r))
    case Failure(ls) => Failure(ls)
  }

  def flatMap[Out](f: S => Validation[Err, Out]): Validation[Err, Out] = this match {
    case Success(r)  => f(r)
    case Failure(ls) => Failure(ls)
  }

  protected def errs: Seq[Err]
}

object Validation {
  implicit val unitMapper = new ValidationMerger[Unit, Unit, Unit] { def merge(in1: Unit, in2: Unit) = () }
  implicit def listMerger[X] = new ValidationMerger[List[X], List[X], List[X]] { def merge(in1: List[X], in2: List[X]) = in1 ++ in2 }
  implicit def seqMerger[X] = new ValidationMerger[Seq[X], Seq[X], Seq[X]] { def merge(in1: Seq[X], in2: Seq[X]) = in1 ++ in2 }
  implicit def setMerger[X] = new ValidationMerger[Set[X], Set[X], Set[X]] { def merge(in1: Set[X], in2: Set[X]) = in1 ++ in2 }
  implicit def mapMerger[K, V] = new ValidationMerger[Map[K, V], Map[K, V], Map[K, V]] { def merge(in1: Map[K, V], in2: Map[K, V]) = in1 ++ in2 }

  import scala.language.implicitConversions
  implicit def validationOps[A](a: A): ValidationOps[A] = new ValidationOps(a)

  def merge[Err, S0, S1, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1])(map: (S0, S1) => Out): Validation[Err, Out] =
    (v0, v1) match {
      case (Success(s0), Success(s1)) =>
        val out = map(s0, s1)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs)
    }

  def merge[Err, S0, S1, S2, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2])(merge: (S0, S1, S2) => Out): Validation[Err, Out] =
    (v0, v1, v2) match {
      case (Success(s0), Success(s1), Success(s2)) =>
        val out = merge(s0, s1, s2)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs)
    }

  def merge[Err, S0, S1, S2, S3, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3])(merge: (S0, S1, S2, S3) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3)) =>
        val out = merge(s0, s1, s2, s3)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs)
    }

  def merge[Err, S0, S1, S2, S3, S4, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4])(merge: (S0, S1, S2, S3, S4) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4)) =>
        val out = merge(s0, s1, s2, s3, s4)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs)
    }

  def merge[Err, S0, S1, S2, S3, S4, S5, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5])(merge: (S0, S1, S2, S3, S4, S5) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4, v5) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5)) =>
        val out = merge(s0, s1, s2, s3, s4, s5)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs ++ v5.errs)
    }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6])(merge: (S0, S1, S2, S3, S4, S5, S6) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4, v5, v6) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6)) =>
        val out = merge(s0, s1, s2, s3, s4, s5, s6)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs ++ v5.errs ++ v6.errs)
    }

  def merge[Err, S0, S1, S2, S3, S4, S5, S6, S7, Out](
    v0: Validation[Err, S0],
    v1: Validation[Err, S1],
    v2: Validation[Err, S2],
    v3: Validation[Err, S3],
    v4: Validation[Err, S4],
    v5: Validation[Err, S5],
    v6: Validation[Err, S6],
    v7: Validation[Err, S7])(merge: (S0, S1, S2, S3, S4, S5, S6, S7) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4, v5, v6, v7) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7)) =>
        val out = merge(s0, s1, s2, s3, s4, s5, s6, s7)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs ++ v5.errs ++ v6.errs ++ v7.errs)
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
    v8: Validation[Err, S8])(merge: (S0, S1, S2, S3, S4, S5, S6, S7, S8) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4, v5, v6, v7, v8) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7), Success(s8)) =>
        val out = merge(s0, s1, s2, s3, s4, s5, s6, s7, s8)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs ++ v5.errs ++ v6.errs ++ v7.errs ++ v8.errs)
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
    v9: Validation[Err, S9])(merge: (S0, S1, S2, S3, S4, S5, S6, S7, S8, S9) => Out): Validation[Err, Out] =
    (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) match {
      case (Success(s0), Success(s1), Success(s2), Success(s3), Success(s4), Success(s5), Success(s6), Success(s7), Success(s8), Success(s9)) =>
        val out = merge(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9)
        Success[Err, Out](out)
      case _ =>
        Failure(v0.errs ++ v1.errs ++ v2.errs ++ v3.errs ++ v4.errs ++ v5.errs ++ v6.errs ++ v7.errs ++ v8.errs ++ v9.errs)
    }
}

case class Failure[Err, R](errs: Seq[Err]) extends Validation[Err, R]
case class Success[Err, R](r: R) extends Validation[Err, R] { protected def errs = Nil }

trait ValidationMerger[In1, In2, Out] {
  def merge(in1: In1, in2: In2): Out
}

final class ValidationOps[A](val self: A) extends AnyVal {
  def success[X]: Validation[X, A] = Success[X, A](self)
  def failure[X]: Validation[A, X] = Failure[A, X](List(self))
}