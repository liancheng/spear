package scraper.expressions.typecheck

import scraper.LoggingFunSuite
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Expression
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.{BooleanType, IntegralType, IntType, LongType}

class TypeConstraintSuite extends LoggingFunSuite {
  private def testTypeConstraint(constraintsClass: Class[_ <: TypeConstraint])(f: => Unit): Unit = {
    test(constraintsClass.getSimpleName)(f)
  }

  private def check(expected: Seq[Expression])(constraint: TypeConstraint): Unit = {
    assertResult(expected)(constraint.strictlyTyped.get)
  }

  private def check(first: Expression, rest: Expression*)(constraint: TypeConstraint): Unit = {
    check(first +: rest)(constraint)
  }

  private def check[T <: Throwable: Manifest](constraint: TypeConstraint): Unit = {
    intercept[T](constraint.strictlyTyped.get)
  }

  testTypeConstraint(classOf[PassThrough]) {
    check(1) {
      PassThrough(Seq(1))
    }

    check((1 cast LongType) + 1L) {
      PassThrough(Seq(lit(1) + 1L))
    }
  }

  testTypeConstraint(classOf[Exact]) {
    check(lit(true)) {
      lit(true) ofType BooleanType
    }

    check((1 cast LongType) =:= 1L) {
      (1 =:= 1L) ofType BooleanType
    }

    check[TypeMismatchException] {
      lit(1f) ofType BooleanType
    }
  }

  testTypeConstraint(classOf[ImplicitlyConvertibleTo]) {
    check(lit(true), 1 cast BooleanType) {
      Seq(lit(true), lit(1)) implicitlyConvertibleTo BooleanType
    }

    check[TypeMismatchException] {
      Seq(lit(true), lit(false)) implicitlyConvertibleTo LongType
    }
  }

  testTypeConstraint(classOf[SubtypeOf]) {
    check((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)) {
      Seq(lit(1: Byte), lit(1: Short), lit(1)) subtypeOf IntegralType
    }

    check[TypeMismatchException] {
      Seq(lit(1F), lit(1D)) subtypeOf IntegralType
    }
  }

  testTypeConstraint(classOf[AllCompatible]) {
    check(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)).allCompatible
    }
  }
}
