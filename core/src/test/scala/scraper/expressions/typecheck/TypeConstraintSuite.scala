package scraper.expressions.typecheck

import scraper.LoggingFunSuite
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Expression
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types._

class TypeConstraintSuite extends LoggingFunSuite {
  private def testTypeConstraint(constraintsClass: Class[_ <: TypeConstraint])(f: => Unit): Unit = {
    test(constraintsClass.getSimpleName)(f)
  }

  private def check(expected: Seq[Expression])(constraint: TypeConstraint): Unit = {
    assertResult(expected)(constraint.strictlyTyped.get)
  }

  private def expectExpressions(first: Expression, rest: Expression*)(constraint: TypeConstraint): Unit = {
    check(first +: rest)(constraint)
  }

  private def check[T <: Throwable: Manifest](constraint: TypeConstraint): Unit = {
    intercept[T](constraint.strictlyTyped.get)
  }

  testTypeConstraint(classOf[PassThrough]) {
    expectExpressions(1) {
      PassThrough(Seq(1))
    }

    expectExpressions((1 cast LongType) + 1L) {
      PassThrough(Seq(lit(1) + 1L))
    }
  }

  testTypeConstraint(classOf[Exact]) {
    expectExpressions(lit(true)) {
      lit(true) ofType BooleanType
    }

    expectExpressions((1 cast LongType) =:= 1L) {
      (1 =:= 1L) ofType BooleanType
    }

    check[TypeMismatchException] {
      lit(1f) ofType BooleanType
    }
  }

  testTypeConstraint(classOf[CompatibleWith]) {
    expectExpressions(lit(true), 1 cast BooleanType) {
      Seq(lit(true), lit(1)) compatibleWith BooleanType
    }

    check[TypeMismatchException] {
      Seq(lit(true), lit(false)) compatibleWith LongType
    }
  }

  testTypeConstraint(classOf[SubtypeOf]) {
    expectExpressions((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)) {
      Seq(lit(1: Byte), lit(1: Short), lit(1)) subtypeOf IntegralType
    }

    check[TypeMismatchException] {
      Seq(lit(1F), lit(1D)) subtypeOf IntegralType
    }
  }

  testTypeConstraint(classOf[AllCompatible]) {
    expectExpressions(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)).allCompatible
    }
  }

  testTypeConstraint(classOf[AndThen]) {
    expectExpressions(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)) subtypeOf OrderedType andThen (_.allCompatible)
    }
  }

  testTypeConstraint(classOf[OrElse]) {
    expectExpressions(1) {
      lit(1) ofType StringType orElse (lit(1) subtypeOf IntegralType)
    }

    expectExpressions("1") {
      lit("1") ofType StringType orElse (lit(1) subtypeOf IntegralType)
    }
  }
}
