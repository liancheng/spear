package scraper.expressions.typecheck

import scraper.LoggingFunSuite
import scraper.exceptions.TypeMismatchException
import scraper.expressions._
import scraper.expressions.functions._
import scraper.types._

class TypeConstraintSuite extends LoggingFunSuite {
  private def testTypeConstraint(constraintsClass: Class[_ <: TypeConstraint])(f: => Unit): Unit = {
    test(constraintsClass.getSimpleName)(f)
  }

  private def check(expected: Seq[Expression])(constraint: TypeConstraint): Unit = {
    assertResult(expected)(constraint.enforced.get)
  }

  private def expectExpressions(
    first: Expression, rest: Expression*
  )(constraint: TypeConstraint): Unit = {
    check(first +: rest)(constraint)
  }

  private def expectException[T <: Throwable: Manifest](constraint: TypeConstraint): Unit = {
    intercept[T](constraint.enforced.get)
  }

  testTypeConstraint(classOf[PassThrough]) {
    expectExpressions(1) {
      lit(1).passThrough
    }

    expectExpressions((1 cast LongType) + 1L) {
      (lit(1) + 1L).passThrough
    }
  }

  testTypeConstraint(classOf[SameTypeAs]) {
    expectExpressions(true, 1 cast BooleanType) {
      Seq[Expression](true, 1) sameTypeAs BooleanType
    }

    expectException[TypeMismatchException] {
      Seq[Expression](true, false) sameTypeAs LongType
    }
  }

  testTypeConstraint(classOf[SameSubtypeOf]) {
    expectExpressions((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)) {
      Seq[Expression](1: Byte, 1: Short, 1) sameSubtypeOf IntegralType
    }

    expectException[TypeMismatchException] {
      Seq[Expression](1F, 1D) sameSubtypeOf IntegralType
    }
  }

  testTypeConstraint(classOf[SameType]) {
    expectExpressions(1 cast LongType, 1L) {
      Seq[Expression](1, 1L).sameType
    }
  }

  testTypeConstraint(classOf[Foldable]) {
    expectExpressions(1, "foo") {
      Seq[Expression](1, "foo").foldable
    }

    expectException[TypeMismatchException] {
      ('a of IntType.!).foldable
    }
  }

  test("andThen") {
    expectExpressions(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)) sameSubtypeOf OrderedType andThen (_.sameType)
    }
  }
}
