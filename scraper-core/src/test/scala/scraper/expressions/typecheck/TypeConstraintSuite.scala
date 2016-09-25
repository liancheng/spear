package scraper.expressions.typecheck

import scraper.LoggingFunSuite
import scraper.exceptions.{ImplicitCastException, TypeMismatchException}
import scraper.expressions._
import scraper.expressions.functions._
import scraper.types._

class TypeConstraintSuite extends LoggingFunSuite {
  testTypeConstraint(classOf[StrictlyTyped]) {
    expectExpressions(1) {
      StrictlyTyped(1 :: Nil)
    }

    expectExpressions((1 cast LongType) + 1L) {
      StrictlyTyped((lit(1) + 1L) :: Nil)
    }
  }

  testTypeConstraint(classOf[SameTypeAs]) {
    expectExpressions(true, 1 cast BooleanType) {
      Seq[Expression](true, 1) sameTypeAs BooleanType
    }

    expectException[ImplicitCastException] {
      Seq[Expression](true, false) sameTypeAs LongType
    }
  }

  testTypeConstraint(classOf[SameSubtypeOf]) {
    expectExpressions((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)) {
      Seq[Expression](1: Byte, 1: Short, 1) sameSubtypeOf IntegralType
    }

    expectException[TypeMismatchException] {
      Seq[Expression](1F, 1L) sameSubtypeOf IntegralType
    }
  }

  testTypeConstraint(classOf[SameType]) {
    expectExpressions(1 cast LongType, 1L) {
      SameType(Seq(1, 1L))
    }
  }

  testTypeConstraint(classOf[Foldable]) {
    expectExpressions(1, "foo") {
      Foldable(Seq(1, "foo"))
    }

    expectException[TypeMismatchException] {
      Foldable(('a of IntType.!) :: Nil)
    }
  }

  test("andAlso") {
    expectExpressions(1 cast LongType, 1L) {
      Seq[Expression](1, 1L) sameSubtypeOf OrderedType andAlso (_.sameType)
    }
  }

  test("orElse") {
    expectExpressions(1 cast StringType) {
      lit(1) sameTypeAs StringType orElse (lit(1) subtypeOf ArrayType)
    }

    expectExpressions(1 cast StringType) {
      lit(1) subtypeOf ArrayType orElse (lit(1) sameTypeAs StringType)
    }
  }

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
}
