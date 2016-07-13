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

  private def check[T <: Throwable: Manifest](constraint: TypeConstraint): Unit = {
    intercept[T](constraint.enforced.get)
  }

  testTypeConstraint(classOf[PassThrough]) {
    expectExpressions(1) {
      PassThrough(Seq(1))
    }

    expectExpressions((1 cast LongType) + 1L) {
      PassThrough(Seq(lit(1) + 1L))
    }
  }

  testTypeConstraint(classOf[SameTypeAs]) {
    expectExpressions(lit(true), 1 cast BooleanType) {
      Seq(lit(true), lit(1)) sameTypeAs BooleanType
    }

    check[TypeMismatchException] {
      Seq(lit(true), lit(false)) sameTypeAs LongType
    }
  }

  testTypeConstraint(classOf[SameSubtypesOf]) {
    expectExpressions((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)) {
      Seq(lit(1: Byte), lit(1: Short), lit(1)) sameSubtypeOf IntegralType
    }

    check[TypeMismatchException] {
      Seq(lit(1F), lit(1D)) sameSubtypeOf IntegralType
    }
  }

  testTypeConstraint(classOf[SameType]) {
    expectExpressions(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)).sameType
    }
  }

  testTypeConstraint(classOf[AndThen]) {
    expectExpressions(1 cast LongType, 1L) {
      Seq(lit(1), lit(1L)) sameSubtypeOf OrderedType andThen (_.sameType)
    }
  }

  testTypeConstraint(classOf[OrElse]) {
    expectExpressions(1L) {
      Seq(lit(1L)) sameTypeAs BooleanType orElse (Seq(lit(1L)) sameSubtypeOf IntegralType)
    }

    expectExpressions("1") {
      Seq(lit("1")) sameTypeAs StringType orElse (Seq(lit(1)) sameSubtypeOf IntegralType)
    }
  }
}
