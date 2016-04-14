package scraper.expressions.typecheck

import scala.util.Success

import scraper.LoggingFunSuite
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.{BooleanType, IntegralType, IntType, LongType}

class TypeConstraintSuite extends LoggingFunSuite {
  def testTypeConstraint(constraintsClass: Class[_ <: TypeConstraint])(f: => Unit): Unit = {
    test(constraintsClass.getName)(f)
  }

  testTypeConstraint(classOf[ImplicitlyConvertibleTo]) {
    assertResult(Success(Seq(lit(true), 1 cast BooleanType))) {
      (Seq(lit(true), lit(1)) implicitlyConvertibleTo BooleanType).strictlyTyped
    }
  }

  testTypeConstraint(classOf[SubtypeOf]) {
    assertResult(Success(Seq((1: Byte) cast IntType, (1: Short) cast IntType, lit(1)))) {
      SubtypeOf(IntegralType, Seq(1: Byte, 1: Short, 1)).strictlyTyped
    }
  }

  testTypeConstraint(classOf[AllCompatible]) {
    assertResult(Success(Seq(1 cast LongType, lit(1L)))) {
      AllCompatible(Seq(1, 1L)).strictlyTyped
    }
  }
}
