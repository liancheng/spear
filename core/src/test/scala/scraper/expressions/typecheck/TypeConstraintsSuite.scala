package scraper.expressions.typecheck

import scala.util.Success

import scraper.LoggingFunSuite
import scraper.expressions.dsl._
import scraper.expressions.functions._
import scraper.types.{BooleanType, IntType, IntegralType, LongType}

class TypeConstraintsSuite extends LoggingFunSuite {
  def testTypeConstraints(constraintsClass: Class[_ <: TypeConstraints])(f: => Unit): Unit = {
    test(constraintsClass.getName)(f)
  }

  testTypeConstraints(classOf[Exact]) {
    assertResult(Success(Seq(lit(true), lit(false)))) {
      Exact(BooleanType, Seq(true, false)).strictlyTyped
    }
  }

  testTypeConstraints(classOf[AllBelongTo]) {
    assertResult(Success(Seq((1: Byte) cast IntType, (1: Short) cast IntType, lit(1: Int)))) {
      AllBelongTo(IntegralType, Seq(1: Byte, 1: Short, 1: Int)).strictlyTyped
    }
  }

  testTypeConstraints(classOf[AllCompatible]) {
    assertResult(Success(Seq(1 cast LongType, lit(1L)))) {
      AllCompatible(Seq(1, 1L)).strictlyTyped
    }
  }
}
