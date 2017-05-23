package spear.generators.expressions

import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers

import spear.{LoggingFunSuite, TestUtils}
import spear.Test._
import spear.expressions.Expression

class ExpressionGeneratorSuite extends LoggingFunSuite with Checkers with TestUtils {
  test("strictness") {
    implicit val arbExpression = Arbitrary(genExpression(Nil))
    check((_: Expression).isStrictlyTyped)
  }
}
