package scraper.generators.expressions

import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers

import scraper.{TestUtils, LoggingFunSuite}
import scraper.Test._
import scraper.expressions.Expression

class ExpressionGeneratorSuite extends LoggingFunSuite with Checkers with TestUtils {
  test("strictness") {
    implicit val arbExpression = Arbitrary(genExpression(Nil))
    check((_: Expression).isStrictlyTyped)
  }
}
