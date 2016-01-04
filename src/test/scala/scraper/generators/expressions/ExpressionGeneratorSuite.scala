package scraper.generators.expressions

import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers

import scraper.LoggingFunSuite
import scraper.Test._
import scraper.expressions.Expression
import scraper.types.TestUtils

class ExpressionGeneratorSuite extends LoggingFunSuite with Checkers with TestUtils {
  test("strictness") {
    implicit val arbExpression = Arbitrary(genExpression(Nil))
    check((_: Expression).strictlyTyped)
  }
}
