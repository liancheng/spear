package scraper.generators.expressions

import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers

import scraper.LoggingFunSuite
import scraper.expressions.Expression
import scraper.types.TestUtils
import scraper.Test._

class ExpressionGeneratorSuite extends LoggingFunSuite with Checkers with TestUtils {
  test("foo") {
    implicit val arbExpression = Arbitrary(genExpression(Nil))

    check { e: Expression =>
      e.strictlyTyped
    }
  }
}
