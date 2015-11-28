package scraper.expressions

import org.scalatest.prop.Checkers
import scraper.LoggingFunSuite
import scraper.types.TestUtils

class ComparisonSuite extends LoggingFunSuite with TestUtils with Checkers {
  test("a = b") {
    check { (a: Int, b: Int) =>
      Eq(Literal(a), Literal(b)).evaluated == (a == b)
    }
  }

  test("a > b") {
    check { (a: Int, b: Int) =>
      Gt(Literal(a), Literal(b)).evaluated == (a > b)
    }
  }

  test("a < b") {
    check { (a: Int, b: Int) =>
      Lt(Literal(a), Literal(b)).evaluated == (a < b)
    }
  }

  test("a >= b") {
    check { (a: Int, b: Int) =>
      GtEq(Literal(a), Literal(b)).evaluated == (a >= b)
    }
  }

  test("a <= b") {
    check { (a: Int, b: Int) =>
      LtEq(Literal(a), Literal(b)).evaluated == (a <= b)
    }
  }
}
