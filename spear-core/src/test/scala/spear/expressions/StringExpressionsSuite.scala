package spear.expressions

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

import spear.expressions.functions._
import spear.LoggingFunSuite

class StringExpressionsSuite extends LoggingFunSuite with Checkers {
  test("concat") {
    check {
      forAll { (a: String, b: String) =>
        concat(a, b).strictlyTyped.evaluated == a + b
      }
    }
  }

  test("rlike") {
    check {
      forAll(Gen.alphaStr) { a: String =>
        rlike(a, "a.*").strictlyTyped.evaluated == a.startsWith("a")
      }
    }
  }
}
