package scraper

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.{all, forAll, BooleanOperators}
import org.scalatest.prop.Checkers

import scraper.Name.{caseInsensitive, caseSensitive}

class NameSuite extends LoggingFunSuite with Checkers {
  test("basic properties") {
    check {
      forAll(alphaStr) { name =>
        all(
          caseSensitive(name).isCaseSensitive,

          !caseInsensitive(name).isCaseSensitive,

          caseSensitive(name).casePreserving == name,

          caseInsensitive(name).casePreserving == name
        )
      }
    }
  }

  test("equality") {
    check {
      forAll(alphaStr) { name =>
        all(
          (name != name.toLowerCase) ==>
            all(
              "inequality: case-insensitive v.s. case-sensitive" |:
                caseInsensitive(name.toLowerCase) != caseSensitive(name),

              "inequality: case-sensitive v.s. case-insensitive" |:
                caseSensitive(name.toLowerCase) != caseInsensitive(name),

              "inequality: case-sensitive v.s. case-sensitive" |:
                caseSensitive(name.toLowerCase) != caseSensitive(name),

              "inequality: case-sensitive v.s. other classes" |:
                caseSensitive(name) != (name: Any),

              "inequality: case-insensitive v.s. other classes" |:
                caseSensitive(name) != (name: Any)
            ),

          "equality: case-sensitive v.s. case-sensitive" |:
            caseSensitive(name) == caseSensitive(name),

          "equality: case-sensitive v.s. case-insensitive" |:
            caseSensitive(name) == caseInsensitive(name),

          "equality: case-insensitive v.s. case-sensitive" |:
            caseInsensitive(name) == caseSensitive(name),

          "equality: case-insensitive v.s. case-insensitive (in the same case)" |:
            caseInsensitive(name) == caseInsensitive(name),

          "equality: case-insensitive v.s. case-insensitive (in different cases)" |:
            caseInsensitive(name) == caseInsensitive(name.toLowerCase)
        )
      }
    }
  }

  test("hashCode and equals contract") {
    val genName: Gen[Name] = for {
      name <- alphaStr
      isCaseSensitive <- arbitrary[Boolean]
    } yield Name(name, isCaseSensitive)

    check {
      forAll(genName, genName) {
        case (lhs: Name, rhs: Name) if lhs.## != rhs.## => lhs != rhs
        // TODO Probability of reaching this branch is too small, can't be well tested
        case (lhs: Name, rhs: Name) if lhs == rhs => lhs.## == rhs.##
        case _ => true
      }
    }
  }

  test("interpolation") {
    val caseInsensitive = i"Hello"
    assert(!caseInsensitive.isCaseSensitive)
    assert(caseInsensitive.casePreserving == "Hello")
  }

  test("implicit conversion") {
    val caseSensitive: Name = "Hello"
    val caseInsensitive: Name = 'Hello

    assert(caseSensitive.isCaseSensitive)
    assert(caseSensitive.casePreserving == "Hello")

    assert(!caseInsensitive.isCaseSensitive)
    assert(caseInsensitive.casePreserving == "Hello")
  }
}
