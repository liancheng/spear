package scraper

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.{all, forAll, BooleanOperators}
import org.scalatest.prop.Checkers

import scraper.Name.{caseInsensitive, caseSensitive}

class NameSuite extends LoggingFunSuite with Checkers {
  test("equality") {
    check(all(
      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-sensitive" |: {
          val lhs = caseSensitive(name)
          val rhs = caseSensitive(name)
          lhs == rhs
        }
      },

      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-insensitive" |: {
          val lhs = caseSensitive(name)
          val rhs = caseInsensitive(name)
          lhs == rhs
        }
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-sensitive" |: {
          val lhs = caseInsensitive(name)
          val rhs = caseSensitive(name)
          lhs == rhs
        }
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-insensitive (in the same case)" |: {
          val lhs = caseInsensitive(name)
          val rhs = caseInsensitive(name)
          lhs == rhs
        }
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-insensitive (in different cases)" |: {
          val lhs = caseInsensitive(name)
          val rhs = caseInsensitive(name.toLowerCase)
          lhs == rhs
        }
      }
    ))
  }

  test("hashCode and equals contract") {
    val genName: Gen[Name] = for {
      name <- alphaStr
      isCaseSensitive <- arbitrary[Boolean]
    } yield Name(name, isCaseSensitive)

    check {
      forAll(genName, genName) {
        case (lhs: Name, rhs: Name) if lhs == rhs => lhs.## == rhs.##
        case (lhs: Name, rhs: Name) if lhs.## != rhs.## => lhs != rhs
        case _ => true
      }
    }
  }
}
