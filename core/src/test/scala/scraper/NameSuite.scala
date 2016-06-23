package scraper

import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.{all, forAll, BooleanOperators}
import org.scalatest.prop.Checkers

import scraper.Name.{caseInsensitive, caseSensitive}

class NameSuite extends LoggingFunSuite with Checkers {
  test("Name comparison") {
    check(all(
      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-sensitive" |:
          caseSensitive(name) == caseSensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-insensitive" |:
          caseSensitive(name) == caseInsensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-sensitive" |:
          caseInsensitive(name) == caseSensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-insensitive" |:
          caseInsensitive(name) == caseInsensitive(name.toLowerCase)
      }
    ))
  }

  test("Name hashCode") {
    implicit val arbString = alphaStr

    check(all(
      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-sensitive" |:
          (caseSensitive(name).## == caseSensitive(name).##)
      },

      forAll(alphaStr) { name: String =>
        "case-sensitive v.s. case-insensitive" |: {
          name != name.toLowerCase || caseSensitive(name).## == caseInsensitive(name).##
        }
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-sensitive" |: {
          name != name.toLowerCase || caseInsensitive(name).## == caseSensitive(name).##
        }
      },

      forAll(alphaStr) { name: String =>
        "case-insensitive v.s. case-insensitive" |:
          caseInsensitive(name).## == caseInsensitive(name.toLowerCase).##
      }
    ))
  }
}
