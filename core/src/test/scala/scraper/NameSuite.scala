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
        (name != name.toLowerCase) ==> {
          "inequality: case-insensitive v.s. case-sensitive" |:
            caseInsensitive(name.toLowerCase) != caseSensitive(name)
        }
      },

      forAll(alphaStr) { name: String =>
        (name != name.toLowerCase) ==> {
          "inequality: case-sensitive v.s. case-insensitive" |:
            caseSensitive(name.toLowerCase) != caseInsensitive(name)
        }
      },

      forAll(alphaStr) { name: String =>
        (name != name.toLowerCase) ==> {
          "inequality: case-sensitive v.s. case-sensitive" |:
            caseSensitive(name.toLowerCase) != caseSensitive(name)
        }
      },

      forAll(alphaStr) { name: String =>
        "equality: case-sensitive v.s. case-sensitive" |:
          caseSensitive(name) == caseSensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "equality: case-sensitive v.s. case-insensitive" |:
          caseSensitive(name) == caseInsensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "equality: case-insensitive v.s. case-sensitive" |:
          caseInsensitive(name) == caseSensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "equality: case-insensitive v.s. case-insensitive (in the same case)" |:
          caseInsensitive(name) == caseInsensitive(name)
      },

      forAll(alphaStr) { name: String =>
        "equality: case-insensitive v.s. case-insensitive (in different cases)" |:
          caseInsensitive(name) == caseInsensitive(name.toLowerCase)
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
