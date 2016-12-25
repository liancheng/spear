package scraper.fastparser

import fastparse.all._
import fastparse.core.Logger

import scraper.LoggingFunSuite

class IdentifierSuite extends LoggingFunSuite {
  import WhitespaceApi._

  private implicit val parsingLogger = Logger(println)

  private def fullyParse[T](parser: P[T], input: String) = {
    (Start ~~ parser ~~ End parse input).get.value
  }

  private def testSuccessfulParse[T](input: String, expected: T): Unit = {
    test(s"[o] identifier: $input") {
      assertResult(expected) {
        fullyParse(Identifier.identifier, input)
      }
    }
  }

  private def testFailedParse[T](input: String): Unit = {
    test(s"[x] identifier: $input") {
      intercept[ParseError] {
        fullyParse(Identifier.identifier, input)
      }
    }
  }

  Seq(
    "data" -> Some("data"),
    "数据" -> Some("数据"),
    "_data" -> None,

    "\"data\"" -> Some("data"),
    "\"数据\"" -> Some("数据"),
    "\"double\"\"quote\"" -> Some("double\"quote"),
    "\"double\\\"quote\"" -> None,

    "U&\"data\"" -> Some("data"),
    "U&\"d!0061t!+000061\" UESCAPE '!'" -> Some("data"),
    "U&\"!!\" UESCAPE '!'" -> Some("!"),
    "U&\"!!\"" -> Some("!!"),
    "U&\"\\\\\"" -> Some("\\"),
    "U&\"\\\\\" UESCAPE '!'" -> Some("\\\\"),
    "U&\"\\0064\\0061\\0074\\0061\"" -> Some("data"),
    "U&\"!\" UESCAPE '!'" -> None,
    "U&\"\\\"" -> None
  ) foreach {
    case (input, Some(expected)) => testSuccessfulParse(input, expected)
    case (input, None)           => testFailedParse(input)
  }
}
