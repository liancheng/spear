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
      val cause = intercept[ParseError] {
        fullyParse(Identifier.identifier, input)
      }

      println(cause.getMessage)
    }
  }

  val successfulCases = Seq(
    "data" -> "data",
    "数据" -> "数据",

    "\"data\"" -> "data",
    "\"数据\"" -> "数据",
    "\"double\"\"quote\"" -> "double\"quote",

    "U&\"data\"" -> "data",
    "U&\"\\6570\\636e\"" -> "数据",
    "U&\"\\0064\\0061\\0074\\0061\"" -> "data",

    "U&\"d!0061t!+000061\" UESCAPE '!'" -> "data",
    "U&\"!!\" UESCAPE '!'" -> "!",
    "U&\"\\\\\" UESCAPE '!'" -> "\\\\",

    "U&\"!!\"" -> "!!",
    "U&\"\\\\\"" -> "\\"
  )

  successfulCases foreach {
    case (input, expected) =>
      testSuccessfulParse(input, expected)
  }

  val failedCases = Seq(
    "_data",
    "\"double\\\"quote\"",
    "U&\"!\" UESCAPE '!'",
    "U&\"\\\""
  )

  failedCases foreach testFailedParse
}
