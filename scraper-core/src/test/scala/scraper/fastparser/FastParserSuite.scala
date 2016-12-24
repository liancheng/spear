package scraper.fastparser

import scraper.LoggingFunSuite

class FastParserSuite extends LoggingFunSuite {
  import fastparse.all._

  test("Pass") {
    val Parsed.Success((), 0) = Pass.parse("_")
  }

  test("PassWith") {
    val Parsed.Success(1, 0) = PassWith(1).parse("_")
  }

  test("Fail") {
    val Parsed.Failure(Fail, 0, _) = Fail.parse("_")
  }

  test("AnyElem") {
    val Parsed.Success((), 1) = AnyElem.parse("_")
    val Parsed.Success((), 2) = AnyElem(2).parse("__")
  }

  test("Start/End") {
    val Parsed.Success((), 1) = (Start ~ AnyElem).parse("_")
    val Parsed.Success((), 1) = (AnyElem ~ End).parse("_")
  }
}
