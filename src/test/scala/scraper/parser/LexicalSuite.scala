package scraper.parser

import scraper.LoggingFunSuite
import scraper.types.TestUtils

class LexicalSuite extends LoggingFunSuite with TestUtils {
  val testLexical = new Lexical(Set("KW1", "KW2"))

  test("single token") {
    assertResult(testLexical.Keyword("KW1")) {
      new testLexical.Scanner(" KW1").first
    }

    assertResult(testLexical.Identifier("id")) {
      new testLexical.Scanner(" id").first
    }
  }
}
