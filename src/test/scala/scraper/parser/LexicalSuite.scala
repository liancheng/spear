package scraper.parser

import scraper.{TestUtils, LoggingFunSuite}

class LexicalSuite extends LoggingFunSuite with TestUtils {
  val testLexical = new Lexical(Set("kw1", "kw2"))

  test("single token") {
    assertResult(testLexical.Keyword("kw1")) {
      new testLexical.Scanner("KW1").first
    }

    assertResult(testLexical.Identifier("id")) {
      new testLexical.Scanner("id").first
    }
  }
}
