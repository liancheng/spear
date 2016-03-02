package scraper.parser

import scraper.{LoggingFunSuite, TestUtils}

class LexicalSuite extends LoggingFunSuite with TestUtils {
  private val testLexical = new Lexical(Set("kw1", "kw2"))

  private type Token = testLexical.Token

  private def checkToken(input: String, token: Token): Unit = {
    assertResult(token) {
      new testLexical.Scanner(input).first
    }
  }

  test("keyword") {
    checkToken("KW1", testLexical.Keyword("kw1"))
    checkToken("kw2", testLexical.Keyword("kw2"))
  }

  test("identifier") {
    checkToken("id", testLexical.Identifier("id"))
    checkToken("`id`", testLexical.Identifier("id"))
    checkToken("`i d`", testLexical.Identifier("i d"))
    checkToken("`i``d`", testLexical.Identifier("i`d"))
  }
}
