package scraper.parser

import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream}

import scraper.antlr4.{HelloLexer, HelloParser}

object Main {
  def main(args: Array[String]) {
    val text = args mkString " "
    val input = new ANTLRInputStream(text)
    val lexer = new HelloLexer(input)
    val tokens = new CommonTokenStream(lexer)
    val parser = new HelloParser(tokens)
    val tree = parser.attribute()
    println(tree.toStringTree(parser))
  }
}
