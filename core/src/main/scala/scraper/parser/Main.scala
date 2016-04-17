package scraper.parser

import org.antlr.v4.runtime.CommonTokenStream

object Main {
  def main(args: Array[String]) {
    val text = args mkString " "
    val input = new CaseInsensitiveANTLRInputStream(text)
    val lexer = new DataTypeLexer(input)
    val tokens = new CommonTokenStream(lexer)
    val parser = new DataTypeParser(tokens)
    val visitor = new DataTypeVisitorImpl
    println(visitor.visit(parser.dataType()).prettyTree)
  }
}
