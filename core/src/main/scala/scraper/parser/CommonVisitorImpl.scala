package scraper.parser

import scala.collection.JavaConverters._

import scraper.parser.CommonParser.{IdentifierContext, QuotedStringContext}

class CommonVisitorImpl extends CommonBaseVisitor[String] {
  override def visitIdentifier(ctx: IdentifierContext): String =
    Option(ctx.UNQUOTED_IDENTIFIER())
      .map(_.getText)
      .getOrElse {
        val raw = ctx.QUOTED_IDENTIFIER().getText
        raw.substring(0, raw.length - 1).replace("``", "`")
      }

  override def visitQuotedString(context: QuotedStringContext): String =
    (context.QUOTED_STRING().asScala map (_.getText) map unquoteString).mkString

  private def unquoteString(quoted: String): String = {
    val quoteChar = if (quoted.startsWith("'")) '\'' else '"'
    val escaped = quoted.substring(1, quoted.length - 1)
    val builder = StringBuilder.newBuilder

    var i = 0
    while (i < escaped.length) {
      val ch = escaped(i)
      if (ch == '\\') {
        i += 1

        escaped(i) match {
          case `quoteChar` => builder += quoteChar
          case 'r'         => builder += '\r'
          case 'n'         => builder += '\n'
          case 't'         => builder += '\t'
          case '\\'        => builder += '\\'
        }
      } else {
        builder += ch
      }
    }

    builder.toString()
  }
}
