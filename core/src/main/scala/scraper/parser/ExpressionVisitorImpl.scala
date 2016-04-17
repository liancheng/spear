package scraper.parser

import scala.collection.JavaConverters._

import scraper.exceptions.ParsingException
import scraper.expressions.{Expression, Literal}
import scraper.parser.ExpressionParser._

class ExpressionVisitorImpl extends ExpressionBaseVisitor[Expression] {
  override def visitBooleanLiteral(context: BooleanLiteralContext): Literal =
    Option(context.TRUE()) map (_ => Literal.True) getOrElse Literal.False

  override def visitNullLiteral(context: NullLiteralContext): Literal = Literal(null)

  override def visitStringLiteral(context: StringLiteralContext): Expression =
    Literal((context.STRING_LITERAL.asScala map (_.getText) map unquoteString).mkString)

  override def visitIntLiteral(context: IntLiteralContext): Expression =
    BigDecimal(context.getText) match {
      case v if v.isValidInt  => Literal(v.intValue())
      case v if v.isValidLong => Literal(v.longValue())
      case _                  => throw new NotImplementedError("Decimal type not implemented yet")
    }

  override def visitTinyIntLiteral(context: TinyIntLiteralContext): Expression =
    suffixedNumericLiteral(context)(_.toByte)

  override def visitSmallIntLiteral(context: SmallIntLiteralContext): Expression =
    suffixedNumericLiteral(context)(_.toShort)

  override def visitBigIntLiteral(context: BigIntLiteralContext): Expression =
    suffixedNumericLiteral(context)(_.toLong)

  override def visitDoubleLiteral(context: DoubleLiteralContext): Expression =
    suffixedNumericLiteral(context)(_.toDouble)

  private def suffixedNumericLiteral(context: NumberContext)(f: String => Any): Literal = {
    val raw = context.getText
    try Literal(f(raw.substring(0, raw.length - 1))) catch {
      case cause: NumberFormatException =>
        throw new ParsingException(cause.getMessage)
    }
  }

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
