package scraper.parser

import org.antlr.v4.runtime.ParserRuleContext

import scraper.exceptions.ParsingException
import scraper.expressions.{ArithmeticExpression, BinaryComparison, Expression, Literal}
import scraper.parser.ExpressionParser._

class ExpressionVisitorImpl extends ExpressionBaseVisitor[Expression] {
  override def visitNegation(context: NegationContext): Expression =
    !visitPredicate(context.predicate())

  override def visitComparison(context: ComparisonContext): BinaryComparison =
    context.operator.getText match {
      case "="  => visitTermExpression(context.left) =:= visitTermExpression(context.right)
      case "<>" => visitTermExpression(context.left) =/= visitTermExpression(context.right)
      case "<"  => visitTermExpression(context.left) < visitTermExpression(context.right)
      case ">"  => visitTermExpression(context.left) > visitTermExpression(context.right)
      case "<=" => visitTermExpression(context.left) <= visitTermExpression(context.right)
      case ">=" => visitTermExpression(context.left) >= visitTermExpression(context.right)
    }

  override def visitTermExpression(context: TermExpressionContext): ArithmeticExpression =
    Option(context.unaryOperator) map { _ =>
      -visitProductExpression(context.unaryOperand)
    } getOrElse {
      context.operator.getText match {
        case "+" => visitProductExpression(context.left) + visitProductExpression(context.right)
        case "-" => visitProductExpression(context.left) - visitProductExpression(context.right)
      }
    }

  override def visitProductExpression(context: ProductExpressionContext): ArithmeticExpression =
    context.operator.getText match {
      case "*" => visitPrimaryExpression(context.left) * visitPrimaryExpression(context.right)
      case "/" => visitPrimaryExpression(context.left) / visitPrimaryExpression(context.right)
    }

  override def visitNullLiteral(context: NullLiteralContext): Expression = Literal(null)

  override def visitBooleanLiteral(context: BooleanLiteralContext): Literal =
    if (context.TRUE() != null) Literal.True else Literal.False

  override def visitStringLiteral(context: StringLiteralContext): Expression = {
    visitQuotedString(context.quotedString())
  }

  override def visitTinyIntLiteral(context: TinyIntLiteralContext): Literal =
    suffixedNumericLiteral(context)(_.toByte)

  override def visitSmallIntLiteral(context: SmallIntLiteralContext): Literal =
    suffixedNumericLiteral(context)(_.toShort)

  override def visitIntLiteral(context: IntLiteralContext): Literal =
    BigDecimal(context.getText) match {
      case v if v.isValidInt  => Literal(v.intValue())
      case v if v.isValidLong => Literal(v.longValue())
      case _                  => throw new NotImplementedError("Decimal type not implemented yet")
    }

  override def visitBigIntLiteral(context: BigIntLiteralContext): Literal =
    suffixedNumericLiteral(context)(_.toLong)

  override def visitDoubleLiteral(context: DoubleLiteralContext): Literal =
    suffixedNumericLiteral(context)(_.toDouble)

  private def suffixedNumericLiteral[Context <: ParserRuleContext](
    context: Context
  )(f: String => Any): Literal = {
    val raw = context.getText
    try Literal(f(raw.substring(0, raw.length - 1))) catch {
      case cause: NumberFormatException =>
        throw new ParsingException(cause.getMessage)
    }
  }
}
