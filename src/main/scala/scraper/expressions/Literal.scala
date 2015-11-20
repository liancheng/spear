package scraper.expressions

import scraper.Row
import scraper.types._

trait LiteralExpression extends LeafExpression {
  override def foldable: Boolean = true
}

case class Literal(value: Any, dataType: DataType) extends LiteralExpression {
  assert(dataType != BooleanType, {
    val logicalLiteralClass = classOf[LogicalLiteral].getSimpleName.stripSuffix("$")
    val literalClass = classOf[Literal].getSimpleName.stripSuffix("$")
    s"Please use $logicalLiteralClass instead of $literalClass"
  })

  override def nullable: Boolean = value == null

  override def evaluate(input: Row): Any = value

  override def caption: String = s"$value: ${dataType.simpleName}"
}

object Literal {
  val True: LogicalLiteral = LogicalLiteral(true)

  val False: LogicalLiteral = LogicalLiteral(false)

  val Zero: Literal = Literal(0, IntType)

  val One: Literal = Literal(1, IntType)

  def apply(value: Any): Literal = {
    value match {
      case v: Byte   => Literal(v, ByteType)
      case v: Short  => Literal(v, ShortType)
      case v: Int    => Literal(v, IntType)
      case v: Long   => Literal(v, LongType)
      case v: Float  => Literal(v, FloatType)
      case v: Double => Literal(v, DoubleType)
      case v: String => Literal(v, StringType)
      case null      => Literal(null, NullType)
      case v =>
        throw new UnsupportedOperationException(s"Unsupported literal type ${v.getClass} $v")
    }
  }
}

case class LogicalLiteral(value: Boolean) extends LiteralExpression with LeafPredicate {
  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def evaluate(input: Row): Any = value

  override def caption: String = if (value) "TRUE" else "FALSE"
}
