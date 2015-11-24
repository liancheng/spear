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

  override def annotatedString: String = s"$value: ${dataType.simpleName}"

  override def sql: String = value match {
    case v: String => s"'$value'" // TODO escaping
    case v         => v.toString
  }
}

object Literal {
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

  override def annotatedString: String = if (value) "TRUE" else "FALSE"

  override def sql: String = annotatedString
}

object LogicalLiteral {
  val True: LogicalLiteral = LogicalLiteral(true)

  val False: LogicalLiteral = LogicalLiteral(false)
}
