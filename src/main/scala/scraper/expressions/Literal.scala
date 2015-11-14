package scraper.expressions

import scraper.Row
import scraper.types._

case class Literal(value: Any, dataType: DataType) extends LeafExpression {
  override def foldable: Boolean = true

  override def evaluate(input: Row): Any = value

  override def caption: String = s"($value: ${dataType.simpleName})"
}

object Literal {
  val True: Literal = Literal(true, BooleanType)

  val False: Literal = Literal(false, BooleanType)

  val Zero: Literal = Literal(0, IntType)

  val One: Literal = Literal(1, IntType)

  def apply(value: Any): Literal = {
    value match {
      case v: Int    => Literal(v, IntType)
      case v: Double => Literal(v, DoubleType)
      case v: String => Literal(v, StringType)
      case null      => Literal(null, NullType)
      case v =>
        throw new UnsupportedOperationException(s"Unsupported literal type ${v.getClass} $v")
    }
  }
}
