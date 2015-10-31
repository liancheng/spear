package scraper.expressions

import scraper.Row
import scraper.types._

case class Literal(value: Any, dataType: DataType) extends LeafExpression {
  override def foldable: Boolean = true

  override def evaluate(input: Row): Any = value
}

object Literal {
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
