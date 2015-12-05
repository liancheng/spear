package scraper.expressions

import scraper.Row
import scraper.types._

case class Literal(value: Any, dataType: PrimitiveType) extends LeafExpression {
  override def nullable: Boolean = value == null

  override def evaluate(input: Row): Any = value

  override def annotatedString: String = s"$value: ${dataType.simpleName}"

  override def sql: String = value match {
    case v: String => s"'$value'" // TODO escaping
    case v         => v.toString
  }
}

object Literal {
  val Null: Literal = Literal(null)

  val Zero: Literal = Literal(0, IntType)

  val One: Literal = Literal(1, IntType)

  val True: Literal = Literal(true)

  val False: Literal = Literal(false)

  def apply(value: Any): Literal = {
    value match {
      case v: Boolean => Literal(v, BooleanType)
      case v: Byte    => Literal(v, ByteType)
      case v: Short   => Literal(v, ShortType)
      case v: Int     => Literal(v, IntType)
      case v: Long    => Literal(v, LongType)
      case v: Float   => Literal(v, FloatType)
      case v: Double  => Literal(v, DoubleType)
      case v: String  => Literal(v, StringType)
      case null       => Literal(null, NullType)
      case v =>
        throw new UnsupportedOperationException(s"Unsupported literal type ${v.getClass} $v")
    }
  }
}
