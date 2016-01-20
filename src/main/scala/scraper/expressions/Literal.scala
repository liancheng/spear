package scraper.expressions

import org.apache.commons.lang.StringEscapeUtils

import scraper.Row
import scraper.types._

case class Literal(value: Any, override val dataType: PrimitiveType) extends LeafExpression {
  override def nullable: Boolean = value == null

  override def evaluate(input: Row): Any = value

  override def debugString: String = s"$value:${dataType.sql}"

  override def sql: Option[String] = Some((value, dataType) match {
    case (v: String, StringType) => '"' + StringEscapeUtils.escapeJavaScript(v) + '"'
    case (v: Byte, ByteType)     => s"CAST($v AS ${ByteType.sql})"
    case (v: Short, ShortType)   => s"CAST($v AS ${ShortType.sql})"
    case (v: Long, LongType)     => s"CAST($v AS ${LongType.sql})"
    case (v: Float, FloatType)   => s"CAST($v AS ${FloatType.sql})"
    case (v: Double, DoubleType) => s"CAST($v AS ${DoubleType.sql})"
    case (v, _)                  => v.toString
  })
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
      case v => throw new UnsupportedOperationException(
        s"Unsupported literal type ${v.getClass} $v"
      )
    }
  }
}
