package spear.expressions

import scala.util.Try

import spear.Row
import spear.expressions.Literal.quote
import spear.types._

case class Literal(value: Any, override val dataType: DataType) extends LeafExpression {
  override def isNullable: Boolean = value == null

  override def evaluate(input: Row): Any = value

  override def debugString: String = value match {
    case v: String => s"${quote(v)}:${dataType.sql}"
    case v         => s"$v:${dataType.sql}"
  }

  override def sql: Try[String] = Try((value, dataType) match {
    case (v: String, StringType)   => quote(v)
    case (v: Boolean, BooleanType) => v.toString.toUpperCase
    case (v: Byte, ByteType)       => s"CAST($v AS ${ByteType.sql})"
    case (v: Short, ShortType)     => s"CAST($v AS ${ShortType.sql})"
    case (v: Long, LongType)       => s"CAST($v AS ${LongType.sql})"
    case (v: Float, FloatType)     => s"CAST($v AS ${FloatType.sql})"
    case (v: Double, DoubleType)   => s"CAST($v AS ${DoubleType.sql})"
    case (v, _) if v == null       => "NULL"
    case (_, _: ComplexType)       => throw new UnsupportedOperationException
    case (v, _)                    => v.toString
  })
}

object Literal {
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

  def quote(string: String): String = "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'"
}
