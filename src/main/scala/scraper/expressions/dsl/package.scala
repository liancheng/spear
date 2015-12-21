package scraper.expressions

import scala.language.implicitConversions

import scraper.types._

package object dsl {
  implicit def `Boolean->Literal`(value: Boolean): Literal = Literal(value, BooleanType)

  implicit def `Byte->Literal`(value: Byte): Literal = Literal(value, ByteType)

  implicit def `Short->Literal`(value: Short): Literal = Literal(value, ShortType)

  implicit def `Int->Literal`(value: Int): Literal = Literal(value, IntType)

  implicit def `Long->Literal`(value: Long): Literal = Literal(value, LongType)

  implicit def `Float->Literal`(value: Double): Literal = Literal(value, FloatType)

  implicit def `Double->Literal`(value: Double): Literal = Literal(value, DoubleType)

  implicit def `String->Literal`(value: String): Literal = Literal(value, StringType)

  implicit def `Symbol->UnresolvedAttribute`(name: Symbol): UnresolvedAttribute =
    UnresolvedAttribute(name.name)

  trait BinaryPattern[T <: BinaryExpression] {
    def unapply(op: T): Option[(Expression, Expression)] = Some((op.left, op.right))
  }

  object ! {
    def unapply(not: Not): Option[Expression] = Some(not.child)
  }

  object || extends BinaryPattern[Or]

  object && extends BinaryPattern[And]

  object =:= extends BinaryPattern[Eq]

  object =/= extends BinaryPattern[NotEq]

  object > extends BinaryPattern[Gt]

  object < extends BinaryPattern[Lt]

  object >= extends BinaryPattern[GtEq]

  object <= extends BinaryPattern[LtEq]
}
