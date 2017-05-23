package spear.expressions

import spear.Row
import spear.expressions.typecheck.TypeConstraint
import spear.types._

case class Length(child: Expression) extends UnaryExpression {
  override protected def typeConstraint: TypeConstraint =
    child oneOf (StringType, ArrayType, MapType)

  override def dataType: DataType = IntType

  override def evaluate(input: Row): Any = child evaluate input match {
    case v: String             => v.length
    case v: Array[_]           => v.length
    case v: TraversableOnce[_] => v.size
  }
}
