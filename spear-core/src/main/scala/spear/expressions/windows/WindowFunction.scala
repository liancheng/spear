package spear.expressions.windows

import spear.expressions.{BinaryExpression, Expression}
import spear.types.DataType

case class WindowFunction(function: Expression, window: WindowSpec) extends BinaryExpression {
  override def dataType: DataType = function.dataType

  override def isNullable: Boolean = function.isNullable

  override lazy val isFoldable: Boolean = function.isFoldable

  override def left: Expression = function

  override def right: Expression = window

  override protected def template(childList: Seq[String]): String = childList mkString " OVER "
}
