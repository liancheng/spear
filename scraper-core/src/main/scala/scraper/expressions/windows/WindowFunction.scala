package scraper.expressions.windows

import scraper.expressions.{BinaryExpression, Expression}
import scraper.types.DataType

case class WindowFunction(function: Expression, window: WindowSpec) extends BinaryExpression {
  override def dataType: DataType = function.dataType

  override def isNullable: Boolean = function.isNullable

  override lazy val isFoldable: Boolean = function.isFoldable

  override def left: Expression = function

  override def right: Expression = window

  override protected def template(childList: Seq[String]): String = childList mkString " OVER "
}
