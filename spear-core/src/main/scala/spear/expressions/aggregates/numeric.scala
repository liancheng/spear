package spear.expressions.aggregates

import spear.expressions._
import spear.expressions.aggregates.FoldLeft.AccumulateFunction
import spear.expressions.functions._
import spear.expressions.typecheck.TypeConstraint
import spear.types.{DataType, DoubleType, NumericType}

case class Average(child: Expression) extends UnaryExpression with AggregateFunction {
  override def nodeName: String = "avg"

  override def dataType: DataType = DoubleType

  override lazy val stateAttributes: Seq[Attribute] = Seq(sum, count)

  override lazy val initialValues: Seq[Expression] = Seq(Literal(null, child.dataType), 0L)

  override lazy val accumulateExpressions: Seq[Expression] = Seq(
    coalesce((child cast dataType) + sum, child cast dataType, sum),
    if (child.isNullable) If(child.isNull, count, count + 1L) else count + 1L
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  override lazy val resultExpression: Expression =
    If(count === 0L, lit(null), sum / (count cast dataType))

  override protected lazy val typeConstraint: TypeConstraint = child subtypeOf NumericType

  private lazy val sum = 'sum of dataType nullable child.isNullable

  private lazy val count = 'count.long.!
}

case class Sum(child: Expression) extends NullableReduceLeft {
  override val accumulateFunction: AccumulateFunction = Plus

  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf NumericType
}

case class Product_(child: Expression) extends NullableReduceLeft {
  override def nodeName: String = "product"

  override val accumulateFunction: AccumulateFunction = Multiply

  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf NumericType
}
