package scraper.expressions.aggregates

import scraper.Name
import scraper.expressions._
import scraper.expressions.aggregates.FoldLeft.UpdateFunction
import scraper.expressions.functions._
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.{DataType, DoubleType, NumericType}

case class Average(child: Expression) extends UnaryExpression with DeclarativeAggregateFunction {
  override def nodeName: Name = "avg"

  override def dataType: DataType = DoubleType

  override lazy val aggBufferAttributes: Seq[AttributeRef] = Seq(sum, count)

  override lazy val zeroValues: Seq[Expression] = Seq(Literal(null, child.dataType), 0L)

  override lazy val updateExpressions: Seq[Expression] = Seq(
    coalesce((child cast dataType) + sum, child cast dataType, sum),
    if (child.isNullable) If(child.isNull, count, count + 1L) else count + 1L
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  override lazy val resultExpression: Expression =
    If(count === 0L, lit(null), sum / (count cast dataType))

  override protected lazy val typeConstraint: TypeConstraint = Seq(child) sameSubtypeOf NumericType

  private lazy val sum = 'sum of dataType withNullability child.isNullable

  private lazy val count = 'count.long.!
}

abstract class NumericNullableReduceLeft extends NullableReduceLeft {
  override protected lazy val typeConstraint: TypeConstraint = children sameSubtypeOf NumericType
}

case class Sum(child: Expression) extends NumericNullableReduceLeft {
  override val updateFunction: UpdateFunction = Plus
}

case class Product_(child: Expression) extends NumericNullableReduceLeft {
  override def nodeName: Name = "product"

  override val updateFunction: UpdateFunction = Multiply
}
