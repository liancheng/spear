package spear.expressions.aggregates

import scala.collection.mutable.ArrayBuffer

import spear.Row
import spear.expressions._
import spear.expressions.aggregates.FoldLeft.{MergeFunction, UpdateFunction}
import spear.expressions.functions._
import spear.expressions.typecheck.TypeConstraint
import spear.types.{ArrayType, BooleanType, DataType, OrderedType}

case class Count(child: Expression) extends FoldLeft {
  override lazy val zeroValue: Expression = 0L

  override lazy val updateFunction: UpdateFunction = if (child.isNullable) {
    (count: Expression, input: Expression) => count + If(input.isNull, 0L, 1L)
  } else {
    (count: Expression, _) => count + 1L
  }

  override def mergeFunction: MergeFunction = Plus

  override protected lazy val value: AttributeRef = 'value of dataType.!
}

case class Max(child: Expression) extends NullableReduceLeft with DuplicateInsensitive {
  override val updateFunction: UpdateFunction = Greatest(_, _)

  override protected def typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType
}

case class Min(child: Expression) extends NullableReduceLeft with DuplicateInsensitive {
  override val updateFunction: UpdateFunction = Least(_, _)

  override protected def typeConstraint: TypeConstraint = children sameSubtypeOf OrderedType
}

abstract class FirstLike(child: Expression, ignoresNull: Expression)
  extends AggregateFunction with DuplicateInsensitive {

  override lazy val isPure: Boolean = false

  override def children: Seq[Expression] = Seq(child, ignoresNull)

  override def isNullable: Boolean = child.isNullable

  override protected def typeConstraint: TypeConstraint = child.anyType concat ignoresNull.foldable

  override protected lazy val strictDataType: DataType = child.dataType

  protected lazy val ignoresNullBool: Boolean = ignoresNull.evaluated.asInstanceOf[Boolean]
}

case class First(child: Expression, ignoresNull: Expression) extends FirstLike(child, ignoresNull) {
  def this(child: Expression) = this(child, lit(true))

  override def nodeName: String = "first_value"

  override lazy val stateAttributes: Seq[Attribute] = Seq(first, valueSet)

  override lazy val initialValues: Seq[Expression] = Seq(Literal(null, child.dataType), false)

  override lazy val updateExpressions: Seq[Expression] =
    if (child.isNullable && ignoresNullBool) {
      Seq(
        If(!valueSet, coalesce(child, first), first),
        valueSet || child.isNotNull
      )
    } else {
      Seq(
        If(valueSet, first, child),
        true
      )
    }

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    If(valueSet.left, first.left, first.right),
    valueSet.left || valueSet.right
  )

  override lazy val resultExpression: Expression = first

  private lazy val first = 'first of dataType nullable isNullable

  private lazy val valueSet = 'valueSet of BooleanType.!
}

case class Last(child: Expression, ignoresNull: Expression) extends FirstLike(child, ignoresNull) {
  def this(child: Expression) = this(child, lit(true))

  override def nodeName: String = "last_value"

  override lazy val stateAttributes: Seq[Attribute] = Seq(last)

  override lazy val initialValues: Seq[Expression] = Seq(Literal(null, child.dataType))

  override lazy val updateExpressions: Seq[Expression] = Seq(
    if (child.isNullable && ignoresNullBool) coalesce(child, last) else child
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    coalesce(last.right, last.left)
  )

  override lazy val resultExpression: Expression = last

  private lazy val last = 'last of dataType nullable isNullable
}

case class ArrayAgg(child: Expression)
  extends ImperativeAggregateFunction[ArrayBuffer[Any]] with UnaryExpression {

  override def isNullable: Boolean = false

  override protected lazy val strictDataType: DataType = ArrayType(child.dataType, child.isNullable)

  override def nodeName: String = "array_agg"

  override def initialState: State = ArrayBuffer.empty[Any]

  override def update(state: State, input: Row): State = state += child.evaluate(input)

  override def merge(state: State, inputState: State): State = state ++= inputState

  override def result(state: State): Any = state
}
