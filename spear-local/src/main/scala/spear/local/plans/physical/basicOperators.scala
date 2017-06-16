package spear.local.plans.physical

import scala.collection.mutable.ArrayBuffer

import spear._
import spear.annotations.Explain
import spear.execution.MutableProjection
import spear.expressions._
import spear.expressions.Literal.True
import spear.plans.physical.{BinaryPhysicalPlan, LeafPhysicalPlan, PhysicalPlan, UnaryPhysicalPlan}
import spear.trees.TreeNode

case class LocalRelation(
  @Explain(hidden = true) data: Iterable[Row],
  @Explain(hidden = true) override val output: Seq[Attribute]
) extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data.iterator

  override protected def nestedTrees: Seq[TreeNode[_]] = Nil

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = copy(
    output = newExpressions.toArray map { case e: Attribute => e }
  )
}

case class Project(child: PhysicalPlan, projectList: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override lazy val output: Seq[Attribute] = projectList map { _.attr }

  override def requireMaterialization: Boolean = true

  override def iterator: Iterator[Row] = child.iterator map projection

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = copy(
    projectList = newExpressions.toArray map { case e: NamedExpression => e }
  )

  private lazy val projection = MutableProjection(projectList map { _ bindTo child.output })
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator filter {
    boundCondition.evaluate(_).asInstanceOf[Boolean]
  }

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = {
    val Seq(newCondition) = newExpressions
    copy(condition = newCondition)
  }

  private lazy val boundCondition = condition bindTo child.output
}

case class Limit(child: PhysicalPlan, count: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator take count.evaluated.asInstanceOf[Int]

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = {
    val Seq(newCount) = newExpressions
    copy(count = newCount)
  }
}

case class Union(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable || a2.isNullable)
    }

  override def iterator: Iterator[Row] = left.iterator ++ right.iterator

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = this
}

case class Intersect(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable && a2.isNullable)
    }

  override def iterator: Iterator[Row] =
    (left.iterator.toSeq intersect right.iterator.toSeq).iterator

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = this
}

case class Except(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = left.output

  override def iterator: Iterator[Row] = (left.iterator.toSeq diff right.iterator.toSeq).iterator

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = this
}

case class CartesianJoin(
  left: PhysicalPlan,
  right: PhysicalPlan,
  condition: Option[Expression]
) extends BinaryPhysicalPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = for {
    leftRow <- left.iterator
    rightRow <- right.iterator if evaluateBoundCondition(join(leftRow, rightRow))
  } yield join

  override def requireMaterialization: Boolean = true

  def on(condition: Expression): CartesianJoin = copy(condition = Some(condition))

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = copy(
    condition = newExpressions.headOption
  )

  private lazy val boundCondition = condition map { _ bindTo output } getOrElse True

  private val join = new JoinedRow()

  private def evaluateBoundCondition(input: Row): Boolean =
    boundCondition evaluate input match { case result: Boolean => result }
}

case class Sort(child: PhysicalPlan, order: Seq[SortOrder]) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = {
    val buffer = ArrayBuffer.empty[Row]
    child.iterator foreach { buffer += _.copy() }
    buffer.sorted(rowOrdering).iterator
  }

  override protected def withExpressions(newExpressions: Seq[Expression]): PhysicalPlan = copy(
    order = newExpressions.toArray map { case e: SortOrder => e }
  )

  private lazy val rowOrdering = new RowOrdering(order, child.output)
}
