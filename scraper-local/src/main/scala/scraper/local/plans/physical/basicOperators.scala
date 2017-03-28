package scraper.local.plans.physical

import scala.collection.mutable.ArrayBuffer

import scraper._
import scraper.annotations.Explain
import scraper.execution.MutableProjection
import scraper.expressions._
import scraper.expressions.BoundRef._
import scraper.expressions.Literal.True
import scraper.plans.physical.{BinaryPhysicalPlan, LeafPhysicalPlan, PhysicalPlan, UnaryPhysicalPlan}
import scraper.trees.TreeNode

case class LocalRelation(
  @Explain(hidden = true) data: Iterable[Row],
  @Explain(hidden = true) override val output: Seq[Attribute]
) extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data.iterator

  override protected def nestedTrees: Seq[TreeNode[_]] = Nil
}

case class Project(child: PhysicalPlan, projectList: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override lazy val output: Seq[Attribute] = projectList map (_.attr)

  override def requireMaterialization: Boolean = true

  private lazy val projection = MutableProjection(projectList map bindTo(child.output))

  override def iterator: Iterator[Row] = child.iterator map projection
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  private lazy val boundCondition = bindTo(child.output)(condition)

  override def iterator: Iterator[Row] = child.iterator filter {
    boundCondition.evaluate(_).asInstanceOf[Boolean]
  }
}

case class Limit(child: PhysicalPlan, limit: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator take limit.evaluated.asInstanceOf[Int]
}

case class Union(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable || a2.isNullable)
    }

  override def iterator: Iterator[Row] = left.iterator ++ right.iterator
}

case class Intersect(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.nullable(a1.isNullable && a2.isNullable)
    }

  override def iterator: Iterator[Row] =
    (left.iterator.toSeq intersect right.iterator.toSeq).iterator
}

case class Except(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = left.output

  override def iterator: Iterator[Row] = (left.iterator.toSeq diff right.iterator.toSeq).iterator
}

case class NestedLoopJoin(
  left: PhysicalPlan,
  right: PhysicalPlan,
  condition: Option[Expression]
) extends BinaryPhysicalPlan {
  def evaluateBoundCondition(input: Row): Boolean =
    boundCondition evaluate input match { case result: Boolean => result }

  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = for {
    leftRow <- left.iterator
    rightRow <- right.iterator if evaluateBoundCondition(join(leftRow, rightRow))
  } yield join

  override def requireMaterialization: Boolean = true

  def on(condition: Expression): NestedLoopJoin = copy(condition = Some(condition))

  private lazy val boundCondition = condition map bindTo(output) getOrElse True

  private val join = new JoinedRow()
}

case class Sort(child: PhysicalPlan, order: Seq[SortOrder]) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  private lazy val rowOrdering = new RowOrdering(order, child.output)

  override def iterator: Iterator[Row] = {
    val buffer = ArrayBuffer.empty[Row]
    child.iterator foreach { buffer += _.copy() }
    buffer.sorted(rowOrdering).iterator
  }
}
