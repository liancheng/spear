package scraper.local.plans.physical

import scala.collection.mutable.ArrayBuffer

import scraper._
import scraper.annotations.Explain
import scraper.expressions._
import scraper.expressions.BoundRef.bind
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

  override lazy val output: Seq[Attribute] = projectList map (_.toAttribute)

  private lazy val boundProjectList = projectList map bind(child.output)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    Row.fromSeq(boundProjectList map (_ evaluate row))
  }
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  private lazy val boundCondition = bind(child.output)(condition)

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
        a1.withNullability(a1.isNullable || a2.isNullable)
    }

  override def iterator: Iterator[Row] = left.iterator ++ right.iterator
}

case class Intersect(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.withNullability(a1.isNullable && a2.isNullable)
    }

  override def iterator: Iterator[Row] =
    (left.iterator.toSeq intersect right.iterator.toSeq).iterator
}

case class Except(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = left.output

  override def iterator: Iterator[Row] = (left.iterator.toSeq diff right.iterator.toSeq).iterator
}

case class CartesianProduct(
  left: PhysicalPlan,
  right: PhysicalPlan,
  condition: Option[Expression]
) extends BinaryPhysicalPlan {
  def evaluateBoundCondition(input: Row): Boolean =
    boundCondition evaluate input match { case result: Boolean => result }

  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = for {
    leftRow <- left.iterator
    rightRow <- right.iterator if evaluateBoundCondition(joinedRow(leftRow, rightRow))
  } yield joinedRow

  def on(condition: Expression): CartesianProduct = copy(condition = Some(condition))

  private lazy val boundCondition = condition map bind(output) getOrElse True

  private val joinedRow = new JoinedRow()
}

case class Sort(child: PhysicalPlan, order: Seq[SortOrder]) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  private lazy val rowOrdering = new RowOrdering(order, child.output)

  override def iterator: Iterator[Row] = {
    val buffer = ArrayBuffer.empty[Row]

    child.iterator.foreach { row =>
      val copy = Array.fill[Any](output.length)(null)
      row.copyToArray(copy)
      buffer += Row.fromSeq(copy)
    }

    buffer.sorted(rowOrdering).iterator
  }
}
