package scraper.local.plans.physical

import scala.collection.mutable

import scraper.expressions.BoundRef.bind
import scraper.expressions.Literal.True
import scraper.expressions._
import scraper.plans.physical.{BinaryPhysicalPlan, LeafPhysicalPlan, PhysicalPlan, UnaryPhysicalPlan}
import scraper.{JoinedRow, MutableRow, Row, RowOrdering}

case class LocalRelation(data: Iterable[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data.iterator

  override protected def argStrings: Seq[String] = Nil
}

case class Project(child: PhysicalPlan, override val expressions: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override lazy val output: Seq[Attribute] = expressions map (_.toAttribute)

  private lazy val boundProjectList = expressions map (bind(_, child.output))

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    Row.fromSeq(boundProjectList map (_ evaluate row))
  }
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  private lazy val boundCondition = bind(condition, child.output)

  override def iterator: Iterator[Row] = child.iterator filter { row =>
    (boundCondition evaluate row).asInstanceOf[Boolean]
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
        a1.withNullability(a1.nullable || a2.nullable)
    }

  override def iterator: Iterator[Row] = left.iterator ++ right.iterator
}

case class Intersect(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override lazy val output: Seq[Attribute] =
    left.output zip right.output map {
      case (a1, a2) =>
        a1.withNullability(a1.nullable && a2.nullable)
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
  private lazy val boundCondition = condition map (bind(_, output)) getOrElse True

  def evaluateBoundCondition(input: Row): Boolean =
    boundCondition evaluate input match { case result: Boolean => result }

  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = for {
    leftRow <- left.iterator
    rightRow <- right.iterator
    joinedRow = new JoinedRow(leftRow, rightRow) if evaluateBoundCondition(joinedRow)
  } yield new JoinedRow(leftRow, rightRow)

  def on(condition: Expression): CartesianProduct = copy(condition = Some(condition))
}

case class Sort(child: PhysicalPlan, order: Seq[SortOrder]) extends UnaryPhysicalPlan {
  override def output: Seq[Attribute] = child.output

  private lazy val rowOrdering = new RowOrdering(order, child.output)

  override def iterator: Iterator[Row] = child.iterator.toArray.sorted(rowOrdering).toIterator
}

case class HashAggregate(
  child: PhysicalPlan,
  groupingList: Seq[GroupingAlias],
  aggregateList: Seq[NamedExpression]
) extends UnaryPhysicalPlan {
  private val hashMap: mutable.HashMap[Row, MutableRow] = mutable.HashMap.empty[Row, MutableRow]

  private val groupingOutput = groupingList map (_.toAttribute)

  private val boundGroupingList: Seq[Expression] =
    groupingList.map(BoundRef.bind(_, child.output))

  private val boundAggregateList: Seq[Expression] =
    aggregateList.map(BoundRef.bind(_, groupingOutput ++ child.output))

  override def output: Seq[Attribute] = (groupingList ++ aggregateList) map (_.toAttribute)

  override def iterator: Iterator[Row] = ???
}
