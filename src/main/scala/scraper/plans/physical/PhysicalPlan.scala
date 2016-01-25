package scraper.plans.physical

import scalaz.Scalaz._

import scraper.expressions.BoundRef.bind
import scraper.expressions.Literal.True
import scraper.expressions._
import scraper.expressions.functions._
import scraper.plans.QueryPlan
import scraper.{JoinedRow, Row}

trait PhysicalPlan extends QueryPlan[PhysicalPlan] {
  def iterator: Iterator[Row]

  def select(projectList: Seq[Expression]): Project =
    Project(this, projectList.zipWithIndex map {
      case (UnresolvedAttribute("*"), _) => Star
      case (e: NamedExpression, _)       => e
      case (e, ordinal)                  => e as (e.sql getOrElse s"col$ordinal")
    })

  def select(first: Expression, rest: Expression*): Project = select(first +: rest)

  def filter(condition: Expression): Filter = Filter(this, condition)

  def where(condition: Expression): Filter = filter(condition)

  def limit(n: Expression): Limit = Limit(this, n)

  def limit(n: Int): Limit = limit(lit(n))

  def orderBy(order: Seq[SortOrder]): Sort = Sort(this, order)

  def orderBy(first: SortOrder, rest: SortOrder*): Sort = this orderBy (first +: rest)

  def cartesian(that: PhysicalPlan): CartesianProduct = CartesianProduct(this, that, None)

  def union(that: PhysicalPlan): Union = Union(this, that)

  def intersect(that: PhysicalPlan): Intersect = Intersect(this, that)

  def except(that: PhysicalPlan): Except = Except(this, that)
}

trait LeafPhysicalPlan extends PhysicalPlan {
  override def children: Seq[PhysicalPlan] = Nil
}

trait UnaryPhysicalPlan extends PhysicalPlan {
  def child: PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(child)
}

trait BinaryPhysicalPlan extends PhysicalPlan {
  def left: PhysicalPlan

  def right: PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(left, right)
}

case object EmptyRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator.empty

  override val output: Seq[Attribute] = Nil
}

case object SingleRowRelation extends LeafPhysicalPlan {
  override def iterator: Iterator[Row] = Iterator single Row.empty

  override val output: Seq[Attribute] = Nil
}

case class LocalRelation(data: Iterable[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data.iterator

  override def nodeCaption: String = s"$nodeName output=$outputString"
}

case class Project(child: PhysicalPlan, override val expressions: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override val output: Seq[Attribute] = expressions map (_.toAttribute)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    val boundProjectList = expressions map (bind(_, child.output))
    Row.fromSeq(boundProjectList map (_ evaluate row))
  }
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = {
    val boundCondition = bind(condition, child.output)
    child.iterator filter { row =>
      (boundCondition evaluate row).asInstanceOf[Boolean]
    }
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
  maybeCondition: Option[Expression]
) extends BinaryPhysicalPlan {

  private val boundCondition = maybeCondition map (BoundRef.bind(_, output)) getOrElse True

  def evaluateBoundCondition(input: Row): Boolean =
    boundCondition.evaluate(input) match { case result: Boolean => result }

  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = {
    for {
      leftRow <- left.iterator
      rightRow <- right.iterator
      joinedRow = JoinedRow(leftRow, rightRow) if evaluateBoundCondition(joinedRow)
    } yield JoinedRow(leftRow, rightRow)
  }

  def on(condition: Expression): CartesianProduct = copy(maybeCondition = condition.some)
}

case class Sort(child: PhysicalPlan, order: Seq[SortOrder]) extends UnaryPhysicalPlan {
  override def output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] =
    child.iterator.toArray.sorted(new RowOrdering(order, child.output)).toIterator
}
