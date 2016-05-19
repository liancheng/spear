package scraper.local.plans.physical

import scala.collection.mutable.ArrayBuffer

import scraper._
import scraper.expressions._
import scraper.expressions.BoundRef.bind
import scraper.expressions.Literal.True
import scraper.plans.physical.{BinaryPhysicalPlan, LeafPhysicalPlan, PhysicalPlan, UnaryPhysicalPlan}

case class LocalRelation(data: Iterable[Row], override val output: Seq[Attribute])
  extends LeafPhysicalPlan {

  override def iterator: Iterator[Row] = data.iterator

  // Overrides this to avoid showing individual local data entry
  override protected def argValueStrings: Seq[Option[String]] = Some("<local-data>") :: None :: Nil

  // The only expression nodes of `LocalRelation` are output attributes, which are not interesting
  // to be shown in the query plan tree
  override protected def buildNestedTree(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder
  ): Unit = ()
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
  private lazy val boundCondition = condition map bind(output) getOrElse True

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

case class Expand(child: PhysicalPlan, projectLists: Seq[Seq[NamedExpression]])
  extends UnaryPhysicalPlan {

  private lazy val boundProjectLists = projectLists map (_ map bind(child.output))

  override def output: Seq[Attribute] = projectLists.head map (_.toAttribute)

  override def iterator: Iterator[Row] = for {
    row <- child.iterator
    projectList <- boundProjectLists
  } yield Row.fromSeq(projectList map (_ evaluate row))
}
