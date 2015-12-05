package scraper.plans.physical

import scraper.{JoinedRow, Row}
import scraper.expressions.BoundRef.bind
import scraper.expressions._
import scraper.plans.QueryPlan

trait PhysicalPlan extends QueryPlan[PhysicalPlan] {
  def iterator: Iterator[Row]
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

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${output map (_.annotatedString) mkString ", "}"
}

case class Project(child: PhysicalPlan, override val expressions: Seq[NamedExpression])
  extends UnaryPhysicalPlan {

  override val output: Seq[Attribute] = expressions.map(_.toAttribute)

  override def iterator: Iterator[Row] = child.iterator.map { row =>
    val boundProjections = expressions map (bind(_, child.output))
    Row.fromSeq(boundProjections map (_ evaluate row))
  }

  override def nodeCaption: String =
    s"${getClass.getSimpleName} ${expressions map (_.annotatedString) mkString ", "}"
}

case class Filter(child: PhysicalPlan, condition: Expression) extends UnaryPhysicalPlan {
  override val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = {
    val boundCondition = bind(condition, child.output)
    child.iterator filter { row =>
      (boundCondition evaluate row).asInstanceOf[Boolean]
    }
  }

  override def nodeCaption: String = s"${getClass.getSimpleName} ${condition.annotatedString}"
}

case class Limit(child: PhysicalPlan, limit: Expression) extends UnaryPhysicalPlan {
  override lazy val output: Seq[Attribute] = child.output

  override def iterator: Iterator[Row] = child.iterator take limit.evaluated.asInstanceOf[Int]

  override def nodeCaption: String = s"${getClass.getSimpleName} ${limit.annotatedString}"
}

case class CartesianProduct(left: PhysicalPlan, right: PhysicalPlan) extends BinaryPhysicalPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  override def iterator: Iterator[Row] = for {
    leftRow <- left.iterator
    rightRow <- right.iterator
  } yield JoinedRow(leftRow, rightRow)

  override def nodeCaption: String = getClass.getSimpleName
}

case class Aggregate(
  child: PhysicalPlan,
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression]
) extends UnaryPhysicalPlan {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def iterator: Iterator[Row] = {
    val input = child.iterator.toArray

    val boundGroupings = groupingExpressions.map(bind(_, child.output))
    val boundAggs = aggregateExpressions.map(bind(_, child.output))

    val aggs = boundAggs.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val finalExprs = boundAggs.map(_.transformDown {
      case a: AggregateExpression => BoundRef(aggs.indexOf(a), a.dataType, true)
      case e =>
        val index = boundGroupings.indexOf(e)
        if (index == -1) {
          e
        } else {
          BoundRef(aggs.length + index, e.dataType, true)
        }
    })

    input.groupBy(row => boundGroupings.map(_.evaluate(row))).map {
      case (key, values) =>
        val agged = aggs.map(_.agg(values))
        val buffer = JoinedRow(Row.fromSeq(agged), Row.fromSeq(key))
        Row.fromSeq(finalExprs.map(_.evaluate(buffer)))
    }.toIterator
  }
}
