package scraper.local.plans.physical

import scala.collection.mutable

import scraper.{MutableRow, _}
import scraper.expressions.BoundRef.bind
import scraper.expressions.{AggregateFunction, GroupingAlias, NamedExpression, _}
import scraper.plans.physical.{PhysicalPlan, UnaryPhysicalPlan}

case class HashAggregate(
                          child: PhysicalPlan,
                          groupingList: Seq[GroupingAlias],
                          aggregateList: Seq[AggregationAlias]
) extends UnaryPhysicalPlan {
  type AccumulatorMap = Map[AggregateFunction, MutableRow]

  private val hashMap: mutable.HashMap[Row, (MutableRow, AccumulatorMap)] =
    mutable.HashMap.empty[Row, (MutableRow, AccumulatorMap)]

  private val groupingOutput: Seq[Attribute] = groupingList map (_.toAttribute)

  private val boundGroupingList: Seq[GroupingAlias] = groupingList map (bind(_, child.output))

  private val boundAggregateList: Seq[NamedExpression] =
    aggregateList map (bind(_, groupingOutput ++ child.output))

  override def output: Seq[Attribute] = (groupingList ++ aggregateList) map (_.toAttribute)

  private def allocateAccumulators(
    aggs: Seq[AggregateFunction]
  ): (MutableRow, Map[AggregateFunction, MutableRow]) = {
    val schemaLengths = aggs.map(_.accumulatorSchema.length)
    val accumulator = new BasicMutableRow(schemaLengths.sum)
    val begins = schemaLengths.inits.toSeq.reverse.init.map(_.sum)
    val perAggAccumulators = begins.zip(schemaLengths).map {
      case (begin, length) => new MutableRowSlice(accumulator, begin, length)
    }
    (accumulator, aggs.zip(perAggAccumulators).toMap)
  }

  override def iterator: Iterator[Row] = {
    hashMap.clear()

    val boundAggFunctions = for {
      named <- boundAggregateList
      agg <- named collect { case f: AggregateFunction => f }
    } yield agg

    val aggOutput = boundAggFunctions.zipWithIndex.map {
      case (f, i) =>
        UnresolvedAttribute(s"agg$i") of f.dataType withNullability f.isNullable
    }

    val rewriteAgg = boundAggFunctions.zip(aggOutput).toMap

    val rewrittenAggList = boundAggregateList.map(_.transformDown {
      case f: AggregateFunction => rewriteAgg(f)
    })

    child.iterator foreach { row =>
      val key = Row.fromSeq(boundGroupingList map (_ evaluate row))

      val accumulators = hashMap.getOrElseUpdate(key, {
        val (buffer, accumulators) = allocateAccumulators(boundAggFunctions)
        accumulators.foreach { case (agg, acc) => agg.zero(acc) }
        (buffer, accumulators)
      })

      accumulators._2.foreach {
        case (agg, acc) =>
          agg.accumulate(acc, row)
      }
    }

    val resultRow = new BasicMutableRow(aggregateList.length)
    val boundRewrittenAggList = rewrittenAggList.map(bind(_, groupingOutput ++ aggOutput))

    hashMap.iterator.map {
      case (keyRow, (buffer, _)) =>
        val joinedRow = new JoinedRow(keyRow, buffer)
        boundRewrittenAggList.zipWithIndex.foreach {
          case (a, i) =>
            resultRow(i) = a.evaluate(joinedRow)
        }

        new JoinedRow(keyRow, resultRow)
    }
  }
}
