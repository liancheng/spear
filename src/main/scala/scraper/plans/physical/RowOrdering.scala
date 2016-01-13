package scraper.plans.physical

import scraper.Row
import scraper.expressions._
import scraper.types.{DataType, OrderedType}

class NullSafeOrdering(dataType: DataType, nullsFirst: Boolean = true)
  extends Ordering[Any] {

  private val baseOrdering = dataType match {
    case t: OrderedType => t.genericOrdering
  }

  override def compare(lhs: Any, rhs: Any): Int = (lhs, rhs) match {
    case (null, null) => 0
    case (null, _)    => if (nullsFirst) -1 else 1
    case (_, null)    => if (nullsFirst) 1 else -1
    case _            => baseOrdering.compare(lhs, rhs)
  }
}

class RowOrdering(boundSortOrders: Seq[SortOrder]) extends Ordering[Row] {
  def this(unboundSortOrders: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(unboundSortOrders map (BoundRef.bind(_, inputSchema)))

  private val nullSafeOrderings = boundSortOrders.map {
    case order @ SortOrder(_, Ascending)  => new NullSafeOrdering(order.dataType)
    case order @ SortOrder(_, Descending) => new NullSafeOrdering(order.dataType).reverse
  }

  def compare(a: Row, b: Row): Int =
    (boundSortOrders map (_.child) zip nullSafeOrderings).iterator map {
      case (e, ordering) => ordering.compare(e evaluate a, e evaluate b)
    } find (_ != 0) getOrElse 0
}
