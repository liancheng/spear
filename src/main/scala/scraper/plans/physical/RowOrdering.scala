package scraper.plans.physical

import scraper.Row
import scraper.expressions._
import scraper.types.{DataType, OrderedType}

class NullSafeOrdering(dataType: DataType, nullsLarger: Boolean) extends Ordering[Any] {
  private val baseOrdering = dataType match {
    case t: OrderedType => t.genericOrdering
  }

  override def compare(lhs: Any, rhs: Any): Int = (lhs, rhs) match {
    case (null, null) => 0
    case (null, _)    => if (nullsLarger) 1 else -1
    case (_, null)    => if (nullsLarger) -1 else 1
    case _            => baseOrdering.compare(lhs, rhs)
  }
}

class RowOrdering(boundSortOrders: Seq[SortOrder]) extends Ordering[Row] {
  def this(unboundSortOrders: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(unboundSortOrders map (BoundRef.bind(_, inputSchema)))

  private val nullSafeOrderings = boundSortOrders.map {
    case order @ SortOrder(_, Ascending, _) =>
      new NullSafeOrdering(order.dataType, order.nullsLarger)

    case order @ SortOrder(_, Descending, _) =>
      new NullSafeOrdering(order.dataType, order.nullsLarger).reverse
  }

  def compare(a: Row, b: Row): Int = {
    val children = boundSortOrders map (_.child)
    (children zip nullSafeOrderings).iterator map {
      case (e, ordering) =>
        ordering.compare(e evaluate a, e evaluate b)
    } find (_ != 0) getOrElse 0
  }
}
