package scraper.plans.physical

import scraper.Row
import scraper.expressions._
import scraper.types.{DataType, PrimitiveType}

class NullSafeOrdering(dataType: DataType, nullsFirst: Boolean = true)
  extends Ordering[Any] {

  private val baseOrdering = dataType match {
    case t: PrimitiveType => t.genericOrdering
  }

  override def compare(lhs: Any, rhs: Any): Int = (lhs, rhs) match {
    case (null, null) => 0
    case (null, _)    => if (nullsFirst) -1 else 1
    case (_, null)    => if (nullsFirst) 1 else -1
    case _            => baseOrdering.compare(lhs, rhs)
  }
}

class RowOrdering(sortOrders: Seq[SortOrder]) extends Ordering[Row] {
  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering map (BoundRef.bind(_, inputSchema)))

  private val nullSafeOrderings = sortOrders.map {
    case order @ SortOrder(_, Ascending)  => new NullSafeOrdering(order.dataType)
    case order @ SortOrder(_, Descending) => new NullSafeOrdering(order.dataType).reverse
  }

  def compare(a: Row, b: Row): Int =
    (sortOrders map (_.child) zip nullSafeOrderings).iterator map {
      case (e, ordering) => ordering.compare(e evaluate a, e evaluate b)
    } find (_ != 0) getOrElse 0
}
