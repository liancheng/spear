package scraper.plans.physical

import scraper.Row
import scraper.expressions._
import scraper.types.PrimitiveType

/**
 * An interpreted row ordering comparator.
 */
class InterpretedOrdering(ordering: Seq[SortOrder]) extends Ordering[Row] {

  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BoundRef.bind(_, inputSchema)))

  def compare(a: Row, b: Row): Int = {
    var i = 0
    while (i < ordering.size) {
      val order = ordering(i)
      val left = order.child.evaluate(a)
      val right = order.child.evaluate(b)

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.direction == Ascending) -1 else 1
      } else if (right == null) {
        return if (order.direction == Ascending) 1 else -1
      } else {
        val comparison = order.dataType match {
          case dt: PrimitiveType if order.direction == Ascending =>
            dt.ordering.asInstanceOf[Ordering[Any]].compare(left, right)
          case dt: PrimitiveType if order.direction == Descending =>
            dt.ordering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
          case other =>
            throw new IllegalArgumentException(s"Type $other does not support ordered operations")
        }
        if (comparison != 0) {
          return comparison
        }
      }
      i += 1
    }
    return 0
  }
}
