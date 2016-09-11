package scraper.expressions.windows

import scraper.expressions.{Expression, SortOrder, UnevaluableExpression}

sealed trait WindowFrameType

case object RowsFrame extends WindowFrameType {
  override def toString: String = "ROWS"
}

case object RangeFrame extends WindowFrameType {
  override def toString: String = "RANGE"
}

sealed trait FrameBoundary extends Ordered[FrameBoundary]

case object CurrentRow extends FrameBoundary {
  override def compare(that: FrameBoundary): Int = that match {
    case UnboundedPreceding => 1
    case UnboundedFollowing => -1
    case CurrentRow         => 0
    case Preceding(n)       => 0L compareTo n
    case Following(n)       => 0L compareTo -n
  }

  override def toString: String = "CURRENT ROW"
}

case object UnboundedPreceding extends FrameBoundary {
  override def compare(that: FrameBoundary): Int = that match {
    case UnboundedPreceding => 0
    case _                  => -1
  }

  override def toString: String = "UNBOUNDED PRECEDING"
}

case object UnboundedFollowing extends FrameBoundary {
  override def compare(that: FrameBoundary): Int = that match {
    case UnboundedFollowing => 0
    case _                  => 1
  }

  override def toString: String = "UNBOUNDED FOLLOWING"
}

case class Preceding(n: Long) extends FrameBoundary {
  override def compare(o: FrameBoundary): Int = o match {
    case UnboundedPreceding => 1
    case UnboundedFollowing => -1
    case CurrentRow         => -n compare 0L
    case Preceding(m)       => -n compare -m
    case Following(m)       => -n compare m
  }

  override def toString: String = s"$n PRECEDING"
}

case class Following(n: Long) extends FrameBoundary {
  override def compare(o: FrameBoundary): Int = o match {
    case UnboundedPreceding => 1
    case UnboundedFollowing => -1
    case CurrentRow         => n compare 0L
    case Preceding(m)       => n compare -m
    case Following(m)       => n compare m
  }

  override def toString: String = s"$n FOLLOWING"
}

case class WindowFrame(
  frameType: WindowFrameType = RowsFrame,
  begin: FrameBoundary = UnboundedPreceding,
  end: FrameBoundary = UnboundedFollowing
) {
  require(begin <= end, s"The lower bound of window frame $this is greater than its upper bound.")

  override def toString: String = s"$frameType BETWEEN $begin AND $end"
}

object WindowFrame {
  val Default: WindowFrame = WindowFrame()
}

case class WindowSpec(
  partitionSpec: Seq[Expression] = Nil,
  orderSpec: Seq[SortOrder] = Nil,
  windowFrame: WindowFrame = WindowFrame.Default
) extends UnevaluableExpression {
  override def children: Seq[Expression] = partitionSpec ++ orderSpec

  def partitionBy(spec: Seq[Expression]): WindowSpec = copy(partitionSpec = spec)

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[SortOrder]): WindowSpec = copy(orderSpec = spec)

  def orderBy(first: SortOrder, rest: SortOrder*): WindowSpec = orderBy(first +: rest)

  def rowsBetween(begin: FrameBoundary, end: FrameBoundary): WindowSpec =
    copy(windowFrame = WindowFrame(RowsFrame, begin, end))

  def rangeBetween(begin: FrameBoundary, end: FrameBoundary): WindowSpec =
    copy(windowFrame = WindowFrame(RangeFrame, begin, end))

  override protected def template(childList: Seq[String]): String = {
    val (partitions, orders) = childList.splitAt(partitionSpec.length)
    val partitionBy = if (partitions.isEmpty) "" else partitions.mkString("PARTITION BY ", ", ", "")
    val orderBy = if (orders.isEmpty) "" else orders.mkString("ORDER BY ", ", ", "")
    Seq(partitionBy, orderBy, windowFrame.toString) filter (_.nonEmpty) mkString " "
  }
}

object Window {
  val Default: WindowSpec = WindowSpec()

  def partitionBy(spec: Seq[Expression]): WindowSpec = Default.copy(partitionSpec = spec)

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[SortOrder]): WindowSpec = Default.copy(orderSpec = spec)

  def orderBy(first: SortOrder, rest: SortOrder*): WindowSpec = orderBy(first +: rest)

  def rowsBetween(begin: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rowsBetween(begin, end)

  def rangeBetween(begin: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rangeBetween(begin, end)
}
