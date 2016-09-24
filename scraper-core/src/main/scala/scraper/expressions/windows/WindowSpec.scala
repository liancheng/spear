package scraper.expressions.windows

import scraper.expressions.{Expression, SortOrder, UnevaluableExpression}

sealed trait WindowFrameType

case object RowsFrame extends WindowFrameType {
  override def toString: String = "ROWS"
}

case object RangeFrame extends WindowFrameType {
  override def toString: String = "RANGE"
}

sealed trait FrameBoundary extends Ordered[FrameBoundary] {
  val offset: Long

  override def compare(that: FrameBoundary): Int = this.offset compare that.offset
}

case object CurrentRow extends FrameBoundary {
  override def toString: String = "CURRENT ROW"

  override val offset: Long = 0
}

case object UnboundedPreceding extends FrameBoundary {
  override def toString: String = "UNBOUNDED PRECEDING"

  override val offset: Long = Long.MinValue
}

case object UnboundedFollowing extends FrameBoundary {
  override def toString: String = "UNBOUNDED FOLLOWING"

  override val offset: Long = Long.MaxValue
}

case class Preceding(n: Long) extends FrameBoundary {
  require(n >= 0, "Frame starting offset must not be negative")

  override def toString: String = s"$n PRECEDING"

  override val offset: Long = -n
}

case class Following(n: Long) extends FrameBoundary {
  require(n >= 0, "Frame ending offset must not be negative")

  override def toString: String = s"$n FOLLOWING"

  override val offset: Long = n
}

case class WindowFrame(
  frameType: WindowFrameType = RowsFrame,
  start: FrameBoundary = UnboundedPreceding,
  end: FrameBoundary = UnboundedFollowing
) {
  require(start <= end, s"Frame starting offset $start is greater than ending offset $end.")

  override def toString: String = s"$frameType BETWEEN $start AND $end"
}

object WindowFrame {
  val Default: WindowFrame = WindowFrame()

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RowsFrame, start, end)

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RangeFrame, start, end)
}

case class WindowSpec(
  partitionSpec: Seq[Expression] = Nil,
  orderSpec: Seq[SortOrder] = Nil,
  windowFrame: WindowFrame = WindowFrame.Default
) extends UnevaluableExpression {
  override def children: Seq[Expression] = partitionSpec ++ orderSpec

  def partitionBy(spec: Seq[Expression]): WindowSpec = copy(partitionSpec = spec)

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[Expression]): WindowSpec = copy(orderSpec = spec map {
    case e: SortOrder => e
    case e            => e.asc
  })

  def orderBy(first: Expression, rest: Expression*): WindowSpec = orderBy(first +: rest)

  def between(windowFrame: WindowFrame): WindowSpec = copy(windowFrame = windowFrame)

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    between(WindowFrame.rowsBetween(start, end))

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    between(WindowFrame.rangeBetween(start, end))

  override protected def template(childList: Seq[String]): String = {
    val (partitions, orders) = childList.splitAt(partitionSpec.length)
    val partitionBy = if (partitions.isEmpty) "" else partitions.mkString("PARTITION BY ", ", ", "")
    val orderBy = if (orders.isEmpty) "" else orders.mkString("ORDER BY ", ", ", "")
    Seq(partitionBy, orderBy, windowFrame.toString) filter (_.nonEmpty) mkString ("(", " ", ")")
  }
}

object Window {
  val Default: WindowSpec = WindowSpec()

  def partitionBy(spec: Seq[Expression]): WindowSpec = Default.copy(partitionSpec = spec)

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[SortOrder]): WindowSpec = Default.copy(orderSpec = spec)

  def orderBy(first: SortOrder, rest: SortOrder*): WindowSpec = orderBy(first +: rest)

  def between(windowFrame: WindowFrame): WindowSpec = Default.between(windowFrame)

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rowsBetween(start, end)

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rangeBetween(start, end)
}
