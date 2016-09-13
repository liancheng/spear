package scraper.expressions.windows

import scraper.expressions.{Expression, SortOrder, UnevaluableExpression}
import scraper.expressions.windows.FrameBoundary.{EndingOffset, StartingOffset}

sealed trait WindowFrameType

case object RowsFrame extends WindowFrameType {
  override def toString: String = "ROWS"
}

case object RangeFrame extends WindowFrameType {
  override def toString: String = "RANGE"
}

sealed trait FrameBoundary

object FrameBoundary {
  sealed trait StartingOffset

  sealed trait EndingOffset
}

case object CurrentRow extends FrameBoundary with StartingOffset with EndingOffset {
  override def toString: String = "CURRENT ROW"
}

case object UnboundedPreceding extends FrameBoundary with StartingOffset {
  override def toString: String = "UNBOUNDED PRECEDING"
}

case object UnboundedFollowing extends FrameBoundary with EndingOffset {
  override def toString: String = "UNBOUNDED FOLLOWING"
}

case class Preceding(n: Long) extends FrameBoundary with StartingOffset {
  require(n >= 0, "Frame starting offset must not be negative")

  override def toString: String = s"$n PRECEDING"
}

case class Following(n: Long) extends FrameBoundary with EndingOffset {
  require(n >= 0, "Frame ending offset must not be negative")

  override def toString: String = s"$n FOLLOWING"
}

case class WindowFrame(
  frameType: WindowFrameType = RowsFrame,
  start: StartingOffset = UnboundedPreceding,
  end: EndingOffset = UnboundedFollowing
) {
  override def toString: String = s"$frameType BETWEEN $start AND $end"
}

object WindowFrame {
  val Default: WindowFrame = WindowFrame()

  def rowsBetween(start: StartingOffset, end: EndingOffset): WindowFrame =
    WindowFrame(RowsFrame, start, end)

  def rangeBetween(start: StartingOffset, end: EndingOffset): WindowFrame =
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

  def orderBy(spec: Seq[SortOrder]): WindowSpec = copy(orderSpec = spec)

  def orderBy(first: SortOrder, rest: SortOrder*): WindowSpec = orderBy(first +: rest)

  def between(windowFrame: WindowFrame): WindowSpec = copy(windowFrame = windowFrame)

  def rowsBetween(start: StartingOffset, end: EndingOffset): WindowSpec =
    between(WindowFrame.rowsBetween(start, end))

  def rangeBetween(start: StartingOffset, end: EndingOffset): WindowSpec =
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

  def rowsBetween(start: StartingOffset, end: EndingOffset): WindowSpec =
    Default.rowsBetween(start, end)

  def rangeBetween(start: StartingOffset, end: EndingOffset): WindowSpec =
    Default.rangeBetween(start, end)
}
