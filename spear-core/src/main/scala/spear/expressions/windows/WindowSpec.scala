package spear.expressions.windows

import scala.language.implicitConversions

import spear.Name
import spear.exceptions.ExpressionUnresolvedException
import spear.expressions.{Expression, LeafExpression, SortOrder, UnaryExpression, UnevaluableExpression, UnresolvedExpression}
import spear.expressions.functions.lit
import spear.expressions.typecheck.TypeConstraint
import spear.types.IntegralType

sealed trait WindowFrameType {
  def extent(start: FrameBoundary, end: FrameBoundary): WindowFrame
}

case object RowsFrame extends WindowFrameType {
  override def extent(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RowsFrame, start, end)

  override def toString: String = "ROWS"
}

case object RangeFrame extends WindowFrameType {
  override def extent(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RangeFrame, start, end)

  override def toString: String = "RANGE"
}

sealed trait FrameBoundary extends UnevaluableExpression

object FrameBoundary {
  implicit def `Int->FrameBoundary`(offset: Int): FrameBoundary = offset match {
    case 0               => CurrentRow
    case _ if offset < 0 => Preceding(lit(-offset))
    case _               => Following(lit(offset))
  }
}

case object CurrentRow extends FrameBoundary with LeafExpression {
  override protected def template(childList: Seq[String]): String = "CURRENT ROW"

  override def debugString: String = "0"
}

case object UnboundedPreceding extends FrameBoundary with LeafExpression {
  override protected def template(childList: Seq[String]): String = "UNBOUNDED PRECEDING"

  override def debugString: String = "-∞"
}

case object UnboundedFollowing extends FrameBoundary with LeafExpression {
  override protected def template(childList: Seq[String]): String = "UNBOUNDED FOLLOWING"

  override def debugString: String = "+∞"
}

case class Preceding(offset: Expression) extends FrameBoundary with UnaryExpression {
  override def child: Expression = offset

  override protected def typeConstraint: TypeConstraint = offset subtypeOf IntegralType

  override protected def template(childString: String): String = s"$childString PRECEDING"

  override def debugString: String = (-offset).debugString
}

case class Following(offset: Expression) extends FrameBoundary with UnaryExpression {
  override def child: Expression = offset

  override protected def typeConstraint: TypeConstraint = offset subtypeOf IntegralType

  override protected def template(childString: String): String = s"$childString FOLLOWING"

  override def debugString: String = offset.debugString
}

case class WindowFrame(
  frameType: WindowFrameType = RowsFrame,
  start: FrameBoundary = UnboundedPreceding,
  end: FrameBoundary = CurrentRow
) extends UnevaluableExpression {
  override def children: Seq[Expression] = start :: end :: Nil

  override protected def template(childList: Seq[String]): String = {
    val Seq(startString, endString) = childList
    s"$frameType BETWEEN $startString AND $endString"
  }

  override def debugString: String =
    s"$frameType BETWEEN [${start.debugString}, ${end.debugString}]"
}

object WindowFrame {
  val Default: WindowFrame = WindowFrame()

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RowsFrame, start, end)

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowFrame =
    WindowFrame(RangeFrame, start, end)
}

trait WindowSpec extends UnevaluableExpression {
  def partitionSpec: Seq[Expression]

  def orderSpec: Seq[SortOrder]

  def windowFrame: Option[WindowFrame]

  def partitionBy(spec: Seq[Expression]): WindowSpec

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[Expression]): WindowSpec

  def orderBy(first: Expression, rest: Expression*): WindowSpec = orderBy(first +: rest)

  def between(windowFrame: WindowFrame): WindowSpec

  def between(frame: Option[WindowFrame]): WindowSpec

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    between(WindowFrame.rowsBetween(start, end))

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    between(WindowFrame.rangeBetween(start, end))
}

object WindowSpec {
  val Default: BasicWindowSpec = BasicWindowSpec()
}

case class BasicWindowSpec(
  partitionSpec: Seq[Expression] = Nil,
  orderSpec: Seq[SortOrder] = Nil,
  windowFrame: Option[WindowFrame] = None
) extends WindowSpec {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec ++ windowFrame

  def partitionBy(spec: Seq[Expression]): WindowSpec = copy(partitionSpec = spec)

  def orderBy(spec: Seq[Expression]): WindowSpec = copy(orderSpec = spec map {
    case e: SortOrder => e
    case e            => e.asc
  })

  def between(frame: WindowFrame): WindowSpec = copy(windowFrame = Some(frame))

  def between(frame: Option[WindowFrame]): WindowSpec = copy(windowFrame = frame)

  override protected def template(childList: Seq[String]): String = {
    val (partitions, tail) = childList splitAt partitionSpec.length
    val (orders, frame) = tail splitAt orderSpec.length
    val partitionBy = if (partitions.isEmpty) "" else partitions.mkString("PARTITION BY ", ", ", "")
    val orderBy = if (orders.isEmpty) "" else orders.mkString("ORDER BY ", ", ", "")
    Seq(partitionBy, orderBy) ++ frame filter { _.nonEmpty } mkString ("(", " ", ")")
  }
}

case class WindowSpecRef(name: Name, windowFrame: Option[WindowFrame] = None)
  extends WindowSpec with UnresolvedExpression {

  override def children: Seq[Expression] = windowFrame.toSeq

  override def partitionSpec: Seq[Expression] = throw new ExpressionUnresolvedException(this)

  override def orderSpec: Seq[SortOrder] = throw new ExpressionUnresolvedException(this)

  override def partitionBy(spec: Seq[Expression]): WindowSpec =
    throw new ExpressionUnresolvedException(this)

  override def orderBy(spec: Seq[Expression]): WindowSpec =
    throw new ExpressionUnresolvedException(this)

  override def between(frame: WindowFrame): WindowSpec = copy(windowFrame = Some(frame))

  override def between(frame: Option[WindowFrame]): WindowSpec = copy(windowFrame = frame)

  override protected def template(childList: Seq[String]): String =
    name +: childList mkString ("(", " ", ")")
}

object Window {
  val Default: WindowSpec = WindowSpec.Default

  def apply(name: Name): WindowSpecRef = WindowSpecRef(name)

  def partitionBy(spec: Seq[Expression]): WindowSpec = Default partitionBy spec

  def partitionBy(first: Expression, rest: Expression*): WindowSpec = partitionBy(first +: rest)

  def orderBy(spec: Seq[Expression]): WindowSpec = Default orderBy spec

  def orderBy(first: Expression, rest: Expression*): WindowSpec = orderBy(first +: rest)

  def between(windowFrame: WindowFrame): WindowSpec = Default.between(windowFrame)

  def rowsBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rowsBetween(start, end)

  def rangeBetween(start: FrameBoundary, end: FrameBoundary): WindowSpec =
    Default.rangeBetween(start, end)
}
