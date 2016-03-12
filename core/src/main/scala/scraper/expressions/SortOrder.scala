package scraper.expressions

import scala.language.higherKinds
import scala.util.Try
import scalaz.Scalaz._
import scalaz._

import scraper.exceptions.TypeMismatchException
import scraper.types.{DataType, OrderedType}

abstract sealed class SortDirection

case object Ascending extends SortDirection {
  override def toString: String = "ASC"
}

case object Descending extends SortDirection {
  override def toString: String = "DESC"
}

case class SortOrder(child: Expression, direction: SortDirection, nullsLarger: Boolean)
  extends UnaryExpression with UnevaluableExpression {

  override lazy val strictlyTyped: Try[Expression] = for {
    strictChild <- child.strictlyTyped map {
      case OrderedType(e) => e
      case e              => throw new TypeMismatchException(e, classOf[OrderedType])
    }
  } yield if (strictChild same child) this else copy(child = strictChild)

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  def nullsFirst: SortOrder = direction match {
    case Ascending  => copy(nullsLarger = false)
    case Descending => copy(nullsLarger = true)
  }

  def nullsLast: SortOrder = direction match {
    case Ascending  => copy(nullsLarger = true)
    case Descending => copy(nullsLarger = false)
  }

  def isAscending: Boolean = direction == Ascending

  def isNullsFirst: Boolean = isAscending ^ nullsLarger

  override protected def template[T[_]: Applicative](f: Expression => T[String]): T[String] = {
    val nullsFirstOrLast = (direction, nullsLarger) match {
      case (Ascending, false) => "FIRST"
      case (Descending, true) => "FIRST"
      case _                  => "LAST"
    }

    f(child) map (_ + s" $direction NULLS $nullsFirstOrLast")
  }
}
