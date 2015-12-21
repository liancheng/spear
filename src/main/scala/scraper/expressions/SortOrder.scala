package scraper.expressions

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.types.{DataType, PrimitiveType}

abstract sealed class SortDirection

case object Ascending extends SortDirection {
  override def toString: String = "ASCE"
}

case object Descending extends SortDirection {
  override def toString: String = "DESC"
}

case class SortOrder(child: Expression, direction: SortDirection)
  extends UnaryExpression with UnevaluableExpression {

  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }
  } yield if (e sameOrEqual child) this else copy(child = e)

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def annotatedString: String = s"$child $direction"

  def isAscending: Boolean = direction == Ascending

  override def sql: String = ???
}
