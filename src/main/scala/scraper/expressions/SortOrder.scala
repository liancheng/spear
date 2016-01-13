package scraper.expressions

import scala.util.Try
import scalaz.Scalaz._
import scalaz._

import scraper.exceptions.TypeMismatchException
import scraper.types.{DataType, PrimitiveType}

abstract sealed class SortDirection

case object Ascending extends SortDirection {
  override def toString: String = "ASC"
}

case object Descending extends SortDirection {
  override def toString: String = "DESC"
}

case class SortOrder(child: Expression, direction: SortDirection)
  extends UnaryExpression with UnevaluableExpression {

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  def isAscending: Boolean = direction == Ascending

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    f(child) map (str => s"$str $direction")
}
