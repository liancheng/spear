package scraper.expressions

import scala.util.Try

import scraper.Row
import scraper.exceptions.{TypeMismatchException, ContractBrokenException}
import scraper.types.{PrimitiveType, DataType}

abstract sealed class SortDirection
case object Ascending extends SortDirection
case object Descending extends SortDirection

/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 */
case class SortOrder(child: Expression, direction: SortDirection)
  extends UnaryExpression {

  /** Sort order is not foldable because we don't have an eval for it. */
  override def foldable: Boolean = false

  override def evaluate(input: Row): Any =
    throw new ContractBrokenException("Cannot call SortOrder.evaluate directly")

  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }
  } yield if (e sameOrEqual child) this else copy(child = e)

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  override def toString: String = s"$child ${if (direction == Ascending) "ASC" else "DESC"}"

  def isAscending: Boolean = direction == Ascending

  override def sql: String = ???

  override def annotatedString: String = ???
}
