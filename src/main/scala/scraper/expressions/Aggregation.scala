package scraper.expressions

import scala.util.Try

import scraper.Row
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.promoteDataType
import scraper.types.{DataType, LongType, NumericType}

trait Aggregation extends UnevaluableExpression {
  override def foldable: Boolean = false

  def agg(rows: Seq[Row]): Any
}

case class Count(child: Expression) extends Aggregation with UnaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm
  } yield if (e sameOrEqual child) this else copy(child = e)

  override def dataType: DataType = LongType

  override def agg(rows: Seq[Row]): Any = rows.length

  override def annotatedString: String = s"COUNT(${child.annotatedString})"

  override def sql: String = s"COUNT(${child.sql})"
}

trait UnaryArithmeticAggregation extends Aggregation with UnaryExpression {
  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => promoteDataType(e, NumericType.defaultType)
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }
  } yield if (strictChild sameOrEqual child) this else makeCopy(strictChild :: Nil)

  override def dataType: NumericType = whenStrictlyTyped(child.dataType match {
    case t: NumericType => t
  })

  protected lazy val numeric = dataType.genericNumeric

  protected lazy val ordering = dataType.genericOrdering
}

case class Sum(child: Expression) extends UnaryArithmeticAggregation {
  override def agg(rows: Seq[Row]): Any = rows map child.evaluate sum numeric

  override def annotatedString: String = s"SUM(${child.annotatedString})"

  override def sql: String = s"SUM(${child.sql})"
}

case class Max(child: Expression) extends UnaryArithmeticAggregation {
  override def agg(rows: Seq[Row]): Any = rows map child.evaluate max ordering

  override def annotatedString: String = s"MAX(${child.annotatedString})"

  override def sql: String = s"MAX(${child.sql})"
}

case class Min(child: Expression) extends UnaryArithmeticAggregation {
  override def agg(rows: Seq[Row]): Any = rows map child.evaluate min ordering

  override def annotatedString: String = s"MIN(${child.annotatedString})"

  override def sql: String = s"MIN(${child.sql})"
}
