package scraper.expressions

import scraper.Row
import scraper.exceptions.{ContractBrokenException, TypeMismatchException}
import scraper.types.{LongType, DoubleType, NumericType, DataType}

import scala.util.Try

trait AggregateExpression extends UnaryExpression {
  override def foldable: Boolean = false

  override def evaluate(input: Row): Any =
    throw new ContractBrokenException("Cannot call AggregateExpression.evaluate directly")

  override def sql: String = ???

  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm map {
      case DoubleType(e)            => e
      case DoubleType.Implicitly(e) => e
      case e                        => throw new TypeMismatchException(e, classOf[NumericType])
    }
  } yield if (sameChildren(Seq(e))) this else makeCopy(Seq(e))

  override def dataType: DataType = whenStrictlyTyped(child.dataType)

  override def annotatedString: String = ???

  def agg(rows: Seq[Row]): Any = _agg(rows.map(child.evaluate))

  protected def _agg(values: Seq[Any]): Any
}

case class Count(child: Expression) extends AggregateExpression {
  override lazy val strictlyTypedForm: Try[Expression] = for {
    e <- child.strictlyTypedForm
  } yield if (sameChildren(Seq(e))) this else makeCopy(Seq(e))

  override def dataType: DataType = LongType

  override def _agg(values: Seq[Any]): Any = values.length
}

case class Sum(child: Expression) extends AggregateExpression {
  override def _agg(values: Seq[Any]): Any =
    values.map(_.asInstanceOf[java.lang.Number].doubleValue()).sum
}

case class Max(child: Expression) extends AggregateExpression {
  override def _agg(values: Seq[Any]): Any =
    values.map(_.asInstanceOf[java.lang.Number].doubleValue()).max
}

case class Min(child: Expression) extends AggregateExpression {
  override def _agg(values: Seq[Any]): Any =
    values.map(_.asInstanceOf[java.lang.Number].doubleValue()).min
}
