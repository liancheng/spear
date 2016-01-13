package scraper.expressions

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.promoteDataType
import scraper.plans.physical.NullSafeOrdering
import scraper.types.{DataType, LongType, NumericType, PrimitiveType}
import scraper.{MutableRow, Row}

trait AggregateFunction extends Expression {
  def partialResultType: DataType = dataType

  def zero(row: MutableRow, ordinal: Int): Unit

  def partial(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit

  def merge(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit =
    partial(accumulator, ordinal, rows)

  def result(accumulator: Row, ordinal: Int): Any
}

case class Count(child: Expression) extends UnaryExpression with AggregateFunction {
  override def dataType: DataType = LongType

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  override def zero(row: MutableRow, ordinal: Int): Unit = row(ordinal) = 0L

  override def partial(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit = {
    accumulator(ordinal) = rows.length.toLong
  }

  override def merge(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit = {
    val initial = accumulator(ordinal).asInstanceOf[Long]
    accumulator(ordinal) = initial + rows.map(_.head.asInstanceOf[Long]).sum
  }

  override def result(accumulator: Row, ordinal: Int): Any = accumulator(ordinal)
}

case class Sum(child: Expression) extends UnaryExpression with AggregateFunction {
  override protected def strictDataType: DataType = child.dataType

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case NumericType(e)            => e
      case NumericType.Implicitly(e) => promoteDataType(e, NumericType.defaultType)
      case e                         => throw new TypeMismatchException(e, classOf[NumericType])
    }
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  private lazy val numeric: Numeric[Any] = dataType match {
    case t: NumericType => t.genericNumeric
  }

  override def zero(row: MutableRow, ordinal: Int): Unit = row(ordinal) = numeric.zero

  override def partial(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit = {
    numeric.plus(accumulator(ordinal), rows map child.evaluate sum numeric)
  }

  override def result(accumulator: Row, ordinal: Int): Any = accumulator(ordinal)
}

case class Max(child: Expression) extends UnaryExpression with AggregateFunction {
  override protected def strictDataType: DataType = child.dataType

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  private lazy val ordering: Ordering[Any] = new NullSafeOrdering(dataType, nullsFirst = true)

  override def zero(row: MutableRow, ordinal: Int): Unit = row(ordinal) = null

  override def result(accumulator: Row, ordinal: Int): Any = accumulator(ordinal)

  override def partial(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit = {
    accumulator(ordinal) = ordering.max(accumulator(ordinal), rows map child.evaluate max ordering)
  }
}

case class Min(child: Expression) extends UnaryExpression with AggregateFunction {
  override protected def strictDataType: DataType = child.dataType

  override lazy val strictlyTypedForm: Try[Expression] = for {
    strictChild <- child.strictlyTypedForm map {
      case PrimitiveType(e) => e
      case e                => throw new TypeMismatchException(e, classOf[PrimitiveType])
    }
  } yield if (strictChild sameOrEqual child) this else copy(child = strictChild)

  private lazy val ordering: Ordering[Any] = new NullSafeOrdering(dataType, nullsFirst = false)

  override def zero(row: MutableRow, ordinal: Int): Unit = row(ordinal) = null

  override def result(accumulator: Row, ordinal: Int): Any = accumulator(ordinal)

  override def partial(accumulator: MutableRow, ordinal: Int, rows: Iterator[Row]): Unit = {
    accumulator(ordinal) = ordering.min(accumulator(ordinal), rows map child.evaluate min ordering)
  }
}
