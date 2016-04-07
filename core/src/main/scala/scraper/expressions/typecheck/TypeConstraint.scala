package scraper.expressions.typecheck

import scala.util.{Success, Try}

import scraper.exceptions.TypeCheckException
import scraper.expressions.{Cast, Expression}
import scraper.expressions.Cast.widestTypeOf
import scraper.types.DataType
import scraper.utils.trySequence

sealed trait TypeConstraint {
  def strictlyTyped: Try[Seq[Expression]]
}

case class PassThrough(expressions: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = Success(expressions)
}

case class Exact(dataType: DataType, expressions: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
  } yield strictOnes map {
    case `dataType`(e) => e
    case e => throw new TypeCheckException(
      s"Expect data type ${dataType.sql}, but expression $e has type ${e.dataType.sql}"
    )
  }
}

case class Compatible(expressions: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    widestType <- widestTypeOf(strictOnes map (_.dataType))
  } yield strictOnes map {
    case e if e.dataType == widestType => e
    case e Cast t                      => e cast widestType
    case e                             => e cast widestType
  }
}
