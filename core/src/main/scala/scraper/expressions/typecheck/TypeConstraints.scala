package scraper.expressions.typecheck

import scala.util.{Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

sealed trait TypeConstraints {
  def strictlyTyped: Try[Seq[Expression]]

  def ~(that: TypeConstraints): AndAlso = AndAlso(this, that)
}

case class PassThrough(expressions: Seq[Expression]) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] =
    Success(expressions)
}

case class Exact(dataType: DataType, expressions: Seq[Expression]) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
  } yield strictOnes map {
    case `dataType`(e)            => e
    case `dataType`.Implicitly(e) => promoteDataType(e, dataType)
    case e                        => throw new TypeMismatchException(e, dataType)
  }
}

case class AllBelongTo(parentType: AbstractDataType, expressions: Seq[Expression])
  extends TypeConstraints {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    // TODO Shouldn't compute widest type first
    widestType <- widestTypeOf(strictOnes map (_.dataType))
    widenedOnes = strictOnes map (promoteDataType(_, widestType))
  } yield widenedOnes map {
    case `parentType`(e)            => e
    case `parentType`.Implicitly(e) => promoteDataType(e, parentType)
    case e                          => throw new TypeMismatchException(e, parentType.getClass)
  }
}

case class AllCompatible(expressions: Seq[Expression]) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    widestType <- widestTypeOf(strictOnes map (_.dataType))
  } yield strictOnes map (promoteDataType(_, widestType))
}

case class AndAlso(left: TypeConstraints, right: TypeConstraints) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictLeft <- left.strictlyTyped
    strictRight <- right.strictlyTyped
  } yield strictLeft ++ strictRight
}
