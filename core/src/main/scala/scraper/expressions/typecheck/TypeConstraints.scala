package scraper.expressions.typecheck

import scala.util.{Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

sealed trait TypeConstraints {
  def strictlyTyped: Try[Seq[Expression]]

  def ~(that: TypeConstraints): Concat = Concat(this, that)
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

case class AllSubtypesOf(parentType: AbstractDataType, expressions: Seq[Expression])
  extends TypeConstraints {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    candidates = strictOnes collect { case `parentType`(e) => e }
    widestSubType <- if (candidates.nonEmpty) {
      widestTypeOf(candidates map (_.dataType))
    } else {
      throw new TypeMismatchException(expressions.head, parentType)
    }
  } yield strictOnes map (promoteDataType(_, widestSubType))
}

case class AllCompatible(expressions: Seq[Expression]) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    widestType <- widestTypeOf(strictOnes map (_.dataType))
  } yield strictOnes map (promoteDataType(_, widestType))
}

case class Concat(left: TypeConstraints, right: TypeConstraints) extends TypeConstraints {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictLeft <- left.strictlyTyped
    strictRight <- right.strictlyTyped
  } yield strictLeft ++ strictRight
}
