package scraper.expressions.typecheck

import scala.util.{Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
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
    case e             => throw new TypeMismatchException(e, dataType)
  }
}

case class BelongTo(parentType: AbstractDataType, expressions: Seq[Expression])
  extends TypeConstraint {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))

    promotedOnes = strictOnes map {
      case `parentType`(e)            => e
      case `parentType`.Implicitly(e) => promoteDataType(e, parentType.defaultType.get)
      case e                          => throw new TypeMismatchException(e, parentType.getClass)
    }

    widestType <- widestTypeOf(promotedOnes map (_.dataType))
  } yield promotedOnes map (promoteDataType(_, widestType))
}

case class Compatible(expressions: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictOnes <- trySequence(expressions map (_.strictlyTyped))
    widestType <- widestTypeOf(strictOnes map (_.dataType))
  } yield strictOnes map (promoteDataType(_, widestType))
}
