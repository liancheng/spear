package scraper.expressions.typecheck

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{promoteDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

/**
 * A trait that helps in both type checking and type coercion for expression arguments.
 */
trait TypeConstraint {
  def strictlyTyped: Try[Seq[Expression]]

  def ++(that: TypeConstraint): Concat = Concat(this, that)

  def andThen(andThen: Seq[Expression] => TypeConstraint): AndThen = AndThen(this, andThen)
}

/**
 * A [[TypeConstraint]] that imposes no requirements to data types of strictly-typed child
 * expressions.
 */
case class PassThrough(args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = trySequence(args map (_.strictlyTyped))
}

/**
 * A [[TypeConstraint]] that requires data types of all strictly-typed child expressions to be
 * exactly `targetType`.
 */
case class Exact(targetType: DataType, args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
  } yield strictArgs map {
    case `targetType`(e) => e
    case e               => throw new TypeMismatchException(e, targetType)
  }
}

/**
 * A [[TypeConstraint]] that requires strict data types of all argument expressions in `args` to be
 * implicitly convertible to `targetType`.
 */
case class ImplicitlyConvertibleTo(targetType: DataType, args: Seq[Expression])
  extends TypeConstraint {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
  } yield strictArgs map {
    case `targetType`.Implicitly(e) => promoteDataType(e, targetType)
    case e                          => throw new TypeMismatchException(e, targetType)
  }
}

/**
 * A [[TypeConstraint]] that requires strict data types of all argument expressions in `args` to be
 * subtypes of abstract data type `superType`.
 */
case class SubtypeOf(superType: AbstractDataType, args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
    candidates = strictArgs collect { case `superType`(e) => e }
    widestSubType <- if (candidates.nonEmpty) {
      widestTypeOf(candidates map (_.dataType))
    } else {
      throw new TypeMismatchException(args.head, superType)
    }
  } yield strictArgs map (promoteDataType(_, widestSubType))
}

/**
 * A [[TypeConstraint]] that requires strict data types of all argument expressions in `args` to be
 * compatible with data type of expression `target`. A data type `x` is considered compatible with
 * another data type `y` iff `x` equals to `y` or is implicitly convertible to `y`.
 */
case class CompatibleWith(target: Expression, args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictTarget <- target.strictlyTyped
    targetType = strictTarget.dataType
    strictArgs <- trySequence(args map (_.strictlyTyped))
  } yield strictArgs map {
    case `targetType`(e)            => e
    case `targetType`.Implicitly(e) => promoteDataType(e, targetType)
    case e                          => throw new TypeMismatchException(e, targetType)
  }
}

case class AllCompatible(args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
    widestType <- widestTypeOf(strictArgs map (_.dataType))
  } yield strictArgs map (promoteDataType(_, widestType))
}

/**
 * A [[TypeConstraint]] that concatenates results of two [[TypeConstraint]]s.
 */
case class Concat(left: TypeConstraint, right: TypeConstraint) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictLeft <- left.strictlyTyped
    strictRight <- right.strictlyTyped
  } yield strictLeft ++ strictRight
}

case class AndThen(first: TypeConstraint, andThen: Seq[Expression] => TypeConstraint)
  extends TypeConstraint {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    firstResult <- first.strictlyTyped
    secondResult <- andThen(firstResult).strictlyTyped
  } yield secondResult
}
