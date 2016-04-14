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

  def orElse(that: TypeConstraint): OrElse = OrElse(this, that)
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
    case e if e.dataType == targetType => e
    case e                             => throw new TypeMismatchException(e, targetType)
  }
}

/**
 * A [[TypeConstraint]] that requires strict data types of all argument expressions in `args` to be
 * [[scraper.expressions.Cast.compatible compatible]] with `targetType`.
 */
case class CompatibleWith(targetType: DataType, args: Seq[Expression]) extends TypeConstraint {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
  } yield strictArgs map {
    case e if e.dataType compatibleWith targetType => promoteDataType(e, targetType)
    case e                                         => throw new TypeMismatchException(e, targetType)
  }
}

/**
 * A [[TypeConstraint]] that requires strict data types of all argument expressions in `args` to be
 * subtypes of abstract data type `superType`.
 */
case class SubtypeOf(supertype: AbstractDataType, args: Seq[Expression]) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))

    // Finds all expressions whose data types are already subtype of `superType`.
    candidates = for (e <- strictArgs if e.dataType subtypeOf supertype) yield e

    // Ensures that there's at least one expression whose data type is directly a subtype of
    // `superType`. In this way, expressions like
    //
    //   - "1":STRING + (2:INT)
    //   - 1:INT + 2:BIGINT
    //
    // are allowed, but
    //
    //   - "1":STRING + "2":STRING
    //
    // can be rejected. This behavior is consistent with PostgreSQL.
    widestSubType <- if (candidates.nonEmpty) {
      widestTypeOf(candidates map (_.dataType))
    } else {
      throw new TypeMismatchException(args.head, supertype)
    }
  } yield strictArgs map (promoteDataType(_, widestSubType))
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

  override def strictlyTyped: Try[Seq[Expression]] =
    first.strictlyTyped flatMap (andThen(_).strictlyTyped)
}

case class OrElse(left: TypeConstraint, right: TypeConstraint) extends TypeConstraint {
  override def strictlyTyped: Try[Seq[Expression]] = left.strictlyTyped orElse right.strictlyTyped
}
