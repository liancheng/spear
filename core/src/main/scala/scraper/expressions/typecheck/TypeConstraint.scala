package scraper.expressions.typecheck

import scala.util.Try

import scraper.exceptions.{ContractBrokenException, TypeMismatchException}
import scraper.expressions.Cast.{widenDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

/**
 * A trait used to define and enforce type constraints over input expressions.
 *
 * @see [[Expression.strictlyTyped]]
 * @see [[Expression.dataType]]
 */
trait TypeConstraint {
  /**
   * Tries to return a copy of argument expressions that satisfy the constraint defined by this
   * [[TypeConstraint]].
   */
  def enforced: Try[Seq[Expression]]

  /**
   * Builds a new [[TypeConstraint]] that concatenate results of this and `that`
   * [[TypeConstraint]]s.
   */
  def ++(that: TypeConstraint): Concat = Concat(this, that)

  /**
   * Builds a new [[TypeConstraint]] that first tries to [[TypeConstraint.enforced enforce]] this
   * [[TypeConstraint]], and then pipes the result to the `next` function to build another
   * [[TypeConstraint]].
   */
  def andThen(next: Seq[Expression] => TypeConstraint): AndThen = AndThen(this, next)

  /**
   * Builds a new [[TypeConstraint]] that requires the argument expressions to conform to either
   * this or `that` [[TypeConstraint]].
   */
  def orElse(that: TypeConstraint): OrElse = OrElse(this, that)
}

/**
 * A [[TypeConstraint]] that simply converts all `input` expressions to their strictly-typed form
 * without enforcing any further constraints.
 */
case class PassThrough(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = trySequence(input map (_.strictlyTyped))
}

/**
 * A [[TypeConstraint]] that first converts all `input` expressions to their strictly-typed form,
 * and then casts them to `targetType` when necessary. It requires data types of all `input`
 * expressions to be [[scraper.types.DataType.isCompatibleWith compatible with]] `targetType`.
 */
case class SameTypeAs(targetType: DataType, input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(input map (_.strictlyTyped))
  } yield strictArgs map {
    case e if e.dataType isCompatibleWith targetType => widenDataType(e, targetType)
    case e => throw new TypeMismatchException(e, targetType)
  }
}

/**
 * A [[TypeConstraint]] that first converts all `input` expressions to their strictly-typed form,
 * and then casts them to the same subtype of `supertype` when necessary. It requires data type of
 * at least one `input` expression is a subtype `t` of `supertype`, and data types of all the rest
 * `input` expressions to be [[scraper.types.DataType.isCompatibleWith compatible with]] `t`.
 */
case class SameSubtypesOf(supertype: AbstractDataType, input: Seq[Expression])
  extends TypeConstraint {

  override def enforced: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(input map (_.strictlyTyped))

    // Finds all expressions whose data types are already subtype of `superType`.
    candidates = for (e <- strictArgs if e.dataType isSubtypeOf supertype) yield e

    // Ensures that there's at least one expression whose data type is directly a subtype of
    // `superType`. For example, the following expressions are valid
    //
    //   "1":STRING + (2:INT)
    //   1:INT + 2:BIGINT
    //
    // while
    //
    //   "1":STRING + "2":STRING
    //
    // is invalid. This behavior is consistent with PostgreSQL.
    widestSubType <- if (candidates.nonEmpty) {
      widestTypeOf(candidates map (_.dataType))
    } else {
      throw new TypeMismatchException(input.head, supertype)
    }
  } yield strictArgs map (widenDataType(_, widestSubType))
}

/**
 * A [[TypeConstraint]] that first converts all `input` expressions to their strictly-typed form,
 * finds the widest data type `t` among all the `input` expressions, and then casts them to `t` when
 * necessary. It requires data types of all `input` expressions to be
 * [[scraper.types.DataType.isCompatibleWith compatible with]] each other.
 */
case class SameType(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(input map (_.strictlyTyped))
    widestType <- widestTypeOf(strictArgs map (_.dataType))
  } yield strictArgs map (widenDataType(_, widestType))
}

/**
 * A [[TypeConstraint]] that requires all the `input` expressions to be foldable constants.
 */
case class Foldable(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(input map (_.strictlyTyped))
  } yield strictArgs map {
    case e if e.isFoldable => e
    case e => throw new ContractBrokenException(
      s"Expression $e is not a foldable constant."
    )
  }
}

/**
 * A [[TypeConstraint]] that concatenates results of two [[TypeConstraint]]s.
 */
case class Concat(left: TypeConstraint, right: TypeConstraint) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = for {
    strictLeft <- left.enforced
    strictRight <- right.enforced
  } yield strictLeft ++ strictRight
}

/**
 * A [[TypeConstraint]] that first tries to [[TypeConstraint.enforced enforce]] the `first`
 * [[TypeConstraint]], and then pipes the result to the `next` function to build another
 * [[TypeConstraint]].
 */
case class AndThen(first: TypeConstraint, next: Seq[Expression] => TypeConstraint)
  extends TypeConstraint {

  override def enforced: Try[Seq[Expression]] = first.enforced flatMap (next(_).enforced)
}

/**
 * A [[TypeConstraint]] that requires the argument expressions to conform to either `left` or
 * `right` [[TypeConstraint]].
 */
case class OrElse(left: TypeConstraint, right: TypeConstraint) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = left.enforced orElse right.enforced
}
