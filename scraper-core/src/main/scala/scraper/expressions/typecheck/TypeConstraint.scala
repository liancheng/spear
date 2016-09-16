package scraper.expressions.typecheck

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast._
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

/**
 * A trait used to define and enforce type constraints over lists of expressions.
 *
 * @see [[Expression.strictlyTyped]]
 * @see [[Expression.dataType]]
 */
trait TypeConstraint { self =>
  /**
   * Tries to return a list of strictly-typed expressions that satisfy this [[TypeConstraint]].
   * Whenever a failure occurs, a `Failure` is returned to indicate the cause.
   */
  def enforced: Try[Seq[Expression]]

  /**
   * Returns a new [[TypeConstraint]] that concatenates results of this and `that`
   * [[TypeConstraint]].
   */
  def ++(that: TypeConstraint): TypeConstraint = new TypeConstraint {
    override def enforced: Try[Seq[Expression]] = for {
      selfEnforced <- self.enforced
      thatEnforced <- that.enforced
    } yield selfEnforced ++ thatEnforced
  }

  /**
   * Returns a new [[TypeConstraint]] that first tries to [[TypeConstraint.enforced enforce]] this
   * [[TypeConstraint]], and then pipes the result to the `next` function to build and enforce
   * another [[TypeConstraint]].
   */
  def andAlso(next: Seq[Expression] => TypeConstraint): TypeConstraint = new TypeConstraint {
    override def enforced: Try[Seq[Expression]] = self.enforced flatMap (next(_).enforced)
  }
}

/**
 * A [[TypeConstraint]] that transforms well-formed `input` expressions into their strictly-typed
 * form. Fails when any of the `input` expressions is not well-formed.
 */
case class StrictlyTyped(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] = trySequence(input map (_.strictlyTyped))
}

/**
 * A [[TypeConstraint]] that implicitly casts all `input` expressions to the `target` data type.
 * Fails when any of the `input` expressions can't be implicitly casted to the `target` data type.
 */
case class SameTypeAs(input: Seq[Expression], target: DataType) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] =
    StrictlyTyped(input).enforced map (_ map widenDataTypeTo(target))
}

/**
 * A [[TypeConstraint]] that casts all `input` expressions to the same subtype `T` of a given
 * [[scraper.types.AbstractDataType abstract data type]] `supertype`, where `T` is the widest
 * `input` expression data types. Fails when any of the `input` expressions is not a subtype of
 * `supertype`.
 */
case class SameSubtypeOf(input: Seq[Expression], supertype: AbstractDataType)
  extends TypeConstraint {

  require(input.nonEmpty)

  override def enforced: Try[Seq[Expression]] = for {
    strictInput <- StrictlyTyped(input).enforced
    strictTypes = strictInput map (_.dataType)
    widestType <- widestTypeOf(strictTypes) filter (_ isSubtypeOf supertype) orElse {
      val violators = input filterNot (_.dataType isSubtypeOf supertype)
      throw new TypeMismatchException(violators, supertype)
    }
  } yield strictInput map widenDataTypeTo(widestType)
}

/**
 * A [[TypeConstraint]] that finds a narrowest common data type `T` for data types of all `input`
 * expressions and casts all `input` expressions to `T`. Fails when no such `T` exists.
 */
case class SameType(input: Seq[Expression]) extends TypeConstraint {
  require(input.nonEmpty)

  override def enforced: Try[Seq[Expression]] = for {
    strictInput <- StrictlyTyped(input).enforced
    widestType = widestTypeOf(strictInput map (_.dataType)) getOrElse {
      throw new TypeMismatchException(
        s"""Cannot find a common data type for data types of all the following expressions:
           |${input map { e => s" - Expression $e of type ${e.dataType.sql}" } mkString "\n"}
           |""".stripMargin
      )
    }
  } yield strictInput map widenDataTypeTo(widestType)
}

/**
 * A [[TypeConstraint]] that requires all the `input` expressions to be foldable constants.
 */
case class Foldable(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Try[Seq[Expression]] =
    StrictlyTyped(input).enforced map (_ map {
      case e if e.isFoldable => e
      case e                 => throw new TypeMismatchException(s"Expression $e is not foldable.")
    })
}
