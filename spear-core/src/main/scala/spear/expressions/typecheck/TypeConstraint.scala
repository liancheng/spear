package spear.expressions.typecheck

import scala.util.Try
import scala.util.control.NonFatal

import spear.exceptions.TypeMismatchException
import spear.expressions.Cast._
import spear.expressions.Expression
import spear.types.{AbstractDataType, DataType}

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
  def enforced: Seq[Expression]

  /**
   * Returns a new [[TypeConstraint]] that concatenates results of this and `that`
   * [[TypeConstraint]].
   */
  def concat(that: TypeConstraint): TypeConstraint = new TypeConstraint {
    override def enforced: Seq[Expression] = self.enforced ++ that.enforced
  }

  /**
   * Returns a new [[TypeConstraint]] that reverses the result of this [[TypeConstraint]].
   */
  def reverse: TypeConstraint = new TypeConstraint {
    override def enforced: Seq[Expression] = self.enforced.reverse
  }

  /**
   * Returns a new [[TypeConstraint]] that first tries to [[TypeConstraint.enforced enforce]] this
   * [[TypeConstraint]], and then pipes the result to the `next` function to build and enforce
   * another [[TypeConstraint]].
   *
   * Essentially, [[TypeConstraint]] is a monad with [[andAlso]] being the `flatMap` method.
   */
  def andAlso(next: Seq[Expression] => TypeConstraint): TypeConstraint = new TypeConstraint {
    override def enforced: Seq[Expression] = next(self.enforced).enforced
  }

  /**
   * Returns a new [[TypeConstraint]] that succeeds when either this [[TypeConstraint]] or `that`
   * [[TypeConstraint]] can be [[TypeConstraint.enforced enforced]].
   */
  def orElse(that: TypeConstraint): TypeConstraint = new TypeConstraint {
    override def enforced: Seq[Expression] = Try(self.enforced) getOrElse that.enforced
  }
}

/**
 * A [[TypeConstraint]] that transforms well-formed `input` expressions into their strictly-typed
 * form. Fails when any of the `input` expressions is not well-formed.
 */
case class StrictlyTyped(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Seq[Expression] = input map { _.strictlyTyped.get }
}

/**
 * A [[TypeConstraint]] that implicitly casts all `input` expressions to the `target` data type.
 * Fails when any of the `input` expressions can't be implicitly casted to the `target` data type.
 */
case class SameTypeAs(input: Seq[Expression], target: DataType) extends TypeConstraint {
  override def enforced: Seq[Expression] = input.anyType.enforced map widenDataTypeTo(target)
}

/**
 * A [[TypeConstraint]] that casts all `input` expressions to the same subtype `T` of a given
 * [[spear.types.AbstractDataType abstract data type]] `supertype`, where `T` is the widest
 * `input` expression data types. Fails when any of the `input` expressions is not a subtype of
 * `supertype`.
 */
case class SameSubtypeOf(input: Seq[Expression], supertype: AbstractDataType)
  extends TypeConstraint {

  require(input.nonEmpty)

  override def enforced: Seq[Expression] = {
    val strictInput = input.anyType.enforced
    val strictTypes = strictInput map { _.dataType }

    val widestType = try {
      widestTypeOf(strictTypes).filter { _ isSubtypeOf supertype }.get
    } catch {
      case NonFatal(cause) =>
        val violators = input filterNot { _.dataType isSubtypeOf supertype }

        if (violators.nonEmpty) {
          // Reports all expressions whose data type is not a subtype of `supertype`, if any.
          throw new TypeMismatchException(violators, supertype)
        } else {
          throw new TypeMismatchException(cause.getMessage, cause)
        }
    }

    strictInput map widenDataTypeTo(widestType)
  }
}

/**
 * A [[TypeConstraint]] that finds a narrowest common data type `T` for data types of all `input`
 * expressions and casts all `input` expressions to `T`. Fails when no such `T` exists.
 */
case class SameType(input: Seq[Expression]) extends TypeConstraint {
  require(input.nonEmpty)

  override def enforced: Seq[Expression] = {
    val strictInput = input.anyType.enforced
    val strictTypes = strictInput map { _.dataType }

    val widestType = widestTypeOf(strictTypes) getOrElse {
      throw new TypeMismatchException(
        s"""Cannot find a common data type for data types of all the following expressions:
           |${input map { e => s" - Expression $e of type ${e.dataType.sql}" } mkString "\n"}
           |""".stripMargin
      )
    }

    strictInput map widenDataTypeTo(widestType)
  }
}

/**
 * A [[TypeConstraint]] that requires all the `input` expressions to be foldable constants.
 */
case class Foldable(input: Seq[Expression]) extends TypeConstraint {
  override def enforced: Seq[Expression] = input.anyType.enforced.map {
    case e if e.isFoldable => e
    case e =>
      throw new TypeMismatchException(s"Expression ${e.sqlLike} is not foldable.")
  }
}
