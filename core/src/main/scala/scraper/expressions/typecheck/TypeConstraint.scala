package scraper.expressions.typecheck

import scala.util.Try

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{convertible, implicitlyConvertible, promoteDataType, widestTypeOf}
import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}
import scraper.utils.trySequence

/**
 * A trait that helps in both type checking and type coercion for expression arguments.
 */
sealed trait TypeConstraint {
  def args: Seq[Expression]

  def strictlyTyped: Try[Seq[Expression]]

  def ~(that: TypeConstraint): Concat = Concat(this, that)

  def &&(that: TypeConstraint): AndThen = AndThen(this, that)
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
 * convertible to `targetType`. Only implicit conversion is allowed if `implicitOnly` is `true`.
 */
case class ConvertibleTo(targetType: DataType, args: Seq[Expression], implicitOnly: Boolean)
  extends TypeConstraint {

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictArgs <- trySequence(args map (_.strictlyTyped))
  } yield strictArgs map {
    case e if implicitlyConvertible(e.dataType, targetType) =>
      promoteDataType(e, targetType)

    case e if !implicitOnly && convertible(e.dataType, targetType) =>
      e cast targetType

    case e =>
      throw new TypeMismatchException(e, targetType)
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
  override def args: Seq[Expression] = left.args ++ right.args

  override def strictlyTyped: Try[Seq[Expression]] = for {
    strictLeft <- left.strictlyTyped
    strictRight <- right.strictlyTyped
  } yield strictLeft ++ strictRight
}

case class AndThen(left: TypeConstraint, right: TypeConstraint) extends TypeConstraint {
  require(left.args.length == right.args.length)

  override def args: Seq[Expression] = left.args

  override def strictlyTyped: Try[Seq[Expression]] = for {
    _ <- left.strictlyTyped
    strictArgs <- right.strictlyTyped
  } yield strictArgs
}
