package scraper.generators

import org.scalacheck.Gen
import scraper.config.Settings
import scraper.config.Settings.Key
import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.implicitlyConvertible
import scraper.expressions._
import scraper.generators.values._
import scraper.types.{BooleanType, FieldSpec, NumericType, PrimitiveType}
import scraper.utils.Logging

package object expr extends Logging {
  val NullProbabilities = Key("scraper.test.values.probabilities.null").double

  def genExpression(
    input: Seq[Expression], outputSpec: FieldSpec
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType    => genPredicate(input, outputSpec)(settings)
    case _: NumericType => genArithmetic(input, outputSpec)(settings)
  }

  def genArithmetic(
    input: Seq[Expression], outputSpec: FieldSpec
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType => genTermExpression(input, outputSpec)(settings)
  }

  def genTermExpression(
    input: Seq[Expression], outputSpec: FieldSpec
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType =>
      for {
        size <- Gen.size

        genProduct = genProductExpression(input, outputSpec)(settings)
        genNegate = Gen.resize(size - 1, genProduct map Negate)
        genTerm = genUnaryOrBinary(genProduct, Add, Minus)

        term <- size match {
          case s if s < 2 => genProduct
          case s if s < 3 => Gen oneOf (genProduct, genNegate)
          case s          => Gen oneOf (genProduct, genNegate, genTerm)
        }
      } yield term

    case BooleanType =>
      genPredicate(input, outputSpec)(settings)
  }

  def genProductExpression(
    input: Seq[Expression], outputSpec: FieldSpec
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case _: NumericType =>
      Gen.sized {
        case size if size < 2 =>
          genBaseExpression(input, outputSpec)(settings)

        case size =>
          val genBranch = genBaseExpression(input, outputSpec)(settings)
          genUnaryOrBinary(genBranch, Multiply, Divide)
      }
  }

  def genBaseExpression(
    input: Seq[Expression], outputSpec: FieldSpec
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = {
    val candidates = input.filter { e =>
      e.nullable == outputSpec.nullable && (
        e.dataType == outputSpec.dataType ||
        implicitlyConvertible(e.dataType, outputSpec.dataType)
      )
    }

    val genLeaf = if (candidates.nonEmpty) {
      Gen oneOf candidates
    } else {
      genLiteral(outputSpec)(settings)
    }

    Gen.sized {
      case 1 =>
        logDebug(s"genBaseExpression: 1")
        genLeaf
      case size =>
        logDebug(s"genBaseExpression: $size")
        Gen oneOf (genLeaf, Gen lzy genExpression(input, outputSpec)(settings))
    }
  }

  def genPredicate(
    input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType => genOrExpression(input, outputSpec)(settings)
  }

  def genOrExpression(
    input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      val genBranch = genAndExpression(input, outputSpec)(settings)
      genUnaryOrBinary(genBranch, Or)
  }

  def genAndExpression(
    input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      val genBranch = Gen.sized {
        case size if size < 2 =>
          genComparison(input, outputSpec)(settings)

        case _ =>
          Gen.oneOf(
            genNotExpression(input, outputSpec)(settings),
            genComparison(input, outputSpec)(settings)
          )
      }

      genUnaryOrBinary(genBranch, And)
  }

  def genNotExpression(
    input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      for {
        size <- Gen.size
        predicate <- Gen.resize(size - 1, genPredicate(input, outputSpec)(settings))
      } yield Not(predicate)
  }

  def genComparison(
    input: Seq[Expression], outputSpec: FieldSpec = BooleanType.?
  )(
    implicit
    settings: Settings
  ): Gen[Expression] = outputSpec.dataType match {
    case BooleanType =>
      Gen.sized {
        case size if size < 2 =>
          genLiteral(outputSpec.copy(dataType = BooleanType))

        case _ =>
          val genBranch = Gen lzy genTermExpression(input, outputSpec)(settings)
          Gen.oneOf(
            genBinary(genBranch, Gt, GtEq, Lt, LtEq, Eq, NotEq),
            genLiteral(outputSpec.copy(dataType = BooleanType))
          )
      }
  }

  def genLiteral(outputSpec: FieldSpec)(implicit settings: Settings): Gen[Literal] = {
    val (dataType, nullable) = outputSpec match {
      case FieldSpec(t: PrimitiveType, n) => (t, n)
      case FieldSpec(t, _) =>
        throw new TypeMismatchException(
          s"Literal only accepts primitive type while a ${t.sql} was found"
        )
    }

    val nullFreq = if (nullable) (settings(NullProbabilities) * 100).toInt else 0
    val nonNullFreq = 100 - nullFreq

    Gen.frequency(
      nullFreq -> Gen.const(Literal(null, dataType)),
      nonNullFreq -> genValueForPrimitiveType(dataType).map(Literal(_, dataType))
    )
  }

  private def genUnaryOrBinary[T <: Expression](genBranch: Gen[T], ops: ((T, T) => T)*): Gen[T] =
    Gen.sized {
      case size if size < 3 => genBranch
      case size             => Gen oneOf (genBranch, genBinary(genBranch, ops: _*))
    }

  private def genBinary[T <: Expression, R <: Expression](
    genBranch: Gen[T], ops: ((T, T) => R)*
  ): Gen[R] = for {
    size <- Gen.size
    lhs <- Gen resize ((size - 1) / 2, genBranch)
    rhs <- Gen resize ((size - 1) / 2, genBranch)
    op <- Gen oneOf ops
  } yield op(lhs, rhs)
}
