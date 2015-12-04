package scraper.generators

import org.scalacheck.Shrink.shrink
import org.scalacheck.{Gen, Shrink}
import scraper.config.Settings
import scraper.expressions.Cast.implicitlyConvertible
import scraper.expressions._
import scraper.generators.values._
import scraper.types.{BooleanType, DataType, PrimitiveType}

package object expressions {
  def genExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    Gen sized {
      case size if size < 3 =>
        genTermExpression(input, dataType)(settings)

      case size =>
        Gen oneOf (
          genTermExpression(input, dataType)(settings),
          genPredicate(input, dataType)(settings)
        )
    }

  def genPredicate(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    genOrExpression(input, dataType)(settings)

  def genOrExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    genUnaryOrBinary(genAndExpression(input, dataType)(settings), Or)

  def genAndExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] = {
    val genBranch = Gen oneOf (
      genNotExpression(input, dataType)(settings),
      genComparison(input, dataType)(settings)
    )
    genUnaryOrBinary(genBranch, And)
  }

  def genNotExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    for {
      size <- Gen.size
      comparison <- Gen resize (size - 1, genComparison(input, dataType)(settings))
    } yield !comparison

  def genComparison(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    Gen oneOf (
      genLiteral(BooleanType),
      genBinary(genTermExpression(input, dataType)(settings), Eq, NotEq, Gt, GtEq, Lt, LtEq),
      input collect { case BooleanType(e) => Gen const e }: _*
    )

  def genTermExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    genUnaryOrBinary(genProductExpression(input, dataType)(settings), Add, Minus)

  def genProductExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    genUnaryOrBinary(genBaseExpression(input, dataType)(settings), Multiply, Divide)

  def genBaseExpression(input: Seq[Expression], dataType: DataType)(
    implicit
    settings: Settings
  ): Gen[Expression] =
    dataType match {
      case t: PrimitiveType =>
        Gen oneOf (
          Gen oneOf input,
          genLiteral(t),
          genExpression(input, dataType)(settings)
        )

      case _ =>
        val es = input filter (e => implicitlyConvertible(e.dataType, dataType))
        if (es.isEmpty) {
          genExpression(input, dataType)(settings)
        } else {
          Gen oneOf (Gen oneOf es, genExpression(input, dataType)(settings))
        }
    }

  def genLiteral(dataType: PrimitiveType): Gen[Literal] =
    genValueForPrimitiveType(dataType) map Literal.apply

  implicit val shrinkLiteral: Shrink[Literal] = Shrink {
    case lit @ Literal(value: Boolean, _) => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Byte, _)    => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Short, _)   => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Int, _)     => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Long, _)    => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Float, _)   => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: Double, _)  => shrink(value) map (v => lit.copy(value = v))
    case lit @ Literal(value: String, _)  => shrink(value) map (v => lit.copy(value = v))
  }

  private def genUnaryOrBinary[T <: Expression](genBranch: Gen[T], ops: ((T, T) => T)*): Gen[T] =
    Gen.sized {
      case size if size < 3 => genBranch
      case size             => Gen oneOf (genBranch, genBinary(genBranch, ops: _*))
    }

  private def genBinary[T <: Expression, R <: Expression](
    genBranch: Gen[T], ops: ((T, T) => R)*
  ): Gen[R] =
    for {
      size <- Gen.size
      lhs <- Gen resize ((size - 1) / 2, genBranch)
      rhs <- Gen resize ((size - 1) / 2, genBranch)
      op <- Gen oneOf ops
    } yield op(lhs, rhs)
}
