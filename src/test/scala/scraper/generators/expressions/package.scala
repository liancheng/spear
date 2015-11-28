package scraper.generators

import org.scalacheck.Gen
import scraper.config.Settings
import scraper.expressions.Literal.{False, True}
import scraper.expressions._
import scraper.generators.types._
import scraper.generators.values._
import scraper.types.{BooleanType, PrimitiveType, TupleType}

package object expressions {
  def genExpression(schema: TupleType)(implicit settings: Settings): Gen[Expression] =
    genExpression(schema.toAttributes)(settings)

  def genExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    Gen sized {
      case size if size < 3 =>
        genTermExpression(input)(settings)

      case size =>
        Gen oneOf (genTermExpression(input)(settings), genPredicate(input)(settings))
    }

  def genPredicate(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genOrExpression(input)(settings)

  def genOrExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genUnaryOrBinary(genAndExpression(input)(settings), Or)

  def genAndExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] = {
    val genBranch = Gen oneOf (genNotExpression(input)(settings), Gen oneOf (True, False))
    genUnaryOrBinary(genBranch, And)
  }

  def genNotExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    for {
      size <- Gen.size
      comparison <- Gen resize (size - 1, genComparison(input)(settings))
    } yield !comparison

  def genComparison(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    Gen oneOf (
      Gen oneOf (True, False),
      genBinary(genTermExpression(input)(settings), Eq, NotEq, Gt, GtEq, Lt, LtEq)
    )

  def genTermExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genUnaryOrBinary(genProductExpression(input)(settings), Add, Minus)

  def genProductExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genUnaryOrBinary(genBaseExpression(input)(settings), Multiply, Divide)

  def genBaseExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    Gen oneOf (
      Gen oneOf input,
      for {
        dataType <- genPrimitiveType(settings)
        literal <- genLiteral(dataType)
      } yield literal,
      genExpression(input)(settings)
    )

  def genLiteral(dataType: PrimitiveType): Gen[Expression] =
    dataType match {
      case BooleanType => Gen oneOf (True, False)
      case _           => genValueForPrimitiveType(dataType) map Literal.apply
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
