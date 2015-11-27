package scraper.generators

import org.scalacheck.{Arbitrary, Gen}
import scraper.config.Settings
import scraper.expressions.LogicalLiteral.{False, True}
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

  def genPredicate(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Predicate] =
    genOrExpression(input)(settings)

  def genOrExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Predicate] =
    genUnaryOrBinary(genAndExpression(input)(settings), Or)

  def genAndExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Predicate] =
    genUnaryOrBinary(genNotExpression(input)(settings), And)

  def genNotExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Predicate] =
    Gen oneOf (
      Gen oneOf (Gen const True, Gen const False),
      for {
        size <- Gen.size
        comparison <- Gen resize (size - 1, genComparison(input)(settings))
        not <- Arbitrary.arbitrary[Boolean]
      } yield if (not) Not(comparison) else comparison
    )

  def genComparison(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Predicate] =
    genBinary(genTermExpression(input)(settings), Eq, NotEq, Gt, GtEq, Lt, LtEq)

  def genTermExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genUnaryOrBinary(genProductExpression(input)(settings), Add, Minus)

  def genProductExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    genUnaryOrBinary(genBaseExpression(input)(settings), Multiply, Divide)

  def genBaseExpression(input: Seq[AttributeRef])(implicit settings: Settings): Gen[Expression] =
    Gen oneOf (
      Gen oneOf input,
      for {
        dataType <- genPrimitiveType(settings)
        literal <- genLiteralExpression(dataType)
      } yield literal,
      genExpression(input)(settings)
    )

  def genLiteralExpression(dataType: PrimitiveType): Gen[LiteralExpression] =
    dataType match {
      case BooleanType => Gen oneOf (True, False)
      case _           => genValueForPrimitiveType(dataType) map Literal.apply
    }

  private def genUnaryOrBinary[T <: Expression](genUnary: Gen[T], ops: ((T, T) => T)*): Gen[T] =
    Gen.sized {
      case size if size < 3 => genUnary
      case size             => Gen oneOf (genUnary, genBinary(genUnary, ops: _*))
    }

  private def genBinary[T <: Expression, R <: Expression](
    genUnary: Gen[T],
    ops: ((T, T) => R)*
  ): Gen[R] =
    for {
      size <- Gen.size
      lhs <- Gen resize ((size - 1) / 2, genUnary)
      rhs <- Gen resize ((size - 1) / 2, genUnary)
      op <- Gen oneOf ops
    } yield op(lhs, rhs)
}
