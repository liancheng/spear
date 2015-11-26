package scraper.generators

import org.scalacheck.Gen
import scraper.expressions.LogicalLiteral.{False, True}
import scraper.expressions.{Literal, LiteralExpression}
import scraper.generators.values._
import scraper.types.{BooleanType, PrimitiveType}

package object expressions {
  def genLiteralExpression(dataType: PrimitiveType): Gen[LiteralExpression] = dataType match {
    case BooleanType => Gen.oneOf(Gen.const(True), Gen.const(False))
    case _           => genValueForPrimitiveType(dataType) map Literal.apply
  }
}
