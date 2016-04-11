package scraper.expressions.typecheck

import scraper.expressions.Expression
import scraper.types.{AbstractDataType, DataType}

package object dsl {
  implicit class FromExpressions(expressions: Seq[Expression]) {
    def ofType(dataType: DataType): Exact = Exact(dataType, expressions)

    def subtypesOf(parentType: AbstractDataType): AllSubtypesOf =
      AllSubtypesOf(parentType, expressions)

    def allCompatible: AllCompatible = AllCompatible(expressions)
  }

  implicit class FromExpression(expression: Expression) {
    def ofType(dataType: DataType): Exact = Exact(dataType, expression :: Nil)

    def subtypeOf(parentType: AbstractDataType): AllSubtypesOf =
      AllSubtypesOf(parentType, expression :: Nil)
  }
}
