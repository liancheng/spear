package scraper.fastparser

import fastparse.all._

object Predicate {
  import WhitespaceApi._

  val comparisonPredicate: P0 =
    ???

  val predicate: P0 =
    comparisonPredicate opaque "predicate"
}

object ValueExpressionPrimary {
  val nonparenthesizedValueExpressionPrimary: P0 =
    ???
}

// SQL06 section 6.34
object BooleanValueExpression {
  import KeywordParser._
  import Predicate._
  import ValueExpressionPrimary._
  import WhitespaceApi._

  val parenthesizedBooleanValueExpression: P0 =
    "(" ~ booleanValueExpression ~ ")" opaque "parenthesized-boolean-value-expression"

  val booleanPredicand: P0 = (
    parenthesizedBooleanValueExpression
    | nonparenthesizedValueExpressionPrimary
  ) opaque "boolean-predicand"

  val booleanPrimary: P0 =
    predicate | booleanPredicand opaque "boolean-primary"

  val truthValue: P0 =
    TRUE | FALSE | UNKNOWN opaque "truth-value"

  val booleanTest: P0 =
    booleanPrimary ~ (IS ~ NOT.? ~ truthValue).? opaque "boolean-test"

  val booleanFactor: P0 =
    NOT.? ~ booleanTest opaque "boolean-factor"

  val booleanTerm: P0 = (
    booleanFactor
    | booleanTerm ~ AND ~ booleanFactor
  ) opaque "boolean-term"

  lazy val booleanValueExpression: P0 = (
    booleanTerm
    | booleanValueExpression ~ OR ~ booleanTerm
  ) opaque "boolean-value-expression"
}

// SQL06 section 6.25
object ValueExpression {
  import BooleanValueExpression._
  import WhitespaceApi._

  val valueExpression: P0 =
    booleanValueExpression opaque "value-expression"
}
