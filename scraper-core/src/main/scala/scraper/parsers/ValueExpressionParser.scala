package scraper.parsers

import fastparse.all._

import scraper.annotations.ExtendedSQLSyntax
import scraper.expressions._
import scraper.expressions.functions._
import scraper.expressions.Literal.{False, True}
import scraper.plans.logical.LogicalPlan

// SQL06 section 5.3
object StringParser extends LoggingParser {
  import IdentifierParser._
  import WhitespaceApi._

  private val nonquoteCharacter: P[Char] =
    !P("'") ~ AnyChar.char opaque "nonquote-character"

  private val characterRepresentation: P[Char] =
    nonquoteCharacter | P("''").char opaque "character-representation"

  private val quotedCharacterRepresentations: P[String] = (
    "'" ~~ characterRepresentation.repX ~~ "'"
    map { _.mkString }
    opaque "quoted-character-representations"
  )

  private val quotedCharacterRepresentationsList: P[String] = (
    quotedCharacterRepresentations rep 1 map { _.mkString }
    opaque "quoted-character-representations-list"
  )

  val characterStringLiteral: P[Literal] =
    quotedCharacterRepresentationsList map Literal.apply opaque "character-string-literal"

  val unicodeCharacterStringLiteral: P[Literal] = (
    "U&" ~~ quotedCharacterRepresentationsList ~ unicodeEscapeSpecifier
    map (parseUnicodeRepresentations _).tupled
    map Literal.apply
    opaque "unicode-character-string-literal"
  )
}

// SQL06 section 5.3
object NumericParser extends LoggingParser {
  import WhitespaceApi._

  val sign: P[Int] =
    (P("+") attach 1) | (P("-") attach -1) opaque "sign"

  private val digit: P0 =
    CharIn('0' to '9') opaque "digit"

  val unsignedInteger: P[BigInt] =
    (digit rep 1).! map { BigInt(_) } opaque "unsigned-integer"

  private val exactNumeric: P[BigDecimal] = (
    unsignedInteger ~~ ("." ~~ unsignedInteger.?).?
    | "." ~~ unsignedInteger
  ).! map { BigDecimal(_) } opaque "exact-numeric"

  private val mantissa: P[BigDecimal] =
    exactNumeric opaque "mantissa"

  private val signedInteger: P[BigInt] = (
    (sign.? ~ unsignedInteger).!
    map { BigInt(_, 10) }
    opaque "signed-integer"
  )

  private val exponent: P[BigInt] =
    signedInteger opaque "exponent"

  private val approximateNumeric: P[BigDecimal] = (
    (mantissa ~ "E" ~ exponent).!
    map { BigDecimal(_) }
    opaque "approximate-numeric"
  )

  private val unsignedNumeric: P[BigDecimal] =
    exactNumeric | approximateNumeric opaque "unsigned-numeric"

  private val signedNumeric: P[BigDecimal] = (
    sign.? ~ unsignedNumeric map {
      case (Some(signum), unsigned) => signum * unsigned
      case (_, unsigned)            => unsigned
    }
    opaque "signed-numeric"
  )

  private def toLiteral(d: BigDecimal): Literal = d match {
    case _ if d.isValidInt     => Literal(d.toInt)
    case _ if d.isValidLong    => Literal(d.toLong)
    case _ if d.isBinaryFloat  => Literal(d.toFloat)
    case _ if d.isBinaryDouble => Literal(d.toDouble)
  }

  val unsignedNumericLiteral: P[Literal] =
    unsignedNumeric map toLiteral opaque "unsigned-numeric-literal"

  val signedNumericLiteral: P[Literal] =
    signedNumeric map toLiteral opaque "unsigned-numeric-literal"
}

// SQL06 section 5.3
object LiteralParser extends LoggingParser {
  import KeywordParser._
  import NumericParser._
  import StringParser._
  import WhitespaceApi._

  val booleanLiteral: P[Literal] =
    (TRUE attach True) | (FALSE attach False) opaque "boolean-literal"

  private val generalLiteral: P[Literal] = (
    characterStringLiteral
    | unicodeCharacterStringLiteral
    | booleanLiteral
    opaque "general-literal"
  )

  val literal: P[Literal] = (
    signedNumericLiteral
    | generalLiteral
    opaque "literal"
  )

  val unsignedLiteral: P[Literal] = (
    unsignedNumericLiteral
    | generalLiteral
    opaque "unsigned-literal"
  )
}

// SQL06 section 6.3
object ValueExpressionPrimaryParser extends LoggingParser {
  import CaseExpressionParser._
  import CastSpecificationParser._
  import ColumnReferenceParser._
  import ValueExpressionParser._
  import ValueSpecificationParser._
  import WhitespaceApi._

  private val parenthesizedValueExpressionPrimary: P[Expression] =
    "(" ~ P(valueExpression) ~ ")" opaque "parenthesized-value-expression-primary"

  val nonparenthesizedValueExpressionPrimary: P[Expression] = (
    functionCall
    | unsignedValueSpecification
    | columnReference
    | caseExpression
    | castSpecification
    opaque "nonparenthesized-value-expression-primary"
  )

  val valueExpressionPrimary: P[Expression] = (
    parenthesizedValueExpressionPrimary
    | nonparenthesizedValueExpressionPrimary
    opaque "value-expression-primary"
  )
}

// SQL06 section 6.4
object ValueSpecificationParser extends LoggingParser {
  import LiteralParser._
  import WhitespaceApi._

  val unsignedValueSpecification: P[Literal] =
    unsignedLiteral opaque "unsigned-value-specification"
}

// SQL06 section 6.7
object ColumnReferenceParser extends LoggingParser {
  import IdentifierChainParser._
  import scraper.expressions._

  val columnReference: P[Attribute] =
    basicIdentifierChain map {
      case Seq(qualifier, column) => column of qualifier
      case Seq(column)            => column: Attribute
    } opaque "column-reference"
}

// SQL06 section 6.11
object CaseExpressionParser extends LoggingParser {
  import BooleanValueExpressionParser._
  import KeywordParser._
  import PredicateParser._
  import RowValueExpressionParser._
  import SearchConditionParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val nullif: P[Expression] =
    NULLIF ~ "(" ~ P(valueExpression) ~ "," ~ P(valueExpression) ~ ")" map {
      case (condition, value) => If(condition, Literal(null), value)
    } opaque "nullif"

  private val coalesce: P[Expression] = (
    COALESCE ~ "(" ~ P(valueExpression).rep(min = 1, sep = ",") ~ ")"
    map { Coalesce(_) }
    opaque "coalesce"
  )

  @ExtendedSQLSyntax
  private val ifExpression: P[Expression] = {
    val test = P(booleanValueExpression)
    val yes = P(valueExpression)
    val no = P(valueExpression)
    IF ~ "(" ~ test ~ "," ~ yes ~ "," ~ no ~ ")" map If.tupled opaque "if-expression"
  }

  private val caseAbbreviation: P[Expression] =
    ifExpression | nullif | coalesce opaque "case-abbreviation"

  private val caseOperand: P[Expression] =
    P(rowValuePredicand) opaque "row-value-predicand"

  private val whenOperand: P[Expression => Expression] = (
    P(rowValuePredicand).map { rhs => (_: Expression) === rhs }
    | P(comparisonPredicatePart2)
  )

  private val whenOperandList: P[Expression => Expression] =
    whenOperand rep (min = 1, sep = ",") map { makeCondition => (key: Expression) =>
      makeCondition map { _ apply key } reduce Or
    } opaque "when-operand-list"

  private val result: P[Expression] =
    P(valueExpression) opaque "result"

  private val simpleWhenClause: P[(Expression => Expression, Expression)] =
    WHEN ~ whenOperandList ~ THEN ~ result opaque "simple-when-clause"

  private val elseClause: P[Expression] = ELSE ~ result opaque "else-clause"

  private val simpleCase: P[Expression] = (
    CASE ~ caseOperand ~ simpleWhenClause.rep(1) ~ elseClause.? ~ END
    map {
      case (key, whenClauses, alternative) =>
        val (conditions, consequences) = whenClauses.map {
          case (makeCondition, consequence) =>
            makeCondition(key) -> consequence
        }.unzip

        CaseWhen(conditions, consequences, alternative)
    }
    opaque "simple-case"
  )

  private val searchedWhenClause: P[(Expression, Expression)] =
    WHEN ~ searchCondition ~ THEN ~ result opaque "when-operand"

  private val searchedCase: P[Expression] =
    CASE ~ (searchedWhenClause rep 1) ~ elseClause.? ~ END map {
      case (whenClauses, alternative) =>
        val (conditions, consequences) = whenClauses.unzip
        CaseWhen(conditions, consequences, alternative)
    } opaque "searched-case"

  private val caseSpecification: P[Expression] =
    simpleCase | searchedCase opaque "case-specification"

  val caseExpression: P[Expression] =
    caseAbbreviation | caseSpecification opaque "case-expression"
}

object CastSpecificationParser extends LoggingParser {
  import DataTypeParser._
  import KeywordParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val castOperand: P[Expression] = P(valueExpression)

  val castSpecification: P[Expression] = (
    CAST ~ "(" ~ castOperand ~ AS ~ dataType ~ ")"
    map { case (operand, targetType) => operand cast targetType }
    opaque "cast-specification"
  )
}

// SQL06 section 6.25
object ValueExpressionParser extends LoggingParser {
  import BooleanValueExpressionParser._
  import KeywordParser._
  import NameParser._
  import StringValueExpressionParser._
  import WhitespaceApi._

  val functionCall: P[UnresolvedFunction] = (
    functionName ~ "(" ~ DISTINCT.!.? ~ P(valueExpression).rep(sep = ",") ~ ")"
    map { case (name, isDistinct, args) => (name, args, isDistinct.isDefined) }
    map UnresolvedFunction.tupled
    opaque "function-call"
  )

  val commonValueExpression: P[Expression] = stringValueExpression opaque "common-value-expression"

  lazy val valueExpression: P[Expression] = booleanValueExpression opaque "value-expression"
}

// SQL06 section 6.26
object NumericValueExpressionParser extends LoggingParser {
  import NumericParser._
  import ValueExpressionPrimaryParser._
  import WhitespaceApi._

  private val numericPrimary: P[Expression] =
    valueExpressionPrimary opaque "numeric-primary"

  private val base: P[Expression] =
    signedNumericLiteral | (sign.? ~ numericPrimary).map {
      case (Some(-1), n) => -n
      case (_, n)        => n
    } opaque "base"

  @ExtendedSQLSyntax
  private val factor: P[Expression] = base chain (P("^") attach Power) opaque "factor"

  private val term: P[Expression] = {
    @ExtendedSQLSyntax
    val remainder = P("%") attach Remainder
    val operator = P("*").attach(Multiply) | P("/").attach(Divide) | remainder

    factor chain operator opaque "term"
  }

  val numericValueExpression: P[Expression] = {
    val operator = P("+").attach(Plus) | P("-").attach(Minus)
    term chain operator opaque "numeric-value-expression"
  }
}

// SQL06 section 6.28
object StringValueExpressionParser extends LoggingParser {
  import NumericValueExpressionParser._
  import WhitespaceApi._

  private val characterPrimary: P[Expression] =
    numericValueExpression opaque "character-primary"

  private val concatenation: P[Expression] = {
    val operator = P("||") attach { concat(_: Expression, _: Expression) }
    characterPrimary chain operator opaque "concatenation"
  }

  lazy val stringValueExpression: P[Expression] =
    concatenation opaque "string-value-expression"
}

// SQL06 section 6.34
object BooleanValueExpressionParser extends LoggingParser {
  import KeywordParser._
  import LiteralParser._
  import PredicateParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  private val parenthesizedBooleanValueExpression: P[Expression] = (
    "(" ~ P(booleanValueExpression) ~ ")"
    opaque "parenthesized-boolean-value-expression"
  )

  val booleanPredicand: P[Expression] = (
    parenthesizedBooleanValueExpression
    | P(commonValueExpression)
    opaque "boolean-predicand"
  )

  private val booleanPrimary: P[Expression] =
    predicate | booleanPredicand opaque "boolean-primary"

  private val truthValue: P[Literal] =
    booleanLiteral opaque "truth-value"

  private val booleanTestSuffix: P[Expression => Expression] = {
    IS ~ NOT.!.? ~ truthValue map {
      case (Some(_), True) => Not
      case (Some(_), _)    => identity[Expression] _
      case (None, True)    => identity[Expression] _
      case (None, _)       => Not
    }
  }.? map {
    _ getOrElse identity[Expression] _
  } opaque "boolean-test-suffix"

  private val booleanTest: P[Expression] = (
    booleanPrimary ~ booleanTestSuffix
    map { case (bool, f) => f(bool) }
    opaque "boolean-test"
  )

  private val booleanFactor: P[Expression] = (
    (NOT ~ booleanTest map Not)
    | booleanTest
    opaque "boolean-factor"
  )

  private val booleanTerm: P[Expression] =
    booleanFactor chain (AND attach And) opaque "boolean-term"

  lazy val booleanValueExpression: P[Expression] =
    booleanTerm chain (OR attach Or) opaque "boolean-value-expression"
}

// SQL06 section 7.1
object RowValueConstructorParser extends LoggingParser {
  import BooleanValueExpressionParser._
  import ValueExpressionParser._
  import WhitespaceApi._

  val rowValueConstructorPredicand: P[Expression] = (
    commonValueExpression
    | booleanPredicand
    opaque "row-value-constructor-predicand"
  )
}

// SQL06 section 7.2
object RowValueExpressionParser extends LoggingParser {
  import RowValueConstructorParser._
  import ValueExpressionPrimaryParser._
  import WhitespaceApi._

  private val rowValueSpecialCase: P[Expression] =
    nonparenthesizedValueExpressionPrimary opaque "row-value-special-case"

  val rowValuePredicand: P[Expression] = (
    rowValueConstructorPredicand
    | rowValueSpecialCase
    opaque "row-value-predicand"
  )
}

// SQL06 section 8.1
object PredicateParser extends LoggingParser {
  import NullPredicateParser._
  import RowValueExpressionParser._
  import WhitespaceApi._

  private val compOp: P[(Expression, Expression) => Expression] = (
    P("=").attach(Eq)
    | P("<>").attach(NotEq)
    | P("<=").attach(LtEq)
    | P(">=").attach(GtEq)
    | P("<").attach(Lt)
    | P(">").attach(Gt)
    opaque "comp-op"
  )

  val comparisonPredicatePart2: P[Expression => Expression] = (
    compOp ~ rowValuePredicand
    map { case (comparator, rhs) => comparator(_: Expression, rhs) }
    opaque "comparison-predicate-part-2"
  )

  private val comparisonPredicate: P[Expression] = (
    rowValuePredicand ~ comparisonPredicatePart2
    map { case (lhs, makeComparison) => makeComparison(lhs) }
    opaque "comparison-predicate"
  )

  val predicate: P[Expression] = (
    comparisonPredicate
    | nullPredicate
    opaque "predicate"
  )
}

// SQL06 section 8.8
object NullPredicateParser extends LoggingParser {
  import KeywordParser._
  import RowValueExpressionParser._
  import WhitespaceApi._

  private val nullPredicatePart2: P[Expression => Expression] =
    IS ~ NOT.!.? ~ NULL map {
      case Some(_) => (_: Expression).isNotNull
      case _       => (_: Expression).isNull
    } opaque "null-predicate-part-2"

  val nullPredicate: P[Expression] = (
    rowValuePredicand ~ nullPredicatePart2
    map { case (value, predicate) => predicate(value) }
    opaque "null-predicate"
  )
}

// SQL06 section 8.20
object SearchConditionParser extends LoggingParser {
  import BooleanValueExpressionParser._

  val searchCondition: P[Expression] = P(booleanValueExpression)
}

// SQL06 section 10.9
object AggregateFunctionParser extends LoggingParser {
  import KeywordParser._
  import WhitespaceApi._

  val setQuantifier: P[LogicalPlan => LogicalPlan] = (
    ALL.attach(identity[LogicalPlan] _)
    | DISTINCT.attach { (_: LogicalPlan).distinct }
    opaque "setQuantifier"
  )
}
