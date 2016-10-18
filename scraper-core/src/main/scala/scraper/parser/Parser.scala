package scraper.parser

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.parsing.input.CharArrayReader.EofCh

import scraper._
import scraper.Name
import scraper.Name.{caseInsensitive, caseSensitive}
import scraper.exceptions.ParsingException
import scraper.expressions._
import scraper.expressions.AutoAlias.named
import scraper.expressions.Literal.{False, True}
import scraper.plans.logical._
import scraper.types._

trait Tokens extends StdTokens {
  case class IntegralLit(chars: String) extends Token {
    override def toString: String = chars
  }

  case class FractionalLit(chars: String) extends Token {
    override def toString: String = chars
  }

  case class UnquotedIdentifier(chars: String) extends Token {
    override def toString: String = chars
  }

  case class QuotedIdentifier(chars: String) extends Token {
    override def toString: String = chars
  }
}

abstract class TokenParser[T] extends StdTokenParsers {
  override type Tokens = Lexical

  import lexical.{IntegralLit, FractionalLit, QuotedIdentifier, UnquotedIdentifier}

  override lazy val lexical: Tokens = new Lexical(keywords.toSet)

  def parse(input: String): T = synchronized {
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError   => throw new ParsingException(failureOrError.toString)
    }
  }

  def integralLit: Parser[String] =
    elem("integral", _.isInstanceOf[IntegralLit]) ^^ (_.chars)

  def fractionalLit: Parser[String] =
    elem("fractional", _.isInstanceOf[FractionalLit]) ^^ (_.chars)

  def quotedIdent: Parser[Name] =
    elem("quoted identifier", _.isInstanceOf[QuotedIdentifier]) ^^ {
      token => caseSensitive(token.chars)
    }

  def unquotedIdent: Parser[Name] =
    elem("unquoted identifier", _.isInstanceOf[UnquotedIdentifier]) ^^ {
      token => caseInsensitive(token.chars)
    }

  protected case class Keyword(name: String) {
    keywords += normalized

    def normalized: String = name.toLowerCase

    def asParser: Parser[String] = normalized
  }

  protected def start: Parser[T]

  protected implicit def `Keyword->Parser[String]`(k: Keyword): Parser[String] = k.asParser

  private val keywords = mutable.Set.empty[String]
}

class Parser extends TokenParser[LogicalPlan] {
  def parseAttribute(input: String): NamedExpression =
    phrase(attribute | star)(new lexical.Scanner(input)) match {
      case Success(a, _)  => a
      case failureOrError => throw new ParsingException(failureOrError.toString)
    }

  override protected def start: Parser[LogicalPlan] =
    query

  private val ALL = Keyword("ALL")
  private val AND = Keyword("AND")
  private val ARRAY = Keyword("ARRAY")
  private val AS = Keyword("AS")
  private val ASC = Keyword("ASC")
  private val BIGINT = Keyword("BIGINT")
  private val BOOLEAN = Keyword("BOOLEAN")
  private val BY = Keyword("BY")
  private val CASE = Keyword("CASE")
  private val CAST = Keyword("CAST")
  private val DESC = Keyword("DESC")
  private val DISTINCT = Keyword("DISTINCT")
  private val DOUBLE = Keyword("DOUBLE")
  private val ELSE = Keyword("ELSE")
  private val END = Keyword("END")
  private val EXCEPT = Keyword("EXCEPT")
  private val FALSE = Keyword("FALSE")
  private val FIRST = Keyword("FIRST")
  private val FLOAT = Keyword("FLOAT")
  private val FROM = Keyword("FROM")
  private val FULL = Keyword("FULL")
  private val GROUP = Keyword("GROUP")
  private val HAVING = Keyword("HAVING")
  private val IF = Keyword("IF")
  private val IN = Keyword("IN")
  private val INNER = Keyword("INNER")
  private val INT = Keyword("INT")
  private val INTERSECT = Keyword("INTERSECT")
  private val IS = Keyword("IS")
  private val JOIN = Keyword("JOIN")
  private val LAST = Keyword("LAST")
  private val LEFT = Keyword("LEFT")
  private val LIMIT = Keyword("LIMIT")
  private val MAP = Keyword("MAP")
  private val NOT = Keyword("NOT")
  private val NULL = Keyword("NULL")
  private val NULLS = Keyword("NULLS")
  private val ON = Keyword("ON")
  private val OR = Keyword("OR")
  private val ORDER = Keyword("ORDER")
  private val OUTER = Keyword("OUTER")
  private val RIGHT = Keyword("RIGHT")
  private val RLIKE = Keyword("RLIKE")
  private val SELECT = Keyword("SELECT")
  private val SEMI = Keyword("SEMI")
  private val SMALLINT = Keyword("SMALLINT")
  private val STRING = Keyword("STRING")
  private val STRUCT = Keyword("STRUCT")
  private val THEN = Keyword("THEN")
  private val TINYINT = Keyword("TINYINT")
  private val TRUE = Keyword("TRUE")
  private val UNION = Keyword("UNION")
  private val WHEN = Keyword("WHEN")
  private val WHERE = Keyword("WHERE")
  private val WITH = Keyword("WITH")

  private def query: Parser[LogicalPlan] =
    cteDeclaration.? ~ queryNoWith ^^ {
      case c ~ q => (c fold q) { _ apply q }
    }

  private def cteDeclaration: Parser[LogicalPlan => LogicalPlan] =
    WITH ~> rep1sep(namedQuery, ",") ^^ { _ reduce (_ andThen _) }

  private def namedQuery: Parser[LogicalPlan => LogicalPlan] =
    identifier ~ (AS.? ~ "(" ~> queryNoWith <~ ")") ^^ {
      case n ~ q => With(_: LogicalPlan, n, q)
    }

  def identifier: Parser[Name] = quotedIdent | unquotedIdent

  private def queryNoWith: Parser[LogicalPlan] =
    queryTerm ~ orderBy.? ~ limit.? ^^ {
      case q ~ o ~ n => (o ++ n reduceOption { _ andThen _ } fold q) { _ apply q }
    }

  private def queryTerm: Parser[LogicalPlan] =
    queryPrimary * (
      INTERSECT ^^^ Intersect
      | (UNION <~ ALL) ^^^ Union
      | EXCEPT ^^^ Except
    )

  private def queryPrimary: Parser[LogicalPlan] = (
    querySpecification
    | "(" ~> queryNoWith <~ ")"
  )

  private def querySpecification: Parser[LogicalPlan] = (
    SELECT ~> DISTINCT.? ~ projectList
    ~ (FROM ~> relations).?
    ~ (WHERE ~> predicate).?
    ~ (GROUP ~ BY ~> rep1sep(expression, ",")).?
    ~ (HAVING ~> predicate).? ^^ {
      case d ~ ps ~ rs ~ f ~ gs ~ h =>
        val filtered = rs getOrElse SingleRowRelation filterOption f.toSeq
        val projected = gs map (filtered groupBy _ agg ps) getOrElse (filtered select ps)
        (d fold projected) { _ => projected.distinct } filterOption h.toSeq
    }
  )

  private def projectList: Parser[Seq[NamedExpression]] =
    rep1sep(projection | star, ",")

  private def projection: Parser[NamedExpression] =
    expression ~ (AS.? ~> identifier).? ^^ {
      case e ~ Some(a) => e as a
      case e ~ _       => named(e)
    }

  private def expression: Parser[Expression] =
    predicate | termExpression

  private def termExpression: Parser[Expression] = (
    productExpression * ("+" ^^^ Plus | "-" ^^^ Minus)
    | "-" ~> productExpression ^^ Negate
  )

  private def productExpression: Parser[Expression] =
    powerExpression * (
      "*" ^^^ Multiply
      | "/" ^^^ Divide
      | "%" ^^^ Remainder
    )

  private def powerExpression: Parser[Expression] =
    primaryExpression * ("^" ^^^ Power)

  private def primaryExpression: Parser[Expression] = (
    literal
    | functionCall
    | attribute
    | cast
    | caseWhen
    | condition
    | "(" ~> expression <~ ")"
  )

  private def literal: Parser[Literal] = (
    numericLiteral
    | stringLiteral
    | booleanLiteral
  )

  private def numericLiteral: Parser[Literal] =
    integral ^^ {
      i => Literal(narrowestIntegralValueOf(i))
    }

  private def integral: Parser[String] =
    sign.? ~ integralLit ^^ {
      case s ~ n => s.mkString + n
    }

  private def narrowestIntegralValueOf(numeric: String): Any = BigInt(numeric) match {
    case i if i.isValidInt  => i.intValue()
    case i if i.isValidLong => i.longValue()
  }

  private def sign: Parser[String] =
    "+" | "-"

  private def stringLiteral: Parser[Literal] =
    stringLit.+ ^^ (_.mkString: Literal)

  private def booleanLiteral: Parser[Literal] =
    TRUE ^^^ True | FALSE ^^^ False

  private def functionCall: Parser[Expression] =
    (identifier | specialFunction) ~ ("(" ~> functionArgs <~ ")") ^^ { case n ~ f => f apply n }

  private def specialFunction: Parser[Name] =
    (ARRAY | STRUCT | MAP) ^^ Name.caseInsensitive

  private def functionArgs: Parser[Name => Expression] = (
    star ^^ { s => function(_: Name, s) }
    | DISTINCT.? ~ repsep(expression, ",") ^^ {
      case Some(_) ~ es => distinctFunction(_: Name, es: _*)
      case None ~ es    => function(_: Name, es: _*)
    }
  )

  private def attribute: Parser[UnresolvedAttribute] =
    (identifier <~ ".").? ~ identifier ^^ {
      case Some(q) ~ n => n of q
      case None ~ n    => n
    }

  private def cast: Parser[Cast] =
    CAST ~ "(" ~> expression ~ (AS ~> dataType) <~ ")" ^^ {
      case e ~ t => Cast(e, t)
    }

  private def caseWhen: Parser[CaseWhen] = (
    CASE ~> expression.?
    ~ (WHEN ~> expression ~ (THEN ~> expression)).+
    ~ (ELSE ~> expression).?
    <~ END ^^ {
      case k ~ bs ~ a =>
        val (conditions, values) = bs.map { case c ~ v => c -> v }.unzip
        k map (CaseKeyWhen(_, conditions, values, a)) getOrElse CaseWhen(conditions, values, a)
    }
  )

  private def condition: Parser[If] =
    IF ~ "(" ~> predicate ~ ("," ~> expression) ~ ("," ~> expression) <~ ")" ^^ {
      case k ~ c ~ a => If(k, c, a)
    }

  private def predicate: Parser[Expression] =
    negation | disjunction

  private def negation: Parser[Expression] =
    NOT ~> predicate ^^ Not

  private def disjunction: Parser[Expression] =
    conjunction * (OR ^^^ Or)

  private def conjunction: Parser[Expression] =
    (comparison | termExpression) * (AND ^^^ And)

  private def comparison: Parser[Expression] = (
    termExpression ~ ("=" ~> termExpression) ^^ { case e1 ~ e2 => e1 === e2 }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => e1 =/= e2 }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => e1 =/= e2 }
    | termExpression ~ (">" ~> termExpression) ^^ { case e1 ~ e2 => e1 > e2 }
    | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => e1 >= e2 }
    | termExpression ~ ("<" ~> termExpression) ^^ { case e1 ~ e2 => e1 < e2 }
    | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => e1 <= e2 }
    | termExpression <~ IS ~ NULL ^^ IsNull
    | termExpression <~ IS ~ NOT ~ NULL ^^ IsNotNull
    | termExpression ~ (IN ~> termExpressionList) ^^ { case e ~ es => e in es }
    | termExpression ~ (RLIKE ~> termExpression) ^^ { case e1 ~ e2 => e1 rlike e2 }
  )

  private def termExpressionList: Parser[Seq[Expression]] =
    "(" ~> rep1sep(termExpression, ",") <~ ")"

  private def dataType: Parser[DataType] = (
    primitiveType
    | arrayType
    | mapType
    | structType
  )

  private def primitiveType: Parser[PrimitiveType] = (
    TINYINT ^^^ ByteType
    | SMALLINT ^^^ ShortType
    | INT ^^^ IntType
    | BIGINT ^^^ LongType
    | FLOAT ^^^ FloatType
    | DOUBLE ^^^ DoubleType
    | BOOLEAN ^^^ BooleanType
    | STRING ^^^ StringType
  )

  private def arrayType: Parser[ArrayType] =
    ARRAY ~ "<" ~> dataType <~ ">" ^^ (t => ArrayType(t.?))

  private def mapType: Parser[MapType] =
    MAP ~ "<" ~> dataType ~ ("," ~> dataType) <~ ">" ^^ {
      case kt ~ vt => MapType(kt, vt.?)
    }

  private def structType: Parser[StructType] =
    STRUCT ~ "<" ~> rep1sep(structField, ",") <~ ">" ^^ (StructType(_))

  private def structField: Parser[StructField] =
    identifier ~ (":" ~> dataType) ^^ {
      case i ~ t => i -> t.?
    }

  private def star: Parser[Star] =
    (identifier <~ ".").? <~ "*" ^^ Star

  private def relations: Parser[LogicalPlan] =
    relation * ("," ^^^ (Join(_: LogicalPlan, _: LogicalPlan, Inner, None)))

  private def relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor

  private def joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ joinRhs.+ ^^ { case r ~ js => js reduce (_ andThen _) apply r }

  private def joinRhs: Parser[LogicalPlan => Join] =
    (joinType.? <~ JOIN) ~ relationFactor ~ joinCondition.? ^^ {
      case t ~ f ~ c => _ join (f, t getOrElse Inner) onOption c.toSeq
    }

  private def relationFactor: Parser[LogicalPlan] = (
    identifier ~ (AS.? ~> identifier.?) ^^ {
      case t ~ Some(a) => table(t) subquery a
      case t ~ None    => table(t)
    }
    | ("(" ~> queryNoWith <~ ")") ~ (AS.? ~> identifier) ^^ {
      case s ~ a => s subquery a
    }
  )

  private def joinType: Parser[JoinType] = (
    INNER ^^^ Inner
    | LEFT ~ SEMI ^^^ LeftSemi
    | LEFT ~ OUTER.? ^^^ LeftOuter
    | RIGHT ~ OUTER.? ^^^ RightOuter
    | FULL ~ OUTER.? ^^^ FullOuter
  )

  private def joinCondition: Parser[Expression] =
    ON ~> predicate

  private def orderBy: Parser[LogicalPlan => LogicalPlan] =
    ORDER ~ BY ~> sortOrders ^^ { os => _ orderByOption os }

  private def limit: Parser[LogicalPlan => LogicalPlan] =
    LIMIT ~> expression ^^ { n => _ limit n }

  private def sortOrders: Parser[Seq[SortOrder]] =
    rep1sep(sortOrder, ",")

  private def sortOrder: Parser[SortOrder] =
    expression ~ direction.? ~ nullsFirst.? ^^ {
      case e ~ d ~ n =>
        val direction = d getOrElse Ascending
        val nullsLarger = n forall ((direction == Ascending) ^ _)
        SortOrder(e, direction, nullsLarger)
    }

  private def direction: Parser[SortDirection] =
    ASC ^^^ Ascending | DESC ^^^ Descending

  private def nullsFirst: Parser[Boolean] =
    NULLS ~> (FIRST ^^^ true | LAST ^^^ false)
}

class Lexical(keywords: Set[String]) extends StdLexical with Tokens {
  delimiters ++= Set(
    // Arithmetic operators
    "*", "+", "-", "/", "%",

    // Comparison operators
    "<", ">", "<=", ">=", "=", "<>", "!=", "<=>",

    // Bitwise operators
    "&", "|", "^", "~",

    // Other punctuations
    "(", ")", "[", "]", ",", ";", ":", "."
  )

  reserved ++= keywords

  override def token: Parser[Token] = (
    // Identifiers and keywords
    identBegin ~ identBody.* ^^ {
      case c ~ cs => processIdent((c :: cs).mkString)
    }

    // Back-quoted identifiers
    | '`' ~> (chrExcept('`', '\n', EofCh) | ('`' ~ '`') ^^^ '`').* <~ '`' ^^ {
      cs => QuotedIdentifier(cs.mkString)
    }

    // Integral and fractional numeric literals
    | digit.+ ~ ('.' ~> digit.*).? ^^ {
      case i ~ Some(f) => FractionalLit(s"${i.mkString}.${f.mkString}")
      case i ~ None    => IntegralLit(i.mkString)
    }

    // Single-quoted string literals
    // TODO Handles escaped characters
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^ {
      cs => StringLit(cs.mkString)
    }

    // Double-quoted string literals
    // TODO Handles escaped characters
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^ {
      cs => StringLit(cs.mkString)
    }

    // End of input
    | EofCh ^^^ EOF

    // Unclosed strings
    | '\'' ~> failure("unclosed string literal")
    | '"' ~> failure("unclosed string literal")

    // Delimiters
    | delim

    // Other illegal inputs
    | failure("illegal character")
  )

  override def whitespace: Parser[Any] = (
    // Normal whitespace characters
    whitespaceChar

    // Multi-line comment
    | '/' ~ '*' ~ comment

    // Single-line comment
    | '/' ~ '/' ~ chrExcept('\n', EofCh).*
    | '#' ~ chrExcept('\n', EofCh).*
    | '-' ~ '-' ~ chrExcept('\n', EofCh).*

    // Illegal inputs
    | '/' ~ '*' ~ failure("unclosed multi-line comment")
  ).*

  override protected def processIdent(name: String) = {
    val lowerCased = name.toLowerCase
    if (reserved contains lowerCased) Keyword(lowerCased) else UnquotedIdentifier(name)
  }

  private def identBegin: Parser[Char] = letter | '_'

  private def identBody: Parser[Char] = letter | digit | '_'
}
