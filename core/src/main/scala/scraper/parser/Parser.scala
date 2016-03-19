package scraper.parser

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.parsing.input.CharArrayReader.EofCh

import scraper.config.Keys.NullsLarger
import scraper.config.Settings
import scraper.exceptions.ParsingException
import scraper.expressions._
import scraper.expressions.AutoAlias.named
import scraper.expressions.Literal.{False, True}
import scraper.plans.logical._
import scraper.plans.logical.dsl._
import scraper.types._

trait Tokens extends StdTokens {
  case class FloatLit(chars: String) extends Token {
    override def toString: String = chars
  }
}

abstract class TokenParser[T] extends StdTokenParsers {
  override type Tokens = Lexical

  private val keywords = mutable.Set.empty[String]

  protected case class Keyword(name: String) {
    keywords += normalized

    def normalized: String = name.toLowerCase

    def asParser: Parser[String] = normalized
  }

  override lazy val lexical: Tokens = new Lexical(keywords.toSet)

  protected implicit def `Keyword->Parser[String]`(k: Keyword): Parser[String] = k.asParser

  def parse(input: String): T = synchronized {
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError   => throw new ParsingException(failureOrError.toString)
    }
  }

  protected def start: Parser[T]
}

class Parser(settings: Settings) extends TokenParser[LogicalPlan] {
  def parseAttribute(input: String): UnresolvedAttribute = synchronized {
    val start = attribute | star ^^ {
      case Star(qualifier) => UnresolvedAttribute("*", qualifier)
    }

    phrase(start)(new lexical.Scanner(input)) match {
      case Success(a, _)  => a
      case failureOrError => throw new ParsingException(failureOrError.toString)
    }
  }

  private val ALL = Keyword("ALL")
  private val AND = Keyword("AND")
  private val ARRAY = Keyword("ARRAY")
  private val AS = Keyword("AS")
  private val ASC = Keyword("ASC")
  private val BIGINT = Keyword("BIGINT")
  private val BOOLEAN = Keyword("BOOLEAN")
  private val BY = Keyword("BY")
  private val CAST = Keyword("CAST")
  private val DESC = Keyword("DESC")
  private val DISTINCT = Keyword("DISTINCT")
  private val DOUBLE = Keyword("DOUBLE")
  private val EXCEPT = Keyword("EXCEPT")
  private val FALSE = Keyword("FALSE")
  private val FIRST = Keyword("FIRST")
  private val FLOAT = Keyword("FLOAT")
  private val FROM = Keyword("FROM")
  private val FULL = Keyword("FULL")
  private val GROUP = Keyword("GROUP")
  private val HAVING = Keyword("HAVING")
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
  private val SELECT = Keyword("SELECT")
  private val SEMI = Keyword("SEMI")
  private val SMALLINT = Keyword("SMALLINT")
  private val STRING = Keyword("STRING")
  private val STRUCT = Keyword("STRUCT")
  private val TINYINT = Keyword("TINYINT")
  private val TRUE = Keyword("TRUE")
  private val UNION = Keyword("UNION")
  private val WHERE = Keyword("WHERE")

  override protected def start: Parser[LogicalPlan] =
    select * (
      UNION ~ ALL ^^^ Union
      | INTERSECT ^^^ Intersect
      | EXCEPT ^^^ Except
    )

  private def select: Parser[LogicalPlan] = (
    SELECT ~> DISTINCT.? ~ projectList
    ~ (FROM ~> relations).?
    ~ (WHERE ~> predicate).?
    ~ (GROUP ~ BY ~> rep1sep(expression, ",")).?
    ~ (HAVING ~> predicate).?
    ~ (ORDER ~ BY ~> sortOrders).?
    ~ (LIMIT ~> expression).? ^^ {
      case d ~ ps ~ rs ~ f ~ gs ~ h ~ o ~ n =>
        val base = rs getOrElse SingleRowRelation
        val withFilter = f map base.filter getOrElse base
        val withProject = gs map (withFilter.groupBy(_).agg(ps)) getOrElse (withFilter select ps)
        val withDistinct = d map (_ => withProject.distinct) getOrElse withProject
        val withHaving = h map withDistinct.filter getOrElse withProject
        val withOrder = o map withHaving.orderBy getOrElse withHaving
        val withLimit = n map withOrder.limit getOrElse withOrder
        withLimit
    }
  )

  private def sortOrders: Parser[Seq[SortOrder]] =
    rep1sep(sortOrder, ",")

  private def sortOrder: Parser[SortOrder] =
    expression ~ direction.? ~ nullsFirst.? ^^ {
      case e ~ d ~ n =>
        val direction = d getOrElse Ascending
        val nullsLarger = n map (direction -> _) map {
          case (Ascending, nullsFirst @ true)   => false
          case (Ascending, nullsFirst @ false)  => true
          case (Descending, nullsFirst @ true)  => true
          case (Descending, nullsFirst @ false) => false
        } getOrElse settings(NullsLarger)

        SortOrder(e, direction, nullsLarger)
    }

  private def nullsFirst: Parser[Boolean] =
    NULLS ~> (FIRST ^^^ true | LAST ^^^ false)

  private def direction: Parser[SortDirection] =
    ASC ^^^ Ascending | DESC ^^^ Descending

  private def relations: Parser[LogicalPlan] =
    relation * ("," ^^^ (Join(_: LogicalPlan, _: LogicalPlan, Inner, None)))

  private def relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor

  private def joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ ((joinType.? <~ JOIN) ~ relationFactor ~ joinCondition.?).+ ^^ {
      case r ~ joins =>
        (joins foldLeft r) {
          case (lhs, t ~ rhs ~ c) =>
            Join(lhs, rhs, t getOrElse Inner, c)
        }
    }

  private def joinType: Parser[JoinType] = (
    INNER ^^^ Inner
    | LEFT ~ SEMI ^^^ LeftSemi
    | LEFT ~ OUTER.? ^^^ LeftOuter
    | RIGHT ~ OUTER.? ^^^ RightOuter
    | FULL ~ OUTER.? ^^^ FullOuter
  )

  private def joinCondition: Parser[Expression] =
    ON ~> predicate

  private def relationFactor: Parser[LogicalPlan] = (
    ident ~ (AS.? ~> ident.?) ^^ {
      case t ~ Some(a) => Subquery(UnresolvedRelation(t), a)
      case t ~ None    => UnresolvedRelation(t)
    }
    | ("(" ~> select <~ ")") ~ (AS.? ~> ident) ^^ {
      case s ~ a => Subquery(s, a)
    }
  )

  private def projectList: Parser[Seq[NamedExpression]] =
    rep1sep(projection | star, ",") ^^ {
      case ps => ps map named
    }

  private def projection: Parser[Expression] =
    expression ~ (AS.? ~> ident).? ^^ {
      case e ~ Some(a) => e as a
      case e ~ _       => e
    }

  private def star: Parser[Star] =
    (ident <~ ".").? <~ "*" ^^ Star

  private def expression: Parser[Expression] =
    arithmetic ||| predicate

  private def arithmetic: Parser[Expression] =
    termExpression

  private def predicate: Parser[Expression] =
    orExpression ||| "(" ~> predicate <~ ")"

  private def orExpression: Parser[Expression] =
    andExpression * (OR ^^^ Or)

  private def andExpression: Parser[Expression] =
    (notExpression ||| comparison ||| booleanLiteral) * (AND ^^^ And)

  private def notExpression: Parser[Expression] =
    NOT ~> predicate ^^ Not

  private def comparison: Parser[Expression] = (
    termExpression ~ ("=" ~> termExpression) ^^ { case e1 ~ e2 => Eq(e1, e2) }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => NotEq(e1, e2) }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => NotEq(e1, e2) }
    | termExpression ~ (">" ~> termExpression) ^^ { case e1 ~ e2 => Gt(e1, e2) }
    | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GtEq(e1, e2) }
    | termExpression ~ ("<" ~> termExpression) ^^ { case e1 ~ e2 => Lt(e1, e2) }
    | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LtEq(e1, e2) }
    | termExpression <~ IS ~ NULL ^^ IsNull
    | termExpression <~ IS ~ NOT ~ NULL ^^ IsNotNull
  )

  private def termExpression: Parser[Expression] = (
    "-" ~> productExpression ^^ Negate
    | productExpression * (
      "+" ^^^ Plus
      | "-" ^^^ Minus
    )
  )

  private def productExpression: Parser[Expression] =
    baseExpression * (
      "*" ^^^ Multiply
      | "/" ^^^ Divide
    )

  private def baseExpression: Parser[Expression] =
    primary

  private def primary: Parser[Expression] = (
    literal
    | function
    | attribute
    | cast
    | "(" ~> expression <~ ")"
  )

  private def attribute: Parser[UnresolvedAttribute] =
    (ident <~ ".").? ~ ident ^^ {
      case qualifier ~ name =>
        UnresolvedAttribute(name, qualifier)
    }

  private def literal: Parser[Literal] = (
    numericLiteral
    | stringLiteral
    | booleanLiteral
  )

  private def stringLiteral: Parser[Literal] =
    stringLit ^^ (Literal(_, StringType))

  private def booleanLiteral: Parser[Literal] = (
    TRUE ^^^ True
    | FALSE ^^^ False
  )

  private def numericLiteral: Parser[Literal] =
    integral ^^ {
      case i => Literal(narrowestIntegralValueOf(i))
    }

  private def integral: Parser[String] =
    sign.? ~ numericLit ^^ {
      case s ~ n => s.mkString + n
    }

  private def sign: Parser[String] =
    "+" | "-"

  private def narrowestIntegralValueOf(numeric: String): Any = {
    val bigInt = BigInt(numeric)

    bigInt match {
      case i if i.isValidInt  => i.intValue()
      case i if i.isValidLong => i.longValue()
    }
  }

  private def cast: Parser[Cast] =
    CAST ~ "(" ~> expression ~ (AS ~> dataType) <~ ")" ^^ { case e ~ t => Cast(e, t) }

  private def function: Parser[Expression] =
    ident ~ ("(" ~> functionArgs <~ ")") ^^ {
      case functionName ~ args => UnresolvedFunction(functionName, args)
    }

  private def functionArgs: Parser[Seq[Expression]] = (
    repsep(expression, ",")
    | star ^^ (_ :: Nil)
  )

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
    ident ~ (":" ~> dataType) ^^ {
      case i ~ t => StructField(i, t.?)
    }
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
    digit.* ~ identChar ~ (identChar | digit).* ^^ {
      case ds ~ c ~ cs =>
        processIdent((ds ::: (c :: cs)).mkString)
    }

    // Back-quoted identifiers
    | '`' ~> (chrExcept('`', '\n', EofCh) | ('`' ~ '`') ^^^ '`').* <~ '`' ^^ {
      case cs => Identifier(cs.mkString)
    }

    // Integral and fractional numeric literals
    | digit.+ ~ ('.' ~> digit.*).? ^^ {
      case i ~ None    => NumericLit(i.mkString)
      case i ~ Some(f) => FloatLit(s"${i.mkString}.${f.mkString}")
    }

    // Single-quoted string literals
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^ {
      case cs => StringLit(cs.mkString)
    }

    // Double-quoted string literals
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^ {
      case cs => StringLit(cs.mkString)
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
    if (reserved contains lowerCased) Keyword(lowerCased) else Identifier(name)
  }
}
