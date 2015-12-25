package scraper.parser

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.parsing.input.CharArrayReader.EofCh

import scraper.exceptions.ParsingException
import scraper.expressions.Literal.{False, True}
import scraper.expressions._
import scraper.plans.logical._
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

class Parser extends TokenParser[LogicalPlan] {
  private val SELECT = Keyword("SELECT")
  private val AS = Keyword("AS")
  private val FROM = Keyword("FROM")
  private val WHERE = Keyword("WHERE")
  private val LIMIT = Keyword("LIMIT")

  private val AND = Keyword("AND")
  private val OR = Keyword("OR")
  private val NOT = Keyword("NOT")

  private val TRUE = Keyword("TRUE")
  private val FALSE = Keyword("FALSE")

  private val JOIN = Keyword("JOIN")
  private val INNER = Keyword("INNER")
  private val OUTER = Keyword("OUTER")
  private val LEFT = Keyword("LEFT")
  private val RIGHT = Keyword("RIGHT")
  private val SEMI = Keyword("SEMI")
  private val FULL = Keyword("FULL")
  private val ON = Keyword("ON")

  private val GROUP = Keyword("GROUP")
  private val BY = Keyword("BY")
  private val HAVING = Keyword("HAVING")

  private val IS = Keyword("IS")
  private val NULL = Keyword("NULL")

  override protected def start: Parser[LogicalPlan] =
    select

  private def select: Parser[LogicalPlan] = (
    SELECT ~> projections
    ~ (FROM ~> relations).?
    ~ (WHERE ~> predicate).?
    ~ (GROUP ~> BY ~> rep1sep(expression, ",")).?
    ~ (HAVING ~> predicate).?
    ~ (LIMIT ~> expression).? ^^ {
      case ps ~ rs ~ f ~ gs ~ h ~ n =>
        val base = rs getOrElse SingleRowRelation
        val withFilter = f map (Filter(base, _)) getOrElse base
        val withProject = gs map (Aggregate(withFilter, _, ps)) getOrElse Project(withFilter, ps)
        val withHaving = h map (Filter(withProject, _)) getOrElse withProject
        val withLimit = n map (Limit(withHaving, _)) getOrElse withHaving
        withLimit
    }
  )

  private def relations: Parser[LogicalPlan] =
    relation * ("," ^^^ (Join(_, _, Inner, None)))

  private def relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor

  private def joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ (joinType.? ~ (JOIN ~> relationFactor) ~ joinCondition.?).+ ^^ {
      case r ~ joins =>
        joins.foldLeft(r) {
          case (lhs, t ~ rhs ~ c) => Join(lhs, rhs, t getOrElse Inner, c)
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

  private def projections: Parser[Seq[NamedExpression]] =
    rep1sep(projection | star, ",") ^^ {
      case ps =>
        ps.zipWithIndex.map {
          case (e: NamedExpression, _) => e
          case (e: Expression, i)      => Alias(s"col$i", e)
        }
    }

  private def projection: Parser[Expression] =
    expression ~ (AS.? ~> ident).? ^^ {
      case e ~ Some(a) => Alias(a, e)
      case e ~ _       => e
    }

  private def star: Parser[Star.type] = "*" ^^^ Star

  private def expression: Parser[Expression] =
    arithmetic ||| predicate

  private def arithmetic: Parser[Expression] =
    termExpression

  private def predicate: Parser[Expression] =
    orExpression

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
      "+" ^^^ Add
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
    | ident ^^ UnresolvedAttribute
    | "(" ~> expression <~ ")"
  )

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
    | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^ {
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
}
