package scraper.parser

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.parsing.input.CharArrayReader.EofCh

import scraper.ParsingError
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
    keywords += name
  }

  override lazy val lexical: Tokens = new Lexical(keywords.toSet)

  protected implicit def keywordAsParser(k: Keyword): Parser[String] = k.name

  def parse(input: String): T = synchronized {
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError   => throw ParsingError(failureOrError.toString)
    }
  }

  protected def start: Parser[T]
}

class Parser extends TokenParser[LogicalPlan] {
  private val SELECT = Keyword("SELECT")
  private val AS = Keyword("AS")
  private val FROM = Keyword("FROM")
  private val WHERE = Keyword("WHERE")

  private val AND = Keyword("AND")
  private val OR = Keyword("OR")
  private val NOT = Keyword("NOT")

  private val TRUE = Keyword("TRUE")
  private val FALSE = Keyword("FALSE")

  override protected def start: Parser[LogicalPlan] =
    select

  private def select: Parser[LogicalPlan] = (
    SELECT ~> projections.?
    ~ (FROM ~> relations).?
    ~ (WHERE ~> predicate).? ^^ {
      case ps ~ r ~ f =>
        val base = r.getOrElse(SingleRowRelation)
        val withFilter = f.map(Filter(_, base)).getOrElse(base)
        val withProjections = ps.map(Project(_, withFilter)).getOrElse(withFilter)
        withProjections
    }
  )

  private def relations: Parser[LogicalPlan] =
    ident ^^ UnresolvedRelation

  private def projections: Parser[Seq[NamedExpression]] =
    rep1sep(projection, ",") ^^ {
      case ps =>
        ps.zipWithIndex.map {
          case (e: NamedExpression, i) => e
          case (e: Expression, i)      => Alias(s"col$i", e)
        }
    }

  private def projection: Parser[Expression] =
    expression ~ (AS.? ~> ident).? ^^ {
      case e ~ Some(a) => Alias(a, e)
      case e ~ _       => e
    }

  private def expression: Parser[Expression] =
    termExpression | predicate

  private def predicate: Parser[Predicate] =
    orExpression

  private def orExpression: Parser[Predicate] =
    andExpression * (OR ^^^ Or)

  private def andExpression: Parser[Predicate] =
    notExpression * (AND ^^^ And)

  private def notExpression: Parser[Predicate] = (
    NOT ~> comparison ^^ Not
    | comparison
    | logicalLiteral
  )

  private def comparison: Parser[Predicate] = (
    termExpression ~ ("=" ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
  )

  private def termExpression: Parser[Expression] =
    productExpression * (
      "+" ^^^ Add
      | "-" ^^^ Minus
    )

  private def productExpression: Parser[Expression] =
    baseExpression * (
      "*" ^^^ Multiply
    )

  private def baseExpression: Parser[Expression] =
    primary

  private def primary: Parser[Expression] = (
    literal
    | "(" ~> expression <~ ")"
  )

  private def literal: Parser[Literal] = (
    numericLiteral
    | stringLiteral
  )

  private def stringLiteral: Parser[Literal] =
    stringLit ^^ (Literal(_, StringType))

  private def logicalLiteral: Parser[LogicalLiteral] = (
    TRUE ^^^ Literal.True
    | FALSE ^^^ Literal.False
  )

  private def sign: Parser[String] =
    "+" | "-"

  private def numericLiteral: Parser[Literal] =
    integral ^^ {
      case i => Literal(narrowestIntegralValueOf(i))
    }

  private def integral: Parser[String] =
    sign.? ~ numericLit ^^ {
      case s ~ n => s.mkString + n
    }

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
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",",
    ";", "%", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
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
  )
}
