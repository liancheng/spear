package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.{Success, Try}

import scraper.{Name, Row}
import scraper.Name.caseInsensitive
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.functions._
import scraper.types._

case class ExpressionID(id: Long)

trait NamedExpression extends Expression {
  def name: Name

  def expressionID: ExpressionID

  def toAttribute: Attribute

  def attr: Attribute = toAttribute
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionID: ExpressionID = throw new ExpressionUnresolvedException(this)
}

object NamedExpression {
  private val currentID = new AtomicLong(0L)

  def newExpressionID(): ExpressionID = ExpressionID(currentID.getAndIncrement())

  def unapply(e: NamedExpression): Option[(Name, DataType)] = Some((e.name, e.dataType))

  /**
   * Auxiliary class only used for removing back-ticks and double-quotes from auto-generated column
   * names.  For example, for expression `id + 1`, we'd like to generate column name `(id + 1)`
   * instead of `(&#96;id&#96; + 1)`.
   */
  case class UnquotedName(named: NamedExpression)
    extends LeafExpression with UnevaluableExpression {

    override lazy val isResolved: Boolean = named.isResolved

    override lazy val dataType: DataType = named.dataType

    override lazy val isNullable: Boolean = named.isNullable

    override def sql: Try[String] = Try(named.name.toString)
  }

  object UnquotedName {
    def apply(stringLiteral: String): UnquotedName =
      UnquotedName(lit(stringLiteral) as Name.caseInsensitive(stringLiteral))
  }
}

case class Star(qualifier: Option[Name]) extends LeafExpression with UnresolvedNamedExpression {
  override def name: Name = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template: String =
    (qualifier map (_.toString)).toSeq :+ "*" mkString "."
}

case class Alias(
  child: Expression,
  name: Name,
  override val expressionID: ExpressionID
) extends NamedExpression with UnaryExpression {
  override lazy val isFoldable: Boolean = false

  override protected lazy val strictDataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override lazy val toAttribute: Attribute = if (child.isResolved) {
    AttributeRef(name, child.dataType, child.isNullable, expressionID)
  } else {
    UnresolvedAttribute(name)
  }

  override def debugString: String =
    s"(${child.debugString} AS ${name.toString}#${expressionID.id})"

  override def sql: Try[String] =
    child.sql map (childSQL => s"$childSQL AS ${name.toString}")

  def withID(id: ExpressionID): Alias = copy(expressionID = id)
}

object Alias {
  def collectAliases(expressions: Seq[NamedExpression]): Map[Attribute, Expression] =
    expressions
      .collect { case a: Alias => a }
      .map(a => a.attr -> a.child)
      .toMap

  def betaReduction(expression: Expression, aliases: Map[Attribute, Expression]): Expression =
    expression transformUp {
      case a: AttributeRef => aliases.getOrElse(a, a)
    }

  def betaReduction(expression: Expression, targets: Seq[NamedExpression]): Expression =
    betaReduction(expression, collectAliases(targets))
}

/**
 * Wherever a [[NamedExpression]] is required but an arbitrary [[Expression]] is given, we use
 * [[AutoAlias]] to wrap the given expression and defer decision of the final alias name until
 * analysis time.  The final alias name is usually the SQL representation of the finally resolved
 * expression.  If the resolved expression doesn't have a SQL representation (e.g., Scala UDF),
 * a default name `?column?` will be used.
 */
case class AutoAlias private (child: Expression)
  extends NamedExpression
  with UnaryExpression
  with UnresolvedNamedExpression
  with UnevaluableExpression {

  override def name: Name = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override def debugString: String = s"${child.debugString} AS ?alias?"
}

object AutoAlias {
  val AnonymousColumnName: String = "?column?"

  def named(child: Expression): NamedExpression = child match {
    case e: NamedExpression => e
    case _                  => AutoAlias(child)
  }
}

trait Attribute extends NamedExpression with LeafExpression {
  override lazy val isFoldable: Boolean = false

  override lazy val references: Seq[Attribute] = Seq(this)

  override def toAttribute: Attribute = this

  def withID(id: ExpressionID): Attribute

  def withNullability(nullability: Boolean): Attribute = this

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: Name, qualifier: Option[Name] = None)
  extends Attribute with UnresolvedNamedExpression {

  override protected def template: String =
    (qualifier.map(_.toString).toSeq :+ name.toString) mkString "."

  override def withID(id: ExpressionID): Attribute = this

  def qualifiedBy(qualifier: Option[Name]): UnresolvedAttribute = copy(qualifier = qualifier)

  def of(qualifier: Name): UnresolvedAttribute = qualifiedBy(Some(qualifier))

  def of(dataType: DataType): AttributeRef =
    AttributeRef(name, dataType, isNullable = true, newExpressionID())

  def boolean: AttributeRef = this of BooleanType

  def byte: AttributeRef = this of ByteType

  def short: AttributeRef = this of ShortType

  def long: AttributeRef = this of LongType

  def int: AttributeRef = this of IntType

  def float: AttributeRef = this of FloatType

  def double: AttributeRef = this of DoubleType

  def string: AttributeRef = this of StringType
}

trait ResolvedAttribute extends Attribute {
  override def debugString: String = {
    val nullability = if (isNullable) "?" else "!"
    s"${name.toString}:${dataType.sql}$nullability#${expressionID.id}"
  }

  override def sql: Try[String] = Success(s"${name.toString}")

  def at(ordinal: Int): BoundRef = BoundRef(ordinal, dataType, isNullable)
}

case class AttributeRef(
  name: Name,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID,
  qualifier: Option[Name] = None
) extends ResolvedAttribute with UnevaluableExpression {

  /**
   * Returns a copy of this [[AttributeRef]] with a new [[ExpressionID]].
   */
  override def withID(id: ExpressionID): AttributeRef = copy(expressionID = id)

  /**
   * Returns a nullable copy of this [[AttributeRef]].
   */
  override def ? : AttributeRef = withNullability(true)

  /**
   * Returns a non-nullable copy of this [[AttributeRef]].
   */
  override def ! : AttributeRef = withNullability(false)

  /**
   * Returns a copy of this [[AttributeRef]] with given nullability.
   */
  override def withNullability(nullable: Boolean): AttributeRef = copy(isNullable = nullable)

  override def debugString: String =
    (qualifier.map(_.toString).toSeq :+ super.debugString) mkString "."

  /**
   * Returns a copy of this [[AttributeRef]] with given qualifier.
   */
  def qualifiedBy(qualifier: Option[Name]): AttributeRef = copy(qualifier = qualifier)

  /**
   * Returns a copy of this [[AttributeRef]] with given qualifier.
   */
  def of(qualifier: Name): AttributeRef = qualifiedBy(Some(qualifier))
}

case class BoundRef(ordinal: Int, override val dataType: DataType, override val isNullable: Boolean)
  extends NamedExpression with LeafExpression with NonSQLExpression {

  override val name: Name = caseInsensitive(s"input[$ordinal]")

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionID: ExpressionID = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = {
    val nullability = if (isNullable) "?" else "!"
    s"$name$nullability:${dataType.sql}"
  }

  /**
   * Returns a copy of this [[BoundRef]] bound at given ordinal.
   */
  def at(ordinal: Int): BoundRef = copy(ordinal = ordinal)
}

object BoundRef {
  def bind[A <: Expression](input: Seq[Attribute])(expression: A): A = {
    expression.transformUp {
      case ref: ResolvedAttribute =>
        val ordinal = input.indexWhere(_.expressionID == ref.expressionID)
        if (ordinal == -1) {
          throw new ResolutionFailureException({
            val inputAttributes = input.map(_.nodeCaption).mkString(", ")
            s"Failed to bind attribute reference $ref to any input attributes: $inputAttributes"
          })
        } else {
          BoundRef(ordinal, ref.dataType, ref.isNullable)
        }
    }.asInstanceOf[A]
  }
}
