package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.{Success, Try}

import scraper.{Name, Row}
import scraper.Name.caseSensitive
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types._

case class ExpressionID(id: Long)

trait NamedExpression extends Expression {
  def name: Name

  def expressionID: ExpressionID

  def attr: Attribute

  def withID(id: ExpressionID): NamedExpression
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionID: ExpressionID = throw new ExpressionUnresolvedException(this)

  override def withID(id: ExpressionID): UnresolvedNamedExpression = this
}

object NamedExpression {
  private val currentID = new AtomicLong(0L)

  def newExpressionID(): ExpressionID = ExpressionID(currentID.getAndIncrement())

  def unapply(e: NamedExpression): Option[(Name, DataType)] = Some((e.name, e.dataType))
}

case class Star(qualifier: Option[Name]) extends LeafExpression with UnresolvedNamedExpression {
  override def name: Name = throw new ExpressionUnresolvedException(this)

  override def attr: Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template(childList: Seq[String]): String =
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

  override lazy val attr: Attribute = if (child.isResolved) {
    AttributeRef(name, child.dataType, child.isNullable, expressionID)
  } else {
    UnresolvedAttribute(name)
  }

  override def debugString: String =
    s"(${template(child.debugString :: Nil)}#${expressionID.id})"

  override def template(childList: Seq[String]): String = {
    val Seq(childString) = childList
    s"$childString AS ${name.toString}"
  }

  override def withID(id: ExpressionID): Alias = copy(expressionID = id)
}

object Alias {
  def unaliasUsing[E <: Expression](projectList: Seq[NamedExpression])(expression: E): E = {
    val aliases = projectList.collect { case a: Alias => a.attr -> a.child }.toMap
    expression transformUp { case a: AttributeRef => aliases.getOrElse(a, a) }
  }.asInstanceOf[E]
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

  override def attr: Attribute = throw new ExpressionUnresolvedException(this)

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

  override def attr: Attribute = this

  def withID(id: ExpressionID): Attribute

  def withNullability(nullability: Boolean): Attribute = this

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: Name, qualifier: Option[Name] = None)
  extends Attribute with UnresolvedNamedExpression {

  override protected def template(childList: Seq[String]): String =
    (qualifier.map(_.toString).toSeq :+ name.toString) mkString "."

  override def withID(id: ExpressionID): UnresolvedAttribute = this

  def qualifiedBy(qualifier: Option[Name]): UnresolvedAttribute = copy(qualifier = qualifier)

  def of(qualifier: Name): UnresolvedAttribute = qualifiedBy(Some(qualifier))

  def of(dataType: DataType): AttributeRef =
    AttributeRef(name, dataType, isNullable = true, newExpressionID())

  def of(fieldSpec: FieldSpec): AttributeRef = {
    val FieldSpec(dataType, isNullable) = fieldSpec
    AttributeRef(name, dataType, isNullable, newExpressionID())
  }

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

  override lazy val isBound: Boolean = false

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

  override def sql: Try[String] = super.sql map { sql =>
    (qualifier.map(_.toString).toSeq :+ sql) mkString "."
  }

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

  override val name: Name = caseSensitive(s"input[$ordinal]")

  override lazy val isBound: Boolean = true

  override def attr: Attribute = throw new UnsupportedOperationException

  override def expressionID: ExpressionID = throw new UnsupportedOperationException

  override def withID(id: ExpressionID): NamedExpression = this

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = {
    val nullability = if (isNullable) "?" else "!"
    s"$name:${dataType.sql}$nullability"
  }

  /**
   * Returns a copy of this [[BoundRef]] bound at given ordinal.
   */
  def at(ordinal: Int): BoundRef = copy(ordinal = ordinal)
}

object BoundRef {
  def bindTo[E <: Expression](input: Seq[Attribute])(expression: E): E = expression.transformUp {
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
  }.asInstanceOf[E]
}
