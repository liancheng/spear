package spear.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.{Success, Try}

import spear.{Name, Row}
import spear.exceptions.{ExpressionUnresolvedException, NameUnresolvedException}
import spear.expressions.NamedExpression.newExpressionID
import spear.types._

case class ExpressionID(id: Long)

trait NamedExpression extends Expression {
  def name: Name

  def namespace: String = name.namespace

  def expressionID: ExpressionID = throw new ExpressionUnresolvedException(this)

  def attr: Attribute

  def withID(id: ExpressionID): NamedExpression = throw new ExpressionUnresolvedException(this)
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression

object NamedExpression {
  private val currentID = new AtomicLong(0L)

  def newExpressionID(): ExpressionID = ExpressionID(currentID.getAndIncrement())

  def unapply(e: NamedExpression): Option[(Name, DataType)] = Some((e.name, e.dataType))

  val AnonymousColumnName: String = "?column?"

  def apply(child: Expression): NamedExpression = child match {
    case e: NamedExpression => e
    case _                  => UnresolvedAlias(child)
  }
}

case class Star(qualifier: Option[Name]) extends LeafExpression with UnresolvedNamedExpression {
  override def name: Name = throw new NameUnresolvedException(this)

  override def attr: Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template(childList: Seq[String]): String =
    qualifier.map { _.toString }.toSeq :+ "*" mkString "."

  def of(qualifier: Name): Star = copy(qualifier = Some(qualifier))
}

case class Alias(
  child: Expression,
  name: Name,
  override val expressionID: ExpressionID
) extends NamedExpression with UnaryExpression {
  override lazy val isFoldable: Boolean = false

  override def evaluate(input: Row): Any = child evaluate input

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

  override protected lazy val strictDataType: DataType = child.dataType
}

/**
 * Wherever a [[NamedExpression]] is required but an arbitrary unresolved [[Expression]] is given,
 * we use [[UnresolvedAlias]] to wrap the given expression and defer decision of the final alias
 * name until analysis time. The final alias name is usually the SQL representation of the finally
 * resolved expression. If the resolved expression doesn't have a SQL representation (e.g., Scala
 * UDF), a default name `?column?` will be used.
 */
case class UnresolvedAlias private (child: Expression)
  extends NamedExpression
  with UnaryExpression
  with UnresolvedNamedExpression
  with UnevaluableExpression {

  override def name: Name = throw new NameUnresolvedException(this)

  override def attr: Attribute = throw new ExpressionUnresolvedException(this)

  override def debugString: String = s"${child.debugString} AS ?alias?"
}

trait Attribute extends NamedExpression with LeafExpression {
  override lazy val isFoldable: Boolean = false

  override lazy val references: Seq[Attribute] = Seq(this)

  override def attr: Attribute = this

  override def withID(id: ExpressionID): Attribute = super.withID(id) match {
    case e: Attribute => e
  }

  def nullable(nullability: Boolean): Attribute = this

  def ? : Attribute = nullable(true)

  def ! : Attribute = nullable(false)
}

case class UnresolvedAttribute(name: Name, qualifier: Option[Name] = None)
  extends Attribute with UnresolvedNamedExpression {

  override def withID(id: ExpressionID): Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template(childList: Seq[String]): String =
    (qualifier.map { _.toString }.toSeq :+ name.toString) mkString "."

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
    s"${name.toString}#${expressionID.id}:${dataType.sql}$nullability"
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
  override def ? : AttributeRef = nullable(true)

  /**
   * Returns a non-nullable copy of this [[AttributeRef]].
   */
  override def ! : AttributeRef = nullable(false)

  /**
   * Returns a copy of this [[AttributeRef]] with given nullability.
   */
  override def nullable(isNullable: Boolean): AttributeRef = copy(isNullable = isNullable)

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

case class BoundRef(
  ordinal: Int,
  override val dataType: DataType,
  override val isNullable: Boolean
) extends LeafExpression with NonSQLExpression {

  override lazy val isBound: Boolean = true

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = {
    val nullable = if (isNullable) "?" else "!"
    s"output[$ordinal]:${dataType.sql}$nullable"
  }

  /**
   * Returns a copy of this [[BoundRef]] bound at given ordinal.
   */
  def at(ordinal: Int): BoundRef = copy(ordinal = ordinal)

  def shift(offset: Int): BoundRef = at(ordinal + offset)
}
