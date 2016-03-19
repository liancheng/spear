package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scraper.Row
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.expressions.functions._
import scraper.types._
import scraper.utils._

import scala.language.higherKinds
import scala.util.{Success, Try}

case class ExpressionID(id: Long)

trait NamedExpression extends Expression {
  def name: String

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

  def unapply(e: NamedExpression): Option[(String, DataType)] = Some((e.name, e.dataType))

  /**
   * Auxiliary class only used for removing back-ticks and double-quotes from auto-generated column
   * names.  For example, for expression `id + 1`, we'd like to generate column name `(id + 1)`
   * instead of `(&#96;id&#96; + 1)`.
   */
  case class UnquotedName(named: NamedExpression)
    extends LeafExpression with UnevaluableExpression {

    override def isResolved: Boolean = named.isResolved

    override def dataType: DataType = named.dataType

    override def isNullable: Boolean = named.isNullable

    override def sql: Try[String] = Try(named.name)
  }

  object UnquotedName {
    def apply(stringLiteral: String): UnquotedName =
      UnquotedName(lit(stringLiteral) as stringLiteral)
  }
}

case class Star(qualifier: Option[String]) extends LeafExpression with UnresolvedNamedExpression {
  override def name: String = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template: String = (qualifier map quote).toSeq :+ "*" mkString "."
}

case class Alias(
  child: Expression,
  name: String,
  override val expressionID: ExpressionID
) extends NamedExpression with UnaryExpression {
  override def isFoldable: Boolean = false

  override protected def strictDataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override lazy val toAttribute: Attribute = if (child.isResolved) {
    AttributeRef(name, child.dataType, child.isNullable, expressionID)
  } else {
    UnresolvedAttribute(name)
  }

  override def debugString: String = s"(${child.debugString} AS ${quote(name)})#${expressionID.id}"

  override def sql: Try[String] = child.sql map (childSQL => s"$childSQL AS ${quote(name)}")

  def withID(id: ExpressionID): Alias = copy(expressionID = id)
}

/**
 * When user gives an arbitrary [[Expression]] where a [[NamedExpression]] is required, we use
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

  override def name: String = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override def debugString: String = s"${child.debugString} AS ?alias?"
}

object AutoAlias {
  val AnonymousColumnName = "?column?"

  def named(child: Expression): NamedExpression = child match {
    case e: NamedExpression => e
    case _                  => AutoAlias(child)
  }
}

trait Attribute extends NamedExpression with LeafExpression {
  override def isFoldable: Boolean = false

  override lazy val references: Set[Attribute] = Set(this)

  override def toAttribute: Attribute = this

  def withID(id: ExpressionID): Attribute

  def withNullability(nullability: Boolean): Attribute = this

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: String, qualifier: Option[String] = None)
  extends Attribute with UnresolvedNamedExpression {

  override protected def template: String = (qualifier.toSeq :+ name) map quote mkString "."

  override def withID(id: ExpressionID): Attribute = this

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
    s"${quote(name)}#${expressionID.id}:${dataType.sql}$nullability"
  }

  override def sql: Try[String] = Success(s"${quote(name)}")

  def at(ordinal: Int): BoundRef = BoundRef(ordinal, dataType, isNullable)
}

case class AttributeRef(
  name: String,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID,
  qualifier: Option[String] = None
) extends ResolvedAttribute with UnevaluableExpression {

  override def withID(id: ExpressionID): AttributeRef = copy(expressionID = id)

  override def ? : AttributeRef = withNullability(true)

  override def ! : AttributeRef = withNullability(false)

  override def withNullability(nullable: Boolean): AttributeRef = copy(isNullable = nullable)

  override def debugString: String = ((qualifier.toSeq map quote) :+ super.debugString) mkString "."

  def qualifiedBy(qualifier: String): AttributeRef = copy(qualifier = Some(qualifier))

  def qualifiedBy(qualifier: Symbol): AttributeRef = qualifiedBy(qualifier.name)
}

case class BoundRef(ordinal: Int, override val dataType: DataType, override val isNullable: Boolean)
  extends NamedExpression with LeafExpression with NonSQLExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionID: ExpressionID = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = {
    val nullability = if (isNullable) "?" else "!"
    name + ":" + dataType.sql + nullability
  }
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
