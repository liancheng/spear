package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.language.higherKinds
import scala.util.{Success, Try}
import scalaz.Scalaz._
import scalaz._

import scraper.Row
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.NamedExpression.newExpressionId
import scraper.types._
import scraper.utils._

case class ExpressionId(id: Long)

trait NamedExpression extends Expression {
  def name: String

  def expressionId: ExpressionId

  def toAttribute: Attribute
}

trait GeneratedNamedExpression extends NamedExpression

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionId: ExpressionId = throw new ExpressionUnresolvedException(this)
}

object NamedExpression {
  private val currentId = new AtomicLong(0L)

  def newExpressionId(): ExpressionId = ExpressionId(currentId.getAndIncrement())

  def unapply(e: NamedExpression): Option[(String, DataType)] = Some((e.name, e.dataType))
}

case object Star extends LeafExpression with UnresolvedNamedExpression {
  override def name: String = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    implicitly[Applicative[T]] point "*"
}

case class Alias(
  child: Expression,
  name: String,
  override val expressionId: ExpressionId = newExpressionId()
) extends NamedExpression with UnaryExpression {
  override def foldable: Boolean = false

  override protected def strictDataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override lazy val toAttribute: Attribute = if (child.resolved) {
    AttributeRef(name, child.dataType, child.nullable, expressionId)
  } else {
    UnresolvedAttribute(name)
  }

  override def debugString: String = s"${child.debugString} AS ${quote(name)}#${expressionId.id}"

  override def sql: Try[String] = child.sql map (childSQL => s"$childSQL AS ${quote(name)}")

  override lazy val strictlyTypedForm: Try[Alias] = for {
    e <- child.strictlyTypedForm
  } yield if (e sameOrEqual child) this else copy(child = e)
}

case class GroupingAlias(
  child: Expression,
  override val expressionId: ExpressionId = newExpressionId()
) extends UnaryExpression with GeneratedNamedExpression {
  require(child.resolved)

  override lazy val strictlyTypedForm: Try[GroupingAlias] = for {
    e <- child.strictlyTypedForm
  } yield if (e sameOrEqual child) this else copy(child = e)

  override def name: String = GroupingAlias.Prefix + expressionId.id

  override def toAttribute: Attribute =
    GroupingAttribute(child.dataType, child.nullable, expressionId)

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    f(child) map (childString => s"$childString AS ${quote(name)}")
}

object GroupingAlias {
  val Prefix = "group$"
}

trait Attribute extends NamedExpression with LeafExpression {
  override def foldable: Boolean = false

  override def toAttribute: Attribute = this

  def newInstance(): Attribute

  def withNullability(nullability: Boolean): Attribute

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: String) extends Attribute with UnresolvedNamedExpression {
  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    implicitly[Applicative[T]].point(quote(name))

  override def newInstance(): Attribute = this

  override def withNullability(nullability: Boolean): Attribute = this

  def of(dataType: DataType): AttributeRef =
    AttributeRef(name, dataType, nullable = true, newExpressionId())

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
    val nullability = if (nullable) "?" else "!"
    s"${quote(name)}#${expressionId.id}:${dataType.sql}$nullability"
  }

  override def sql: Try[String] = Success(s"${quote(name)}")

  def at(ordinal: Int): BoundRef = BoundRef(ordinal, dataType, nullable)
}

case class AttributeRef(
  name: String,
  override val dataType: DataType,
  override val nullable: Boolean,
  override val expressionId: ExpressionId
) extends ResolvedAttribute with UnevaluableExpression {
  override def newInstance(): Attribute = copy(expressionId = NamedExpression.newExpressionId())

  override def ? : AttributeRef = withNullability(true)

  override def ! : AttributeRef = withNullability(false)

  override def withNullability(nullable: Boolean): AttributeRef = copy(nullable = nullable)
}

case class GroupingAttribute(
  override val dataType: DataType,
  override val nullable: Boolean,
  override val expressionId: ExpressionId
) extends ResolvedAttribute with UnevaluableExpression {
  override def newInstance(): Attribute = copy(expressionId = NamedExpression.newExpressionId())

  override def withNullability(nullable: Boolean): GroupingAttribute = copy(nullable = nullable)

  override val name: String = GroupingAlias.Prefix + expressionId.id
}

case class BoundRef(ordinal: Int, override val dataType: DataType, override val nullable: Boolean)
  extends NamedExpression with LeafExpression with NonSQLExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionId: ExpressionId = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = {
    val nullability = if (nullable) "?" else "!"
    name + ":" + dataType.sql + nullability
  }
}

object BoundRef {
  def bind[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    expression.transformUp {
      case ref: ResolvedAttribute =>
        val ordinal = input.indexWhere(_.expressionId == ref.expressionId)
        if (ordinal == -1) {
          throw new ResolutionFailureException({
            val inputAttributes = input.map(_.nodeCaption).mkString(", ")
            s"Failed to bind attribute reference $ref to any input attributes: $inputAttributes"
          })
        } else {
          BoundRef(ordinal, ref.dataType, ref.nullable)
        }
    }.asInstanceOf[A]
  }
}
