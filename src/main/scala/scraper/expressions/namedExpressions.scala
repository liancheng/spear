package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.Try
import scalaz.Scalaz._

import scraper.Row
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.NamedExpression.newExpressionId
import scraper.types.DataType

case class ExpressionId(id: Long)

trait NamedExpression extends Expression {
  def name: String

  def expressionId: ExpressionId

  def toAttribute: Attribute
}

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

  override def debugString: String = "*"

  override def sql: Option[String] = "*".some
}

case class Alias(
  name: String,
  child: Expression,
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

  override def debugString: String = s"(${child.debugString} AS `$name`#${expressionId.id})"

  override def sql: Option[String] = child.sql map (childSQL => s"$childSQL AS `$name`")

  override lazy val strictlyTypedForm: Try[Alias] = for {
    e <- child.strictlyTypedForm
  } yield if (e sameOrEqual child) this else copy(child = e)
}

trait Attribute extends NamedExpression with LeafExpression {
  override def foldable: Boolean = false

  override def toAttribute: Attribute = this

  def withNullability(nullability: Boolean): Attribute

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: String) extends Attribute with UnresolvedNamedExpression {
  override def debugString: String = s"`$name`"

  override def sql: Option[String] = s"`$name`".some

  override def withNullability(nullability: Boolean): Attribute = this

  def of(dataType: DataType): AttributeRef =
    AttributeRef(name, dataType, nullable = true, newExpressionId())
}

case class AttributeRef(
  name: String,
  override val dataType: DataType,
  override val nullable: Boolean,
  override val expressionId: ExpressionId
) extends Attribute with UnevaluableExpression {

  override def debugString: String = {
    val nullability = if (nullable) "?" else "!"
    s"`$name`#${expressionId.id}: ${dataType.sql}$nullability"
  }

  override def sql: Option[String] = s"`$name`".some

  override def withNullability(nullable: Boolean): Attribute = copy(nullable = nullable)

  def at(ordinal: Int): BoundRef = BoundRef(ordinal, dataType, nullable)
}

case class BoundRef(ordinal: Int, override val dataType: DataType, override val nullable: Boolean)
  extends NamedExpression with LeafExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionId: ExpressionId = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def debugString: String = name

  override def sql: Option[String] = None
}

object BoundRef {
  def bind[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    expression.transformUp {
      case ref: AttributeRef =>
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
