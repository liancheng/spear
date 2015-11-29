package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.Try

import scraper.expressions.NamedExpression.newExpressionId
import scraper.types.DataType
import scraper.{ExpressionUnresolvedException, ResolutionFailureException, Row}

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

  override def sql: String = "*"

  override def annotatedString: String = "*"
}

case class Alias(
  name: String,
  child: Expression,
  override val expressionId: ExpressionId = newExpressionId()
) extends NamedExpression with UnaryExpression {

  override def foldable: Boolean = false

  override def dataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override lazy val toAttribute: Attribute = if (child.resolved) {
    AttributeRef(name, child.dataType, child.nullable, expressionId)
  } else {
    UnresolvedAttribute(name)
  }

  override def annotatedString: String = s"(${child.annotatedString} AS `$name`#${expressionId.id})"

  override def sql: String = s"(${child.sql} AS `$name`)"

  override lazy val strictlyTyped: Try[Expression] = for {
    e <- child.strictlyTyped
  } yield if (e sameOrEqual child) this else copy(child = e)
}

trait Attribute extends NamedExpression with LeafExpression {
  override def foldable: Boolean = false

  override def toAttribute: Attribute = this

  def ? : Attribute

  def ! : Attribute
}

case class UnresolvedAttribute(name: String) extends Attribute with UnresolvedNamedExpression {
  override def annotatedString: String = s"`$name`?"

  override def sql: String = s"`$name`"

  override def ? : Attribute = this

  override def ! : Attribute = this
}

case class AttributeRef(
  name: String,
  dataType: DataType,
  override val nullable: Boolean,
  override val expressionId: ExpressionId
) extends Attribute with UnevaluableExpression {

  override def annotatedString: String = s"`$name`#${expressionId.id}: ${dataType.simpleName}"

  override def sql: String = s"`$name`"

  override def ? : Attribute = copy(nullable = true)

  override def ! : Attribute = copy(nullable = false)
}

case class BoundRef(ordinal: Int, dataType: DataType, override val nullable: Boolean)
  extends NamedExpression with LeafExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionId: ExpressionId = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def annotatedString: String = name

  override def sql: String =
    throw new UnsupportedOperationException(s"${getClass.getSimpleName} doesn't have a SQL form")
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
