package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.util.Try

import scraper.expressions.NamedExpression.newExpressionId
import scraper.types.DataType
import scraper.{ExpressionUnresolvedException, ResolutionFailureException, Row}

case class ExpressionId(id: Long)

trait NamedExpression extends Expression {
  val name: String

  def expressionId: ExpressionId

  def toAttribute: Attribute
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionId: ExpressionId = throw ExpressionUnresolvedException(this)
}

object NamedExpression {
  private val currentId = new AtomicLong(0L)

  def newExpressionId(): ExpressionId = ExpressionId(currentId.getAndIncrement())

  def unapply(e: NamedExpression): Option[(String, DataType)] = Some((e.name, e.dataType))
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

  override def nodeCaption: String =
    s"(${child.nodeCaption} AS $name#${expressionId.id})"

  override lazy val strictlyTyped: Try[Expression] = for {
    e <- child.strictlyTyped
  } yield if (e sameOrEqual child) this else copy(child = e)
}

trait Attribute extends NamedExpression with LeafExpression {
  override def foldable: Boolean = false

  override def toAttribute: Attribute = this
}

case class UnresolvedAttribute(name: String) extends Attribute with UnresolvedNamedExpression {
  override def nodeCaption: String = s"`$name`?"
}

case class AttributeRef(
  name: String,
  dataType: DataType,
  override val nullable: Boolean,
  override val expressionId: ExpressionId
) extends Attribute with UnevaluableExpression {

  override def nodeCaption: String = s"`$name`#${expressionId.id}: ${dataType.simpleName}"
}

case class BoundRef(ordinal: Int, dataType: DataType, override val nullable: Boolean)
  extends NamedExpression with LeafExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionId: ExpressionId = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)

  override def nodeCaption: String = name
}

object BoundRef {
  def bind[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    expression.transformUp {
      case ref: AttributeRef =>
        val ordinal = input.indexWhere(_.expressionId == ref.expressionId)
        if (ordinal == -1) {
          throw ResolutionFailureException({
            val inputAttributes = input.map(_.nodeCaption).mkString(", ")
            s"Failed to bind attribute reference $ref to any input attributes: $inputAttributes"
          })
        } else {
          BoundRef(ordinal, ref.dataType, ref.nullable)
        }
    }.asInstanceOf[A]
  }
}
