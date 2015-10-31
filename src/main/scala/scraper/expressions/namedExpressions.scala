package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scraper.types.DataType
import scraper.{ ExpressionUnresolved, ResolutionFailure, Row }

case class ExpressionId(id: Long)

trait NamedExpression extends Expression {
  val name: String

  def expressionId: ExpressionId

  def toAttribute: Attribute
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionId: ExpressionId = throw ExpressionUnresolved(this)
}

object NamedExpression {
  private val currentId = new AtomicLong(0L)

  def newExpressionId(): ExpressionId = ExpressionId(currentId.getAndIncrement())

  def unapply(e: NamedExpression): Option[(String, DataType)] = Some((e.name, e.dataType))
}

case class Alias(
  name: String,
  child: Expression
)(
  override val expressionId: ExpressionId = NamedExpression.newExpressionId()
) extends NamedExpression with UnaryExpression {

  override def dataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override def toAttribute: Attribute = UnresolvedAttribute(name)
}

trait Attribute extends NamedExpression with LeafExpression {
  override def foldable: Boolean = false

  override def toAttribute: Attribute = this
}

case class UnresolvedAttribute(name: String) extends Attribute with UnresolvedNamedExpression

case class AttributeRef(
  name: String,
  dataType: DataType,
  nullable: Boolean
)(
  override val expressionId: ExpressionId = NamedExpression.newExpressionId()
) extends Attribute with UnevaluableExpression {
  override def nodeDescription: String = s"$name#${expressionId.id}: ${dataType.simpleName}"
}

case class BoundRef(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends NamedExpression with LeafExpression {

  override val name: String = s"input[$ordinal]"

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def expressionId: ExpressionId = throw new UnsupportedOperationException

  override def evaluate(input: Row): Any = input(ordinal)
}

object BoundRef {
  def bind[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    expression.transformUp {
      case ref @ AttributeRef(_, dataType, nullable) =>
        val ordinal = input.indexWhere(_.expressionId == ref.expressionId)
        if (ordinal == -1) {
          throw ResolutionFailure(s"Failed to bind attribute reference $ref")
        } else {
          BoundRef(ordinal, dataType, nullable)
        }
    }.asInstanceOf[A]
  }
}
