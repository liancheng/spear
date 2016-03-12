package scraper.expressions

import java.util.concurrent.atomic.AtomicLong

import scala.language.higherKinds
import scala.util.{Success, Try}
import scalaz.Scalaz._
import scalaz._

import scraper.Row
import scraper.exceptions.{ExpressionUnresolvedException, ResolutionFailureException}
import scraper.expressions.GeneratedNamedExpression.Purpose
import scraper.expressions.NamedExpression.newExpressionID
import scraper.types._
import scraper.utils._

case class ExpressionID(id: Long)

trait NamedExpression extends Expression {
  def name: String

  def expressionID: ExpressionID

  def toAttribute: Attribute
}

trait UnresolvedNamedExpression extends UnresolvedExpression with NamedExpression {
  override def expressionID: ExpressionID = throw new ExpressionUnresolvedException(this)
}

object NamedExpression {
  private val currentID = new AtomicLong(0L)

  val AnonymousColumnName = "?column?"

  def newExpressionID(): ExpressionID = ExpressionID(currentID.getAndIncrement())

  def unapply(e: NamedExpression): Option[(String, DataType)] = Some((e.name, e.dataType))

  /**
   * Auxiliary class only used for removing back-ticks from auto-generated column names.  For
   * example, for expression `id + 1`, we'd like to generate column name `(id + 1)` instead of
   * `(&#96;id&#96; + 1)`.
   */
  case class UnquotedAttribute(named: Attribute) extends LeafExpression with UnevaluableExpression {
    override def isResolved: Boolean = named.isResolved

    override def dataType: DataType = named.dataType

    override def isNullable: Boolean = named.isNullable

    override def sql: Try[String] = Try(named.name)
  }
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
  override val expressionID: ExpressionID = newExpressionID()
) extends NamedExpression with UnaryExpression {
  override def isFoldable: Boolean = false

  override protected def strictDataType: DataType = child.dataType

  override def evaluate(input: Row): Any = child.evaluate(input)

  override lazy val toAttribute: Attribute = if (child.isResolved) {
    AttributeRef(name, child.dataType, child.isNullable, expressionID)
  } else {
    UnresolvedAttribute(name)
  }

  override def debugString: String = s"${child.debugString} AS ${quote(name)}#${expressionID.id}"

  override def sql: Try[String] = child.sql map (childSQL => s"$childSQL AS ${quote(name)}")
}

case class UnresolvedAlias(child: Expression)
  extends NamedExpression
  with UnaryExpression
  with UnresolvedNamedExpression
  with UnevaluableExpression {

  override def name: String = throw new ExpressionUnresolvedException(this)

  override def toAttribute: Attribute = throw new ExpressionUnresolvedException(this)

  override def debugString: String = s"${child.debugString} AS ???"
}

trait Attribute extends NamedExpression with LeafExpression {
  override def isFoldable: Boolean = false

  override lazy val references: Set[Attribute] = Set(this)

  override def toAttribute: Attribute = this

  def newInstance(): Attribute

  def withNullability(nullability: Boolean): Attribute

  def ? : Attribute = withNullability(true)

  def ! : Attribute = withNullability(false)
}

case class UnresolvedAttribute(name: String, qualifier: Option[String] = None)
  extends Attribute with UnresolvedNamedExpression {

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    implicitly[Applicative[T]].point {
      (qualifier.toSeq :+ name) map quote mkString "."
    }

  override def newInstance(): Attribute = this

  override def withNullability(nullability: Boolean): Attribute = this

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

object ResolvedAttribute {
  def intersectByID(lhs: Set[Attribute], rhs: Set[Attribute]): Set[Attribute] = {
    require(lhs.forall(_.isResolved) && rhs.forall(_.isResolved))
    lhs filter (a => rhs exists (_.expressionID == a.expressionID))
  }
}

case class AttributeRef(
  name: String,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID,
  qualifier: Option[String] = None
) extends ResolvedAttribute with UnevaluableExpression {

  override def newInstance(): Attribute = copy(expressionID = NamedExpression.newExpressionID())

  override def ? : AttributeRef = withNullability(true)

  override def ! : AttributeRef = withNullability(false)

  override def withNullability(nullable: Boolean): AttributeRef = copy(isNullable = nullable)

  override def debugString: String = ((qualifier.toSeq map quote) :+ super.debugString) mkString "."
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
  def bind[A <: Expression](expression: A, input: Seq[Attribute]): A = {
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

sealed trait GeneratedNamedExpression extends NamedExpression {
  val purpose: Purpose

  override def name: String = purpose.prefix + "#" + expressionID.id
}

object GeneratedNamedExpression {
  /**
   * Indicates the purpose of a [[GeneratedNamedExpression]].
   */
  sealed trait Purpose {
    /**
     * Name prefix of a [[GeneratedNamedExpression]].  The full name is in the format of
     * `&lt;prefix&gt;#&lt;expression-ID&gt;`.
     */
    val prefix: String
  }

  /**
   * Marks [[GeneratedNamedExpression]]s used to wrap/reference grouping expressions.
   */
  case object ForGrouping extends Purpose {
    override val prefix: String = "group"
  }

  /**
   * Marks [[GeneratedNamedExpression]]s used to wrap/reference aggregate functions.
   */
  case object ForAggregation extends Purpose {
    override val prefix: String = "agg"
  }
}

case class GeneratedAlias[P <: Purpose] private[scraper] (
  purpose: P,
  child: Expression,
  override val expressionID: ExpressionID = newExpressionID()
) extends GeneratedNamedExpression with UnaryExpression {
  override def toAttribute: Attribute =
    GeneratedAttribute(purpose, child.dataType, child.isNullable, expressionID)

  override protected def template[T[_]: Applicative](f: (Expression) => T[String]): T[String] =
    f(child) map (childString => s"$childString AS ${quote(name)}")

  override def dataType: DataType = child.dataType

  override def isNullable: Boolean = child.isNullable

  override def isFoldable: Boolean = false
}

case class GeneratedAttribute[P <: Purpose](
  purpose: P,
  override val dataType: DataType,
  override val isNullable: Boolean,
  override val expressionID: ExpressionID
) extends GeneratedNamedExpression with ResolvedAttribute with UnevaluableExpression {
  override def newInstance(): Attribute = copy(expressionID = NamedExpression.newExpressionID())

  override def withNullability(nullable: Boolean): GeneratedAttribute[P] =
    copy(isNullable = nullable)
}
