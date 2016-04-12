package scraper.types

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.implicitlyConvertible
import scraper.expressions.Expression
import scraper.trees.TreeNode

trait DataType { self =>
  /** Returns a nullable [[FieldSpec]] of this [[DataType]]. */
  def ? : FieldSpec = FieldSpec(this, nullable = true)

  /** Returns a non-nullable [[FieldSpec]] of this [[DataType]]. */
  def ! : FieldSpec = FieldSpec(this, nullable = false)

  def withNullability(allow: Boolean): FieldSpec = FieldSpec(this, nullable = allow)

  /** Shortcut method for [[scraper.expressions.Cast.compatible]] */
  def narrowerThan(that: DataType): Boolean = implicitlyConvertible(this, that)

  /**
   * Tries to figure out the widest type of between `this` and `that` [[DataType]].  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` is `y` [[narrowerThan]] `x`.
   */
  def widest(that: DataType): Try[DataType] = (this, that) match {
    case _ if this narrowerThan that => Success(that)
    case _ if that narrowerThan this => Success(this)
    case _ => Failure(new TypeMismatchException(
      s"Could not find common type for ${this.sql} and ${that.sql}"
    ))
  }

  /** Returns a pretty-printed tree string of this [[DataType]]. */
  def prettyTree: String = DataType.`DataType->DataTypeNode`(this).prettyTree

  /** A brief name of this [[DataType]]. */
  def simpleName: String = (getClass.getSimpleName stripSuffix "$" stripSuffix "Type").toLowerCase

  def sql: String

  def unapply(e: Expression): Option[Expression] = e match {
    case _ if e.dataType == this => Some(e)
    case _                       => None
  }

  object Implicitly {
    def unapply(e: Expression): Option[Expression] = e.dataType match {
      case t if t narrowerThan self => Some(e)
      case _                        => None
    }

    def unapply(dataType: DataType): Option[DataType] = dataType match {
      case t if t narrowerThan self => Some(t)
      case _                        => None
    }
  }
}

object DataType {
  implicit def `DataType->FieldSpec`(dataType: DataType): FieldSpec = dataType.?

  /**
   * A trait for wrapping [[DataType]]s into [[scraper.trees.TreeNode TreeNode]]s, so that we can
   * easily apply recursive transformations to a nested [[DataType]].
   */
  sealed trait DataTypeNode extends TreeNode[DataTypeNode] {
    def children: Seq[DataTypeNode]

    protected def fieldSpecString(dataType: DataType, nullable: Boolean): String = {
      val nullabilityMark = if (nullable) "?" else ""
      s"${dataType.simpleName}$nullabilityMark"
    }
  }

  /**
   * Converts a [[DataType]] to a [[scraper.trees.TreeNode TreeNode]].  Useful for tasks like pretty
   * printing.
   */
  implicit def `DataType->DataTypeNode`(dataType: DataType): DataTypeNode = dataType match {
    case t: PrimitiveType => PrimitiveTypeNode(t)
    case t: StructType    => StructTypeNode(t)
    case t: ArrayType     => ArrayTypeNode(t)
    case t: MapType       => MapTypeNode(t)
  }

  private[scraper] trait HasDataType { this: DataTypeNode =>
    def dataType: DataType

    override def nodeCaption: String = dataType.simpleName
  }

  case class PrimitiveTypeNode(dataType: DataType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = Nil
  }

  case class StructFieldNode(field: StructField) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = field.dataType.children

    override def nodeCaption: String =
      s"${field.name}: ${fieldSpecString(field.dataType, field.nullable)}"
  }

  case class StructTypeNode(dataType: StructType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = dataType.fields.map(StructFieldNode)
  }

  case class KeyTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.keyType.children

    override def nodeCaption: String = s"key: ${mapType.keyType.simpleName}"
  }

  case class ValueTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.valueType.children

    override def nodeCaption: String =
      s"value: ${fieldSpecString(mapType.valueType, mapType.valueNullable)}"
  }

  case class MapTypeNode(dataType: MapType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = {
      Seq(KeyTypeNode(dataType), ValueTypeNode(dataType))
    }
  }

  case class ElementTypeNode(arrayType: ArrayType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = arrayType.elementType.children

    override def nodeCaption: String =
      s"element: ${fieldSpecString(arrayType.elementType, arrayType.elementNullable)}"
  }

  case class ArrayTypeNode(dataType: ArrayType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = Seq(ElementTypeNode(dataType))
  }
}

trait AbstractDataType {
  val defaultType: Option[DataType]

  def unapply(e: Expression): Option[Expression]

  object Implicitly {
    def unapply(e: Expression): Option[Expression] = defaultType flatMap { d =>
      e.dataType match {
        case t if t narrowerThan d => Some(e)
        case _                     => None
      }
    }

    def unapply(dataType: DataType): Option[DataType] = defaultType flatMap { d =>
      dataType match {
        case t if t narrowerThan d => Some(t)
        case _                     => None
      }
    }
  }
}

case class FieldSpec(dataType: DataType, nullable: Boolean)

trait OrderedType { this: DataType =>
  type InternalType

  def ordering: Ordering[InternalType]

  def genericOrdering: Ordering[Any] = ordering.asInstanceOf[Ordering[Any]]
}

object OrderedType extends AbstractDataType {
  override val defaultType: Option[DataType] = None

  override def unapply(e: Expression): Option[Expression] = e.dataType match {
    case _: OrderedType => Some(e)
    case _              => None
  }

  override def toString: String = "ordered type"
}

trait PrimitiveType extends DataType

object PrimitiveType extends AbstractDataType {
  override val defaultType: Option[DataType] = None

  override def unapply(e: Expression): Option[Expression] = e.dataType match {
    case _: PrimitiveType => Some(e)
    case _                => None
  }

  override def toString: String = "primitive type"
}

case object NullType extends PrimitiveType with OrderedType {
  override type InternalType = Null

  override val ordering: Ordering[Null] = implicitly[Ordering[Null]]

  override def sql: String = "NULL"
}

case object StringType extends PrimitiveType with OrderedType {
  override type InternalType = String

  override val ordering: Ordering[String] = implicitly[Ordering[String]]

  override def sql: String = "STRING"
}

case object BooleanType extends PrimitiveType with OrderedType {
  override type InternalType = Boolean

  override val ordering: Ordering[Boolean] = implicitly[Ordering[Boolean]]

  override def sql: String = "BOOLEAN"
}
