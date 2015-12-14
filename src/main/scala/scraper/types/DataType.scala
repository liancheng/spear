package scraper.types

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import scraper.exceptions.TypeMismatchException
import scraper.expressions.Cast.{convertible, implicitlyCompatible, implicitlyConvertible}
import scraper.expressions.Expression
import scraper.trees.TreeNode

trait DataType { self =>
  /** Returns a nullable [[FieldSpec]] of this [[DataType]]. */
  def ? : FieldSpec = FieldSpec(this, nullable = true)

  /** Returns a non-nullable [[FieldSpec]] of this [[DataType]]. */
  def ! : FieldSpec = FieldSpec(this, nullable = false)

  /** Shortcut method for [[scraper.expressions.Cast.convertible]] */
  def >=>(that: DataType): Boolean = convertible(this, that)

  /** Shortcut method for [[scraper.expressions.Cast.implicitlyConvertible]] */
  def >->(that: DataType): Boolean = implicitlyConvertible(this, that)

  /** Shortcut method for [[scraper.expressions.Cast.implicitlyCompatible]] */
  def <->(that: DataType): Boolean = implicitlyCompatible(this, that)

  /**
   * Tries to figure out the widest type of between `this` and `that` [[DataType]].  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` is `y` [[>->]] `x`.
   */
  def widest(that: DataType): Try[DataType] = (this, that) match {
    case _ if this >-> that => Success(that)
    case _ if that >-> this => Success(this)
    case _ => Failure(new TypeMismatchException(
      s"Could not find common type for: ${this.sql} and ${that.sql}"
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
      case t if t >-> self => Some(e)
      case _               => None
    }

    def unapply(dataType: DataType): Option[DataType] = dataType match {
      case t if t >-> self => Some(t)
      case _               => None
    }
  }
}

case class FieldSpec(dataType: DataType, nullable: Boolean)

object DataType {
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

trait PrimitiveType extends DataType {
  type InternalType

  val ordering: Ordering[InternalType]

  def genericOrdering: Ordering[Any] = ordering.asInstanceOf[Ordering[Any]]
}

object PrimitiveType {
  def unapply(e: Expression): Option[Expression] = e.dataType match {
    case _: PrimitiveType => Some(e)
    case _                => None
  }
}

case object NullType extends PrimitiveType {
  override type InternalType = Null

  override val ordering: Ordering[Null] = implicitly[Ordering[Null]]

  override def sql: String = "NULL"
}

case object StringType extends PrimitiveType {
  override type InternalType = String

  override val ordering: Ordering[String] = implicitly[Ordering[String]]

  override def sql: String = "TEXT"
}

case object BooleanType extends PrimitiveType {
  override type InternalType = Boolean

  override val ordering: Ordering[Boolean] = implicitly[Ordering[Boolean]]

  override def sql: String = "BOOL"
}
