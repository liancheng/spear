package spear.types

import java.sql.Date

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import spear.exceptions.TypeMismatchException
import spear.expressions.Cast.{castable, compatible}
import spear.expressions.Expression
import spear.trees.TreeNode

trait AbstractDataType {
  def isSupertypeOf(dataType: DataType): Boolean

  def unapply(expression: Expression): Option[DataType] =
    Option(expression.dataType) filter isSupertypeOf
}

case class OneOf(supertypes: Seq[AbstractDataType]) extends AbstractDataType {
  require(supertypes.length > 1)

  override def isSupertypeOf(dataType: DataType): Boolean =
    supertypes exists (_ isSupertypeOf dataType)

  override def toString: String = if (supertypes.length == 2) {
    supertypes mkString " or "
  } else {
    supertypes.init.mkString("", ", ", s", or ${supertypes.last}")
  }
}

trait DataType extends AbstractDataType { self =>
  def genericOrdering: Option[Ordering[Any]]

  /** Returns a nullable [[FieldSpec]] of this [[DataType]]. */
  def ? : FieldSpec = FieldSpec(this, nullable = true)

  /** Returns a non-nullable [[FieldSpec]] of this [[DataType]]. */
  def ! : FieldSpec = FieldSpec(this, nullable = false)

  def withNullability(allow: Boolean): FieldSpec = FieldSpec(this, nullable = allow)

  /** Shortcut method for [[spear.expressions.Cast.compatible]] */
  def isCompatibleWith(that: DataType): Boolean = compatible(this, that)

  /** Shortcut method for [[spear.expressions.Cast.castable]] */
  def isCastableTo(that: DataType): Boolean = castable(this, that)

  def isSubtypeOf(supertype: AbstractDataType): Boolean = supertype isSupertypeOf this

  /**
   * Tries to figure out the widest type of between `this` and `that` [[DataType]].  For two types
   * `x` and `y`, `x` is considered to be wider than `y` iff `y` [[isCompatibleWith]] `x`.
   */
  def widest(that: DataType): Try[DataType] = (this, that) match {
    case _ if this isCompatibleWith that => Success(that)
    case _ if that isCompatibleWith this => Success(this)
    case _ => Failure(new TypeMismatchException(
      s"Could not find common type for ${this.sql} and ${that.sql}."
    ))
  }

  /** Returns a pretty-printed tree string of this [[DataType]]. */
  def prettyTree: String = DataType.`DataType->DataTypeNode`(this).prettyTree

  /** A brief name of this [[DataType]]. */
  def simpleName: String = (getClass.getSimpleName stripSuffix "$" stripSuffix "Type").toLowerCase

  def sql: String

  override def isSupertypeOf(dataType: DataType): Boolean = this == dataType

  override def toString: String = sql
}

object DataType {
  implicit def `DataType->FieldSpec`(dataType: DataType): FieldSpec = dataType.?

  /**
   * A trait for wrapping [[DataType]]s into [[spear.trees.TreeNode TreeNode]]s, so that we can
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
   * Converts a [[DataType]] to a [[spear.trees.TreeNode TreeNode]].  Useful for tasks like pretty
   * printing.
   */
  implicit def `DataType->DataTypeNode`(dataType: DataType): DataTypeNode = dataType match {
    case t: PrimitiveType => PrimitiveTypeNode(t)
    case t: StructType    => StructTypeNode(t)
    case t: ArrayType     => ArrayTypeNode(t)
    case t: MapType       => MapTypeNode(t)
  }

  case class PrimitiveTypeNode(dataType: DataType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = Nil

    override def caption: String = dataType.simpleName
  }

  case class StructFieldNode(field: StructField) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = field.dataType.children

    override def caption: String =
      s"${field.name}: ${fieldSpecString(field.dataType, field.isNullable)}"
  }

  case class StructTypeNode(dataType: StructType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = dataType.fields map StructFieldNode

    override def caption: String = dataType.simpleName
  }

  case class KeyTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.keyType.children

    override def caption: String = s"key: ${mapType.keyType.simpleName}"
  }

  case class ValueTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.valueType.children

    override def caption: String =
      s"value: ${fieldSpecString(mapType.valueType, mapType.isValueNullable)}"
  }

  case class MapTypeNode(dataType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = {
      Seq(KeyTypeNode(dataType), ValueTypeNode(dataType))
    }

    override def caption: String = dataType.simpleName
  }

  case class ElementTypeNode(arrayType: ArrayType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = arrayType.elementType.children

    override def caption: String =
      s"element: ${fieldSpecString(arrayType.elementType, arrayType.isElementNullable)}"
  }

  case class ArrayTypeNode(dataType: ArrayType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = Seq(ElementTypeNode(dataType))

    override def caption: String = dataType.simpleName
  }
}

case class FieldSpec(dataType: DataType, nullable: Boolean)

object OrderedType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType.genericOrdering.isDefined

  def orderingOf(dataType: DataType): Ordering[Any] = dataType.genericOrdering getOrElse {
    throw new TypeMismatchException(
      s"Data type ${dataType.sql} doesn't have an ordering"
    )
  }

  override def toString: String = "ordered type"
}

trait PrimitiveType extends DataType {
  type InternalType

  protected def ordering: Option[Ordering[InternalType]] = None

  override def genericOrdering: Option[Ordering[Any]] =
    ordering map { _.asInstanceOf[Ordering[Any]] }
}

object PrimitiveType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType match {
    case _: PrimitiveType => true
    case _                => false
  }

  override def toString: String = "primitive type"
}

case object NullType extends PrimitiveType {
  override type InternalType = Null

  override val ordering: Option[Ordering[Null]] = Some(implicitly[Ordering[Null]])

  override def sql: String = "NULL"
}

case object StringType extends PrimitiveType {
  override type InternalType = String

  override val ordering: Option[Ordering[String]] = Some(Ordering.String)

  override def sql: String = "STRING"
}

case object BooleanType extends PrimitiveType {
  override type InternalType = Boolean

  override val ordering: Option[Ordering[Boolean]] = Some(Ordering.Boolean)

  override def sql: String = "BOOLEAN"
}

case object DateType extends PrimitiveType {
  override type InternalType = Date

  override protected def ordering: Option[Ordering[Date]] = Some(new Ordering[Date] {
    override def compare(lhs: Date, rhs: Date): Int = lhs compareTo rhs
  })

  override def sql: String = "DATE"
}

case class TimestampType(precision: Int) extends PrimitiveType {
  require(0 <= precision && precision <= 6)

  override type InternalType = Long

  override val ordering: Option[Ordering[Long]] = Some(Ordering.Long)

  override def sql: String = s"TIME ($precision)"
}

case class ObjectType(className: String) extends PrimitiveType {
  override type InternalType = AnyRef

  override def genericOrdering: Option[Ordering[Any]] = None

  override def sql: String = s"OBJECT<$className>"
}

object ObjectType extends AbstractDataType {
  override def isSupertypeOf(dataType: DataType): Boolean = dataType match {
    case _: ObjectType => true
    case _             => false
  }

  override def toString: String = "object type"
}
