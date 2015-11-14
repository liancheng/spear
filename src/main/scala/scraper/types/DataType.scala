package scraper.types

import scala.language.implicitConversions

import scraper.trees.TreeNode

trait DataType {
  /** Returns a nullable [[Schema]] of this [[DataType]]. */
  def ? : Schema = Schema(this, nullable = true)

  /** Returns a non-nullable [[Schema]] of this [[DataType]]. */
  def ! : Schema = Schema(this, nullable = false)

  /** Returns a pretty-printed tree string of this [[DataType]]. */
  def prettyTree: String = DataType.`DataType->DataTypeNode`(this).prettyTree

  /** A brief name of this [[DataType]]. */
  def simpleName: String =
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type").toLowerCase

  /**
   * Size of this [[DataType]].
   *
   * The size of a [[DataType]] `S(t)` is defined as:
   *
   *  1. For a [[PrimitiveType]] `p`, `S(p) = 1`
   *  1. For an [[ArrayType]] `a`, `S(a) = 1 + S(a.elementType)`
   *  1. For a [[MapType]] `m`, `S(m) = 1 + S(m.keyType) + S(m.valueType)`
   *  1. For a [[TupleType]] `s`, `S(s) = 1 + sum(S(f) for f in s.fieldTypes)`
   */
  def size: Int

  /**
   * Depth of this [[DataType]].
   *
   * The depth of a [[DataType]] `D(t)` is defined as:
   *
   *  1. For a [[PrimitiveType]] `p`, `D(p) = 1`
   *  1. For an [[ArrayType]] `a`, `D(a) = 1 + D(a.elementType)`
   *  1. For a [[MapType]] `m`, `D(m) = 1 + max(D(m.keyType), D(m.valueType))`
   *  1. For a [[TupleType]] `s`, `D(s) = 1 + max(D(f) for f in s.fieldTypes)`
   */
  def depth: Int
}

case class Schema(dataType: DataType, nullable: Boolean)

object DataType {

  /**
   * A trait for wrapping [[DataType]]s into [[scraper.trees.TreeNode TreeNode]]s, so that we can
   * easily apply recursive transformations to a nested [[DataType]].
   */
  sealed trait DataTypeNode extends TreeNode[DataTypeNode] {
    def children: Seq[DataTypeNode]

    protected def schemaString(dataType: DataType, nullable: Boolean): String = {
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
    case t: TupleType     => TupleTypeNode(t)
    case t: ArrayType     => ArrayTypeNode(t)
    case t: MapType       => MapTypeNode(t)
  }

  private[scraper] trait HasDataType { this: DataTypeNode =>
    def dataType: DataType

    override def caption: String = dataType.simpleName
  }

  case class PrimitiveTypeNode(dataType: DataType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = Nil
  }

  case class TupleFieldNode(field: TupleField) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = field.dataType.children

    override def caption: String =
      s"${field.name}: ${schemaString(field.dataType, field.nullable)}"
  }

  case class TupleTypeNode(dataType: TupleType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = dataType.fields.map(TupleFieldNode)
  }

  case class KeyTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.keyType.children

    override def caption: String = s"key: ${mapType.keyType.simpleName}"
  }

  case class ValueTypeNode(mapType: MapType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = mapType.valueType.children

    override def caption: String =
      s"value: ${schemaString(mapType.valueType, mapType.valueNullable)}"
  }

  case class MapTypeNode(dataType: MapType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = {
      Seq(KeyTypeNode(dataType), ValueTypeNode(dataType))
    }
  }

  case class ElementTypeNode(arrayType: ArrayType) extends DataTypeNode {
    override def children: Seq[DataTypeNode] = arrayType.elementType.children

    override def caption: String =
      s"element: ${schemaString(arrayType.elementType, arrayType.elementNullable)}"
  }

  case class ArrayTypeNode(dataType: ArrayType) extends DataTypeNode with HasDataType {
    override def children: Seq[DataTypeNode] = Seq(ElementTypeNode(dataType))
  }
}

trait PrimitiveType extends DataType {
  type InternalType
  val ordering: Ordering[InternalType]
  override def size: Int = 1
  override def depth: Int = 1
}

case object NullType extends PrimitiveType {
  override type InternalType = Null
  override val ordering: Ordering[Null] = implicitly[Ordering[Null]]
}

case object StringType extends PrimitiveType {
  override type InternalType = String
  override val ordering: Ordering[String] = implicitly[Ordering[String]]
}

case object BooleanType extends PrimitiveType {
  override type InternalType = Boolean
  override val ordering: Ordering[Boolean] = implicitly[Ordering[Boolean]]
}
