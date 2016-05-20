package scraper.types

import scala.language.implicitConversions

import scraper.RowOrdering
import scraper.expressions.{Attribute, AttributeRef, BoundRef}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.utils.quote

trait ComplexType extends DataType

object ComplexType extends AbstractDataType {
  override val defaultType: Option[DataType] = None

  override def supertypeOf(dataType: DataType): Boolean = dataType match {
    case _: ComplexType => true
    case _              => false
  }

  override def toString: String = "complex type"
}

case class ArrayType(elementType: DataType, elementNullable: Boolean) extends ComplexType {
  override def genericOrdering: Option[Ordering[Any]] = elementType.genericOrdering map {
    Ordering.Iterable(_).asInstanceOf[Ordering[Any]]
  }

  override def sql: String = s"ARRAY<${elementType.sql}>"
}

object ArrayType extends AbstractDataType {
  def apply(fieldSpec: FieldSpec): ArrayType = ArrayType(fieldSpec.dataType, fieldSpec.nullable)

  override val defaultType: Option[DataType] = None

  override def supertypeOf(dataType: DataType): Boolean = dataType match {
    case _: ArrayType => true
    case _            => false
  }
}

case class MapType(keyType: DataType, valueType: DataType, valueNullable: Boolean)
  extends ComplexType {

  override def genericOrdering: Option[Ordering[Any]] = None

  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"
}

object MapType extends AbstractDataType {
  def apply(keyType: DataType, valueFieldSpec: FieldSpec): MapType =
    MapType(keyType, valueFieldSpec.dataType, valueFieldSpec.nullable)

  override val defaultType: Option[DataType] = None

  override def supertypeOf(dataType: DataType): Boolean = dataType match {
    case _: MapType => true
    case _          => false
  }
}

case class StructField(name: String, dataType: DataType, nullable: Boolean)

object StructField {
  def apply(name: String, fieldSpec: FieldSpec): StructField =
    StructField(name, fieldSpec.dataType, fieldSpec.nullable)

  implicit def `(String,DataType)->StructField`(pair: (String, DataType)): StructField =
    pair match {
      case (name, dataType) => StructField(name, dataType, nullable = true)
    }

  implicit def `(Symbol,DataType)->StructField`(pair: (Symbol, DataType)): StructField =
    pair match {
      case (name, dataType) => StructField(name.name, dataType, nullable = true)
    }

  implicit def `(String,FieldSpec)->StructField`(pair: (String, FieldSpec)): StructField =
    pair match {
      case (name, fieldSpec) => StructField(name, fieldSpec)
    }

  implicit def `(Symbol,FieldSpec)->StructField`(pair: (Symbol, FieldSpec)): StructField =
    pair match {
      case (name, fieldSpec) => StructField(name.name, fieldSpec)
    }
}

case class StructType(fields: Seq[StructField] = Seq.empty) extends ComplexType {
  override val genericOrdering: Option[Ordering[Any]] =
    if (fields.map(_.dataType.genericOrdering).forall(_.isDefined)) {
      val sortOrders = fields.zipWithIndex map {
        case (field, index) =>
          BoundRef(index, field.dataType, field.nullable).asc
      }
      Some(new RowOrdering(sortOrders).asInstanceOf[Ordering[Any]])
    } else {
      None
    }

  def apply(fieldName: String): StructField = fields.find(_.name == fieldName).get

  def apply(index: Int): StructField = fields(index)

  def length: Int = fields.length

  def fieldTypes: Seq[DataType] = fields map (_.dataType)

  def toAttributes: Seq[AttributeRef] = fields map {
    field => AttributeRef(field.name, field.dataType, field.nullable, newExpressionID())
  }

  def rename(fieldNames: Seq[String]): StructType = {
    assert(fieldNames.length == fields.length)
    StructType(fields zip fieldNames map {
      case (field, name) => field.copy(name = name)
    })
  }

  def rename(firstName: String, restNames: String*): StructType = this rename firstName +: restNames

  override def sql: String = {
    val fieldsString = fields map (f => s"${quote(f.name)}: ${f.dataType.sql}") mkString ", "
    s"STRUCT<$fieldsString>"
  }
}

object StructType extends AbstractDataType {
  val empty: StructType = StructType(Nil)

  def apply(first: StructField, rest: StructField*): StructType = StructType(first +: rest)

  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.isNullable)))

  override val defaultType: Option[DataType] = None

  override def supertypeOf(dataType: DataType): Boolean = dataType match {
    case _: StructType => true
    case _             => false
  }
}
