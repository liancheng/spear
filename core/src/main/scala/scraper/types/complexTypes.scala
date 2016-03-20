package scraper.types

import scala.language.implicitConversions

import scraper.expressions.{Attribute, AttributeRef}
import scraper.expressions.NamedExpression.newExpressionID
import scraper.utils.quote

trait ComplexType extends DataType

case class ArrayType(
  elementType: DataType,
  elementNullable: Boolean
) extends ComplexType {
  override def sql: String = s"ARRAY<${elementType.sql}>"
}

object ArrayType {
  def apply(fieldSpec: FieldSpec): ArrayType = ArrayType(fieldSpec.dataType, fieldSpec.nullable)
}

case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueNullable: Boolean
) extends ComplexType {
  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"
}

object MapType {
  def apply(keyType: DataType, valueFieldSpec: FieldSpec): MapType =
    MapType(keyType, valueFieldSpec.dataType, valueFieldSpec.nullable)
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

object StructType {
  val empty: StructType = StructType(Nil)

  def apply(first: StructField, rest: StructField*): StructType = StructType(first +: rest)

  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.isNullable)))
}
