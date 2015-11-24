package scraper.types

import scraper.expressions.NamedExpression.newExpressionId
import scraper.expressions.{Attribute, AttributeRef}

trait ComplexType extends DataType

case class ArrayType(
  elementType: DataType,
  elementNullable: Boolean
) extends ComplexType

object ArrayType {
  def apply(schema: Schema): ArrayType = ArrayType(schema.dataType, schema.nullable)
}

case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueNullable: Boolean
) extends ComplexType

object MapType {
  def apply(keyType: DataType, valueSchema: Schema): MapType =
    MapType(keyType, valueSchema.dataType, valueSchema.nullable)
}

case class TupleField(name: String, dataType: DataType, nullable: Boolean) {
  /** Makes a nullable copy of this [[TupleField]]. */
  def ? : TupleField = this.copy(nullable = true)

  /** Makes a non-nullable copy of this [[TupleField]]. */
  def ! : TupleField = this.copy(nullable = false)
}

object TupleField {
  def apply(name: String, schema: Schema): TupleField =
    TupleField(name, schema.dataType, schema.nullable)
}

case class TupleType(fields: Seq[TupleField] = Seq.empty) extends ComplexType {
  def apply(fieldName: String): TupleField = fields.find(_.name == fieldName).get

  def apply(index: Int): TupleField = fields(index)

  def fieldTypes: Seq[DataType] = fields.map(_.dataType)

  def toAttributes: Seq[Attribute] = fields map {
    field => AttributeRef(field.name, field.dataType, field.nullable, newExpressionId())
  }

  def rename(fieldNames: Seq[String]): TupleType = {
    assert(fieldNames.length == fields.length)
    TupleType(fields zip fieldNames map {
      case (field, name) => field.copy(name = name)
    })
  }

  def rename(firstName: String, restNames: String*): TupleType = this rename firstName +: restNames
}

object TupleType {
  def apply(first: TupleField, rest: TupleField*): TupleType = TupleType(first +: rest)

  def fromAttributes(attributes: Seq[Attribute]): TupleType =
    TupleType(attributes.map(a => TupleField(a.name, a.dataType, a.nullable)))
}
