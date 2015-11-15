package scraper.types

import scraper.expressions.NamedExpression.newExpressionId
import scraper.expressions.{ Attribute, AttributeRef }

trait ComplexType extends DataType

case class ArrayType(
  elementType: DataType,
  elementNullable: Boolean
) extends ComplexType {
  override def size: Int = 1 + elementType.size

  override def depth: Int = 1 + elementType.depth
}

object ArrayType {
  def apply(schema: Schema): ArrayType = ArrayType(schema.dataType, schema.nullable)
}

case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueNullable: Boolean
) extends ComplexType {
  override def size: Int = 2 + valueType.size

  override def depth: Int = 1 + (keyType.depth max valueType.depth)
}

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

  def toAttributes: Seq[Attribute] = fields.map {
    field =>
      AttributeRef(field.name, field.dataType, field.nullable, newExpressionId())
  }

  override def size: Int = 1 + fieldTypes.map(_.size).sum

  override def depth: Int = 1 + fieldTypes.map(_.depth).max
}

object TupleType {
  def apply(first: TupleField, rest: TupleField*): TupleType = TupleType(first +: rest)

  def fromAttributes(attributes: Seq[AttributeRef]): TupleType =
    TupleType(attributes.map(a => TupleField(a.name, a.dataType, a.nullable)))
}
