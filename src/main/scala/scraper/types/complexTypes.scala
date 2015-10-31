package scraper.types

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

case class StructField(name: String, dataType: DataType, nullable: Boolean) {
  /** Makes a nullable copy of this [[StructField]]. */
  def ? : StructField = this.copy(nullable = true)

  /** Makes a non-nullable copy of this [[StructField]]. */
  def ! : StructField = this.copy(nullable = false)
}

object StructField {
  def apply(name: String, schema: Schema): StructField =
    StructField(name, schema.dataType, schema.nullable)
}

case class StructType(fields: Seq[StructField] = Seq.empty) extends ComplexType {
  def apply(fieldName: String): StructField = fields.find(_.name == fieldName).get

  def apply(index: Int): StructField = fields(index)

  def fieldTypes: Seq[DataType] = fields.map(_.dataType)

  def toAttributes: Seq[Attribute] = fields.map(f => AttributeRef(f.name, f.dataType, f.nullable)())

  override def size: Int = 1 + fieldTypes.map(_.size).sum

  override def depth: Int = 1 + fieldTypes.map(_.depth).max
}

object StructType {
  def apply(first: StructField, rest: StructField*): StructType = StructType(first +: rest)

  def fromAttributes(attributes: Seq[AttributeRef]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable)))
}
