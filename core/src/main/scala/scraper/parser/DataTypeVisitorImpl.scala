package scraper.parser

import scala.collection.JavaConverters._

import scraper.parser.DataTypeParser._
import scraper.types._

class DataTypeVisitorImpl extends DataTypeBaseVisitor[DataType] {
  override def visitPrimitiveType(context: PrimitiveTypeContext): DataType =
    context.name.getText.toLowerCase match {
      case "boolean"  => BooleanType
      case "tinyint"  => ByteType
      case "smallint" => ShortType
      case "int"      => IntType
      case "bigint"   => LongType
      case "float"    => FloatType
      case "double"   => DoubleType
      case "string"   => StringType
    }

  override def visitArrayType(context: ArrayTypeContext): DataType = ArrayType(
    elementType = visitDataType(context.elementType),
    elementNullable = true
  )

  override def visitMapType(context: MapTypeContext): DataType = MapType(
    keyType = visitPrimitiveType(context.primitiveType()),
    valueType = visitDataType(context.dataType()),
    valueNullable = true
  )

  override def visitStructType(context: StructTypeContext): DataType = StructType(
    context.fields.asScala map { fieldCtx =>
      StructField(
        name = fieldCtx.name.getText,
        dataType = visitDataType(fieldCtx.dataType()),
        nullable = true
      )
    }
  )
}
