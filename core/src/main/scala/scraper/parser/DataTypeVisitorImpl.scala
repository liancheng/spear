package scraper.parser

import scala.collection.JavaConverters._

import scraper.antlr4.DataTypeBaseVisitor
import scraper.antlr4.DataTypeParser._
import scraper.types._

class DataTypeVisitorImpl extends DataTypeBaseVisitor[DataType] {
  override def visitDataType(ctx: DataTypeContext): DataType =
    Option(ctx.complexType())
      .map(visitComplexType)
      .getOrElse(visitPrimitiveType(ctx.primitiveType()))

  override def visitPrimitiveType(ctx: PrimitiveTypeContext): DataType =
    ctx.name.getText.toLowerCase match {
      case "boolean"  => BooleanType
      case "tinyint"  => ByteType
      case "smallint" => ShortType
      case "int"      => IntType
      case "bigint"   => LongType
      case "float"    => FloatType
      case "double"   => DoubleType
      case "string"   => StringType
    }

  override def visitArrayType(ctx: ArrayTypeContext): DataType = ArrayType(
    elementType = visitDataType(ctx.elementType),
    elementNullable = true
  )

  override def visitMapType(ctx: MapTypeContext): DataType = MapType(
    keyType = visitPrimitiveType(ctx.primitiveType()),
    valueType = visitDataType(ctx.dataType()),
    valueNullable = true
  )

  override def visitStructType(ctx: StructTypeContext): DataType = StructType(
    ctx.fields.asScala map { fieldCtx =>
      StructField(
        name = fieldCtx.name.getText,
        dataType = visitDataType(fieldCtx.dataType()),
        nullable = true
      )
    }
  )
}
