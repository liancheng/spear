package scraper

import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe.definitions._

import scraper.types._

package object reflection {
  def fieldSpecFor[T: WeakTypeTag]: FieldSpec = fieldSpecFor(weakTypeOf[T])

  private val fieldSpecFor: PartialFunction[Type, FieldSpec] = (
    fieldSpecForUnboxedPrimitiveType
    orElse fieldSpecForBoxedPrimitiveType
    orElse fieldSpecForCompoundType
  )

  private def fieldSpecForCompoundType: PartialFunction[Type, FieldSpec] = {
    case t if t <:< weakTypeOf[Option[_]] =>
      val Seq(elementType) = t.typeArgs
      fieldSpecFor(elementType).dataType.?

    case t if t <:< weakTypeOf[Seq[_]] || t <:< weakTypeOf[Array[_]] =>
      val Seq(elementType) = t.typeArgs
      val FieldSpec(elementDataType, elementOptional) = fieldSpecFor(elementType)
      ArrayType(elementDataType, elementOptional).?

    case t if t <:< weakTypeOf[Map[_, _]] =>
      val Seq(keyType, valueType) = t.typeArgs
      val FieldSpec(keyDataType, _) = fieldSpecFor(keyType)
      val FieldSpec(valueDataType, valueOptional) = fieldSpecFor(valueType)
      MapType(keyDataType, valueDataType, valueOptional).?

    case t if t <:< weakTypeOf[Product] =>
      val formalTypeArgs = t.typeSymbol.asClass.typeParams
      val TypeRef(_, _, actualTypeArgs) = t
      val List(constructorParams) = t.member(termNames.CONSTRUCTOR).asMethod.paramLists
      StructType(constructorParams.map { param =>
        val paramType = param.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
        val FieldSpec(dataType, nullable) = fieldSpecFor(paramType)
        StructField(param.name.toString, dataType, nullable)
      }).?
  }

  private def fieldSpecForBoxedPrimitiveType: PartialFunction[Type, FieldSpec] = {
    case t if t <:< weakTypeOf[String]            => StringType.?
    case t if t <:< weakTypeOf[Integer]           => IntType.?
    case t if t <:< weakTypeOf[java.lang.Boolean] => BooleanType.?
    case t if t <:< weakTypeOf[java.lang.Byte]    => ByteType.?
    case t if t <:< weakTypeOf[java.lang.Short]   => ShortType.?
    case t if t <:< weakTypeOf[java.lang.Long]    => LongType.?
    case t if t <:< weakTypeOf[java.lang.Float]   => FloatType.?
    case t if t <:< weakTypeOf[java.lang.Double]  => DoubleType.?
  }

  private def fieldSpecForUnboxedPrimitiveType: PartialFunction[Type, FieldSpec] = {
    case t if t <:< IntTpe     => IntType.!
    case t if t <:< BooleanTpe => BooleanType.!
    case t if t <:< ByteTpe    => ByteType.!
    case t if t <:< ShortTpe   => ShortType.!
    case t if t <:< LongTpe    => LongType.!
    case t if t <:< FloatTpe   => FloatType.!
    case t if t <:< DoubleTpe  => DoubleType.!
    case t if t <:< NullTpe    => NullType.?
  }
}
