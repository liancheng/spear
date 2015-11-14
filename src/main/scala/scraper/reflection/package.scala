package scraper

import scala.reflect.runtime.universe._

import scraper.types._

package object reflection {
  def schemaOf[T: WeakTypeTag]: Schema = schemaOf(weakTypeOf[T])

  private val schemaOf: PartialFunction[Type, Schema] = (
    schemaOfUnboxedPrimitiveType
    orElse schemaOfBoxedPrimitiveType
    orElse schemaOfCompoundType
  )

  private def schemaOfCompoundType: PartialFunction[Type, Schema] = {
    case t if t <:< weakTypeOf[Option[_]] =>
      val Seq(elementType) = t.typeArgs
      Schema(schemaOf(elementType).dataType, nullable = true)

    case t if t <:< weakTypeOf[Seq[_]] || t <:< weakTypeOf[Array[_]] =>
      val Seq(elementType) = t.typeArgs
      val Schema(elementDataType, elementOptional) = schemaOf(elementType)
      Schema(ArrayType(elementDataType, elementOptional), nullable = true)

    case t if t <:< weakTypeOf[Map[_, _]] =>
      val Seq(keyType, valueType) = t.typeArgs
      val Schema(keyDataType, _) = schemaOf(keyType)
      val Schema(valueDataType, valueOptional) = schemaOf(valueType)
      Schema(MapType(keyDataType, valueDataType, valueOptional), nullable = true)

    case t if t <:< weakTypeOf[Product] =>
      val formalTypeArgs = t.typeSymbol.asClass.typeParams
      val TypeRef(_, _, actualTypeArgs) = t
      val List(constructorParams) = t.member(termNames.CONSTRUCTOR).asMethod.paramLists
      val fields = constructorParams.map { param =>
        val paramType = param.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
        val Schema(dataType, nullable) = schemaOf(paramType)
        TupleField(param.name.toString, dataType, nullable)
      }
      Schema(TupleType(fields), nullable = true)
  }

  private def schemaOfBoxedPrimitiveType: PartialFunction[Type, Schema] = {
    case t if t <:< weakTypeOf[String]            => Schema(StringType, nullable = true)
    case t if t <:< weakTypeOf[Integer]           => Schema(IntType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Boolean] => Schema(BooleanType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Byte]    => Schema(ByteType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Short]   => Schema(ShortType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Long]    => Schema(LongType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Float]   => Schema(FloatType, nullable = true)
    case t if t <:< weakTypeOf[java.lang.Double]  => Schema(DoubleType, nullable = true)
  }

  private def schemaOfUnboxedPrimitiveType: PartialFunction[Type, Schema] = {
    case t if t <:< definitions.IntTpe     => Schema(IntType, nullable = false)
    case t if t <:< definitions.BooleanTpe => Schema(BooleanType, nullable = false)
    case t if t <:< definitions.ByteTpe    => Schema(ByteType, nullable = false)
    case t if t <:< definitions.ShortTpe   => Schema(ShortType, nullable = false)
    case t if t <:< definitions.LongTpe    => Schema(LongType, nullable = false)
    case t if t <:< definitions.FloatTpe   => Schema(FloatType, nullable = false)
    case t if t <:< definitions.DoubleTpe  => Schema(DoubleType, nullable = false)
    case t if t <:< definitions.NullTpe    => Schema(NullType, nullable = true)
  }
}
