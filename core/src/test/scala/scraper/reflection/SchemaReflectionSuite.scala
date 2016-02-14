package scraper.reflection

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import scraper.{TestUtils, LoggingFunSuite}
import scraper.reflection.SchemaReflectionSuite._
import scraper.types._

class SchemaReflectionSuite extends LoggingFunSuite with TestUtils {
  private def testType[T: WeakTypeTag](kind: String)(expected: FieldSpec): Unit = {
    val className = implicitly[WeakTypeTag[T]].tpe.toString
    test(s"schema inference - $kind - $className") {
      checkTree(
        StructType('inferred -> expected),
        StructType('inferred -> fieldSpecFor[T])
      )
    }
  }

  private def testUnboxedPrimitive[T: WeakTypeTag](expected: FieldSpec) =
    testType[T]("unboxed primitive")(expected)

  private def testBoxedPrimitive[T: WeakTypeTag](expected: FieldSpec) =
    testType[T]("boxed primitive")(expected)

  private def testArrayType[T: WeakTypeTag](expected: FieldSpec) =
    testType[T]("array type")(expected)

  private def testMapType[T: WeakTypeTag](expected: FieldSpec) =
    testType[T]("map type")(expected)

  private def testStructType[T: WeakTypeTag](expected: FieldSpec) =
    testType[T]("struct type")(expected)

  testUnboxedPrimitive[Boolean] { BooleanType.! }
  testUnboxedPrimitive[Byte] { ByteType.! }
  testUnboxedPrimitive[Short] { ShortType.! }
  testUnboxedPrimitive[Int] { IntType.! }
  testUnboxedPrimitive[Long] { LongType.! }
  testUnboxedPrimitive[Float] { FloatType.! }
  testUnboxedPrimitive[Double] { DoubleType.! }

  testUnboxedPrimitive[Null] { NullType }

  testBoxedPrimitive[java.lang.Boolean] { BooleanType }
  testBoxedPrimitive[java.lang.Byte] { ByteType }
  testBoxedPrimitive[java.lang.Short] { ShortType }
  testBoxedPrimitive[java.lang.Integer] { IntType }
  testBoxedPrimitive[java.lang.Long] { LongType }
  testBoxedPrimitive[java.lang.Float] { FloatType }
  testBoxedPrimitive[java.lang.Double] { DoubleType }

  testArrayType[Seq[Int]] {
    ArrayType(IntType.!)
  }

  testArrayType[Seq[Integer]] {
    ArrayType(IntType)
  }

  testArrayType[Seq[Map[Int, String]]] {
    ArrayType(MapType(IntType, StringType.?).?).?
  }

  testArrayType[Array[Int]] {
    ArrayType(IntType.!)
  }

  testArrayType[Array[Integer]] {
    ArrayType(IntType)
  }

  testArrayType[Array[Map[Int, String]]] {
    ArrayType(MapType(IntType, StringType))
  }

  testMapType[Map[Int, Long]] {
    MapType(IntType, LongType.!)
  }

  testMapType[Map[Int, java.lang.Long]] {
    MapType(IntType, LongType)
  }

  testMapType[Map[Int, String]] {
    MapType(IntType, StringType)
  }

  testMapType[Map[Int, Array[Double]]] {
    MapType(IntType, ArrayType(DoubleType.!))
  }

  testStructType[(Int, Double)] {
    StructType(
      '_1 -> IntType.!,
      '_2 -> DoubleType.!
    )
  }

  testStructType[(Int, java.lang.Double)] {
    StructType(
      '_1 -> IntType.!,
      '_2 -> DoubleType
    )
  }

  testStructType[(Int, String, Seq[Map[Int, String]])] {
    StructType(
      '_1 -> IntType.!,
      '_2 -> StringType.?,
      '_3 -> ArrayType(MapType(IntType, StringType))
    )
  }

  testStructType[CaseClass1] {
    StructType('f1 -> IntType.!)
  }

  testStructType[CaseClass2] {
    StructType(
      'f1 -> IntType.!,
      'f2 -> DoubleType
    )
  }

  testStructType[CaseClass3] {
    StructType(
      'f1 -> IntType.!,
      'f2 -> DoubleType
    )
  }

  testStructType[CaseClass4] {
    StructType(
      'f1 -> IntType.!,
      'f2 -> StringType,
      'f3 -> MapType(IntType, StringType)
    )
  }

  testStructType[CaseClass5] {
    StructType(
      'f1 -> IntType.!,
      'f2 -> StructType(
        'f1 -> IntType.!,
        'f2 -> StringType,
        'f3 -> MapType(IntType, StringType)
      )
    )
  }

  testStructType[CaseClass6] {
    StructType(
      'f1 -> IntType.!,
      'f2 -> StructType(
        '_1 -> StringType,
        '_2 -> ArrayType(IntType.!)
      )
    )
  }
}

object SchemaReflectionSuite {
  private case class CaseClass1(f1: Int)
  private case class CaseClass2(f1: Int, f2: Option[Double])
  private case class CaseClass3(f1: Int, f2: java.lang.Double)
  private case class CaseClass4(f1: Int, f2: String, f3: Map[Int, String])
  private case class CaseClass5(f1: Int, f2: CaseClass4)
  private case class CaseClass6(f1: Int, f2: (String, Array[Int]))
}
