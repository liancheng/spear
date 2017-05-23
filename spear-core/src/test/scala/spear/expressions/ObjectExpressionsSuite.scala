package spear.expressions

import spear.LoggingFunSuite
import spear.types.{ByteType, IntType, ObjectType}

class ObjectExpressionsSuite extends LoggingFunSuite {
  test("static invoke") {
    assertResult(1: Integer) {
      classOf[Integer]
        .invoke("decode", IntType)
        .withArgs("1")
        .evaluated
    }
  }

  test("invoke") {
    assertResult(1: Byte) {
      Literal(1: Integer, ObjectType("java.lang.Integer"))
        .invoke("byteValue", ByteType)
        .noArgs
        .evaluated
    }
  }
}
