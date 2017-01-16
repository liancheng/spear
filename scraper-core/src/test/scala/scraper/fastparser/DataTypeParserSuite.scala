package scraper.fastparser

import fastparse.core.Logger

import scraper.types._
import scraper.{LoggingFunSuite, TestUtils}

class DataTypeParserSuite extends LoggingFunSuite with TestUtils {
  import fastparse.all._

  testDataTypeParsing("BOOLEAN", BooleanType)

  testDataTypeParsing("TINYINT", ByteType)

  testDataTypeParsing("SMALLINT", ShortType)

  testDataTypeParsing("INT", IntType)

  testDataTypeParsing("BIGINT", LongType)

  testDataTypeParsing("FLOAT", FloatType)

  testDataTypeParsing("DOUBLE", DoubleType)

  testDataTypeParsing(
    "ROW(name STRING, age INT)",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  testDataTypeParsing(
    "ROW(\"name\" STRING, \"age\" INT)",
    StructType(
      'name -> StringType.?,
      'age -> IntType.?
    )
  )

  private def testDataTypeParsing(input: String, expected: DataType): Unit = {
    test(s"parsing expression: $input") {
      checkTree(parse(input), expected)
    }
  }

  private def parse(input: String): DataType = {
    implicit val parserLogger = Logger(logInfo(_))
    (Start ~ DataTypeParser.dataType.log() ~ End parse input.trim).get.value
  }
}
