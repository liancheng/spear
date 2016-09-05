package scraper.execution

import scala.collection.mutable.ArrayBuffer

import scraper.{BasicMutableRow, LoggingFunSuite, Row}

class ProjectionSuite extends LoggingFunSuite {
  test("projection") {
    val projection = Projection(Seq(1, "foo"))
    assert(projection() == Row(1, "foo"))
  }

  test("mutable projection") {
    val row = new BasicMutableRow(ArrayBuffer(1, "foo"))
    val projection = MutableProjection(Seq(2, "bar")) target row
    assert(projection() == Row(2, "bar"))
  }
}
