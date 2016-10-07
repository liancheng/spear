package scraper.expressions.windows

import scala.util.Try

import scraper.LoggingFunSuite
import scraper.expressions._
import scraper.types.{DoubleType, IntType, StringType}

class WindowSpecSuite extends LoggingFunSuite {
  test("rows window frame") {
    assertResult("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
      WindowFrame(RowsFrame, UnboundedPreceding, UnboundedFollowing).sql.get
    }

    assertResult("ROWS BETWEEN 10 PRECEDING AND CURRENT ROW") {
      WindowFrame(RowsFrame, Preceding(10), CurrentRow).sql.get
    }

    assertResult("ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING") {
      WindowFrame(RowsFrame, CurrentRow, Following(10)).sql.get
    }

    assertResult("ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING") {
      WindowFrame(RowsFrame, Preceding(2), Preceding(1)).sql.get
    }

    assertResult("ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING") {
      WindowFrame(RowsFrame, Following(1), Following(2)).sql.get
    }
  }

  test("range window frame") {
    assertResult("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
      WindowFrame(RangeFrame, UnboundedPreceding, UnboundedFollowing).sql.get
    }

    assertResult("RANGE BETWEEN 10 PRECEDING AND CURRENT ROW") {
      WindowFrame(RangeFrame, Preceding(10), CurrentRow).sql.get
    }

    assertResult("RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING") {
      WindowFrame(RangeFrame, CurrentRow, Following(10)).sql.get
    }

    assertResult("RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING") {
      WindowFrame(RangeFrame, Preceding(2), Preceding(1)).sql.get
    }

    assertResult("RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING") {
      WindowFrame(RangeFrame, Following(1), Following(2)).sql.get
    }
  }

  test("arbitrary expressions in window frame boundary") {
    assert(Following('id.long + 1).strictlyTyped.isSuccess)
    assert(Preceding('id.long + "1").strictlyTyped.isSuccess)
  }

  test("illegal window frame boundary") {
    assert(Preceding(true).strictlyTyped.isFailure)
    assert(Following(true).strictlyTyped.isFailure)
  }

  test("window spec") {
    val rowsFrame = WindowFrame.Default.copy(frameType = RowsFrame)
    val rangeFrame = WindowFrame.Default.copy(frameType = RangeFrame)

    def checkWindowSpec(sql: String)(spec: => WindowSpec): Unit = assertResult(Try(sql))(spec.sql)

    Seq(rowsFrame, rangeFrame) foreach { frame =>
      checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST $frame)") {
        Window partitionBy (a, b) orderBy c.asc.nullsFirst between frame
      }

      checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST $frame)") {
        Window orderBy c.asc.nullsFirst partitionBy (a, b) between frame
      }

      checkWindowSpec(s"(ORDER BY c ASC NULLS FIRST $frame)") {
        Window orderBy c.asc.nullsFirst between frame
      }

      checkWindowSpec(s"(PARTITION BY a, b $frame)") {
        Window partitionBy (a, b) between frame
      }

      checkWindowSpec(s"($frame)") {
        Window between frame
      }
    }

    checkWindowSpec(s"($rowsFrame)") {
      Window rowsBetween (UnboundedPreceding, UnboundedFollowing)
    }

    checkWindowSpec(s"($rangeFrame)") {
      Window rangeBetween (UnboundedPreceding, UnboundedFollowing)
    }
  }

  private val (a, b, c) = ('a of IntType, 'b of StringType, 'c of DoubleType)
}
