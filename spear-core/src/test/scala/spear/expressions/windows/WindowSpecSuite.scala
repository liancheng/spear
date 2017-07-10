package spear.expressions.windows

import scala.util.Try

import spear.LoggingFunSuite
import spear.exceptions.TypeMismatchException
import spear.expressions._
import spear.types.{DoubleType, IntType, StringType}

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
    Following('id.long + 1).strictlyTyped
    Preceding('id.long + "1").strictlyTyped
  }

  test("illegal window frame boundary") {
    intercept[TypeMismatchException](Preceding(true).strictlyTyped)
    intercept[TypeMismatchException](Following(true).strictlyTyped)
  }

  test("window spec") {
    def checkWindowSpec(sql: String)(spec: => WindowSpec): Unit = assertResult(Try(sql))(spec.sql)

    val rowsFrame = WindowFrame.Default.copy(frameType = RowsFrame)
    val rangeFrame = WindowFrame.Default.copy(frameType = RangeFrame)

    checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST)") {
      Window partitionBy (a, b) orderBy c.asc.nullsFirst
    }

    checkWindowSpec(s"(PARTITION BY a, b ORDER BY c ASC NULLS FIRST)") {
      Window orderBy c.asc.nullsFirst partitionBy (a, b)
    }

    checkWindowSpec(s"(ORDER BY c ASC NULLS FIRST)") {
      Window orderBy c.asc.nullsFirst
    }

    checkWindowSpec(s"(PARTITION BY a, b)") {
      Window partitionBy (a, b)
    }

    checkWindowSpec("()") {
      Window.Default
    }

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
      Window rowsBetween (UnboundedPreceding, CurrentRow)
    }

    checkWindowSpec(s"($rangeFrame)") {
      Window rangeBetween (UnboundedPreceding, CurrentRow)
    }
  }

  private val (a, b, c) = ('a of IntType, 'b of StringType, 'c of DoubleType)
}
