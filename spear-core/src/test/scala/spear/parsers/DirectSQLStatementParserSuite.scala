package spear.parsers

import fastparse.core.Logger

import spear.{LoggingFunSuite, TestUtils}
import spear.expressions._
import spear.expressions.windows.{CurrentRow, UnboundedPreceding, Window, WindowFrame}
import spear.parsers.DirectSQLStatementParser.directSQLStatement
import spear.plans.logical.{let, table, values, LogicalPlan}

class DirectSQLStatementParserSuite extends LoggingFunSuite with TestUtils {
  import fastparse.all._

  testQueryParsing(
    "SELECT 1",
    values(1)
  )

  testQueryParsing(
    "SELECT 1 AS a FROM t0",
    table('t0) select (1 as 'a)
  )

  testQueryParsing(
    "SELECT * FROM t0",
    table('t0) select *
  )

  testQueryParsing(
    "SELECT t0.* FROM t0",
    table('t0) select $"t0.*"
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a",
    table('t0) subquery 'a select $"a.*"
  )

  testQueryParsing(
    "SELECT a FROM t0 WHERE a > 10",
    table('t0) filter 'a > 10 select 'a
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a",
    table('t0) select * orderBy 'a.asc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC",
    table('t0) select * orderBy 'a.asc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC NULLS FIRST",
    table('t0) select * orderBy 'a.asc.nullsFirst
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a ASC NULLS LAST",
    table('t0) select * orderBy 'a.asc.nullsLast
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC",
    table('t0) select * orderBy 'a.desc
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC NULLS FIRST",
    table('t0) select * orderBy 'a.desc.nullsFirst
  )

  testQueryParsing(
    "SELECT * FROM t0 ORDER BY a DESC NULLS LAST",
    table('t0) select * orderBy 'a.desc.nullsLast
  )

  testQueryParsing(
    "SELECT * FROM t0 WHERE a > 0 ORDER BY a",
    table('t0) filter 'a > 0 select * orderBy 'a.asc
  )

  testQueryParsing(
    "SELECT DISTINCT a FROM t0 WHERE a > 10",
    (table('t0) filter 'a > 10 select 'a).distinct
  )

  testQueryParsing(
    "SELECT * FROM t0, t1",
    table('t0) join table("t1") select *
  )

  testQueryParsing(
    "SELECT 1 AS a UNION ALL SELECT 2 AS a",
    values(1 as 'a) union values(2 as 'a)
  )

  testQueryParsing(
    "SELECT * FROM t0 INTERSECT SELECT * FROM t1",
    table('t0) select * intersect (table('t1) select *)
  )

  testQueryParsing(
    "SELECT * FROM t0 EXCEPT SELECT * FROM t1",
    table('t0) select * except (table('t1) select *)
  )

  testQueryParsing(
    "SELECT count(a) FROM t0",
    table('t0) select 'count('a)
  )

  testQueryParsing(
    "SELECT count(a) FROM t0 GROUP BY b",
    table('t0) groupBy 'b agg 'count('a)
  )

  testQueryParsing(
    "SELECT count(a) FROM t0 GROUP BY b HAVING count(b) > 0",
    table('t0) groupBy 'b agg 'count('a) filter 'count('b) > 0
  )

  testQueryParsing(
    "SELECT count(a) FROM t0 GROUP BY b ORDER BY count(b) ASC NULLS FIRST",
    table('t0) groupBy 'b agg 'count('a) orderBy 'count('b).asc.nullsFirst
  )

  testQueryParsing(
    "SELECT 1 FROM t0 ORDER BY count(1)",
    table('t0) select 1 orderBy 'count(1)
  )

  testQueryParsing(
    "SELECT 1 FROM t0 ORDER BY count(a)",
    table('t0) select 1 orderBy 'count('a)
  )

  testQueryParsing(
    "SELECT 1 FROM t0 HAVING count(1) > 1",
    table('t0) groupBy Nil agg 1 filter 'count(1) > 1
  )

  testQueryParsing(
    "SELECT 1 FROM t0 HAVING count(a) > 1",
    table('t0) groupBy Nil agg 1 filter 'count('a) > 1
  )

  testQueryParsing(
    "SELECT count(DISTINCT a) FROM t0",
    table('t0) select 'count('a).distinct
  )

  testQueryParsing(
    "SELECT t.a FROM (SELECT * FROM t0) t",
    table('t0) select * subquery 't select $"t.a"
  )

  testQueryParsing(
    "WITH c0 AS (SELECT 1) SELECT * FROM c0",
    let('c0, values(1)) {
      table('c0) select *
    }
  )

  testQueryParsing(
    "WITH c0 (n) AS (SELECT 1) SELECT * FROM c0",
    let('c0, values(1) rename 'n) {
      table('c0) select *
    }
  )

  testQueryParsing(
    "SELECT * FROM t0 JOIN t1",
    table('t0) join table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1",
    table('t0) join table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT JOIN t1",
    table('t0) leftJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LEFT OUTER JOIN t1",
    table('t0) leftJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 RIGHT OUTER JOIN t1",
    table('t0) rightJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL JOIN t1",
    table('t0) outerJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 FULL OUTER JOIN t1",
    table('t0) outerJoin table("t1") select *
  )

  testQueryParsing(
    "SELECT * FROM t0 INNER JOIN t1 ON t0.a = t1.a",
    table('t0) join table("t1") on $"t0.a" === $"t1.a" select *
  )

  testQueryParsing(
    "SELECT * FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select *
  )

  testQueryParsing(
    "SELECT a.* FROM t0 a JOIN t1 b",
    table('t0) subquery 'a join (table("t1") subquery 'b) select $"a.*"
  )

  testQueryParsing(
    "WITH c0 AS (SELECT 1), c1 AS (SELECT 2) SELECT * FROM c0 UNION ALL SELECT * FROM c1",
    let('c0, values(1)) {
      let('c1, values(2)) {
        table('c0) select * union (table('c1) select *)
      }
    }
  )

  testQueryParsing(
    "SELECT count(a) OVER () FROM t0",
    table('t0) select 'count('a).over()
  )

  testQueryParsing(
    "SELECT count(a) OVER w0 FROM t0 WINDOW w0 AS ()",
    let('w0, Window.Default) {
      table('t0) select 'count('a).over(Window('w0))
    }
  )

  testQueryParsing(
    "SELECT count(a) OVER w1 FROM t0 WINDOW w0 AS (), w1 AS (w0 ROWS UNBOUNDED PRECEDING)",
    let('w0, Window.Default) {
      let('w1, Window('w0) between WindowFrame.rowsBetween(UnboundedPreceding, CurrentRow)) {
        table('t0) select 'count('a).over(Window('w1))
      }
    }
  )

  testQueryParsing(
    "SELECT * FROM (SELECT 1 FROM t) s",
    table('t) select 1 subquery 's select *
  )

  testQueryParsing(
    "SELECT * FROM t0 LIMIT 1",
    table('t0) select * limit 1
  )

  testQueryParsing(
    "SELECT 1 -- comment",
    values(1)
  )

  testQueryParsing(
    "SELECT /* comment */ 1",
    values(1)
  )

  private def testQueryParsing(sql: String, expectedPlan: LogicalPlan): Unit = {
    test(s"parsing SQL: $sql") {
      checkPlan(parse(sql.split("\n").map(_.trim).mkString(" ")), expectedPlan)
    }
  }

  private def parse(input: String): LogicalPlan = {
    implicit val parserLogger = Logger(logInfo(_))
    (Start ~ directSQLStatement.log() ~ End parse input.trim).get.value
  }
}
