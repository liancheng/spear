package spear.plans.logical.analysis

import spear._
import spear.exceptions.IllegalAggregationException
import spear.expressions._
import spear.expressions.aggregates.AggregateFunction
import spear.expressions.functions._
import spear.expressions.windows.Window
import spear.plans.logical.{table, LocalRelation}
import spear.plans.logical.analysis.AggregationAnalysis.collectAggregateFunctions
import spear.types.{IntType, LongType}

class AggregationAnalysisSuite extends AnalyzerTest { self =>
  test("global aggregate") {
    val `@A: count(a)` = AggregateFunctionAlias(count(self.a of 't))

    checkSQLAnalysis(
      "SELECT count(a) FROM t",

      table('t) select 'count('a),

      relation
        aggregate (Nil, `@A: count(a)` :: Nil)
        select (`@A: count(a)`.attr as "count(t.a)")
    )
  }

  test("global aggregate - only the HAVING clause contains an aggregate function") {
    val `@A: count(a)` = AggregateFunctionAlias(count(self.a of 't))

    checkSQLAnalysis(
      "SELECT 1 AS out FROM t HAVING count(a) > 0",

      table('t)
        groupBy Nil
        agg (1 as 'out)
        filter 'count('a) > 0,

      relation
        aggregate (Nil, `@A: count(a)` :: Nil)
        filter `@A: count(a)`.attr > (0 cast LongType)
        select (1 as 'out)
    )
  }

  test("global aggregate - only the ORDER BY clause contains an aggregate function") {
    val `@A: count(a)` = AggregateFunctionAlias(count(self.a of 't))
    val `@S: count(a)` = SortOrderAlias(`@A: count(a)`.attr, "order0")
    val `1 AS out` = 1 as 'out

    checkSQLAnalysis(
      "SELECT 1 AS out FROM t ORDER BY count(a)",

      table('t) select (1 as 'out) orderBy 'count('a),

      relation
        aggregate (Nil, `@A: count(a)` :: Nil)
        sort `@A: count(a)`.attr.asc
        select (`1 AS out`, `@S: count(a)`)
        select `1 AS out`.attr
    )
  }

  test("global aggregate - the ORDER BY clause contains a count(1)") {
    val `@A: count(1)` = AggregateFunctionAlias(count(1))
    val `@S: count(1)` = SortOrderAlias(`@A: count(1)`.attr, "order0")
    val `1 AS out` = 1 as 'out

    checkSQLAnalysis(
      "SELECT 1 AS out FROM t ORDER BY count(1)",

      table('t) select (1 as 'out) orderBy 'count(1),

      relation
        aggregate (Nil, `@A: count(1)` :: Nil)
        sort `@A: count(1)`.attr.asc
        select (`1 AS out`, `@S: count(1)`)
        select `1 AS out`.attr
    )
  }

  test("global aggregate - only the ORDER BY and HAVING clauses contain aggregate functions") {
    val `@A: count(a)` = AggregateFunctionAlias(count(self.a of 't))
    val `@A: max(a)` = AggregateFunctionAlias(max(self.a of 't))

    checkSQLAnalysis(
      "SELECT 1 AS out FROM t HAVING max(a) > 0 ORDER BY count(a)",

      table('t) groupBy Nil agg (1 as 'out) filter 'max('a) > 0 orderBy 'count('a),

      relation
        aggregate (Nil, Seq(`@A: max(a)`, `@A: count(a)`))
        filter `@A: max(a)`.attr > 0
        sort `@A: count(a)`.attr.asc
        select (1 as 'out)
    )
  }

  test("aggregate with ORDER BY clause referencing projected attribute") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(b)` = AggregateFunctionAlias(count(self.b))

    checkSQLAnalysis(
      "SELECT count(b) AS c FROM t GROUP BY a ORDER BY c DESC",

      table('t)
        groupBy 'a
        agg ('count('b) as 'c)
        orderBy 'c.desc,

      relation
        aggregate (`@G: a` :: Nil, `@A: count(b)` :: Nil)
        sort `@A: count(b)`.attr.desc
        select (`@A: count(b)`.attr as 'c)
    )
  }

  test("aggregate with both HAVING and ORDER BY clauses") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(b)` = AggregateFunctionAlias(count(self.b))

    checkSQLAnalysis(
      "SELECT a FROM t GROUP BY a HAVING a > 1 ORDER BY count(b) ASC",

      table('t) groupBy 'a agg 'a filter 'a > 1 orderBy 'count('b).asc,

      relation
        aggregate (`@G: a` :: Nil, `@A: count(b)` :: Nil)
        filter `@G: a`.attr > 1
        sort `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a withID a.expressionID)
    )
  }

  test("aggregate with multiple ORDER BY clauses") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(b)` = AggregateFunctionAlias(count(self.b))

    checkAnalyzedPlan(
      table('t)
        groupBy 'a
        agg 'count('b)
        sort 'a.asc
        sort 'count('b).asc,

      relation
        aggregate (`@G: a` :: Nil, `@A: count(b)` :: Nil)
        // Only the last sort order should be preserved
        sort `@A: count(b)`.attr.asc
        select (`@A: count(b)`.attr as "count(t.b)")
    )
  }

  test("aggregate with multiple HAVING conditions") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(b)` = AggregateFunctionAlias(count(self.b))

    checkAnalyzedPlan(
      table('t)
        groupBy 'a
        agg 'count('b)
        filter 'a > 1
        filter 'count('b) < 3L,

      relation
        aggregate (`@G: a` :: Nil, `@A: count(b)` :: Nil)
        // All `HAVING` conditions should be preserved
        filter `@G: a`.attr > 1 && `@A: count(b)`.attr < 3L
        select (`@A: count(b)`.attr as "count(t.b)")
    )
  }

  test("aggregate with multiple alternating HAVING and ORDER BY clauses") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(b)` = AggregateFunctionAlias(count(self.b))

    checkAnalyzedPlan(
      table('t)
        groupBy 'a
        agg 'a
        filter 'a > 1
        sort 'a.asc
        filter 'count('b) < 10L
        sort 'count('b).asc,

      relation
        aggregate (`@G: a` :: Nil, `@A: count(b)` :: Nil)
        filter `@G: a`.attr > 1 && (`@A: count(b)`.attr < 10L)
        sort `@A: count(b)`.attr.asc
        select (`@G: a`.attr as 'a withID a.expressionID)
    )
  }

  test("aggregate with count(*)") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@A: count(1)` = AggregateFunctionAlias(count(1))

    checkSQLAnalysis(
      "SELECT count(*) FROM t GROUP BY a",

      table('t) groupBy 'a agg 'count(*),

      relation
        aggregate (`@G: a` :: Nil, `@A: count(1)` :: Nil)
        select (`@A: count(1)`.attr as i"count(1)")
    )
  }

  test("illegal SELECT field") {
    val patterns = Seq(
      "Attribute t.a",
      "SELECT field (((t.a + 1) + t.a) + 1)",
      "[(t.a + 1)]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a + 1 agg 'a + 1 + 'a + 1)
    }
  }

  test("illegal aggregate function in grouping key") {
    val patterns = Seq("Aggregate functions are not allowed in grouping keys")

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'count('a) agg 'count(*))
    }
  }

  test("illegal HAVING condition") {
    val patterns = Seq(
      "Attribute t.b",
      "HAVING condition (t.b > 0)",
      "[t.a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a agg 'count('a) filter 'b > 0)
    }
  }

  test("illegal ORDER BY expression") {
    val patterns = Seq(
      "Attribute t.b",
      "ORDER BY expression t.b ASC NULLS LAST",
      "[t.a]"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy 'a agg 'count('a) sort 'b)
    }
  }

  test("illegal nested aggregate function") {
    val patterns = Seq(
      "Aggregate function can't be nested within another aggregate function",
      "max(count(t.a))"
    )

    checkMessage[IllegalAggregationException](patterns: _*) {
      analyze(table('t) groupBy Nil agg 'max('count('a)))
    }
  }

  test("distinct") {
    val `@G: a` = GroupingKeyAlias(self.a)
    val `@G: b` = GroupingKeyAlias(self.b)

    checkSQLAnalysis(
      "SELECT DISTINCT * FROM t",

      (table('t) select *).distinct,

      relation
        select (a, b)
        aggregate (`@G: a` :: `@G: b` :: Nil, Nil)
        select (
          `@G: a`.attr as 'a withID a.expressionID,
          `@G: b`.attr as 'b withID b.expressionID
        )
    )
  }

  test("collect aggregate functions") {
    def check(expected: AggregateFunction*)(actual: => Seq[AggregateFunction]): Unit = {
      assertResult(expected.toSet) {
        actual.toSet
      }
    }

    val a = 'a of IntType

    check(max(a)) {
      collectAggregateFunctions(max(a))
    }

    check() {
      collectAggregateFunctions(max(a) over ())
    }

    check(avg(a)) {
      collectAggregateFunctions(max(avg(a)) over ())
    }

    check(sum(a)) {
      collectAggregateFunctions(max(a) over (Window partitionBy sum(a)))
    }

    check(min(a)) {
      collectAggregateFunctions(max(a) over (Window orderBy min(a)))
    }

    check(sum(a), min(a)) {
      collectAggregateFunctions(max(a) over (Window partitionBy sum(a) orderBy min(a)))
    }

    check(avg(a), sum(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window partitionBy sum(a)))
    }

    check(avg(a), min(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window orderBy min(a)))
    }

    check(avg(a), sum(a), min(a)) {
      collectAggregateFunctions(max(avg(a)) over (Window partitionBy sum(a) orderBy min(a)))
    }

    check(avg(a), count(a), sum(a), min(a)) {
      collectAggregateFunctions(
        max(avg(a) + count(a)) over (Window partitionBy sum(a) orderBy min(a))
      )
    }
  }

  override protected def afterAll(): Unit = catalog.removeRelation('t)

  private val relation = {
    catalog.registerRelation('t, LocalRelation.empty('a.int.!, 'b.string.?))
    catalog lookupRelation 't
  }

  private val Seq(a: AttributeRef, b: AttributeRef) = relation.output
}
