package scraper

import scraper.expressions.dsl._

class LogicalPlanSQLBuilderSuite extends SQLBuilderTest {
  ignore("single row project") {
    checkSQL(context single 1, "SELECT 1 AS `col0`")
    checkSQL(context single (1 as 'a), "SELECT 1 AS `a`")
  }

  ignore("project with limit") {
    checkSQL(context single 1 limit 1, "SELECT 1 AS `col0` LIMIT 1")
    checkSQL(context single (1 as 'a) limit 1, "SELECT 1 AS `a` LIMIT 1")
  }

  ignore("table lookup") {
    checkSQL(context table "t0", "SELECT `a` FROM `t0`")

    checkSQL(
      context table "t0" filter 'a > 3L,
      "SELECT `a` FROM `t0` WHERE (`a` > CAST(3 AS BIGINT))"
    )
  }

  ignore("join") {
    val t0 = context table "t0"
    val t1 = context table "t1"

    checkSQL(
      t0 join t1,
      "SELECT `a`, `b` FROM `t0` INNER JOIN `t1`"
    )

    checkSQL(
      t0 join t1 on 'a =:= 'b,
      "SELECT `a`, `b` FROM `t0` INNER JOIN `t1` ON (`a` = `b`)"
    )
  }
}
