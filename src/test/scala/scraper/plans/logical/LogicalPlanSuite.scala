package scraper.plans.logical

import scraper.expressions._
import scraper.types.{ IntType, StringType, StructType, TestUtils }
import scraper.{ Analyzer, Row }

class LogicalPlanSuite extends TestUtils {
  test("foo") {
    val relation = LocalRelation(
      Seq(
        Row(1, "a"),
        Row(2, "b")
      ),
      StructType(
        'a -> IntType.!,
        'b -> StringType.?
      )
    )

    val project =
      Project(
        Seq(
          UnresolvedAttribute("b"),
          Alias("s", Add(UnresolvedAttribute("a"), Literal(1)))
        ),
        relation
      )

    checkPlan(
      Project(
        Seq(
          AttributeRef("b", StringType, nullable = true),
          Alias("s", Add(AttributeRef("a", IntType, nullable = false), Literal(1)))
        ),
        relation
      ),
      new Analyzer().apply(project)
    )
  }
}
