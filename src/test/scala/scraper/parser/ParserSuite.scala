package scraper.parser

import scraper.expressions.{ Alias, Literal }
import scraper.plans.logical.{ Project, UnresolvedRelation }
import scraper.types.{ IntType, TestUtils }

class ParserSuite extends TestUtils {
  test("simple query") {
    assertSideBySide(
      new Parser().parse(" SELECT 1 AS a FROM t"),
      Project(Alias("a", Literal(1, IntType))() :: Nil, UnresolvedRelation("t"))
    )
  }
}
