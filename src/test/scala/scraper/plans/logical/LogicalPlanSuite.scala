package scraper.plans.logical

import scraper.expressions._
import scraper.expressions.NamedExpression.newExpressionId
import scraper.plans.logical.LogicalPlanSuite.{ FakeExpr, FakePlan }
import scraper.types._
import scraper.{ Analyzer, LoggingFunSuite, Row }

class LogicalPlanSuite extends LoggingFunSuite with TestUtils {
  test("transformExpressionDown") {
    val plan = FakePlan(
      FakeExpr(1, Seq(
        FakeExpr(2, Seq(
          FakeExpr(4, Nil),
          FakeExpr(5, Nil)
        )),
        FakeExpr(3, Seq(
          FakeExpr(6, Nil),
          FakeExpr(7, Nil)
        ))
      ))
    )

    checkPlan(
      FakePlan(
        FakeExpr(6, Seq(
          FakeExpr(11, Seq(
            FakeExpr(4, Nil),
            FakeExpr(5, Nil)
          )),
          FakeExpr(16, Seq(
            FakeExpr(6, Nil),
            FakeExpr(7, Nil)
          ))
        ))
      ),
      plan.transformExpressionsDown {
        case e @ FakeExpr(i, children) =>
          e.copy(literal = Literal(children.fold(i: Expression)(_ + _).evaluated))
      }
    )
  }

  test("transformExpressionUp") {
    val plan = FakePlan(
      FakeExpr(1, Seq(
        FakeExpr(2, Seq(
          FakeExpr(4, Nil),
          FakeExpr(5, Nil)
        )),
        FakeExpr(3, Seq(
          FakeExpr(6, Nil),
          FakeExpr(7, Nil)
        ))
      ))
    )

    checkPlan(
      FakePlan(
        FakeExpr(28, Seq(
          FakeExpr(11, Seq(
            FakeExpr(4, Nil),
            FakeExpr(5, Nil)
          )),
          FakeExpr(16, Seq(
            FakeExpr(6, Nil),
            FakeExpr(7, Nil)
          ))
        ))
      ),
      plan.transformExpressionsUp {
        case e @ FakeExpr(i, children) =>
          e.copy(literal = Literal(children.fold(i: Expression)(_ + _).evaluated))
      }
    )
  }

  test("analyzer") {
    val relation = LocalRelation(
      Seq(
        Row(1, "a"),
        Row(2, "b")
      ),
      TupleType(
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
          AttributeRef("b", StringType, nullable = true, newExpressionId()),
          Alias(
            "s",
            Add(AttributeRef("a", IntType, nullable = false, newExpressionId()), Literal(1))
          )
        ),
        relation
      ),
      new Analyzer().apply(project)
    )
  }
}

object LogicalPlanSuite {
  case class FakeExpr(literal: Literal, children: Seq[FakeExpr]) extends Expression {
    override def caption: String = s"${getClass.getSimpleName} ${literal.caption}"

    override def dataType: DataType = IntType

    override def evaluate(input: Row): Any = literal.evaluated
  }

  object FakeExpr {
    def apply(value: Int, children: Seq[FakeExpr]): FakeExpr = FakeExpr(Literal(value), children)
  }

  case class FakePlan(expression: Expression) extends LeafLogicalPlan {
    override def output: Seq[Attribute] = Nil

    override def caption: String = s"${getClass.getSimpleName} ${expression.caption}"
  }
}
