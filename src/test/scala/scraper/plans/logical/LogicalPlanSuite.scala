package scraper.plans.logical

import scala.util.{Success, Try}

import scraper.expressions._
import scraper.expressions.dsl._
import scraper.plans.logical.LogicalPlanSuite.{MockExpr, MockPlan}
import scraper.types._
import scraper.{LocalCatalog, LoggingFunSuite, Row}

class LogicalPlanSuite extends LoggingFunSuite with TestUtils {
  val analyze = new Analyzer(new LocalCatalog)

  test("transformExpressionDown") {
    val plan = MockPlan(
      MockExpr(1, Seq(
        MockExpr(2, Seq(
          MockExpr(4, Nil),
          MockExpr(5, Nil)
        )),
        MockExpr(3, Seq(
          MockExpr(6, Nil),
          MockExpr(7, Nil)
        ))
      ))
    )

    checkPlan(
      MockPlan(
        MockExpr(6, Seq(
          MockExpr(11, Seq(
            MockExpr(4, Nil),
            MockExpr(5, Nil)
          )),
          MockExpr(16, Seq(
            MockExpr(6, Nil),
            MockExpr(7, Nil)
          ))
        ))
      ),
      plan.transformExpressionsDown {
        case e @ MockExpr(i, children) =>
          e.copy(literal = Literal(children.fold(i: Expression)(_ + _).evaluated))
      }
    )
  }

  test("transformExpressionUp") {
    val plan = MockPlan(
      MockExpr(1, Seq(
        MockExpr(2, Seq(
          MockExpr(4, Nil),
          MockExpr(5, Nil)
        )),
        MockExpr(3, Seq(
          MockExpr(6, Nil),
          MockExpr(7, Nil)
        ))
      ))
    )

    checkPlan(
      MockPlan(
        MockExpr(28, Seq(
          MockExpr(11, Seq(
            MockExpr(4, Nil),
            MockExpr(5, Nil)
          )),
          MockExpr(16, Seq(
            MockExpr(6, Nil),
            MockExpr(7, Nil)
          ))
        ))
      ),
      plan.transformExpressionsUp {
        case e @ MockExpr(i, children) =>
          e.copy(literal = Literal(children.fold(i: Expression)(_ + _).evaluated))
      }
    )
  }

  test("analyzer") {
    val relation = LocalRelation(
      Seq(Row(1, "a"), Row(2, "b")),
      'a.int.! :: 'b.string.? :: Nil
    )

    checkPlan(
      relation select ('b.string.?, ('a.int.! + 1) as 's),
      analyze(relation select ('b, ('a + 1) as 's))
    )
  }
}

object LogicalPlanSuite {
  case class MockExpr(literal: Literal, children: Seq[MockExpr]) extends Expression {
    override def debugString: String = s"${getClass.getSimpleName} ${literal.debugString}"

    override def dataType: DataType = literal.dataType

    override def evaluate(input: Row): Any = literal.evaluated

    override lazy val strictlyTypedForm: Try[this.type] = Success(this)
  }

  object MockExpr {
    def apply(value: Int, children: Seq[MockExpr]): MockExpr = MockExpr(Literal(value), children)
  }

  case class MockPlan(expression: Expression) extends LeafLogicalPlan {
    override def output: Seq[Attribute] = Nil

    override def nodeCaption: String = s"${getClass.getSimpleName} ${expression.nodeCaption}"
  }
}
