package scraper.plans.logical

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.Checkers

import scraper.expressions._
import scraper.expressions.dsl._
import scraper.generators.genRandomPartitions
import scraper.plans.logical.LogicalPlanSuite.{ExprNode, PlanNode}
import scraper.types._
import scraper.{LocalCatalog, LoggingFunSuite, Row}

class LogicalPlanSuite extends LoggingFunSuite with TestUtils with Checkers {
  private val analyze = new Analyzer(new LocalCatalog)

  def genMockExpr: Gen[ExprNode] = Gen.sized {
    case size if size < 2 => ExprNode(1, Nil)
    case size =>
      for {
        width <- Gen choose (1, size - 1)
        childrenSizes <- genRandomPartitions(size - 1, width)
        children <- Gen sequence (childrenSizes map (Gen.resize(_, genMockExpr)))
      } yield ExprNode(1, children.asScala)
  }

  implicit val argExprNode = Arbitrary(genMockExpr)

  test("transformExpressionDown") {
    check {
      PlanNode(_: ExprNode).transformExpressionsDown {
        case e @ ExprNode(_, children) =>
          e.copy(value = children.map(_.value).sum)
      } match {
        case plan: PlanNode =>
          plan.expression.forall {
            case ExprNode(value, Nil)      => value == 0
            case ExprNode(value, children) => value == children.size
          }
      }
    }
  }

  test("transformExpressionUp") {
    check {
      PlanNode(_: ExprNode).transformExpressionsUp {
        case e @ ExprNode(_, children) =>
          e.copy(value = children.map(_.value).sum)
      } match {
        case plan: PlanNode =>
          plan.expression.forall {
            case ExprNode(value, _) => value == 0
          }
      }
    }
  }

  test("analyzer") {
    val relation = LocalRelation.empty('a.int.! :: 'b.string.? :: Nil)

    checkPlan(
      analyze(relation select ('b, ('a + 1) as 's)),
      relation select ('b.string.?, ('a.int.! + 1) as 's)
    )
  }

  test("star") {
    val relation = LocalRelation.empty('a.int.! :: 'b.string.? :: Nil)

    checkPlan(
      analyze(relation select '*),
      relation select ('a.int.!, 'b.string.?)
    )
  }
}

object LogicalPlanSuite {
  case class ExprNode(value: Int, children: Seq[ExprNode]) extends Expression {
    override def dataType: DataType = IntType

    override def evaluate(input: Row): Any = value

    override lazy val strictlyTypedForm: Try[this.type] = Success(this)
  }

  case class PlanNode(expression: Expression) extends LeafLogicalPlan {
    override def output: Seq[Attribute] = Nil

    override def nodeCaption: String = s"${getClass.getSimpleName} ${expression.nodeCaption}"
  }
}
