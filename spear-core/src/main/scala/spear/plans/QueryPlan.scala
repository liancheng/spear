package spear.plans

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import spear.expressions.{Alias, Attribute, Expression, ExpressionID, InternalAlias, NamedExpression}
import spear.trees.TreeNode
import spear.types.StructType

trait QueryPlan[Plan <: TreeNode[Plan]] extends TreeNode[Plan] { self: Plan =>
  def output: Seq[Attribute]

  lazy val outputSet: Set[Attribute] = output.toSet

  lazy val schema: StructType = StructType fromAttributes output

  lazy val references: Seq[Attribute] = expressions.flatMap { (_: Expression).references }.distinct

  lazy val referenceSet: Set[Attribute] = references.toSet

  lazy val referenceIDs: Set[ExpressionID] = referenceSet map { _.expressionID }

  def expressions: Seq[Expression] = productIterator.flatMap {
    case element: Expression       => element :: Nil
    case Some(element: Expression) => element :: Nil
    case element: Traversable[_]   => element collect { case e: Expression => e }
    case _                         => Nil
  }.toSeq

  def transformExpressionsDown(rule: ExpressionRule): Plan = withExpressions {
    expressions map {
      _ transformDown rule
    }
  }

  def transformExpressionsUp(rule: ExpressionRule): Plan = withExpressions {
    expressions map {
      _ transformUp rule
    }
  }

  def collectFromExpressionsDown[T](rule: PartialFunction[Expression, T]): Seq[T] = {
    val builder = ArrayBuffer.newBuilder[T]

    transformExpressionsDown {
      case e: Expression if rule isDefinedAt e =>
        builder += rule(e)
        e
    }

    builder.result()
  }

  def collectFromExpressionsUp[T](rule: PartialFunction[Expression, T]): Seq[T] = {
    val builder = ArrayBuffer.newBuilder[T]

    transformExpressionsUp {
      case e: Expression if rule isDefinedAt e =>
        builder += rule(e)
        e
    }

    builder.result()
  }

  override def nodeCaption: String = {
    val outputString = outputStrings mkString ("[", ", ", "]")
    val arrow = "\u21d2"
    Seq(super.nodeCaption, arrow, outputString) mkString " "
  }

  /**
   * Returns string representations of each output attribute of this query plan.
   */
  protected def outputStrings: Seq[String] = output map { _.debugString }

  override protected def nestedTrees: Seq[TreeNode[_]] = expressions.zipWithIndex.map {
    case (e, index) => QueryPlan.ExpressionNode(index, e)
  } ++ super.nestedTrees

  override protected def explainParams(show: Any => String): Seq[(String, String)] = {
    val remainingExpressions = mutable.Stack(expressions.indices: _*)

    super.explainParams {
      case _: Expression => s"$$${remainingExpressions.pop()}"
      case other         => other.toString
    }
  }

  private type ExpressionRule = PartialFunction[Expression, Expression]

  protected def withExpressions(newExpressions: Seq[Expression]): Plan = {
    assert(newExpressions.length == expressions.length)

    val remainingNewExpressions = newExpressions.toBuffer
    var changed = false

    def popAndCompare(e: Expression): Expression = {
      val newExpression = remainingNewExpressions.head
      remainingNewExpressions.remove(0)
      changed = changed || !(newExpression same e)
      newExpression
    }

    val newArgs = productIterator.map {
      case e: Expression =>
        popAndCompare(e)

      case Some(e: Expression) =>
        Some(popAndCompare(e))

      case arg: Traversable[_] =>
        arg.map {
          case e: Expression => popAndCompare(e)
          case other         => other
        }.toSeq

      case arg: AnyRef =>
        arg

      case null =>
        null
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }
}

object QueryPlan {
  case class ExpressionNode(index: Int, expression: Expression)
    extends TreeNode[ExpressionNode] {

    override def children: Seq[ExpressionNode] = Nil

    override def nodeCaption: String = s"$$$index: ${expression.debugString}"
  }

  def normalizeExpressionIDs[Plan <: QueryPlan[Plan]](plan: Plan): Plan = {
    // Traverses the plan tree in post-order and collects all `NamedExpression`s with an ID.
    val namedExpressionsWithID = plan.collectUp {
      case node =>
        node collectFromExpressionsUp {
          case e: NamedExpression if e.isResolved => e
          case e: Alias                           => e
          case e: InternalAlias                   => e
        }
    }.flatten.distinct

    val ids = namedExpressionsWithID.map { _.expressionID }.distinct

    val groupedByID = namedExpressionsWithID.groupBy { _.expressionID }.toSeq sortBy {
      // Sorts by ID occurrence order to ensure determinism.
      case (expressionID, _) => ids indexOf expressionID
    }

    val rewrite = for {
      ((_, namedExpressions), index) <- groupedByID.zipWithIndex.toMap
      named <- namedExpressions
    } yield (named: Expression) -> (named withID ExpressionID(index))

    plan transformDown {
      case node => node transformExpressionsDown rewrite
    }
  }
}
