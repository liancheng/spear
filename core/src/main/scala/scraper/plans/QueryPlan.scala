package scraper.plans

import scraper.expressions._
import scraper.plans.QueryPlan.ExpressionString
import scraper.reflection.constructorParams
import scraper.trees.TreeNode
import scraper.types.StructType

trait QueryPlan[Plan <: TreeNode[Plan]] extends TreeNode[Plan] { self: Plan =>
  private type Rule = PartialFunction[Expression, Expression]

  def output: Seq[Attribute]

  lazy val outputSet: Set[Attribute] = output.toSet

  lazy val outputIDSet: Set[ExpressionID] = outputSet map (_.expressionID)

  lazy val schema: StructType = StructType fromAttributes output

  lazy val references: Set[Attribute] = expressions.toSet flatMap ((_: Expression).references)

  lazy val referenceIDs: Set[ExpressionID] = references map (_.expressionID)

  def expressions: Seq[Expression] = productIterator.flatMap {
    case element: Expression       => element :: Nil
    case Some(element: Expression) => element :: Nil
    case element: Traversable[_]   => element collect { case e: Expression => e }
    case _                         => Nil
  }.toSeq

  def transformAllExpressions(rule: Rule): Plan = transformDown {
    case plan: QueryPlan[_] =>
      plan.transformExpressionsDown(rule).asInstanceOf[Plan]
  }

  def transformExpressionsDown(rule: Rule): Plan = transformExpressions(rule, _ transformDown _)

  def transformExpressionsUp(rule: Rule): Plan = transformExpressions(rule, _ transformUp _)

  protected def transformExpressions(rule: Rule, next: (Expression, Rule) => Expression): Plan = {
    def applyRule(e: Expression): (Expression, Boolean) = {
      val transformed = next(e, rule)
      if (e same transformed) e -> false else transformed -> true
    }

    val (newArgs, argsChanged) = productIterator.map {
      case e: Expression =>
        applyRule(e)

      case Some(e: Expression) =>
        val (ruleApplied, changed) = applyRule(e)
        Some(ruleApplied) -> changed

      case arg: Traversable[_] =>
        val (newElements, elementsChanged) = arg.map {
          case e: Expression => applyRule(e)
          case e             => e -> false
        }.unzip
        newElements -> (elementsChanged exists (_ == true))

      case arg: AnyRef =>
        arg -> false
    }.toSeq.unzip

    if (argsChanged contains true) makeCopy(newArgs) else this
  }

  // TODO We can probably replace this with annotation tricks
  /**
   * Returns optional string representations of all constructor argument values in declaration
   * order.  If an argument value is mapped to `None`, it won't be shown in the plan tree.  By
   * default, constructor arguments corresponding to children plans are hidden.
   */
  protected def argValueStrings: Seq[Option[String]] = {
    // Avoids duplicating string representation of expression nodes.  Replaces them with `$n`,
    // where `n` is the index of the expression.
    def expressionHolder(e: Expression): String = s"$$${expressions indexOf e}"

    productIterator.toSeq map {
      case arg if children contains arg =>
        None

      case arg: Seq[_] =>
        val values = arg map {
          case e: Expression                  => Some(expressionHolder(e))
          case plan if children contains plan => None
          case _                              => Some(arg.toString)
        }
        if (values forall (_.nonEmpty)) Some(values.flatten mkString ("[", ", ", "]")) else None

      case None =>
        None

      case arg: Some[_] =>
        val value = arg flatMap {
          case e: Expression                  => Some(expressionHolder(e))
          case plan if children contains plan => None
          case _                              => Some(arg.toString)
        }
        if (value.nonEmpty) Some(value mkString ("Some(", "", ")")) else None

      case arg: Expression =>
        Some(expressionHolder(arg))

      case arg =>
        Some(arg.toString)
    }
  }

  /**
   * Returns string representations of constructor arguments of this query plan in the form of
   * `&lt;arg&gt;=&lt;value&gt;`.
   */
  protected def argStrings: Seq[String] = {
    val argNames: List[String] = constructorParams(getClass) map (_.name.toString)
    argNames zip argValueStrings collect { case (name, Some(arg)) => s"$name=$arg" }
  }

  /**
   * Returns string representations of each output attribute of this query plan.
   */
  protected def outputStrings: Seq[String] = output map (_.debugString)

  override def nodeCaption: String = {
    val argsString = argStrings mkString ", "
    val outputString = outputStrings mkString ("[", ", ", "]")
    val arrow = "\u21d2"
    s"$nodeName: $argsString $arrow $outputString"
  }

  override protected def buildVirtualTreeNodes(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder
  ): Unit = if (expressions.nonEmpty) {
    val expressionStrings = expressions.zipWithIndex.map {
      case (expression, index) => ExpressionString(expression, index)
    }

    expressionStrings.foreach {
      _.buildPrettyTree(depth + 1, lastChildren :+ children.isEmpty, builder)
    }
  }
}

object QueryPlan {
  private case class ExpressionString(child: Expression, index: Int)
    extends LeafExpression with UnevaluableExpression {

    override def nodeCaption: String = s"$$$index: ${child.debugString}"
  }
}
