package scraper.plans

import scraper.expressions._
import scraper.plans.QueryPlan.{ExpressionContainer, ExpressionString}
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
    case element: Expression     => Seq(element)
    case element: Option[_]      => element collect { case e: Expression => e }
    case element: Traversable[_] => element collect { case e: Expression => e }
    case _                       => Nil
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

  /**
   * Returns string representations of each constructor arguments of this query plan
   */
  protected def argStrings: Seq[String] = {
    // Avoids duplicating string representation of expression nodes.  Replaces them with `$expr{n}`,
    // where `n` is the index or the expression.
    def expressionHolder(e: Expression): String = s"$$${expressions indexOf e}"

    // String representations of values of all constructor arguments except those that are children
    // of this query plan.
    val argValues = productIterator.toSeq map {
      case arg if children contains arg =>
        None

      case arg: Seq[_] =>
        val values = arg flatMap {
          case e: Expression                  => Some(expressionHolder(e))
          case plan if children contains plan => None
          case _                              => Some(arg.toString)
        }
        if (values.isEmpty) None else Some(values)

      case arg: Some[_] =>
        val value = arg flatMap {
          case e: Expression                  => Some(expressionHolder(e))
          case plan if children contains plan => None
          case _                              => Some(arg.toString)
        }
        if (value.isEmpty) None else value mkString ("Some(", "", ")")

      case arg: Expression =>
        Some(expressionHolder(arg))

      case arg =>
        Some(arg.toString)
    }

    val argNames: List[String] = constructorParams(getClass) map (_.name.toString)

    argNames zip argValues collect { case (name, Some(arg)) => s"$name=$arg" }
  }

  /**
   * Returns string representations of each output attribute of this query plan.
   */
  protected def outputStrings: Seq[String] = output map (_.debugString)

  override def nodeCaption: String = {
    val argsString = argStrings mkString ", "
    val outputString = outputStrings mkString ("[", ", ", "]")
    s"$nodeName: $argsString ==> $outputString"
  }

  override private[scraper] def buildPrettyTree(
    depth: Int, lastChildren: scala.Seq[Boolean], builder: scala.StringBuilder
  ): scala.StringBuilder = {
    val pipe = "\u2502"
    val tee = "\u251c"
    val corner = "\u2570"
    val bar = "\u2574"

    if (depth > 0) {
      lastChildren.init foreach (isLast => builder ++= (if (isLast) "  " else s"$pipe "))
      builder ++= (if (lastChildren.last) s"$corner$bar" else s"$tee$bar")
    }

    builder ++= nodeCaption
    builder ++= "\n"

    if (expressions.nonEmpty) {
      val expressionStrings = expressions.zipWithIndex.map {
        case (expression, index) => ExpressionString(expression, index)
      }

      ExpressionContainer(expressionStrings).buildPrettyTree(
        depth + 2, lastChildren :+ children.isEmpty :+ true, builder
      )
    }

    if (children.nonEmpty) {
      children.init foreach (_ buildPrettyTree (depth + 1, lastChildren :+ false, builder))
      children.last buildPrettyTree (depth + 1, lastChildren :+ true, builder)
    }

    builder
  }
}

object QueryPlan {
  private case class ExpressionContainer(children: Seq[Expression])
    extends Expression with UnevaluableExpression {

    override def nodeCaption: String = "Expressions"
  }

  private case class ExpressionString(child: Expression, index: Int)
    extends LeafExpression with UnevaluableExpression {

    override def nodeCaption: String = s"$$$index: ${child.debugString}"
  }
}
