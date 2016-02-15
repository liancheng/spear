package scraper.plans

import scala.util.{Success, Try}

import scraper.expressions.{Attribute, Expression, LeafExpression, UnevaluableExpression}
import scraper.plans.QueryPlan.{ExpressionContainer, ExpressionString}
import scraper.reflection.constructorParams
import scraper.trees.TreeNode
import scraper.types.StructType

trait QueryPlan[Plan <: TreeNode[Plan]] extends TreeNode[Plan] { self: Plan =>
  private type Rule = PartialFunction[Expression, Expression]

  def output: Seq[Attribute]

  lazy val outputSet: Set[Attribute] = output.toSet

  lazy val schema: StructType = StructType fromAttributes output

  def references: Set[Attribute] = expressions.toSet flatMap ((_: Expression).references)

  def expressions: Seq[Expression] = productIterator.flatMap {
    case element: Expression     => Seq(element)
    case Some(e: Expression)     => Seq(e)
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
      if (e sameOrEqual transformed) e -> false else transformed -> true
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
    def expressionHolder(e: Expression): String = s"$$expr{${expressions indexOf e}}"

    // Avoids duplicating string representation of child nodes.  Replaces them with `$plan{n}`,
    // where `n` is the index or the child.
    def childHolder(child: Plan): String = s"$$plan{${children indexOf child}}"

    val argNames: List[String] = constructorParams(getClass) map (_.name.toString)

    val argValues = productIterator.toSeq map {
      case arg if children contains arg =>
        childHolder(arg.asInstanceOf[Plan])

      case arg: Seq[_] =>
        arg.map {
          case e: Expression => expressionHolder(e)
          case _             => arg.toString
        } mkString ("[", ", ", "]")

      case arg: Some[_] =>
        arg.map {
          case e: Expression => expressionHolder(e)
          case _             => arg.toString
        } mkString ("Some(", "", ")")

      case arg: Expression =>
        expressionHolder(arg)

      case arg =>
        arg.toString
    }

    (argNames, argValues).zipped map (_ + "=" + _)
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
      ExpressionContainer(expressions map ExpressionString).buildPrettyTree(
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

    override def strictlyTypedForm: Try[Expression] = Success(this)

    override def nodeCaption: String = "Expressions"
  }

  private case class ExpressionString(child: Expression)
    extends LeafExpression with UnevaluableExpression {

    override def nodeCaption: String = child.debugString
  }
}
