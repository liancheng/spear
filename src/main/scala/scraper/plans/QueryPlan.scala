package scraper.plans

import scraper.expressions.{Attribute, Expression}
import scraper.trees.TreeNode
import scraper.types.StructType

trait QueryPlan[Plan <: TreeNode[Plan]] extends TreeNode[Plan] { self: Plan =>
  private type Rule = PartialFunction[Expression, Expression]

  def output: Seq[Attribute]

  lazy val schema: StructType = StructType fromAttributes output

  def references: Set[Attribute] = expressions.toSet.flatMap((_: Expression).references)

  def expressions: Seq[Expression] = productIterator.flatMap {
    case element: Expression     => Seq(element)
    case Some(e: Expression)     => Seq(e)
    case element: Traversable[_] => element.collect { case e: Expression => e }
    case _                       => Nil
  }.toSeq

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

  protected def argsString: String = productIterator.toSeq map {
    case arg if children contains arg =>
      s"$$${children indexOf arg}"

    case arg: Seq[_] =>
      arg.map {
        case e: Expression => e.debugString
        case _             => arg.toString
      } mkString ("Seq(", ", ", ")")

    case arg: Some[_] =>
      arg.map {
        case e: Expression => e.debugString
        case _             => arg.toString
      } mkString ("Some(", "", ")")

    case arg: Expression =>
      arg.debugString

    case arg =>
      arg.toString
  } mkString ("[", ", ", "]")

  protected def outputString: String = output map (_.debugString) mkString ("[", ", ", "]")

  override def nodeCaption: String = s"$nodeName args=$argsString output=$outputString"
}
