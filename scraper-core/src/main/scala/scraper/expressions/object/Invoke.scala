package scraper.expressions.`object`

import scraper.Row
import scraper.expressions.Expression
import scraper.expressions.typecheck.TypeConstraint
import scraper.types.{DataType, ObjectType}

case class Invoke(
  target: Expression,
  methodName: String,
  override val dataType: DataType,
  args: Seq[Expression]
) extends Expression {
  override def children: Seq[Expression] = target +: args

  override def evaluate(input: Row): Any = {
    val evaluatedTarget = target evaluate input
    val evaluatedArgs = args.map(_.evaluate(input).asInstanceOf[AnyRef])
    method.invoke(evaluatedTarget, evaluatedArgs: _*)
  }

  override protected lazy val typeConstraint: TypeConstraint =
    (target subtypeOf ObjectType) ++ args.pass

  private lazy val objectClass = target.dataType match {
    case ObjectType(className) => Class.forName(className)
  }

  private lazy val method = {
    val candidates = objectClass.getMethods.filter(_.getName == methodName)
    require(candidates.length == 1)
    candidates.head
  }
}
