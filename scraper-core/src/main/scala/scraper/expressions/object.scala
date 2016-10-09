package scraper.expressions

import scraper.Row
import scraper.expressions.typecheck.{StrictlyTyped, TypeConstraint}
import scraper.types.{DataType, ObjectType}

case class Invoke(
  target: Expression,
  methodName: String,
  override val dataType: DataType,
  args: Seq[Expression]
) extends Expression {

  override def children: Seq[Expression] = target +: args

  override def evaluate(input: Row): Any = {
    val evaluatedArgs = args map (_ evaluate input) map (_.asInstanceOf[AnyRef])
    method.invoke(target evaluate input, evaluatedArgs: _*)
  }

  override protected def typeConstraint: TypeConstraint =
    (target subtypeOf ObjectType) ++ StrictlyTyped(args)

  private lazy val targetClass: Class[_] = target.dataType match {
    case t: ObjectType => Class.forName(t.className)
  }

  private lazy val method = targetClass.getDeclaredMethods.find(_.getName == methodName).get
}

case class StaticInvoke(
  className: String,
  methodName: String,
  override val dataType: DataType,
  args: Seq[Expression]
) extends Expression {

  override def children: Seq[Expression] = args

  override def evaluate(input: Row): Any = {
    val evaluatedArgs = args map (_ evaluate input) map (_.asInstanceOf[AnyRef])
    method.invoke(null, evaluatedArgs: _*)
  }

  private lazy val targetClass: Class[_] = Class.forName(className)

  private lazy val method = targetClass.getDeclaredMethods.find(_.getName == methodName).get
}
