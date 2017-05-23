package spear.expressions

private[spear] trait BinaryOperatorPattern[T <: BinaryExpression] {
  def unapply(op: T): Option[(Expression, Expression)] = Some((op.left, op.right))
}

private[spear] object ! {
  def unapply(not: Not): Option[Expression] = Some(not.child)
}

private[spear] object || extends BinaryOperatorPattern[Or]

private[spear] object && extends BinaryOperatorPattern[And]

private[spear] object === extends BinaryOperatorPattern[Eq]

private[spear] object =/= extends BinaryOperatorPattern[NotEq]

private[spear] object > extends BinaryOperatorPattern[Gt]

private[spear] object < extends BinaryOperatorPattern[Lt]

private[spear] object >= extends BinaryOperatorPattern[GtEq]

private[spear] object <= extends BinaryOperatorPattern[LtEq]
