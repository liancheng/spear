package scraper.expressions

private[scraper] trait BinaryOperatorPattern[T <: BinaryExpression] {
  def unapply(op: T): Option[(Expression, Expression)] = Some((op.left, op.right))
}

private[scraper] object ! {
  def unapply(not: Not): Option[Expression] = Some(not.child)
}

private[scraper] object || extends BinaryOperatorPattern[Or]

private[scraper] object && extends BinaryOperatorPattern[And]

private[scraper] object === extends BinaryOperatorPattern[Eq]

private[scraper] object =/= extends BinaryOperatorPattern[NotEq]

private[scraper] object > extends BinaryOperatorPattern[Gt]

private[scraper] object < extends BinaryOperatorPattern[Lt]

private[scraper] object >= extends BinaryOperatorPattern[GtEq]

private[scraper] object <= extends BinaryOperatorPattern[LtEq]
