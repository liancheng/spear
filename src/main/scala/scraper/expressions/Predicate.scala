package scraper.expressions

object Predicate {
  private[scraper] def splitConjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left And right => splitConjunction(left) ++ splitConjunction(right)
    case _              => predicate :: Nil
  }

  private[scraper] def splitDisjunction(predicate: Expression): Seq[Expression] = predicate match {
    case left Or right => splitDisjunction(left) ++ splitDisjunction(right)
    case _             => predicate :: Nil
  }

  // I'm surprised that this function seems to be correct since it passes the randomized test...
  def toCNF(p: Expression): Seq[Expression] = splitConjunction(p).flatMap {
    case Not(lhs Or rhs)                => toCNF(!lhs) ++ toCNF(!rhs)
    case Not(lhs And rhs)               => toCNF(!lhs || !rhs)
    case (innerLhs And innerRhs) Or rhs => toCNF(innerLhs || rhs) ++ toCNF(innerRhs || rhs)
    case lhs Or (innerLhs And innerRhs) => toCNF(innerLhs || lhs) ++ toCNF(innerRhs || lhs)
    case e                              => e :: Nil
  }
}
