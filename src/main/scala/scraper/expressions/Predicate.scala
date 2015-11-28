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
}
