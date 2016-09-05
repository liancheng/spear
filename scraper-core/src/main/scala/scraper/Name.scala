package scraper

import scraper.Name.quote

class Name(private val impl: Name.CaseSensitivityAware) {
  def isCaseSensitive: Boolean = impl.isCaseSensitive

  def casePreserving: String = impl.casePreserving

  override def toString: String =
    if (isCaseSensitive) quote(casePreserving) else casePreserving

  override def hashCode(): Int = casePreserving.toUpperCase.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Name if this.isCaseSensitive || that.isCaseSensitive =>
      this.casePreserving == that.casePreserving

    case that: Name =>
      this.casePreserving.compareToIgnoreCase(that.casePreserving) == 0

    case _ =>
      false
  }
}

object Name {
  private trait CaseSensitivityAware {
    def isCaseSensitive: Boolean

    def casePreserving: String
  }

  private case class CaseSensitive(casePreserving: String) extends CaseSensitivityAware {
    override def isCaseSensitive: Boolean = true
  }

  private case class CaseInsensitive(casePreserving: String) extends CaseSensitivityAware {
    override def isCaseSensitive: Boolean = false
  }

  def apply(name: String, isCaseSensitive: Boolean): Name =
    if (isCaseSensitive) caseSensitive(name) else caseInsensitive(name)

  def caseSensitive(name: String): Name = new Name(CaseSensitive(name))

  def caseInsensitive(name: String): Name = new Name(CaseInsensitive(name))

  def quote(name: String): String = "`" + name.replace("`", "``") + "`"
}
