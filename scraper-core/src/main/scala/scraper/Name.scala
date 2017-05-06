package scraper

import scraper.Name.quote

class Name(private val impl: Name.CaseSensitivityAware, private val namespace: String = "") {
  def isCaseSensitive: Boolean = impl.isCaseSensitive

  def casePreserving: String = impl.casePreserving

  def withNamespace(namespace: String): Name = new Name(impl, namespace)

  override def toString: String = {
    val suffix = if (namespace.isEmpty) "" else s"@$namespace"
    (if (isCaseSensitive) quote(casePreserving) else casePreserving) + suffix
  }

  override def hashCode(): Int = casePreserving.toUpperCase.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Name if this.isCaseSensitive || that.isCaseSensitive =>
      this.namespace == that.namespace &&
        this.casePreserving == that.casePreserving

    case that: Name =>
      this.namespace == that.namespace &&
        (this.casePreserving compareToIgnoreCase that.casePreserving) == 0

    case _ =>
      false
  }
}

object Name {
  private sealed trait CaseSensitivityAware {
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

  def quote(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
}
