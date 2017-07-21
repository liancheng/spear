package spear

class Name(private val impl: Name.CaseSensitivityAware, val namespace: String = "") {
  def isCaseSensitive: Boolean = impl.isCaseSensitive

  def casePreserving: String = impl.casePreserving

  def withNamespace(namespace: String): Name = new Name(impl, namespace)

  override def toString: String = s"$impl${if (namespace.isEmpty) "" else s"@$namespace"}"

  override def hashCode(): Int = casePreserving.toUpperCase.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Name if this.isCaseSensitive || that.isCaseSensitive =>
      this.namespace == that.namespace && this.casePreserving == that.casePreserving

    case that: Name =>
      this.namespace == that.namespace && this.casePreserving.equalsIgnoreCase(that.casePreserving)

    case _ =>
      false
  }
}

object Name {
  def apply(name: String, isCaseSensitive: Boolean): Name =
    if (isCaseSensitive) caseSensitive(name) else caseInsensitive(name)

  def caseSensitive(name: String): Name = new Name(CaseSensitive(name))

  def caseInsensitive(name: String): Name = new Name(CaseInsensitive(name))

  private sealed trait CaseSensitivityAware {
    def isCaseSensitive: Boolean

    def casePreserving: String
  }

  private case class CaseSensitive(casePreserving: String) extends CaseSensitivityAware {
    override def isCaseSensitive: Boolean = true

    override def toString: String = quote(casePreserving)
  }

  private case class CaseInsensitive(casePreserving: String) extends CaseSensitivityAware {
    override def isCaseSensitive: Boolean = false

    override def toString: String = casePreserving
  }

  private def quote(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
}
