package scraper

trait Name {
  def isCaseSensitive: Boolean

  def casePreserving: String

  def caseSensitive: Name

  def caseInsensitive: Name

  override def equals(other: Any): Boolean = other match {
    case that: Name if that.isCaseSensitive =>
      that.casePreserving == this.casePreserving

    case that: Name if !that.isCaseSensitive =>
      that.casePreserving.compareToIgnoreCase(this.casePreserving) == 0

    case _ =>
      false
  }

  override def toString: String = casePreserving

  override def hashCode(): Int = isCaseSensitive.hashCode() ^ casePreserving.hashCode
}

object Name {
  def cs(name: String): CaseSensitiveName = new CaseSensitiveName(name)

  def ci(name: String): CaseInsensitiveName = new CaseInsensitiveName(name)
}

final class CaseSensitiveName(val casePreserving: String) extends Name {
  override def isCaseSensitive: Boolean = true

  override def caseSensitive: Name = this

  override lazy val caseInsensitive: Name = new CaseInsensitiveName(casePreserving)
}

final class CaseInsensitiveName(val casePreserving: String) extends Name {
  override def isCaseSensitive: Boolean = false

  override lazy val caseSensitive: Name = new CaseSensitiveName(casePreserving)

  override def caseInsensitive: Name = this
}
