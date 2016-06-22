package scraper

trait Name {
  def isCaseSensitive: Boolean

  def casePreserving: String

  def caseSensitive: Name

  def caseInsensitive: Name

  override def toString: String = casePreserving
}

object Name {
  def caseSensitive(name: String): CaseSensitiveName = new CaseSensitiveName(name)

  def caseInsensitive(name: String): CaseInsensitiveName = new CaseInsensitiveName(name)
}

final class CaseSensitiveName(val casePreserving: String) extends Name {
  override def isCaseSensitive: Boolean = true

  override def caseSensitive: Name = this

  override lazy val caseInsensitive: Name = new CaseInsensitiveName(casePreserving)

  override def toString: String = casePreserving

  override def hashCode(): Int = casePreserving.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Name => this.casePreserving == that.casePreserving
    case _          => false
  }
}

final class CaseInsensitiveName(val casePreserving: String) extends Name {
  override def isCaseSensitive: Boolean = false

  override lazy val caseSensitive: Name = new CaseSensitiveName(casePreserving)

  override def caseInsensitive: Name = this

  override def toString: String = casePreserving.toLowerCase

  override def hashCode(): Int = casePreserving.toLowerCase.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: CaseInsensitiveName =>
      this.casePreserving.compareToIgnoreCase(that.casePreserving) == 0

    case that: CaseSensitiveName =>
      this.casePreserving == that.casePreserving

    case _ =>
      false
  }
}
