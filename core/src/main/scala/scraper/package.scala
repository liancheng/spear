import scala.language.implicitConversions

package object scraper {
  implicit def `String->CaseSensitiveName`(string: String): CaseSensitiveName =
    Name.caseSensitive(string)

  implicit def `Symbol->CaseInsensitiveName`(symbol: Symbol): CaseInsensitiveName =
    Name.caseInsensitive(symbol.name)

  implicit class StringToCaseSensitiveName(sc: StringContext) {
    def cs(args: Any*): CaseSensitiveName = Name.caseSensitive(sc.s(args: _*))
  }

  implicit class StringToCaseInsensitiveName(sc: StringContext) {
    def ci(args: Any*): CaseInsensitiveName = Name.caseInsensitive(sc.s(args: _*))
  }
}
