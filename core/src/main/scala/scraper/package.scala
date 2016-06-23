import scala.language.implicitConversions

package object scraper {
  implicit def `String->CaseSensitiveName`(string: String): Name =
    Name.caseSensitive(string)

  implicit def `Symbol->CaseInsensitiveName`(symbol: Symbol): Name =
    Name.caseInsensitive(symbol.name)

  implicit class StringToCaseSensitiveName(sc: StringContext) {
    def cs(args: Any*): Name = Name.caseSensitive(sc.s(args: _*))
  }

  implicit class StringToCaseInsensitiveName(sc: StringContext) {
    def ci(args: Any*): Name = Name.caseInsensitive(sc.s(args: _*))
  }
}
