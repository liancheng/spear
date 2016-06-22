import scala.language.implicitConversions

package object scraper {
  implicit def `String->CaseSensitiveName`(string: String): CaseSensitiveName =
    Name.cs(string)

  implicit def `Symbol->CaseInsensitiveName`(symbol: Symbol): CaseInsensitiveName =
    Name.ci(symbol.name)

  implicit class StringToCaseSensitiveName(sc: StringContext) {
    def cs(args: Any*): CaseSensitiveName = Name.cs(sc.s(args: _*))
  }

  implicit class StringToCaseInsensitiveName(sc: StringContext) {
    def ci(args: Any*): CaseInsensitiveName = Name.ci(sc.s(args: _*))
  }
}
