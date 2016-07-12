import scala.language.implicitConversions

import scraper.Name.{caseInsensitive, caseSensitive}

package object scraper {
  implicit def `String->CaseSensitiveName`(string: String): Name = caseSensitive(string)

  implicit def `Symbol->CaseInsensitiveName`(symbol: Symbol): Name = caseInsensitive(symbol.name)

  implicit class NameHelper(sc: StringContext) {
    def i(args: Any*): Name = caseInsensitive(sc.s(args: _*))
  }
}
