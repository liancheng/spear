import scala.language.implicitConversions

package object scraper {
  implicit def `String->CaseSensitiveName`(string: String): CaseSensitiveName =
    Name.cs(string)

  implicit def `Symbol->CaseInsensitiveName`(symbol: Symbol): CaseInsensitiveName =
    Name.ci(symbol.name)
}
