package scraper.parser

import org.antlr.v4.runtime.{ANTLRInputStream, IntStream}

private[parser] class CaseInsensitiveANTLRInputStream(input: String)
  extends ANTLRInputStream(input) {

  override def LA(i: Int): Int = {
    val la = super.LA(i)
    if (la == 0 || la == IntStream.EOF) la else Character.toUpperCase(la)
  }
}
