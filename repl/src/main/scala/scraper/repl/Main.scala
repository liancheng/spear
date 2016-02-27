package scraper.repl

import scala.io.Source

import ammonite.repl.{Main => AmmoniteMain}

object Main {
  def main(args: Array[String]) {
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    val predefStream = contextClassLoader.getResourceAsStream("predef.scala")
    AmmoniteMain.run(predef = Source.fromInputStream(predefStream, "UTF-8").mkString)
  }
}
