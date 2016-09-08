package scraper.repl

import scala.io.Source

import ammonite.ops.Path
import ammonite.repl.{Main => AmmoniteMain, Storage}

import scraper.Context

object Main {
  object % {
    def sql(query: String)(implicit context: Context): Unit =
      context.sql(query).show(rowCount = None, truncate = false, out = System.out)
  }

  def main(args: Array[String]) {
    val predef = {
      val contextClassLoader = Thread.currentThread().getContextClassLoader
      val stream = contextClassLoader.getResourceAsStream("predef.scala")
      Source.fromInputStream(stream, "UTF-8").mkString
    }

    AmmoniteMain(
      predef = predef,
      storageBackend = new Storage.Folder(Path.home / ".scraper"),
      welcomeBanner = Some(banner)
    ).run()
  }

  private val scalaVersion = scala.util.Properties.versionNumberString

  private val javaVersion = System.getProperty("java.version")

  private val banner =
    s"""Welcome to
       |     ____
       |    / __/__________ ____  ___ ____
       |   _\\ \\/ __/ __/ _ `/ _ \\/ -_) __/
       |  /___/\\__/_/  \\_,_/ .__/\\__/_/
       |                  /_/
       |
       |Scala version: $scalaVersion
       |Java version: $javaVersion
       |
       |The default context object is available as `context'.
       |""".stripMargin
}
