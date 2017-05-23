package spear.repl

import scala.io.Source

import ammonite.ops.Path
import ammonite.runtime.Storage

import spear.Context

object Main {
  object % {
    def sql(query: String)(implicit context: Context): Unit =
      context.sql(query).show(rowCount = None, truncate = false, out = System.out)
  }

  def main(args: Array[String]) {
    val predef = {
      val stream = getClass.getClassLoader.getResourceAsStream("predef.scala")
      Source.fromInputStream(stream, "UTF-8").mkString
    }

    ammonite.Main(
      predef = predef,
      storageBackend = new Storage.Folder(Path.home / ".spear"),
      welcomeBanner = Some(banner)
    ).run()
  }

  private val scalaVersion = scala.util.Properties.versionNumberString

  private val javaVersion = System.getProperty("java.version")

  private val banner =
    raw"""Welcome to
         |   ____
         |  / __/__  ___ ___ _____
         | _\ \/ _ \/ -_) _ `/ __/
         |/___/ .__/\__/\_,_/_/
         |   /_/
         |
         |Scala version: $scalaVersion
         |Java version: $javaVersion
         |
         |The default context object is available as `context'.
         |""".stripMargin
}
