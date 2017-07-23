package spear.repl

import ammonite.ops.{read, Path, ResourcePath, ResourceRoot}
import ammonite.runtime.Storage
import scopt._

import spear.Context
import spear.utils._

object Main {
  object % {
    def sql(query: String)(implicit context: Context): Unit =
      context.sql(query).show(rowCount = None, truncate = false, out = System.out)
  }

  case class Config(remoteLogging: Boolean = true)

  def main(args: Array[String]) {
    val optionParser = new OptionParser[Config]("spear-shell") {
      opt[Unit]("no-remote-logging")
        .text("Disable remote logging of the number of times a REPL starts and runs command")
        .action { (_, config) => config.copy(remoteLogging = false) }
    }

    for (Config(remoteLoggingEnabled) <- optionParser.parse(args, Config())) {
      import ResourceRoot._

      ammonite.Main(
        predefCode = read(ResourcePath.resource(this.getClass.getClassLoader) / "predef.scala"),
        storageBackend = new Storage.Folder(Path.home / ".spear"),
        welcomeBanner = Some(banner(remoteLoggingEnabled)),
        remoteLogging = remoteLoggingEnabled
      ).run()
    }
  }

  private val scalaVersion = scala.util.Properties.versionNumberString

  private val javaVersion = System.getProperty("java.version")

  private def banner(remoteLoggingEnabled: Boolean): String = {
    val banner =
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

    val remoteLoggingNote =
      s"""NOTE: This project uses the Ammonite REPL. Ammonite anonymously logs the times you start a
         |REPL session and the count of how many commands get run. This is to allow the author of
         |Ammonite to understand usage patterns and prioritize improvements. If you wish to disable
         |it, pass in the "--no-remote-logging" command-line flag.
         |""".oneLine

    Seq(banner) ++ Option(remoteLoggingNote).filter(_ => remoteLoggingEnabled) mkString "\n"
  }
}
