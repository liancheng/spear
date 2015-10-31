package scraper.shell

import jline.console.ConsoleReader
import jline.console.completer.StringsCompleter

object Shell {
  def main(args: Array[String]): Unit = {
    val reader = new ConsoleReader
    reader.setPrompt("> ")
    reader.addCompleter(new StringsCompleter("hello", "world"))

    var line: String = null
    while (line eq null) {
      line = reader.readLine()
      println(s"< $line")
    }
  }
}
