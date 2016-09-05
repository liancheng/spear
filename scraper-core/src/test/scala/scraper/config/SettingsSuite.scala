package scraper.config

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Try}

import com.typesafe.config.{ConfigException, ConfigFactory}

import scraper.LoggingFunSuite
import scraper.config.Settings.Key
import scraper.config.SettingsSuite._
import scraper.exceptions.SettingsValidationException

class SettingsSuite extends LoggingFunSuite {
  private val config = ConfigFactory.parseString(
    s"""$booleanKey = true
       |$numberKey = 2.5
       |$stringKey = "foo"
       |$intKey = 1
       |$longKey = 2
       |$doubleKey = 3.5
       |$anyrefKey = {
       |  some-key = "some-value"
       |}
       |$nanosKey = 1 ns
       |$microsKey = 1 us
       |$millisKey = 1 ms
       |$secondsKey = 1 s
       |$minutesKey = 1 m
       |$hoursKey = 1 h
       |$daysKey = 1 d
     """.stripMargin
  )

  private val settings = Settings(config)

  test("key accessors") {
    assert(settings(booleanKey))
    assert(settings(numberKey) == (2.5F: Number))
    assert(settings(stringKey) == "foo")
    assert(settings(intKey) == 1)
    assert(settings(longKey) == 2L)
    assert(settings(doubleKey) == 3.5D)
    assert(settings(anyrefKey) == Map("some-key" -> "some-value").asJava)
    assert(settings(nanosKey) == 1.nano)
    assert(settings(microsKey) == 1.micro)
    assert(settings(millisKey) == 1.milli)
    assert(settings(secondsKey) == 1.second)
    assert(settings(hoursKey) == 1.hour)
    assert(settings(daysKey) == 1.day)

    intercept[SettingsValidationException](settings(validatedKey))
    intercept[ConfigException.Missing](settings(invalidKey))
  }
}

object SettingsSuite {
  val booleanKey = Key("boolean-value").boolean

  val numberKey = Key("number-value").number

  val stringKey = Key("string-value").string

  val intKey = Key("int-value").int

  val longKey = Key("long-value").long

  val doubleKey = Key("double-value").double

  val anyrefKey = Key("anyref-value").anyref

  val nanosKey = Key("nanos-value").nanos

  val microsKey = Key("micros-value").micros

  val millisKey = Key("millis-value").millis

  val secondsKey = Key("seconds-value").seconds

  val minutesKey = Key("minutes-value").minutes

  val hoursKey = Key("hours-value").hours

  val daysKey = Key("days-value").days

  val validatedKey = Key("int-value").int.validate {
    Try(_) filter (_ % 2 == 0) orElse Failure(new RuntimeException("Expecting an even number."))
  }

  val invalidKey = Key("invalid").int
}
