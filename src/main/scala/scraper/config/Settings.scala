package scraper.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Success, Try}

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scraper.config.Settings.Key
import scraper.exceptions.SettingsValidationException

class Settings(val config: Config) {
  def apply[T](key: Key[T]): T = (key validator (key get config)).recover {
    case cause: Throwable =>
      throw new SettingsValidationException(
        s"Configured value of settings key ${key.name} didn't pass validation: ${cause.getMessage}",
        cause
      )
  }.get

  def withValue(key: String, value: AnyRef): Settings =
    Settings(config.withValue(key, ConfigValueFactory.fromAnyRef(value)))

  def withValue[T](key: Key[T], value: T): Settings =
    withValue(key.name, value.asInstanceOf[AnyRef])
}

object Settings {
  case class Key[T](name: String, get: Config => T, validator: T => Try[T] = Success(_: T)) {
    def validate(validator: T => Try[T]): Key[T] = copy(validator = validator)
  }

  object Key {
    case class KeyBuilder(name: String) {
      def boolean: Key[Boolean] = Key(name, _ getBoolean name)
      def number: Key[Number] = Key(name, _ getNumber name)
      def string: Key[String] = Key(name, _ getString name)
      def int: Key[Int] = Key(name, _ getInt name)
      def long: Key[Long] = Key(name, _ getLong name)
      def double: Key[Double] = Key(name, _ getDouble name)
      def anyref: Key[AnyRef] = Key(name, _ getAnyRef name)

      def nanos: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.NANOSECONDS) nanos)
      def micros: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.MICROSECONDS) micros)
      def millis: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.MICROSECONDS) millis)
      def seconds: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.MICROSECONDS) seconds)
      def minutes: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.MINUTES) minutes)
      def hours: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.HOURS) hours)
      def days: Key[Duration] = Key(name, _ getDuration (name, TimeUnit.DAYS) days)
    }

    def apply(name: String): KeyBuilder = KeyBuilder(name)
  }

  val empty: Settings = new Settings(ConfigFactory.empty())

  def apply(config: Config): Settings = new Settings(config)
}
