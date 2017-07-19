package spear.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.util.{Success, Try}
import scala.util.control.NonFatal

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import spear.config.Settings.Key
import spear.exceptions.SettingsValidationException

class Settings(val config: Config) {
  def apply[T](key: Key[T]): T = (key validator (key get config)).recover {
    case NonFatal(cause) =>
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

    def map[U](f: T => U): Key[U] = copy(get = get andThen f, validator = Success(_: U))

    override def toString: String = name
  }

  object Key {
    case class KeyBuilder(name: String) {
      def boolean: Key[Boolean] = Key[Boolean](name, _ getBoolean name)
      def number: Key[Number] = Key[Number](name, _ getNumber name)
      def string: Key[String] = Key[String](name, _ getString name)
      def int: Key[Int] = Key[Int](name, _ getInt name)
      def long: Key[Long] = Key[Long](name, _ getLong name)
      def double: Key[Double] = Key[Double](name, _ getDouble name)
      def anyref: Key[AnyRef] = Key[AnyRef](name, _ getAnyRef name)
      def className: Key[Class[_]] = Key[String](name, _ getString name) map { Class.forName }

      private def duration(config: Config, unit: TimeUnit): Long = config getDuration (name, unit)

      def nanos: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.NANOSECONDS).nanos)
      def micros: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.MICROSECONDS).micros)
      def millis: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.MILLISECONDS).millis)
      def seconds: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.SECONDS).seconds)
      def minutes: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.MINUTES).minutes)
      def hours: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.HOURS).hours)
      def days: Key[Duration] = Key[Duration](name, duration(_, TimeUnit.DAYS).days)
    }

    def apply(name: String): KeyBuilder = KeyBuilder(name)
  }

  val empty: Settings = new Settings(ConfigFactory.empty())

  def apply(config: Config): Settings = new Settings(config)

  def load(first: String, rest: String*): Settings = Settings(
    ConfigFactory
      // Environment variables takes highest priority and overrides everything else
      .systemEnvironment()
      // System properties comes after environment variables
      .withFallback(ConfigFactory.systemProperties())
      // Then follows user provided configuration files
      .withFallback(first +: rest map ConfigFactory.parseResources reduce { _ withFallback _ })
      // Then follows the reference configuration file
      .withFallback(ConfigFactory.parseResources("spear-reference.conf"))
      // Configurations of all other components (like Akka)
      .withFallback(ConfigFactory.load())
      .resolve()
  )

  def load(): Settings = load("spear.conf")
}
