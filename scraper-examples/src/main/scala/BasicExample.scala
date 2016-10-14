import scraper.Context
import scraper.config.Settings
import scraper.expressions._
import scraper.expressions.functions._

object BasicExample {
  case class Person(name: String, gender: String, age: Int)

  def main(args: Array[String]) {
    val context = new Context(Settings.load())

    import context._

    val people = lift(
      Person("Alice", "F", 9),
      Person("Bob", "M", 15),
      Person("Charlie", "M", 18),
      Person("David", "M", 13),
      Person("Eve", "F", 20),
      Person("Frank", "M", 19)
    )

    val adults = people filter 'age >= 18 select ('name, 'gender)
    adults.explain()
    adults.show()

    val countGender = people groupBy 'gender agg ('gender, count() as 'count)
    countGender.explain()
    countGender.show()

    people.asTable('people)

    val adultsSQL = sql(
      """SELECT name, gender
        |FROM people
        |WHERE age >= 18
        |""".stripMargin
    )

    adultsSQL.explain()
    adultsSQL.show()

    val countGenderSQL = sql(
      """SELECT gender, max(age), count(*)
        |FROM people
        |GROUP BY gender
        |HAVING gender = 'M'
        |""".stripMargin
    )

    countGenderSQL.explain()
    countGenderSQL.show()
  }
}
