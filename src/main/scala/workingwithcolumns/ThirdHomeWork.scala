package workingwithcolumns

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ThirdHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("ThirdHomeWork")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/bike_sharing.csv")
    .load()

  //  Напишите код, который позволит создать колонку is_workday, состоящую из значений 0 и 1 [значение 0 - если значением колонки HOLIDAY является Holiday и значением колонки FUNCTIONING_DAY является No ].
  val isWorkday = col("HOLIDAY") === "Holiday" && col("FUNCTIONING_DAY") === "No"

  val bikesWithNewColumnDF = bikeSharingDF
    .withColumn(
      "is_workday",
      when(isWorkday, 0)
        .otherwise(1)
    )

  //  Выведите на экран строчки, которые включают в себя только уникальные значения из колонок  "HOLIDAY", "FUNCTIONING_DAY", "is_workday"
  val distinctEntries = bikesWithNewColumnDF
    .select("HOLIDAY", "FUNCTIONING_DAY", "is_workday")
    .distinct()

  distinctEntries.show()

  // Ниже приведены различные проверки : is_workday должени быть == 0
  bikesWithNewColumnDF.select("*")
    .where("HOLIDAY LIKE\"Holiday\"")
    .show(3)

  bikesWithNewColumnDF.select("*")
    .where("FUNCTIONING_DAY LIKE \"No\"")
    .show(3)

  bikesWithNewColumnDF.select("*")
    .where("HOLIDAY LIKE\"Holiday\"")
    .where("FUNCTIONING_DAY LIKE \"No\"")
    .show(3)

  val countOfIsWorkday = bikesWithNewColumnDF.select("*")
    .where("is_workday == 0")
    .count()

  println(s"Count of is_workday == 0 : $countOfIsWorkday")

  spark.stop()
}
