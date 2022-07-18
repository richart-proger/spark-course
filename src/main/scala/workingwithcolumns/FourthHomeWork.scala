package workingwithcolumns

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FourthHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FourthHomeWork")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/bike_sharing.csv")
    .load()

  // Напишите код, который рассчитывает минимальное и максимальное значение температуры TEMPERATURE для каждой даты Date. Результат расчетов записывается в колонки min_temp, max_temp и выводится на экран.
  val bikesWithTemperatureDF = bikeSharingDF
    .groupBy("Date")
    .agg(
      min("TEMPERATURE").as("min_temp"),
      max("TEMPERATURE").as("max_temp"))
    .orderBy("Date")

  bikesWithTemperatureDF.show(3)

  spark.stop()
}
