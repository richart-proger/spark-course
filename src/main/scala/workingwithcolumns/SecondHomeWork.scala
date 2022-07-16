package workingwithcolumns

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SecondHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SecondHomeWork")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/bike_sharing.csv")
    .load()

//  Подсчитайте количество дней, когда количество арендованных велосипедов  RENTED_BIKE_COUNT равнялось 254 и температура TEMPERATURE была выше 0.
  val distinctEntries = bikeSharingDF.select("Date")
    .where("RENTED_BIKE_COUNT = 254")
    .where("TEMPERATURE > 0")
    .distinct()
    .count()
  println(s"distinct values in column Date  $distinctEntries")

  spark.stop()
}
