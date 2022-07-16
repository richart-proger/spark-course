package workingwithcolumns

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FirstHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/bike_sharing.csv")
    .load()

  import spark.implicits._

  bikeSharingDF.select(
    $"Hour",
    $"TEMPERATURE",
    $"HUMIDITY",
    $"WIND_SPEED"
  ).show(3, 20)

  spark.stop()
}
