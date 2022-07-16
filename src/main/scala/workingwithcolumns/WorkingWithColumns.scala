package workingwithcolumns

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WorkingWithColumns extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("WorkingWithColumns")
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
    bikeSharingDF.col("Date"),
    col("Date"),
    column("Date"),
    Symbol("Date"),
    $"Date",
    expr("Date")
  ).show(1, 20)

  val newDF = bikeSharingDF.select(
    bikeSharingDF.col("Date"),
    col("Date"),
    column("Date"),
    Symbol("Date"),
    $"Date",
    expr("Date")
  )

  newDF.show(1, 20)

  bikeSharingDF.select("Date", "Hour")

  spark.stop()
}
