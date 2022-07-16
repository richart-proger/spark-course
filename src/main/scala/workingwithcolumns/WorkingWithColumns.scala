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

//  Считаем общее количество записей
  val bikes = bikeSharingDF.select("*").count()
  println(s"number of records $bikes") // number of records 8760


//  Считаем количество уникальных значений в колонке HOLIDAY
  val distinctEntries = bikeSharingDF.select("HOLIDAY").distinct().count()
  println(s"distinct values in column HOLIDAYS  $distinctEntries") // distinct values in column HOLIDAYS  2

//  Переименовываем колонку TEMPERATURE в Temp
  val bikesWithColumnRenamed = bikeSharingDF.withColumnRenamed("TEMPERATURE", "Temp")
  bikesWithColumnRenamed.show(3)

//  Удаляем колонки Date и Hour
  bikeSharingDF.drop("Date", "Hour")


//    Создаем колонку is_holiday, которая будет состоять из значений true (если значение в колонке HOLIDAY равно Holiday), false (если значение в колонке HOLIDAY равно No Holiday), null (если значение в колонке HOLIDAY иное чем Holiday или No Holiday)
  bikeSharingDF
    .withColumn(
      "is_holiday",
      when(col("HOLIDAY") === "No Holiday", false)
        .when(col("HOLIDAY") === "Holiday", true)
        .otherwise(null)
    )

//  для каждой даты Date рассчитываем сумму взятых в аренду велосипедов.  Результат записываем в колонку bikes_total. Конечный датафрейм сортируем по дате посредством orderBy:
    bikeSharingDF
    .groupBy("Date")
  .agg(
    sum("RENTED_BIKE_COUNT").as("bikes_total"))
    .orderBy("Date")
    .show(3)

  spark.stop()
}
