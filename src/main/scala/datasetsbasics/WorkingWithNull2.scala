package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WorkingWithNull2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("WorkingWithNull2")
    .master("local")
    .getOrCreate()

  val employeeDF: DataFrame = spark.read
    .option("header", "true")
    .csv("src/main/resources/employee.csv")

  // Метод isNull проверяет, является ли значение в колонке null:
  employeeDF
    .select("*")
    .where(
      col("birthday").isNull)
    .show()

  // Можно отсортировать данные и вывести null значения в самом начале (в конце тоже можно, для этого подойдет desc_nulls_last):
  employeeDF
    .select("*")
    .orderBy(
      col("date_of_birth").desc_nulls_first)
    .show()

  // Удаление строчек, записи в которых содержат null
  employeeDF
    .na.drop()
    .show()

  // Замена null на определенное значение. Сделать это можно несколькими способами:
  // 1. Замена null во всех указанных колонках на одно и то же значение
  employeeDF
    .na
    .fill("n/a", List("birthday", "date_of_birth"))
    .show()

  // 2. Замена записи в разных колонках на разные значения
  employeeDF.na.fill(Map(
    "birthday" -> "n/a",
    "date_of_birth" -> "Unknown",
  )).show()

  // 3. Возвращает первое не null значение среди указанных колонок или null
  employeeDF
    .select(
      col("name"),
      coalesce(col("birthday"), col("date_of_birth")))
    .show()

  // 3*. lit - создание колонки, каждая строчка в которой содержит указанное значенние
  employeeDF
    .select(
      col("name"),
      coalesce(col("birthday"), col("date_of_birth"), lit("n/a")))
    .show()

  // В качестве аналогов coalesce можно использовать:
  //    ifnull - аналог coalesce
  //    nvl - аналог coalesce
  //    nvl2 - если первое значение не равно null, то возвращает второе, иначе - третье
  employeeDF.selectExpr(
    "name",
    "birthday",
    "date_of_birth",
    "ifnull(birthday, date_of_birth) as ifnull",
    "nvl(birthday, date_of_birth) as nvl",
    "nvl2(birthday, date_of_birth, 'Unknown') as nvl2"
  ).show()

  spark.stop()
}
