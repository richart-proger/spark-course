package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSetsBasics2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DataSetsBasics2")
    .master("local")
    .getOrCreate()

  val customersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/customers.csv")

  customersDF.printSchema()

  println(customersDF.getClass) // выдаст class org.apache.spark.sql.Dataset, но не нужно забывать, что этот датасет типизирован Row

  case class Customer(
                       name: String,
                       surname: String,
                       age: Int,
                       occupation: String,
                       customer_rating: Double
                     )

  import spark.implicits._

  val customersDS = customersDF.as[Customer]

  customersDS.show()

  println(customersDS.getClass) // выдаст class org.apache.spark.sql.Dataset, но этот датасет уже типизирован нужными типами

  spark.stop()
}
