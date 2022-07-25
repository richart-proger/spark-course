package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WorkingWithNull1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("WorkingWithNull1")
    .master("local")
    .getOrCreate()

  val customersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/customers_with_nulls.csv")

//  case class Customer(
//                       name: String,
//                       age: Int,                // в этой строке есть пустое поле
//                       customer_rating: Double) // в этой строке есть пустое поле

  case class Customer(
                       name: String,
                       age: Option[Int],
                       customer_rating: Option[Double])

  import spark.implicits._

  val customersDS = customersDF.as[Customer]

  customersDS
//    .filter(customer => customer.customer_rating > 4.0) // без учета Option
    .filter(customer => customer.customer_rating.getOrElse(0.0) > 4.0) // с учетом Option
    .show()

  spark.stop()
}
