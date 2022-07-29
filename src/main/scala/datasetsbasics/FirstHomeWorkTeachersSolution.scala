package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object FirstHomeWorkTeachersSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .master("local")
    .getOrCreate()

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  import spark.implicits._

  val shoesDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/athletic_shoes.csv")
    .as[Shoes]

  def replaceNull(colName: String, column: Column): Column =
    coalesce(col(colName), column).as(colName)

  val ds = shoesDS
    .na
    .drop(Seq(
      "item_name",
      "item_category"))
    .select(
      col("item_category"),
      col("item_name"),
      replaceNull("item_after_discount", col("item_price")),
      replaceNull("item_rating", lit(0)),
      replaceNull("buyer_gender", lit("unknown")),
      replaceNull("item_price", lit("n/a")),
      replaceNull("percentage_solds", lit(-1)),
      replaceNull("item_shipping", lit("n/a"))
    )

  ds.show(100)

  spark.stop()
}
