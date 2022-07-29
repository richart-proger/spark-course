package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FirstHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .master("local")
    .getOrCreate()

  val athleticShoesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/athletic_shoes.csv")


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

  val athleticShoesDS = athleticShoesDF.as[Shoes]

  athleticShoesDS
    .na.drop(List("item_name", "item_category"))
    .withColumn("item_after_discount", when($"item_after_discount".isNull, $"item_price").otherwise($"item_after_discount"))
    .na.fill(0, List("item_rating"))
    .na.fill("unknown", List("buyer_gender"))
    .na.fill(-1)
    .na.fill("n/a")
    .show(100)

  spark.stop()
}
