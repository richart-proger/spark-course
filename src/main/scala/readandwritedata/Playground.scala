package readandwritedata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Playground extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FourthHomeWork")
    .master("local")
    .getOrCreate()

  val restaurantSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("opened", StringType),
    StructField("photos_url", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  val restaurnatDF = spark.read
    .format("json")
    .schema(restaurantSchema) // либо .option("inferSchema", "true")
    .option("mode", "failFast") // варианты: failFast, dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/restaurant.json")
    .load()

  val restaurants = spark.read
    .format("json")
    .options(Map(
      "inferSchema" -> "true",
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/restaurant.json"
    ))
    .load()

  restaurnatDF.show(3, 20, true)
  restaurants.show(2, 30, true)
  restaurants.printSchema()

  spark.stop()
}
