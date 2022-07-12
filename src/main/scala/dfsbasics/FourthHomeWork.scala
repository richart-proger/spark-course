package dfbasic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object FourthHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FourthHomeWork")
    .master("local")
    .getOrCreate()

  val user_ratingSchema = StructType(Seq(
    StructField("rating_text", StringType),
    StructField("rating_color", StringType),
    StructField("votes", StringType),
    StructField("aggregate_rating", StringType)
  ))

  val restaurant_exSchema = StructType(Seq(
    StructField("has_online_delivery", IntegerType),
    StructField("url", StringType),
    StructField("user_rating", user_ratingSchema),
    StructField("name", StringType),
    StructField("cuisines", StringType),
    StructField("is_delivering_now", IntegerType),
    StructField("deeplink", StringType),
    StructField("menu_url", StringType),
    StructField("average_cost_for_two", LongType)
  ))

  val restaurant_exDF = spark.read
    .schema(restaurant_exSchema)
    .json("src/main/resources/data/restaurant_ex.json")

  restaurant_exDF.show(2, 30, true)
  restaurant_exDF.printSchema()

  restaurant_exSchema.fields.toStream
    .filter(f => f.dataType == IntegerType)
    .foreach(println)

  spark.stop()
}
