package dstransformations

import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Playground extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()

  val channelsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/channel.csv")

  case class Channel(
                      channel_name: String,
                      city: String,
                      country: String,
                      created: String,
                    )

  import spark.implicits._

  val channelsDS = channelsDF.as[Channel]

  channelsDS.show()

  // 1. Получение колонки, значения которой будут написаны с большой буквы:
  // DF
  val cityNamesDF = channelsDF
    .select(
      upper(col("city")).as("city")
    )
  cityNamesDF.show()

  // DS
  val cityNamesDS: Dataset[String] =
    channelsDS.map(channel => channel.city.toUpperCase)

  cityNamesDS.show()

  // 2. Подсчет количества дней, прошедших с момента создания канала:
  // DF
  val channelsWithDaysDF = channelsDF
    .withColumn("today", to_date(lit("04/19/2022"), "MM/dd/yyyy"))
    .withColumn("actual_date", to_date(col("created"), "yyyy MMM dd"))
    .withColumn(
      "channel_age",
      datediff(col("today"), col("actual_date"))
    )
  channelsWithDaysDF.show()

  // DS
  implicit val encoder: ExpressionEncoder[Age] = ExpressionEncoder[Age]

  case class Age(
                  channel_name: String,
                  age: String,
                )

  import java.text.SimpleDateFormat

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.ENGLISH)
    format.parse(date).getTime // milliseconds
  }

  def countChannelAge(channel: Channel): Age = {
    val age = (toDate("04/19/2022", "MM/dd/yyyy") - toDate(channel.created, "yyyy MMM dd")) / (1000 * 60 * 60 * 24)
    Age(channel.channel_name, age.toString)
  }

  val ageDS = channelsDS.map(channel => countChannelAge(channel))

  ageDS.show()

  val joinedDS: Dataset[(Channel, Age)] = channelsDS
    .joinWith(ageDS, channelsDS.col("channel_name") === ageDS.col("channel_name"))

  joinedDS.show()

  spark.stop()
}
