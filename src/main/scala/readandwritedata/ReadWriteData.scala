package readandwritedata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadWriteData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("ReadWriteData")
    .master("local")
    .getOrCreate()

  val moviesSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", TimestampType),
    StructField("release_year", IntegerType),
    StructField("rating", StringType),
    StructField("duration", IntegerType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", IntegerType),
    StructField("month_added", DoubleType),
    StructField("season_count", IntegerType)
  ))

  val moviesDF = spark.read
    .format("csv")
    .schema(moviesSchema)
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/movies_on_netflix.csv")
    .load()

  moviesSchema.printTreeString()

  moviesDF.write
    .mode("overwrite")
    .option("path", "src/main/resources/data/file.parquet")
    .save

  spark.stop()
}
