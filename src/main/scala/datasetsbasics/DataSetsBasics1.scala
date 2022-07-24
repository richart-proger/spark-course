package datasetsbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataSetsBasics1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DataSetsBasics1")
    .master("local")
    .getOrCreate()

//  val data = Seq(
//    Row("Alice", 12),
//    Row("Bob",13)
//  )

  val namesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/names.csv")

//  namesDF.printSchema()

//  val x = namesDF.filter(col("name") =!= "Bob") // это не сработает из-за несовместимости типов

//  val x = namesDF.filter(_ != "Bob") // это не сработает из-за несовместимости типов

  implicit val stringEncoder = Encoders.STRING
  val namesDS: Dataset[String] = namesDF.as[String]

  val x = namesDS.filter(n => n != "Bob") // теперь сработает

  x.show()

  spark.stop()
}
