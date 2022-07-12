package dfbasic

import org.apache.spark.sql.{Row, SparkSession}

object ThirdHomeWork extends App {
  val spark = SparkSession.builder()
    .appName("ThirdHomeWork")
    .master("local")
    .getOrCreate()

  val irisDF = spark.read
    .format("json")
    .option("inferSchema", true)
    .load("src/main/resources/iris.json")

  val irisArray: Array[Row] = irisDF.take(3)
  irisArray.foreach(println)

  irisDF.take(3).foreach(println)

  spark.stop()
}
