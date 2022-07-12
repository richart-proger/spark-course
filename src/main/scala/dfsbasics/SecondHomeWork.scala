package dfbasic

import org.apache.spark.sql.SparkSession

object SecondHomeWork extends App {
  val spark = SparkSession.builder()
    .appName("SecondHomeWork")
    .master("local")
    .getOrCreate()

  val irisDF = spark.read
    .format("json") // проинструктировали spark reader прочитать файл в формате json
    .option("inferSchema", "true") // предоставляем спарку самому составить схему данных
    .load("src/main/resources/iris.json") // указываем путь к файлу

  irisDF.show(2)
  irisDF.printSchema()
  spark.stop()
}
