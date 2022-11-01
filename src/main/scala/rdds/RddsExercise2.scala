package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object RddsExercise2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    //    .appName("RDDs")
    .appName("RddsExercise2")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Store(
                    state: String,
                    location: String,
                    address: String,
                    latitude: Double,
                    longitude: Double
                  )

  //  Существует несколько способов создать RDD на основе данных из этого файла.

  //  1. обычное считывание данных из файла, которое возможно сделать средствами import scala.io.Source
  def readStores(filename: String) =
    Source.fromFile(filename)
      .getLines()
      //удаляем первую строчку, тк в ней содержатся названия колонок
      .drop(1)
      // данные в колонках разделяются запятой
      .map(line => line.split(","))
      // построчно считываем данные в case класс
      .map(values => Store(
      values(0),
      values(1),
      values(2),
      values(3).toDouble,
      values(4).toDouble)
    ).toList

  val storesRDD = sc.parallelize(readStores("src/main/resources/chipotle_stores.csv"))

  println("1. обычное считывание данных из файла, которое возможно сделать средствами import scala.io.Source")
  storesRDD.foreach(println)
  println()

  /*
  2. использование метода  .textFile

Здесь есть один нюанс. RDD представляет из себя распределенный набор больших данных. sc.textFile считывает данные в rdd, значит, они разбиваются на части, причем неизвестно, какая именно часть содержит названия колонок. Следовательно, строчку с названиями колонок требуется как-то идентифицировать.

Известно, что данные в первой колонке содержат одинаковое значение Alabama - этим и воспользуемся:
   */

  val storesRDD2 = sc.textFile("src/main/resources/chipotle_stores.csv")
    .map(line => line.split(","))
    .filter(values => values(0) == "Alabama")
    .map(values => Store(
      values(0),
      values(1),
      values(2),
      values(3).toDouble,
      values(4).toDouble))

  println("2. использование метода  .textFile")
  storesRDD2.foreach(println)

  /*
  3. Из DF в RDD

Самый легкий, но в то же время самый ресурсозатратный метод из-за необходимости проведения конвертации.

3.1 Напрямую из DF в RDD[Row]
   */

  val storesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/chipotle_stores.csv")


  val storesRDD3 = storesDF.rdd

  println(
    """
      |3. Из DF в RDD
      |
      |Самый легкий, но в то же время самый ресурсозатратный метод из-за необходимости проведения конвертации.
      |
      |3.1 Напрямую из DF в RDD[Row]
    """.stripMargin)
  storesRDD3.foreach(println) // [Alabama,Auburn,AL 36832 US,32.606812966051244,-85.48732833164195]
  println()

  /*
  3.2 DF -> DS ->  RDD[Store]
Удастся точно указать структуру данных путем указания типа Store.
   */

  import spark.implicits._

  val storesDS = storesDF.as[Store]
  val storesRDD4 = storesDS.rdd

  println("3.2 DF -> DS ->  RDD[Store]\nУдастся точно указать структуру данных путем указания типа Store.")
  storesRDD4.foreach(println) // Store(Alabama,Auburn,AL 36832 US,32.606812966051244,-85.48732833164195)

  spark.stop()
}
