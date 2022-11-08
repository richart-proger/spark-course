package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object RddsExercise3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    //    .appName("RDDs")
    .appName("RddsExercise3")
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

  // см п.1 RddsExercise2
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

  //  найдем уникальные значения для location
  println("Найдем уникальные значения для location")
  val locationNamesRDD: RDD[String] = storesRDD.map(_.location).distinct()

  locationNamesRDD.foreach(println)
  println()

  //  найдем локацию с самым длинным названием
  println("Найдем локацию с самым длинным названием")
  implicit val storeOrdering: Ordering[Store] =
    Ordering.fromLessThan[Store]((sa: Store, sb: Store) => sa.location.length < sb.location.length)

  val longestLocationName = storesRDD.max().location

  println(s"location = $longestLocationName") // Birmingham
  println()

  //  отфильтруем данные
  println("Отфильтруем данные")
  val locationRDD = storesRDD.filter(_.location == longestLocationName)

  locationRDD.foreach(println)
  println()

  // сгруппируем данные
  println("Сгруппируем данные")
  val groupedStoresRDD = storesRDD.groupBy(_.location)

  groupedStoresRDD.foreach(println)

  spark.stop()
}
