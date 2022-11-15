package rdds

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object FirstHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String)

  def readAvocados(filename: String) =
    Source.fromFile(filename)
      .getLines()
      //удаляем первую строчку, тк в ней содержатся названия колонок
      .drop(1)
      // данные в колонках разделяются запятой
      .map(line => line.split(","))
      // построчно считываем данные в case класс
      .map(values => Avocado(
      values(0).toInt,
      values(1),
      values(2).toDouble,
      values(3).toDouble,
      values(12),
      values(13))
    ).toList

  val avocadosRDD = sc.parallelize(readAvocados("src/main/resources/avocado.csv"))

  // подсчитайте количество уникальных регионов (region), для которых представлена статистика
  val uniqueRegionsRDD: RDD[String] = avocadosRDD.map(_.region).distinct()
  val countUniqueRegionsRDD = uniqueRegionsRDD.count()
  println(s"Количество уникальных регионов (region): $countUniqueRegionsRDD\n")


  // выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11
  println("Выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11:")
  val dateRDD = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val conditionDate: Date = dateFormat.parse("2018-02-11")
    avocadosRDD
      .filter(_.date.nonEmpty)
      .filter(f => dateFormat.parse(f.date).after(conditionDate))
  }
  dateRDD.foreach(println)
  println()


  // найдите месяц, который чаще всего представлен в статистике
  val mostOftenMonthRDD = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val datesMap = avocadosRDD
      .filter(_.date.nonEmpty)
      .map(s => (dateFormat.parse(s.date).getMonth + 1, 1))
    datesMap.reduceByKey((a, b) => a + b)
  }
  println(s"Найдите месяц, который чаще всего представлен в статистике: ${mostOftenMonthRDD.map(_.swap).max()._2}\n")


  // найдите максимальное и минимальное значение avgPrice
  implicit val avocadoOrdering: Ordering[Avocado] =
    Ordering.fromLessThan[Avocado]((aa: Avocado, ab: Avocado) => aa.avgPrice < ab.avgPrice)
  val maxAvgPrice = avocadosRDD.max().avgPrice
  val minAvgPrice = avocadosRDD.min().avgPrice
  println(s"Найдите максимальное и минимальное значение avgPrice: $maxAvgPrice и $minAvgPrice\n")


  // отобразите средний объем продаж (volume) для каждого региона (region)
  println("Отобразите средний объем продаж (volume) для каждого региона (region): ")
  val avgVolumeRegionRDD = {
    avocadosRDD
      .groupBy(_.region)
      .map(s => (s._1, s._2.map(k => k.volume).sum / s._2.size))
  }
  avgVolumeRegionRDD.foreach(println)

  spark.stop()
}
