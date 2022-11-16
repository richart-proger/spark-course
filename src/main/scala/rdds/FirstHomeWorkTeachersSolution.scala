package rdds

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FirstHomeWorkTeachersSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWorkTeachersSolution")
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

  def toAvocado(values: Array[String]) = (
    values(0).toInt,
    values(1),
    values(2).toDouble,
    values(3).toDouble,
    values(12),
    values(13))

  def readAvocados(filename: String) =
    sc.textFile(filename)
      .map(line => line.split(","))
      .filter(
        values => values(0) != "id" &&
          values(1) != "")
      //      .map(values => toAvocado(values))
      .map(values => Avocado(
      values(0).toInt,
      values(1),
      values(2).toDouble,
      values(3).toDouble,
      values(12),
      values(13)))

  val avocadosRDD = readAvocados("src/main/resources/avocado.csv")

  // подсчитайте количество уникальных регионов (region), для которых представлена статистика
  val uniqueRegionsRDD: RDD[String] = avocadosRDD.map(_.region).distinct()
  val countUniqueRegionsRDD = uniqueRegionsRDD.count()
  println(s"Количество уникальных регионов (region): $countUniqueRegionsRDD\n")

  // выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11
  println("Выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11:")
  println("Способ 1:")
  val dateRDD1 = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val conditionDate: Date = dateFormat.parse("2018-02-11")
    val dateFilter = (rec: Avocado) =>
      dateFormat.parse(rec.date).after(conditionDate)
    avocadosRDD
      .filter(f => f.date.nonEmpty && dateFilter(f))
  }
  dateRDD1.foreach(println)
  println()

  println("Способ 2:")
  val targetDate = "2018-02-11"
  val dateRDD2 = avocadosRDD
    .filter(rec => rec.date > targetDate)
  dateRDD2.foreach(println)
  println()

  // найдите месяц, который чаще всего представлен в статистике
  val mostOftenMonthRDD: String = avocadosRDD
    .map(rec => (rec.date.split("-")(1), 1))
    .reduceByKey(_ + _)
    .max()
    ._1

  println(s"Найдите месяц, который чаще всего представлен в статистике: $mostOftenMonthRDD\n")

  // найдите максимальное и минимальное значение avgPrice - БЕЗ ИЗМЕНЕНИЙ
  implicit val avocadoOrdering: Ordering[Avocado] =
    Ordering.fromLessThan[Avocado]((aa: Avocado, ab: Avocado) => aa.avgPrice < ab.avgPrice)
  val maxAvgPrice = avocadosRDD.max().avgPrice
  val minAvgPrice = avocadosRDD.min().avgPrice
  println(s"Найдите максимальное и минимальное значение avgPrice (БЕЗ ИЗМЕНЕНИЙ): $maxAvgPrice и $minAvgPrice\n")

  // отобразите средний объем продаж (volume) для каждого региона (region)
  println("Отобразите средний объем продаж (volume) для каждого региона (region): ")

  case class RegionVolume(region: String, avgVolume: Double)

  val avgVolumeRegionRDD = avocadosRDD
    .groupBy(_.region)
    .map {
      case (region, avocado) => RegionVolume(
        region,
        avocado.map(_.volume).sum / avocado.size
      )
    }
  avgVolumeRegionRDD.foreach(println)

  spark.stop()
}
