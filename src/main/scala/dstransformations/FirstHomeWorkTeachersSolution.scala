package dstransformations

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

object FirstHomeWorkTeachersSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWorkTeachersSolution")
    .master("local")
    .getOrCreate()

  case class Car(
                  id: Int
                  , price: Int
                  , brand: String
                  , carType: String
                  , mileage: Option[Double]
                  , color: String
                  , date_of_purchase: String
                )

  val carsSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("price", IntegerType),
    StructField("brand", StringType),
    StructField("carType", StringType),
    StructField("mileage", DoubleType),
    StructField("color", StringType),
    StructField("date_of_purchase", StringType)
  ))

  import spark.implicits._

  val carsDS = spark.read
    .option("header", "true")
    .schema(carsSchema)
    .csv("src/main/resources/cars.csv")
    .as[Car]

  object Format {
    val withDash = "yyyy-MM-dd"
    val withSpace = "yyyy MM dd"
    val withMonthNam = "yyyy MMM dd"
    val default = "MM/dd/yyyy"
  }

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.US)
    format.parse(date).getTime // milliseconds
  }

  lazy val spaceFormatRegex = "\\d{4} \\d{2} \\d{2}".r
  lazy val dashFormatRegex = "\\d{4}-\\d{2}-\\d{2}".r
  lazy val withMonthNameFormatRegex = "\\d{4} \\w{3} \\d{2}".r

  lazy val defaultDate = LocalDate.now().toString
  lazy val dateNow = toDate(defaultDate, Format.withDash)

  def matchFormat(date: String): String =
    date match {
      case spaceFormatRegex() => Format.withSpace
      case dashFormatRegex() => Format.withDash
      case withMonthNameFormatRegex() => Format.withMonthNam
      case _ => Format.default
    }

  def yearsSincePurchase(purchaseDate: String): Long = {
    val age = {
      val format = matchFormat(purchaseDate)
      (dateNow - toDate(purchaseDate, format)) / (1000 * 60 * 60 * 24) / 365
    }
    age
  }

  def countAvgMiles(cars: Dataset[Car]): Double = {

    val count: (Double, Int) = cars
      .map(car => {
        (car.mileage.getOrElse(0.0), Integer2int(1))
      })
      .reduce((a, b) => {
        (a._1 + b._1, a._2 + b._2)
      })

    val avgMileage = count._1 / count._2

    avgMileage
  }

  val avgMiles = countAvgMiles(carsDS)

  def milesOrZero(mileage: Double): Double = {
    if (mileage > 0.0) avgMiles
    else 0.0
  }

  case class Result(
                     id: Int,
                     price: Int,
                     brand: String,
                     carType: String,
                     mileage: Option[Double],
                     color: String,
                     date_of_purchase: String,
                     years_since_purchase: Long,
                     avg_mileage: Double
                   )

  val ds = carsDS
    .map(car =>
      Result(car.id,
        car.price,
        car.brand,
        car.carType,
        car.mileage,
        car.color,
        car.date_of_purchase,
        yearsSincePurchase(car.date_of_purchase),
        milesOrZero(car.mileage.getOrElse(0.0))
      )
    )

  ds.show()

  spark.stop()
}
