package dstransformations

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object FirstHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .master("local")
    .getOrCreate()

  case class Car(
                  id: Int
                  , price: Int
                  , brand: String
                  , typ: String
                  , mileage: Option[Double]
                  , color: String
                  , date_of_purchase: String
                )

  val carsSrcDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/cars.csv")

  val carsSrcWithRenamedColDF = carsSrcDF
    .withColumnRenamed("type", "typ")

  import spark.implicits._

  val carSrcDS = carsSrcWithRenamedColDF.as[Car]

  def getCounts(cars: Dataset[Car]): (Double, Int) = {
    val counts: (Double, Int) = cars
      .map(car => (car.mileage.getOrElse(0.0), Integer2int(1)))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    (counts._1, counts._2)
  }

  val (mileage, carsQuantity) = getCounts(carSrcDS)

  val isNull = isnull(col("mileage"))

  val avgM: Any = mileage / carsQuantity


  val car1DS: Dataset[Row] = carsSrcWithRenamedColDF
    .withColumn("avg_mileage", when(isNull, lit(0.0).cast(DoubleType))
      .otherwise(lit(avgM).cast(DoubleType)))

  case class Years(
                    car_id: Int
                    , years: String
                  )

  def regexYears(date_of_purchase: String) = {
    if (date_of_purchase.matches("\\d{4} \\d{2} \\d{2}"))
      "yyyy MM dd"
    else if (date_of_purchase.matches("\\d{4}-\\d{2}-\\d{2}"))
      "yyyy-MM-dd"
    else if (date_of_purchase.matches("\\d{4} \\w{3} \\d{2}"))
      "yyyy MMM dd"
  }

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.US)
    format.parse(date).getTime // milliseconds
  }

  def yearsSincePurchase(car: Car): Years = {
    val age = (toDate("09/10/2022", "MM/dd/yyyy") - toDate(car.date_of_purchase, regexYears(car.date_of_purchase).toString)) / (1000 * 60 * 60 * 24) / 365
    Years(car.id, age.toString)
  }

  val car2DS: Dataset[Years] = carSrcDS.map(car => yearsSincePurchase(car))

  val joinedDS: Dataset[Row] = car1DS
    .join(car2DS, car1DS.col("id") === car2DS.col("car_id"))
    .drop("car_id")

  joinedDS.show()

  spark.stop()
}
