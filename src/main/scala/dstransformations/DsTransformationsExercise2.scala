package dstransformations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object DsTransformationsExercise2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DsTransformationsExercise2")
    .master("local")
    .getOrCreate()

  case class Order(
                    orderId: Int,
                    customerId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double
                  )

  val ordersData: Seq[Row] = Seq(
    Row(1, 2, "USB-C Charging Cable", 3, 11.29),
    Row(2, 3, "Google Phone", 1, 600.33),
    Row(2, 3, "Wired Headphones", 2, 11.90),
    Row(3, 2, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, 3, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, 3, "20in Monitor", 1, 109.99)
  )

  val ordersSchema: StructType = Encoders.product[Order].schema

  import spark.implicits._

  val ordersDS = spark
    .createDataFrame(
      spark.sparkContext.parallelize(ordersData),
      ordersSchema
    ).as[Order]

  /**
    * Задание 2
    *
    * Найти общую сумму заказов для каждого покупателя.
    * При решении будем стремиться к тому, чтобы типом возвращаемого значения
    * оставался Dataset (не менялся на DataFrame).
    */
  case class CustomerInfo(
                           customerId: Int,
                           priceTotal: Double
                         )

  val infoDS: Dataset[CustomerInfo] = ordersDS
    .groupByKey(_.customerId)
    .mapGroups { (id, orders) => {
      val priceTotal = orders.map(order => order.priceEach * order.quantity).sum.round

      CustomerInfo(
        id,
        priceTotal
      )

    }
    }

  infoDS.show()

  /*
  +----------+----------+
  |customerId|priceTotal|
  +----------+----------+
  |         2|      49.0|
  |         3|     834.0|
  +----------+----------+
  */

  spark.stop()
}
