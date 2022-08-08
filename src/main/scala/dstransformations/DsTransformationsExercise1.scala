package dstransformations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object DsTransformationsExercise1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DsTransformationsExercise1")
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
    * Задание 1
    * Найти
    *
    * -> сколько всего предметов было куплено (orderQuantity)
    * -> общую сумму всех заказов (price)
    *
    * Так как цель раздела - продемонстрировать возможности функционального
    * программирования при работе с датасетом, то при решении постараемся не
    * использовать DataFrame API (select, aggregate).
    *
    * Запомним, что аналогом select в функциональном программировании является map.
    * Те альтернативой для кода ordersDS.select(col("orderId")) является ordersDS.map(_.orderId)
    *
    * Для нахождения требуемых показателей можно написать следующую функцию:
    */

  def getTotalStats(orders: Dataset[Order]): (Double, Int) = {
    val stats: (Double, Int) = orders
      .map(order => (order.priceEach * order.quantity, order.quantity))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    (stats._1, stats._2)
  }

  val (price, orderQuantity) = getTotalStats(ordersDS) // результат: (883.29, 12)

  println(price, orderQuantity)

  spark.stop()
}
