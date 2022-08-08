package dstransformations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object DsTransformationsExercise3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DsTransformationsExercise3")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  /**
    * Задание 3
    *
    * Возьмем два датасета: customersDS и ordersDS;
    * На основании данных, имеющихся в этих датасетах, составим датасет salesDS.
    */
  case class Sales(
                    customer: String,
                    product: String,
                    price: Double,
                  )

  /**
    * Данные, с которыми будем работать:
    * (информация об id заказа теперь содержится в Customer, поле orders;
    * Orders не включают информацию о клиенте customerId)
    */
  case class Customer(
                       id: Int,
                       email: String,
                       orders: Seq[Int])

  case class Order(
                    orderId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double)


  val customerData: Seq[Row] = Seq(
    Row(1, "Bob@example.com", Seq()),
    Row(2, "alice@example.com", Seq(1, 3)),
    Row(3, "Sam@example.com", Seq(2, 4))
  )

  val ordersData: Seq[Row] = Seq(
    Row(1, "USB-C Charging Cable", 3, 11.29),
    Row(2, "Google Phone", 1, 600.33),
    Row(2, "Wired Headphones", 2, 11.90),
    Row(3, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, "20in Monitor", 1, 109.99)
  )

  /**
    * Так как теперь создаем несколько датасетов,
    * имеет смысл функционал по созданию датасета вынести в отдельную функцию:
    */

  def toDS[T <: Product : Encoder](data: Seq[Row], schema: StructType): Dataset[T] =
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      ).as[T]

  val customerSchema: StructType = Encoders.product[Customer].schema
  val ordersSchema: StructType = Encoders.product[Order].schema

  val customersDS = toDS[Customer](customerData, customerSchema)
  val ordersDS = toDS[Order](ordersData, ordersSchema)

  /**
    * Объединим датасеты, соединив покупателя и сделанный им заказ:
    */

  val joinedDS: Dataset[(Customer, Order)] = customersDS
    .joinWith(
      ordersDS,
      array_contains(customersDS.col("orders"), ordersDS.col("orderId")),
      "outer"
    )

  joinedDS.show(false)

  /*
  +------------------------------+----------------------------------------+
  |_1                            |_2                                      |
  +------------------------------+----------------------------------------+
  |{1, Bob@example.com, []}      |null                                    |
  |{2, alice@example.com, [1, 3]}|{1, USB-C Charging Cable, 3, 11.29}     |
  |{2, alice@example.com, [1, 3]}|{3, AA Batteries (4-pack), 4, 3.85}     |
  |{3, sam@example.com, [2, 4]}  |{2, Google Phone, 1, 600.33}            |
  |{3, sam@example.com, [2, 4]}  |{2, Wired Headphones, 2, 11.9}          |
  |{3, sam@example.com, [2, 4]}  |{4, Bose SoundSport Headphones, 1, 99.9}|
  |{3, sam@example.com, [2, 4]}  |{4, 20in Monitor, 1, 109.99}            |
  +------------------------------+----------------------------------------+
  */

  /**
    * А теперь пройдемся по данным и составим на их основании нужный нам датасет salesDS:
    */

  val salesDS1: Dataset[Sales] = joinedDS
    .filter(record => record._1.orders.nonEmpty)
    .map(record =>
      Sales(
        record._1.email.toLowerCase(),
        record._2.product,
        record._2.quantity * record._2.priceEach
      )
    )

  salesDS1.show()

  /*
  +-----------------+--------------------+------+
  |         customer|             product| price|
  +-----------------+--------------------+------+
  |alice@example.com|USB-C Charging Cable| 33.87|
  |alice@example.com|AA Batteries (4-p...|  15.4|
  |  sam@example.com|        Google Phone|600.33|
  |  sam@example.com|    Wired Headphones|  23.8|
  |  sam@example.com|Bose SoundSport H...|  99.9|
  |  sam@example.com|        20in Monitor|109.99|
  +-----------------+--------------------+------+
  */

  /**
    * Если же хотим включить данные даже о тех клиентах,
    * которые не сделали ни одного заказа, можно прибегнуть к помощи шаблонов:
    */

  val salesDS2: Dataset[Sales] = joinedDS
    .map(record => record._1.orders.isEmpty match {
      case false => Sales(
        record._1.email.toLowerCase(),
        record._2.product,
        record._2.quantity * record._2.priceEach)
      case _ => Sales(
        record._1.email.toLowerCase(),
        "-",
        0.0)
    })

  salesDS2.show()

  /*
  +-----------------+--------------------+------+
  |         customer|             product| price|
  +-----------------+--------------------+------+
  |  bob@example.com|                   -|   0.0|
  |alice@example.com|USB-C Charging Cable| 33.87|
  |alice@example.com|AA Batteries (4-p...|  15.4|
  |  sam@example.com|        Google Phone|600.33|
  |  sam@example.com|    Wired Headphones|  23.8|
  |  sam@example.com|Bose SoundSport H...|  99.9|
  |  sam@example.com|        20in Monitor|109.99|
  +-----------------+--------------------+------+
  */

  spark.stop()
}
