package dfsjoins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FirstHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("FirstHomeWork")
    .master("local")
    .getOrCreate()

  val mallCustomersDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "dropMalformed")
    .option("path", "src/main/resources/mall_customers.csv")
    .load()

  val tempDF = mallCustomersDF
    .withColumn("Age", col("Age") + 2)
    .filter("Age between 30 and 35")
    .groupBy("Gender", "Age")
    .avg("Annual Income (k$)")
    .orderBy("Gender", "Age")

  val genderCondition = col("Gender") === "Male"

  val incomeDF = tempDF
    .withColumn("gender_code", when(genderCondition, 1).otherwise(0))
    .withColumn("avg_annual_income", round(col("avg(Annual Income (k$))"), 1))
    .withColumnRenamed("Age", "age")
    .select(col = "gender_code", "age", "avg_annual_income")

  incomeDF.show()

  incomeDF.write
    .mode("overwrite")
    .option("path", "src/main/resources/data/customers.parquet")
    .save

  spark.stop()
}
