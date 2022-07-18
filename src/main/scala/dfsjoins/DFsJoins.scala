package dfsjoins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DFsJoins extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DFsJoins")
    .master("local")
    .getOrCreate()

  val valuesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_link_value.csv")

  val tagsDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_links.csv")

  val joinCondition = valuesDF.col("id") === tagsDF.col("key")

  println(s"1 Inner Join") // 1 Inner Join
  val innerJoinDF = tagsDF.join(valuesDF, joinCondition, "inner")
  // inner используется по умолчанию, поэтому его можно не указывать
  tagsDF.join(valuesDF, joinCondition)
  innerJoinDF.show()

  println(s"2 Full Outer Join") // 2 Full Outer Join
  val fullOuterDF = tagsDF.join(valuesDF, joinCondition, "outer")
  fullOuterDF.show()

  println(s"3 Left Outer Join") // 3 Left Outer Join
  val leftOuterDF = tagsDF.join(valuesDF, joinCondition, "left_outer")
  leftOuterDF.show()

  println(s"4 Right Outer Join") // 4 Right Outer Join
  val rightOuterDF = tagsDF.join(valuesDF, joinCondition, "right_outer")
  rightOuterDF.show()

  println(s"5 Left Semi Join") // 5 Left Semi Join
  val leftSemiDF = tagsDF.join(valuesDF, joinCondition, "left_semi")
  leftSemiDF.show()

  spark.stop()
}
