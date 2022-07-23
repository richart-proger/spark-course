package dfsjoins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.immutable.SortedMap

object SecondHomeWork extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SecondHomeWork")
    .master("local")
    .getOrCreate()

  val file1 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")

  val file2 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")

  import spark.implicits._

  val map1 = getSortedMap(getCountingWords(getWordsArray(file1)))
  val map2 = getSortedMap(getCountingWords(getWordsArray(file2)))

  val df1 = map1.toSeq.toDF("w_s1", "cnt_s1")
    .orderBy($"w_s1".desc)
    .withColumn("id", monotonically_increasing_id())

  val df2 = map2.toSeq.toDF("w_s2", "cnt_s2")
    .orderBy($"w_s2".desc)
    .withColumn("id", monotonically_increasing_id())

  val resultDF = df2
    .join(df1, "id")
    .select("w_s1", "cnt_s1", "id", "w_s2", "cnt_s2")
    .limit(20)

  resultDF.show()

  resultDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/wordcount.json")

  spark.stop()

  def getWordsArray(df: org.apache.spark.sql.DataFrame): Array[String] = {
    df.map(_.getString(0))
      .collect()
      .mkString(",")
      .toLowerCase()
      .split("\\W+")
      .array
  }

  def getCountingWords(array: Array[String]): Map[Int, String] = {
    array
      .groupMapReduce(identity)(_ => 1)(_ + _)
      .map({ case (k, v) => v -> k })
  }

  def getSortedMap(map: Map[Int, String]): SortedMap[Int, String] = {
    SortedMap[Int, String]() ++ map
  }
}
