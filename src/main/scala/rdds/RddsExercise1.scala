package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object RddsExercise1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("RddsExercise1")
    .master("local")
    .getOrCreate()

  /*
   List(id-0, id-1, id-2, id-3, id-4)
    */

  val ids = List.fill(5)("id-").zipWithIndex.map(x => x._1 + x._2)

  import spark.implicits._

  val idsDS: Dataset[String] = ids.toDF.as[String]

  val idsPartitioned = idsDS.repartition(6)

  val numPartitions = idsPartitioned.rdd.partitions.length
  println(s"partitions = $numPartitions")

  idsPartitioned.rdd
    .mapPartitionsWithIndex(
      (partition: Int, it: Iterator[String]) =>
        it.toList.map(id => {
          println(s" partition = $partition; id = $id")
        }).iterator
    ).collect

  spark.stop()
}
