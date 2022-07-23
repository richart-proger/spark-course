package dfsjoins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SecondHomeWorkTeachersSolution extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SecondHomeWork")
    .master("local")
    .getOrCreate()

  object ColumnEnumeration extends ColumnEnumeration {
    implicit def columnToString(col: ColumnEnumeration.Value): String = col.toString
  }

  trait ColumnEnumeration extends Enumeration {
    val data, id,
    w_s1, w_s2,
    cnt_s1, cnt_s2 = Value
  }

  val schema = StructType(Seq(StructField("data", StringType)))

  def read(file: String) =
    spark.read
      .schema(schema)
      .option("header", "true")
      .csv(s"src/main/resources/${file}.json")

  def withId(df: DataFrame): DataFrame =
    df.withColumn("id", monotonically_increasing_id)

  def extractTop20Words(
                         splitColumn: String,
                         explodeColumn: String,
                         cntColumn: String)(df: DataFrame)
  : DataFrame = {
    val isNotSpaceOrDigit = col(explodeColumn) =!= "" && col(explodeColumn).cast("int").isNull

    df
      .select(explode(
        split(col(splitColumn), "\\W+"))
        .as(explodeColumn))
      .filter(isNotSpaceOrDigit)
      .groupBy(lower(col(explodeColumn)).as(explodeColumn))
      .agg(
        count(explodeColumn).as(cntColumn))
      .orderBy(col(cntColumn).desc)
      .limit(20)
  }

  val subtitlesS1DF = read("subtitles_s1")
  val subtitlesS2DF = read("subtitles_s2")

  val topWordsS1DF = subtitlesS1DF.transform(
    extractTop20Words(
      ColumnEnumeration.data,
      ColumnEnumeration.w_s1,
      ColumnEnumeration.cnt_s1))
    .transform(withId)

  val topWordsS2DF = subtitlesS2DF.transform(
    extractTop20Words(
      ColumnEnumeration.data,
      ColumnEnumeration.w_s2,
      ColumnEnumeration.cnt_s2))
    .transform(withId)

  val topWordsDF = topWordsS1DF.join(topWordsS2DF, ColumnEnumeration.id)

  topWordsDF.show()

  topWordsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/wordcount.json")

  spark.stop()
}
