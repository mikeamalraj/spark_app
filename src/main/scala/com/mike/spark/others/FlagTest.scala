package com.mike.spark.others

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.max

object FlagTest {
  def main(args : Array[String] ): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("WordCount - Dataset")
      .getOrCreate()

    import spark.implicits._ // Need to import, otherwise error will be thrown

    val sourceDF = spark.read.option("header","true").csv("file:///D:\\Big Data\\Input\\flag_test.csv")
    val sourceDF1 = sourceDF.withColumn("date", $"date".cast("date"))

    val windowSpec = Window.partitionBy("id").orderBy("date")
      //.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val resultDF = sourceDF1.withColumn("flag1_result", max("flag1") over windowSpec)
      .withColumn("flag2_result", max("flag2") over windowSpec)
      .withColumn("flag3_result", max("flag3") over windowSpec)
      .withColumn("flag4_result", max("flag4") over windowSpec)

    resultDF.orderBy("id","date").show()

  }
}
