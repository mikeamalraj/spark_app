package com.mike.spark.others

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args : Array[String] ): Unit ={

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      val spark = SparkSession.builder()
        .master("local[2]")
        .appName("WordCount - Dataset")
        .getOrCreate()

      import spark.implicits._ // Need to import, otherwise error will be thrown

      val text = spark.read.textFile("file:///D:\\Big Data\\Input\\word*")
      val wordCount = text.flatMap(line => line.split(" "))
//      val grouped  = wordCount.groupByKey(identity).count()
      val grouped  = wordCount.groupByKey(x => x).count()
      grouped.show()

//      print("Number of Partitions : " + text.rdd.getNumPartitions)

  }
}
