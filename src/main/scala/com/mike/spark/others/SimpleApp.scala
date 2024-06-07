package com.mike.spark.others

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //println(System.getenv("HADOOP_HOME"))
    //System.setProperty("hadoop.home.dir", "D:\\Big Data\\winutils");
    val logFile = "file:///D:/Big Data/Notes" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    /*val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")*/

    import spark.implicits._
    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")

    someDF.write.format("hive").insertInto("test")
    spark.stop()
  }

}
