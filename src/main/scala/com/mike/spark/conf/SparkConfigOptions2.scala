package com.mike.spark.conf

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkConfigOptions2 {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Hello World1")
      .setMaster("local[2]")
    //  .set("key", "value")
    val sc = new SparkContext(conf)

    sc.getConf.set("key1", "value1") // doesn't work - Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified by the user. Spark does not support modifying the configuration at runtime.
    sc.getConf.getAll.foreach(println)

    // val sc = new SparkContext(new SparkConf())  // creating empty config also possible... but values should be passed during spark-submit
    //  ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
    //  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar

    val spark = SparkSession.builder()
      //.config(conf)
      .appName("Hello World2")
      .master("local[4]")
      .config(conf) // it will override existing configs if already set. Ex appName, Master
      .config("key2", "value2")
      .getOrCreate()

    spark.conf.set("key3", "value3")

    spark.conf.getAll.foreach(println)



  }
}
