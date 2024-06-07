package com.mike.spark.conf
import org.apache.spark.sql.SparkSession
object MasterURL {
  def main(args : Array[String]): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    spark.conf.getAll.foreach(println)

    /*
    Exception in thread "main" org.apache.spark.SparkException: A master URL must be set in your configuration
    at org.apache.spark.SparkContext.<init>(SparkContext.scala:385)
    at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2574)
    at org.apache.spark.sql.SparkSession$Builder.$anonfun$getOrCreate$2(SparkSession.scala:934)
    at scala.Option.getOrElse(Option.scala:189)
    at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:928)
    at com.mike.spark.conf.MasterURL$.main(MasterURL.scala:5)
    at com.mike.spark.conf.MasterURL.main(MasterURL.scala)
    */

  }
}
