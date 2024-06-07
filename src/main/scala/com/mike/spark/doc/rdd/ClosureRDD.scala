package com.mike.spark.doc.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ClosureRDD {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("Closure")
    val sc = new SparkContext(conf)
    val data = (1 to 10).toSeq
    val rdd = sc.parallelize(data)
    var counter = 0

    // Wrong: Don't do this!! -> https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-
    rdd.foreach(x => counter += x)

    println("Counter value: " + counter)
  }
}
