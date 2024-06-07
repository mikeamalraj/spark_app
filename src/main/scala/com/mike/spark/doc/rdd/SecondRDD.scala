package com.mike.spark.doc.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SecondRDD {
  def main(args:  Array[String]): Unit ={

    val conf = new SparkConf().setAppName("SecondRDD").setMaster("local") // starts with one core
    val sc = new SparkContext(conf)

    val value = (1 to 10).toList
    val data = sc.parallelize(value)
    data.foreach(println)

    println("Default parallelism : " +  sc.defaultParallelism)  // default: 1

    val value2 = (20 to 30).toArray
    val data2 = sc.parallelize(value2)
    data2.foreach(println)


    val value3 = (30 to 40).toSeq
    val data3 = sc.parallelize(value3)
    data3.foreach(println)

    val value4 = (50 to 60).map(x => Map(x -> 0))
    val data4 = sc.parallelize(value4)
    data4.foreach(println)

  }
}
