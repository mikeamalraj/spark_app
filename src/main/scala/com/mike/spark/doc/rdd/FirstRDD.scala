package com.mike.spark.doc.rdd

import org.apache.spark.{SparkContext,SparkConf}

object FirstRDD {
    def main(args : Array[String]): Unit ={
      val conf = new SparkConf().setMaster("local[4]").setAppName("FirstRDD")  // Starts with 4 cores
      val sc = new SparkContext(conf)

      //USING ARRAY
      val array = Array(1,2,3,4,5,6,7,8,9,10)
      val data = sc.parallelize(array)
      data.foreach(println)

      println("Partitions count : " + data.getNumPartitions) // default: 4
      println("Partitions count : " + data.partitions.size) // default: 4
      println("Partitions count : " + data.partitions.length) // default: 4
      println("defaultMinPartitions : " + sc.defaultMinPartitions)
      println("defaultParallelism : " + sc.defaultParallelism) // default: 4

      val data2 = sc.parallelize(array, 10)
      data2.foreach(println)

      println("Partitions count : " + data2.getNumPartitions)
      println("Partitions count : " + data2.partitions.size)
      println("Partitions count : " + data2.partitions.length)
      println("defaultMinPartitions : " + sc.defaultMinPartitions)
      println("defaultParallelism : " + sc.defaultParallelism)

      // USING SEQ
      val seq = Seq(1,2,3,4,5,6,7,8,9,10)
      val data3 = sc.parallelize(seq)
      data3.foreach(println)

      //USING LIST
      val list = List(1,2,3,4,5,6,7,8,9,10)
      val data4 = sc.parallelize(list)
      data4.foreach(println)



    }
}
