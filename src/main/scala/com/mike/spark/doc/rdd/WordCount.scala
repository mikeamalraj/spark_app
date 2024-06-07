package com.mike.spark.doc.rdd
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:///D:\\Big Data\\Input\\wordcount.txt")
    val data1 = data.flatMap(x => x.split(" "))
    val data2 = data1.map(word => (word, 1))
    val data3 = data2.reduceByKey((value1, value2) => value1 + value2)
    //data3.foreach(println)

    val reduced = data3.reduce((pair1, pair2) => (pair1._1 + pair2._1, pair1._2 + pair2._2))
    //val reduced = data3.reduce((pair1, pair2) => (pair1._1 + pair2._1, pair1._2 + pair2._2))._2
    println(reduced)

  }
}
