package com.mike.spark.doc.rdd
import com.mike.spark.doc.rdd.functions.{MapWords, MapWordsClass}
import org.apache.spark.{SparkConf, SparkContext}

object PassingFunctions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:///D:\\Big Data\\Input\\wordcount.txt")
    val data1 = data.flatMap(x => x.split(" "))

    /*val data2 = data1.map(MapWords.createTuple)
    print(data2.countByKey())*/

    /*val data3 = data1.map(new MapWordsClass().createTuple)
    print(data3.countByKey())*/

    /*val data4 = data1.map(new MapWordsClass().createTuple2)
    print(data4.countByKey())*/

    /*val data5 = data1.map(new MapWordsClass().createTuple3)
    print(data5.countByKey())*/

    val data6 = data1.map( x => {
      println("Hello, Anonymous function")
      val y = 10
      (x, 1)
    })
    print(data6.countByKey())

  }
}