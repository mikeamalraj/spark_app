package com.mike.spark.doc.rdd.transformations
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexTrans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsWithIndexTrans")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, partition) =>{
      partition.map( record => (index, record))
    })
    rdd2.collect().foreach(println)
    println(rdd1.glom().collect()(1).foreach(println))
  }
}
