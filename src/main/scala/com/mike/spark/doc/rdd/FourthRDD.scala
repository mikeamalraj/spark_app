package com.mike.spark.doc.rdd
import org.apache.spark.{SparkConf, SparkContext}

object FourthRDD {
  def main(args : Array[String]): Unit ={
    val conf = new SparkConf().setAppName("FourthRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val value = Seq(("A", 1, "Z"), ("B", 1, "X"))
    sc.parallelize(value).foreach(println)
  }
}
