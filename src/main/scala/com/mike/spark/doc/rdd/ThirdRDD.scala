package com.mike.spark.doc.rdd
import org.apache.spark.{SparkConf, SparkContext}

object ThirdRDD {
  def main(args :  Array[String]): Unit ={
    val conf = new SparkConf().setAppName("ThirdRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //SEQ
    val seqPair = Seq(("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2), ("B", 3), ("C", 1), ("C", 2), ("C", 3))
    val rdd1 = sc.parallelize(seqPair)
    rdd1.foreach(println)

    //ListPair
    val listPair = List(("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2), ("B", 3), ("C", 1), ("C", 2), ("C", 3))
    val rdd2 = sc.parallelize(listPair)
    rdd2.foreach(println)

    //ArrayPair
    val arrayPair = Array(("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2), ("B", 3), ("C", 1), ("C", 2), ("C", 3))
    val rdd3 = sc.parallelize(arrayPair)
    rdd3.foreach(println)

  }
}
