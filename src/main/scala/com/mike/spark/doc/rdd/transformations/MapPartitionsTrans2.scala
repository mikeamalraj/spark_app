package com.mike.spark.doc.rdd.transformations
import org.apache.spark.{SparkConf, SparkContext}

// To test preservesPartitioning - 2nd argument to mapPartitions
// https://medium.com/@corentinanjuna/apache-spark-rdd-partitioning-preservation-2187a93bc33e

object MapPartitionsTrans2 {

  def myFunc(iter: Iterator[Int]) : Iterator[String] = {
    iter.map{case(x) => ("A" + x)}
  }

  def myFuncPair(iter: Iterator[(Int, String)]) : Iterator[(Int, String)] = {
    iter.map{record => (record._1, "Mike")}
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsTrans")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,1,4,1,4,5,6), 3)//.toDebugString // NOT POSSIBLE.partitionBy(new HashPartitioner(7000))
    val rNP1 = rdd1.partitions.size
    val isD1 = rdd1.partitioner.isDefined
    val typ1 = "" //rdd1.partitioner.get
    println(s"rNP1 : $rNP1 , isD1 : $isD1, typ1 : $typ1" )

    val rdd2 = rdd1.mapPartitions(myFunc, true) // or false
    val rNP2 = rdd2.partitions.size
    val isD2 = rdd2.partitioner.isDefined
    val typ2 = "" //rdd1.partitioner.get
    println(s"rNP2 : $rNP2 , isD2 : $isD2, typ2 : $typ2" )

    // Show data and distribution
    val mapped =   rdd2.mapPartitionsWithIndex{
      (index, iterator) => {
        println("Called in Partition -> " + index)
        val myList = iterator.toList
        myList.map(x => x + " -> " + index).iterator
      }
    }
    //println(mapped.collect())

    val rddPair1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,1,4,1,4,5,6), 3).map(x=>(x, "Hello"))
    val rpNP1 = rddPair1.partitions.size
    val isDP1 = rddPair1.partitioner.isDefined
    val typP1 = "" //rddPair1.partitioner.get
    println(s"rpNP1 : $rpNP1 , isDP1 : $isDP1, typP1 : $typP1" )

    val rddPair2 = rddPair1.mapPartitions(myFuncPair, true) // or false
    val rpNP2 = rddPair2.partitions.size
    val isDP2 = rddPair2.partitioner.isDefined
    val typP2 = "" //rddPair2.partitioner.get
    println(s"rpNP2 : $rpNP2 , isDP2 : $isDP2, typP2 : $typP2" )

    rddPair2.collect()

  }
}
