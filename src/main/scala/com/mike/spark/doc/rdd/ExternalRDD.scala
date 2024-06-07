package com.mike.spark.doc.rdd
import org.apache.spark.{SparkConf, SparkContext}

/*
Some notes on reading files with Spark:
  1. If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.
  2. The textFile method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value.
     Note that you cannot have fewer partitions than blocks.
*/
//3.  All of Spark’s file-based input methods, including textFile, support running on directories, compressed files, and wildcards as well. For example, you can use textFile("/my/directory"), textFile("/my/directory/*.txt"), and textFile("/my/directory/*.gz").


object ExternalRDD {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("ExternalRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("file:///D:\\Big Data\\Input\\wordcount.txt")
    println("rdd1 partitions count : " + rdd1.getNumPartitions) // prints 2 , because of sc.defaultMinPartitions & file has less than 1 block size

    val rdd2 = sc.textFile("file:///D:\\Big Data\\Input\\wordcount.txt", 4)
    println("rdd2 partitions count : " + rdd2.getNumPartitions) // prints 4, because of explicit min partition value = 4

    val rdd3 = sc.textFile("file:///D:\\Big Data\\Input\\wordcount.txt", 1)
    println("rdd3 partitions count : " + rdd3.getNumPartitions) // prints 1, because of explicit min partition value = 1 & file has less than 1 block size

    val rdd4 = sc.wholeTextFiles("file:///D:\\Big Data\\Input\\wordcount.txt")
    //rdd4.foreach(println)

    val rdd5 = sc.wholeTextFiles("file:///D:\\Big Data\\Input")
    //rdd5.foreach(println)
    println("rdd5 partitions count : " + rdd5.getNumPartitions)

  }
}
