package com.mike.spark.doc.rdd.transformations
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsTrans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsTrans")
    val sc = new SparkContext(conf)
    val mapData = sc.parallelize(1 to 100, 5).map(x=>(x, "Hello"))

    println("Record count of mapData is : " +  mapData.count())
    println("Partitions count of mapData is : " + mapData.getNumPartitions)

    val mapPart = mapData.mapPartitions(
      partition => {
        //partition is iterator & return value is also iterator
        val exracted = partition.map(element => element._1 )
        exracted // Iterator[Int]
      }
    )

    println("Record count of mapPart is : " +  mapPart.count())
    println("Partitions count of mapPart is : " + mapPart.getNumPartitions)

    val mapData2 = mapPart.map(record => (record, "Mike"))
    mapData2.foreach(println)

    // Proper way to use dbconnection - This is to lookup values from db.. to store data use forEachPartition
    // https://stackoverflow.com/questions/37881042/spark-db-connection-per-spark-rdd-partition-and-do-mappartition/37915154#37915154

    /*val newRd = myRdd.mapPartitions(partition => {
      val connection = new DbConnection /*creates a db connection per partition*/

      val newPartition = partition.map(record => {
        readMatchingFromDB(record, connection)
      }).toList // consumes the iterator, using toList to force eager computation - make it happen now when connection is open

      connection.close()
      newPartition.iterator // create a new iterator
    })*/

  }

}
