package com.mike.spark.others

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Cassandra {
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Cassandra App")
      .withExtensions(new CassandraSparkExtensions)
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    spark.conf.set("spark.cassandra.connection.host","localhost")
    spark.conf.set("spark.cassandra.connection.port","9042")
    spark.conf.set(s"spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

    val df = spark.read.table("casscatalog.mykeyspace.student")
    df.printSchema()
    df.show()

  }
}
