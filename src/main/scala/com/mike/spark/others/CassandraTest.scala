package com.mike.spark.others
//Spark connector
import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Student(id: Int, age: Int, name:String)

object CassandraTest {
  def main(args: Array[String]) {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val spark:SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Cassandra App")
      .withExtensions(new CassandraSparkExtensions)
      .getOrCreate()

    spark.conf.set("spark.cassandra.connection.host","localhost")
    spark.conf.set("spark.cassandra.connection.port","9042")

    //spark.conf.set("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions")

    spark.conf.set(s"spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

   /* val simpleSchema = StructType(Array(
      StructField("id",IntegerType,true),
      StructField("age",IntegerType,true),
      StructField("name",StringType,true)
    ))*/

    //print(simpleSchema)

    /*val schema = StructType.fromDDL("id bigint, age bigint, name string")
    print(schema)*/

    /*val readDF = spark.read.cassandraFormat("student", "mykeyspace", "").load()
    readDF.printSchema()*/

    /*val readDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "student", "keyspace" -> "mykeyspace"))
      .load

    //val readDF2 = spark.createDataFrame(readDF.rdd, simpleSchema)
    readDF.printSchema()*/

    /*val readDS = readDF.as[Student]
    readDS.show()*/

    //print(readDF.rdd.collect())

   /* val rdd = spark.sparkContext.cassandraTable("mykeyspace", "student")
    println(rdd.collect().foreach(println))*/

    val df = spark.read.table("casscatalog.mykeyspace.student")
    df.printSchema()
    df.show()

  }

}
