name := "spark_app"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector-driver
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-driver" % "3.0.0"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0"
// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.10.6"
// https://mvnrepository.com/artifact/io.netty/netty-all
libraryDependencies += "io.netty" % "netty-all" % "4.0.4.Final"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
