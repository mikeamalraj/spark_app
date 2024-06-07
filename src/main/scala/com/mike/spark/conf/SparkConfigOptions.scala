package com.mike.spark.conf
import com.mike.spark.core.MySparkSession

object SparkConfigOptions {
  def main(args : Array[String]): Unit ={
    val spark = new MySparkSession().getSparkSession();
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //sc.getConf.getAll.foreach(println)
    //spark.conf.getAll.foreach(println)

    /*println(sc.startTime)
    println(sc.sparkUser)
    println(sc.applicationAttemptId)
    println(sc.applicationId)
    println(sc.defaultParallelism)
    println(sc.defaultMinPartitions)
    println(sc.version)
    sc.getConf.getExecutorEnv*/

    /*We can set executor environment variables as well
    * http://spark.apache.org/docs/1.3.0/configuration.html#runtime-environment
    * spark.executorEnv.[EnvironmentVariableName]
    *
    *http://spark.apache.org/docs/latest/running-on-yarn.html#spark-properties
    * spark.yarn.appMasterEnv.[EnvironmentVariableName]
    * Note: When running Spark on YARN in cluster mode, environment variables need to be set using the spark.yarn.appMasterEnv.[EnvironmentVariableName] property in your conf/spark-defaults.conf file. Environment variables that are set in spark-env.sh will not be reflected in the YARN Application Master process in cluster mode
    * */

  }
}
