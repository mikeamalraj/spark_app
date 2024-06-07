package com.mike.spark.core

import java.io.IOException
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class MySparkSession {

  @transient private var sparkSession: SparkSession = null
  private var configMap: Map[String, String] = null

  val logger = LoggerFactory.getLogger(classOf[MySparkSession])

  def loadProperties(): Unit ={
    try{
      val properties = new Properties()
      properties.load(getClass().getResourceAsStream("/default.properties"))
      PropertyConfigurator.configure(properties)
      configMap = properties.stringPropertyNames().asScala
        .map { k => (k, properties.getProperty(k)) }
        .toMap
    }catch{
      case e: IOException => println("Exception occurred..." +  e.printStackTrace())
        System.exit(1)
    }
    finally{
      // println("Properties file loaded successfully")
    }
  }

  def getSparkSession() : SparkSession = {
    loadProperties()
    val sparkConf = new SparkConf().setAll(configMap)
    if(sparkSession == null){
      sparkSession = SparkSession.builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    sparkSession
  }
}
