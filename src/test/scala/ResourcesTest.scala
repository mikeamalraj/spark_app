
import com.mike.spark.core.MySparkSession
import java.util.Properties

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object ResourcesTest {
 def main(args : Array[String]): Unit ={
   val connectionParam = new Properties()
   connectionParam.load(getClass().getResourceAsStream("/default.properties"))
   println(connectionParam)

   val confMap = connectionParam.stringPropertyNames().asScala
     .map { k => (k, connectionParam.getProperty(k)) }
     .toMap

   println(confMap)

   val sparkConf = new SparkConf().setAll(confMap);
   sparkConf.getAll.foreach(println)

   val sparkSession = SparkSession.builder()
     .config(sparkConf)
     .enableHiveSupport()
     .getOrCreate()

   println(sparkSession)
 }
}
