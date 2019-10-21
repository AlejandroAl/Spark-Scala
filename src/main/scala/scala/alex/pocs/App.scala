package scala.alex.pocs

import org.apache.spark.sql.SparkSession
/**
 * @author ${user.name}
 */
object App {
  
 def main(args : Array[String]) {
   val spark = SparkSession.builder()
     .appName("BancoBaseSparkBatch")
     .master("local[*]")
     .getOrCreate()

   import spark.implicits._

   val sc = spark.sparkContext

   println(spark.version)


  }
}
