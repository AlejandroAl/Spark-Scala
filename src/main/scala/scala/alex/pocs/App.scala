package scala.alex.pocs

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig

/**
 * @author ${user.name}
 */
object App {
  
 def main(args : Array[String]) {
   val spark = SparkSession.builder()
     .appName("BancoBaseSparkBatch")
     .master("local[*]")
     .config("spark.sql.autoBroadcastJoinThreshold","50485760")
     .config("spark.sql.join.preferSortMergeJoin", "true")
     .config("spark.sql.codegen.wholeStage","true")
     .config("spark.sql.inMemoryColumnarStorage.compressed","true")
     .config("spark.sql.codegen","true")
     .getOrCreate()

   import spark.implicits._

   val sc = spark.sparkContext

   val uriExample = "mongodb://host:port/baseDatos.collection"

   val dfExample = sc.loadFromMongoDB(ReadConfig(Map("uri" -> uriExample))).toDF()

   dfExample.show()


  }
}
