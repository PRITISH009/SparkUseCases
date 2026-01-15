package sparksessionexamples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object CreatingSparkSession extends App with Logging {
  val spark = SparkSession.builder()
    .appName("Creating Spark Session")
    .master("local[*]")
    .getOrCreate()

  log.info(s"Created Spark Session Object with ${spark.sparkContext.appName}")

  spark.stop()
}
