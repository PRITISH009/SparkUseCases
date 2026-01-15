package sparksessionexamples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object CreatingSparkSession extends App {

  // First line: silence Spark noise
  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Creating Spark Session")
    .master("local[*]")
    .getOrCreate()

  log.info(s"Created Spark Session Object with ${spark.sparkContext.appName}")

  spark.stop()
}
