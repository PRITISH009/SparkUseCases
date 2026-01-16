package sparksessionexamples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object MultipleSparkSessions extends App {

  // First line: silence Spark noise
  val log = Logger.getLogger(getClass.getName)

  log.info("Creating Spark Session Object 1")
  val sparkObj1 = SparkSession.builder()
    .master("local[*]")
    .appName("Test App 1")
    .getOrCreate()

  log.info(s"Spark Session Obj 1 App Id - ${sparkObj1.sparkContext.applicationId}")
  log.info(s"Spark Session Obj 1 Hash - ${sparkObj1.hashCode()}")

  log.info("Creating Spark Session Object 2")
  val sparkObj2 = SparkSession.builder()
    .master("local[*]")
    .appName("Test App 2")
    .getOrCreate()

  log.info(s"Spark Session Object 2 AppId - ${sparkObj2.sparkContext.applicationId}")
  log.info(s"Spark Session Object 2 Hash Code - ${sparkObj2.hashCode()}")

  log.info(s"Spark Session are eq - ${sparkObj1 eq sparkObj2}")
  log.info(s"Do both obj share same spark context - ${sparkObj1.sparkContext eq sparkObj2.sparkContext}")

  log.info("Stopping Spark Session Obj 1")
  sparkObj1.stop()

  Thread.sleep(3000)

  log.info("Creating Spark Session Obj 3 after stopping")
  val sparkObj3 = SparkSession.builder()
    .appName("Test App 3")
    .master("local[*]")
    .getOrCreate()

  log.info(s"Spark Session Object 3 App Id = ${sparkObj3.sparkContext.applicationId}")
  log.info(s"Same as Spark 1? -> ${sparkObj1.sparkContext.applicationId == sparkObj3.sparkContext.applicationId}")
  log.info(s"Same as Spark 2? -> ${sparkObj2.sparkContext.applicationId == sparkObj3.sparkContext.applicationId}")

  // getOrCreate function returns the same spark session object if there is an active SparkContext in the JVM period.
  // It only provides new spark session object if the current Spark Session is stopped completely.


}
