package sparksessionexamples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkWorker extends App {
  // First line: silence Spark noise
  val log = Logger.getLogger(getClass.getName)

  if (args.length == 0) {
    log.error("No instance ID provided!")
    System.exit(1)
  }

  val instanceId = args(0)

  log.info(s"=== Starting Spark Worker Instance $instanceId ===")

  val spark = SparkSession.builder()
    .appName(s"Starting Spark Worker Instance $instanceId")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", s"/tmp/warehouse-$instanceId")
    .getOrCreate()

  log.info(s"Spark Session Created in JVM for Instance $instanceId")
  log.info(s"Spark Context App Id = ${spark.sparkContext.applicationId}")
  log.info(s"Spark UI available at: ${spark.sparkContext.uiWebUrl.getOrElse("N/A")}")


  // Do some actual work to make it interesting
  import spark.implicits._
  val df = Seq(
    (s"Instance-$instanceId", 1),
    (s"Instance-$instanceId", 2),
    (s"Instance-$instanceId", 3)
  ).toDF("instance", "value")

  log.info(s"Instance $instanceId processed ${df.count()} rows")

  // Sleep to simulate work
  Thread.sleep(2000)

  spark.stop()

  log.info(s"=== Spark Worker Instance $instanceId Completed ===")
}
