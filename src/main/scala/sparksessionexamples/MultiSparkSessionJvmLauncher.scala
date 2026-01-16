package sparksessionexamples

import org.apache.log4j.Logger

import java.io.File
import java.util.concurrent.Executors

object MultiSparkSessionJvmLauncher extends App {
  // First line: silence Spark noise
  val log = Logger.getLogger(getClass.getName)

  def runJvm(instanceId: Int): Int = {
    val jarPath = "target/spark-usecases-1.0-SNAPSHOT.jar"

    // Verify JAR exists
    if (!new File(jarPath).exists()) {
      log.error(s"JAR not found at $jarPath. Run 'mvn clean package' first!")
      return 1
    }

    val javaCmd = Seq(
      "java",

      // SAFE JVM memory (protect local machine)
      "-Xmx1G",
      "-Xms512M",

      // Fix Java 11+ reflection warnings
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",

      // Per scenario system properties (optional)
      s"-Dscenario.id=$instanceId",

      // Classpath - point to the FAT JAR
      "-cp", jarPath,

      // Main class to run inside the JVM (with package name)
      "sparksessionexamples.SparkWorker",

      // Args passed to SparkWorker.scala
      instanceId.toString
    )

    log.info(s"Launching JVM Instance $instanceId with command:\n${javaCmd.mkString(" ")}\n")

    // Create logs directory if it doesn't exist
    val logsDir = new File("logs")
    if (!logsDir.exists()) {
      logsDir.mkdirs()
      log.info("Created logs directory")
    }

    val processBuilder = new ProcessBuilder(javaCmd: _*)
    processBuilder.redirectErrorStream(true)
    processBuilder.redirectOutput(new File(s"logs/instance-$instanceId.log"))

    val process = processBuilder.start()
    val exitCode = process.waitFor()

    log.info(s"JVM Instance $instanceId exited with code: $exitCode")

    exitCode
  }

  log.info("Starting Multi-JVM Spark Session Launcher")
  val instances = Seq(1, 2, 3, 4)

  val pool = Executors.newFixedThreadPool(4)

  val futures = instances.map { instanceId =>
    pool.submit(() => runJvm(instanceId))
  }

  // Wait for all to complete and check exit codes
  val exitCodes = futures.map(_.get())


  Thread.sleep(300000)
  pool.shutdown()

  log.info("All JVMs Completed")
  exitCodes.zipWithIndex.foreach { case (code, idx) =>
    log.info(s"Instance ${instances(idx)} exit code: $code")
  }

}
