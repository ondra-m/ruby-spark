import java.io._
import scala.io.Source
import org.apache.spark._

object Scala {

  val logFile = new PrintWriter(new File(System.getenv("SCALA_LOG")))

  def log(args: Any*) {
    logFile.write(args.mkString(";"))
    logFile.write("\n")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scala")
    val sc = new SparkContext(conf)

    val workers = System.getenv("WORKERS").toInt
    val numbers = 0 until System.getenv("NUMBERS_COUNT").toInt
    val randomFilePath = System.getenv("RANDOM_FILE_PATH")

    val source = Source.fromFile(randomFilePath)
    val randomStrings = try source.mkString.split("\\s+") finally source.close()


    // =============================================================================
    // Serialization
    // =============================================================================

    var time: Long = 0

    time = System.currentTimeMillis
    val rddNumbers = sc.parallelize(numbers, workers)
    time = System.currentTimeMillis - time

    log("NumbersSerialization", time/1000.0)


    time = System.currentTimeMillis
    val rddStrings = sc.parallelize(randomStrings, workers)
    time = System.currentTimeMillis - time

    log("RandomStringSerialization", time/1000.0)


    time = System.currentTimeMillis
    val rddFileString = sc.textFile(randomFilePath, workers)
    time = System.currentTimeMillis - time

    log("TextFileSerialization", time/1000.0)


    // =============================================================================
    // Computing
    // =============================================================================

    time = System.currentTimeMillis
    rddNumbers.map{x => x*2}.collect()
    time = System.currentTimeMillis - time

    log("X2Computing", time/1000.0)


    time = System.currentTimeMillis
    rddNumbers.map{x => x*2}.map{x => x*3}.map{x => x*4}.collect()
    time = System.currentTimeMillis - time

    log("X2X3X4Computing", time/1000.0)


    time = System.currentTimeMillis
    val rdd = rddFileString.flatMap(line => line.split(" "))
                           .map(word => (word, 1))
                           .reduceByKey(_ + _)
    rdd.collect()
    time = System.currentTimeMillis - time
    log("WordCount", time/1000.0)


    sc.stop()
    logFile.close()
  }
}
