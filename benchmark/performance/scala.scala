import java.io._
import scala.math
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
    val numbersCount = System.getenv("NUMBERS_COUNT").toInt
    val textFile = System.getenv("TEXT_FILE")

    val numbers = 0 until numbersCount
    val floats = numbers.map(_.toDouble)
    val strings = Source.fromFile(textFile).mkString.split("\n")


    // =============================================================================
    // Serialization
    // =============================================================================

    var time: Long = 0

    time = System.currentTimeMillis
    val rddNumbers = sc.parallelize(numbers, workers)
    time = System.currentTimeMillis - time

    log("NumbersSerialization", time/1000.0)


    time = System.currentTimeMillis
    val rddFloats = sc.parallelize(floats, workers)
    time = System.currentTimeMillis - time

    log("FloatsSerialization", time/1000.0)


    time = System.currentTimeMillis
    val rddStrings = sc.parallelize(strings, workers)
    time = System.currentTimeMillis - time

    log("StringsSerialization", time/1000.0)


    // =============================================================================
    // Computing
    // =============================================================================

    // --- Is prime? ---------------------------------------------------------------

    time = System.currentTimeMillis
    val primes = rddNumbers.map{ x =>
      if(x < 2){
        (x, false)
      }
      else if(x == 2){
        (x, true)
      }
      else if(x % 2 == 0){
        (x, false)
      }
      else{
        val upper = math.sqrt(x.toDouble).toInt
        var result = true

        var i = 3
        while(i <= upper && result == true){
          if(x % i == 0){
            result = false
          }
          else{
            i += 2
          }
        }

        (x, result)
      }
    }
    primes.collect()
    time = System.currentTimeMillis - time

    log("IsPrime", time/1000.0)


    // --- Matrix multiplication ---------------------------------------------------

    val matrixSize = System.getenv("MATRIX_SIZE").toInt

    val matrix = new Array[Array[Long]](matrixSize)

    for( row <- 0 until matrixSize ) {
      matrix(row) = new Array[Long](matrixSize)
      for( col <- 0 until matrixSize ) {
        matrix(row)(col) = row + col
      }
    }

    time = System.currentTimeMillis
    val rdd = sc.parallelize(matrix, 1)
    rdd.mapPartitions { it =>
      val matrix = it.toArray
      val size = matrix.size

      val newMatrix = new Array[Array[Long]](size)

      for( row <- 0 until size ) {
        newMatrix(row) = new Array[Long](size)
        for( col <- 0 until size ) {

          var result: Long = 0
          for( i <- 0 until size ) {
            result += matrix(row)(i) * matrix(col)(i)
          }
          newMatrix(row)(col) = result
        }
      }

      newMatrix.toIterator
    }
    time = System.currentTimeMillis - time

    log("MatrixMultiplication", time/1000.0)


    // --- Pi digits ---------------------------------------------------------------
    // http://rosettacode.org/wiki/Pi#Scala

    val piDigit = System.getenv("PI_DIGIT").toInt

    time = System.currentTimeMillis
    val piDigits = sc.parallelize(Array(piDigit), 1)
    piDigits.mapPartitions { it =>
      var size = it.toArray.asInstanceOf[Array[Int]](0)
      var result = ""

      var r: BigInt = 0
      var q, t, k: BigInt = 1
      var n, l: BigInt = 3
      var nr, nn: BigInt = 0

      while(size > 0){
        while((4*q+r-t) >= (n*t)){
          nr = (2*q+r)*l
          nn = (q*(7*k)+2+(r*l))/(t*l)
          q = q * k
          t = t * l
          l = l + 2
          k = k + 1
          n  = nn
          r  = nr
        }

        result += n.toString
        size -= 1
        nr = 10*(r-n*t)
        n  = ((10*(3*q+r))/t)-(10*n)
        q  = q * 10
        r  = nr
      }

      Iterator(result)
    }
    time = System.currentTimeMillis - time

    log("PiDigit", time/1000.0)


    sc.stop()
    logFile.close()
  }
}
