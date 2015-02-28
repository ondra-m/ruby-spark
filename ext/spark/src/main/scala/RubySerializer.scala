package org.apache.spark.api.ruby

import java.io.{DataInputStream, DataOutputStream, ByteArrayInputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}


/* =================================================================================================
 * object RubySerializer
 * =================================================================================================
 */
object RubySerializer {

  /**
   * Convert an RDD of serialized Ruby objects to RDD of objects, that is usable in Java.
   */
  def rubyToJava(rbRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    rbRDD.rdd.mapPartitions { iter =>
      iter.flatMap { item =>
        println(item.map(_.toInt).mkString(" "))

        val obj = Marshal.load(item)
        if(batched){
          println(obj.asInstanceOf[Array[_]].mkString(" "))
          obj.asInstanceOf[Array[_]]
        }
        else{
          Seq(item)
        }

      }
    }.toJavaRDD()
  }

}


/* =================================================================================================
 * class Marshal
 * =================================================================================================
 */
class Marshal(is: DataInputStream) {

  val ENCODING_SYMBOL_SPECIAL = "E"
  val ENCODING_SYMBOL = "encoding"

  // Registered classes
  // used for ';'
  val symbols = ArrayBuffer[String]()

  // Convert Array[Byte] into Java objects
  def load: Any = {
    load(is.readUnsignedByte.toChar)
  }

  def load(dataType: Char): Any = {
    dataType match {
      case '0' => null
      case 'T' => true
      case 'F' => false
      case 'i' => loadInt
      case 'f' => loadFloat
      case '"' => loadString
      case 'u' => loadUserObject
      case 'I' => loadInstanceVar
      case '[' => loadArray
      case _ =>
        // 'o': object
        // '/': regexp
        // ':': symbol
        // '{': hash
        // '}': hash with a default value
        // 'c': class
        // 'm': module
        // 'e': extended
        // 'l': bignum
        // 'S': struct
        // 'U': user new unmarshal
        // 'C': u class unmarshal
        throw new IllegalArgumentException(s"Format is not supported: $dataType.")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Load by type

  def loadInt: Int = {
    var c = is.readByte.toInt

    if (c == 0) {
      return 0
    } else if (4 < c && c < 128) {
      return c - 5
    } else if (-129 < c && c < -4) {
      return c + 5
    }

    var result: Long = 0

    if (c > 0) {
      result = 0
      for( i <- 0 until c ) {
        result |= (is.readUnsignedByte << (8 * i)).toLong
      }
    } else {
      c = -c
      result = -1
      for( i <- 0 until c ) {
        result &= ~((0xff << (8 * i)).toLong)
        result |= (is.readUnsignedByte << (8 * i)).toLong
      }
    }

    result.toInt
  }

  def loadFloat: Double = {
    val string = loadString
    string match {
      case "nan"  => Double.NaN
      case "inf"  => Double.PositiveInfinity
      case "-inf" => Double.NegativeInfinity
      case _ => string.toDouble
    }
  }

  def loadString: String = {
    new String(loadStringBytes)
  }

  // Load String as Array[Byte]
  // Is used by other methods
  def loadStringBytes: Array[Byte] = {
    val size = loadInt
    val buffer = new Array[Byte](size)

    var readSize = 0
    while(readSize < size){
      val read = is.read(buffer, readSize, size-readSize)

      if(read == -1){
        throw new IllegalArgumentException("Marshal too short.")
      }

      readSize += read
    }

    buffer
  }

  // Load object with custom _dump
  def loadUserObject: Object = {
    var klass: String = ""

    is.readByte match {
      case ':' =>
        klass = loadString
        symbols += klass
      case ';' =>
        val index = loadInt
        klass = symbols(index)
    }

    klass match {
      case "Spark::Mllib::LabeledPoint" => loadLabeledPoint
      case other =>
        throw new IllegalArgumentException(s"Object $other is not supported.")
    }
  }

  def loadArray: Array[Any] = {
    val size = loadInt
    val array = new Array[Any](size)

    for( i <- 0 until size ) {
      array(i) = load
    }

    array
  }


  // ----------------------------------------------------------------------------------------------
  // Load others

  // Supported is only String
  def loadInstanceVar: String = {
    val dataType = is.readUnsignedByte.toChar

    if(dataType != '"'){
      throw new IllegalArgumentException("Instance variable can have only String.")
    }

    var bytes = loadStringBytes
    var obj = new String(bytes)

    // Set variables
    val count = loadInt

    for( i <- 0 until count ) {
      val key = unmarshalObject

      key.toString match {
        case ENCODING_SYMBOL_SPECIAL =>
          if(unmarshalObject == true){
            obj = new String(bytes, "UTF-8")
          }
          else{
            obj = new String(bytes, "US-ASCII")
          }
        case ENCODING_SYMBOL =>
          throw new IllegalArgumentException("Supported is UTF-8 or US-ASCII.")
        case _ =>
          // obj.getClass.getMethod(key).invoke(obj)
          throw new IllegalArgumentException("Supported is only encoding.")
      }
    }

    obj
  }

  def unmarshalObject = {
    val dataType = is.readUnsignedByte.toChar

    if(isLinkType(dataType)){
      throw new IllegalArgumentException("Link type is not supported.")
    }
    else{
      load(dataType)
    }
  }


  // ----------------------------------------------------------------------------------------------
  // Load user defined

  def loadLabeledPoint: LabeledPoint = {
    val dataSize = loadInt

    val label = is.readLong
    is.readByte.toChar match {
      case 'd' =>
        val features = new DenseVector(readArray[Double])
        new LabeledPoint(label, features)
      case 's' =>
        val size = is.readInt
        val indices = readArray[Int]
        val values = readArray[Double]

        val features = new SparseVector(size, indices, values)
        new LabeledPoint(label, features)
    }
  }


  // ----------------------------------------------------------------------------------------------
  // Helpers

  def isLinkType(dataType: Char): Boolean = {
    dataType == ';' || dataType == '@'
  }

  def readArray[T: ClassTag]: Array[T] = {
    val size = is.readInt
    val values = new Array[T](size)

    classTag[T].toString match {
      case "Int" =>    for( i <- 0 until size ) values(i) = is.readInt.asInstanceOf[T]
      case "Double" => for( i <- 0 until size ) values(i) = is.readDouble.asInstanceOf[T]
    }

    values
  }
}


/* =================================================================================================
 * object Marshal
 * =================================================================================================
 */
object Marshal {
  def load(bytes: Array[Byte]) = {
    val is = new DataInputStream(new ByteArrayInputStream(bytes))

    val majorVersion = is.readUnsignedByte // 4
    val minorVersion = is.readUnsignedByte // 8

    (new Marshal(is)).load
  }
}
