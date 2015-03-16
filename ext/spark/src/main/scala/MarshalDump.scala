package org.apache.spark.api.ruby.marshal

import java.io.{DataInputStream, DataOutputStream, ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, DenseVector, SparseVector}


/* =================================================================================================
 * class MarshalDump
 * =================================================================================================
 */
class MarshalDump(os: DataOutputStream) {

  val NAN_BYTELIST               = "nan".getBytes
  val NEGATIVE_INFINITY_BYTELIST = "-inf".getBytes
  val INFINITY_BYTELIST          = "inf".getBytes

  def dump(data: Any) {
    data match {
      case null =>
        os.writeByte('0')

      case item: Boolean =>
        val char = if(item) 'T' else 'F'
        os.writeByte(char)

      case item: Int =>
        os.writeByte('i')
        dumpInt(item)

      case item: Array[_] =>
        os.writeByte('[')
        dumpArray(item)

      case item: Double =>
        os.writeByte('f')
        dumpFloat(item)

      case item: ArrayBuffer[Any] => dump(item.toArray)
    }
  }

  def dumpInt(data: Int) {
    if(data == 0){
      os.writeByte(0)
    }
    else if (0 < data && data < 123) {
      os.writeByte(data + 5)
    }
    else if (-124 < data && data < 0) {
      os.writeByte((data - 5) & 0xff)
    }
    else {
      val buffer = new Array[Byte](4)
      var value = data

      var i = 0
      while(i != 4 && value != 0 && value != -1){
        buffer(i) = (value & 0xff).toByte
        value = value >> 8

        i += 1
      }
      val lenght = i + 1
      if(value < 0){
        os.writeByte(-lenght)
      }
      else{
        os.writeByte(lenght)
      }
      os.write(buffer, 0, lenght)
    }
  }

  def dumpArray(array: Array[_]) {
    dumpInt(array.size)

    for(item <- array) {
      dump(item)
    }
  }

  def dumpFloat(value: Double) {
    if(value.isPosInfinity){
      dumpString(NEGATIVE_INFINITY_BYTELIST)
    }
    else if(value.isNegInfinity){
      dumpString(INFINITY_BYTELIST)
    }
    else if(value.isNaN){
      dumpString(NAN_BYTELIST)
    }
    else{
      // dumpString("%.17g".format(value))
      dumpString(value.toString)
    }
  }

  def dumpString(data: String) {
    dumpString(data.getBytes)
  }

  def dumpString(data: Array[Byte]) {
    dumpInt(data.size)
    os.write(data)
  }

}
