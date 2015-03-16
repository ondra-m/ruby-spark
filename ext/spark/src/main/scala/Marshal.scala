package org.apache.spark.api.ruby.marshal

import java.io.{DataInputStream, DataOutputStream, ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._


/* =================================================================================================
 * object Marshal
 * =================================================================================================
 */
object Marshal {
  def load(bytes: Array[Byte]) = {
    val is = new DataInputStream(new ByteArrayInputStream(bytes))

    val majorVersion = is.readUnsignedByte // 4
    val minorVersion = is.readUnsignedByte // 8

    (new MarshalLoad(is)).load
  }

  def dump(data: Any) = {
    val aos = new ByteArrayOutputStream
    val os = new DataOutputStream(aos)

    os.writeByte(4)
    os.writeByte(8)

    (new MarshalDump(os)).dump(data)
    aos.toByteArray
  }
}


/* =================================================================================================
 * class IterableMarshaller
 * =================================================================================================
 */
class IterableMarshaller(iter: Iterator[Any]) extends Iterator[Array[Byte]] {
  private val buffer = new ArrayBuffer[Any]

  override def hasNext: Boolean = iter.hasNext

  override def next(): Array[Byte] = {
    while (iter.hasNext) {
      buffer += iter.next()
    }

    Marshal.dump(buffer)
  }
}
