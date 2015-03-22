package org.apache.spark.api.ruby

import java.io._
import java.net._
import java.util.{List, ArrayList}

import scala.collection.JavaConversions._
import scala.collection.immutable._

import org.apache.spark._
import org.apache.spark.util.Utils

/**
 * Internal class that acts as an `AccumulatorParam` for Ruby accumulators. Inside, it
 * collects a list of pickled strings that we pass to Ruby through a socket.
 */
private class RubyAccumulatorParam(serverHost: String, serverPort: Int)
  extends AccumulatorParam[List[Array[Byte]]] {

  // Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  // Socket shoudl not be serialized
  // Otherwise: SparkException: Task not serializable
  @transient var socket: Socket = null
  @transient var socketOutputStream: DataOutputStream = null
  @transient var socketInputStream:  DataInputStream = null

  def openSocket(){
    synchronized {
      if (socket == null || socket.isClosed) {
        socket = new Socket(serverHost, serverPort)

        socketInputStream  = new DataInputStream(new BufferedInputStream(socket.getInputStream, bufferSize))
        socketOutputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      }
    }
  }

  override def zero(value: List[Array[Byte]]): List[Array[Byte]] = new ArrayList

  override def addInPlace(val1: List[Array[Byte]], val2: List[Array[Byte]]) : List[Array[Byte]] = synchronized {
    if (serverHost == null) {
      // This happens on the worker node, where we just want to remember all the updates
      val1.addAll(val2)
      val1
    } else {
      // This happens on the master, where we pass the updates to Ruby through a socket
      openSocket()

      socketOutputStream.writeInt(val2.size)
      for (array <- val2) {
        socketOutputStream.writeInt(array.length)
        socketOutputStream.write(array)
      }
      socketOutputStream.flush()

      // Wait for acknowledgement
      // http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
      //
      // if(in.readInt() != RubyConstant.ACCUMULATOR_ACK){
      //   throw new SparkException("Accumulator was not acknowledged")
      // }

      new ArrayList
    }
  }
}
